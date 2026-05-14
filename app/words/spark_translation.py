"""Spark-powered translation of lemmas and surfaces into 11 languages.

This module is the engine for "Path B" of the Words project — see
`Thaqalayn/docs/WORDS_PROJECT_PLAN.md` ("Path B — Spark translation of
all words"). It produces a per-language gloss map for every lemma and
surface form in the corpus.

Architecture
------------
Single batched call per item: one Spark/Qwen request returns all 11
languages at once. We can do this safely (unlike Phase 4 verse
translation, which uses N×10 per-language calls) because:

  1. Word-level output is small (~25 tokens per language × 11 langs ≈
     300 output tokens total), well under the `}`-loop pathology
     threshold.
  2. JSON property names are ASCII language codes (`en`, `fa`, ...),
     so the vLLM Arabic-property-name corruption bug doesn't apply.

The same module handles both lemmas (Phase A) and surfaces (Phase B);
they share the schema and validator, differing only in the user-message
template and the input fields collected by the extractors.

Concurrency: `asyncio.Semaphore(workers)`. Default 8 matches the
PHASE4_OPENWEIGHT_BENCHMARK production setting.

Output is persisted by the caller — typical pattern is to write each
response to `ThaqalaynWordSources/translation/{lemma,surface}_responses/{slug}.json`
in JSONL or per-slug files (mirrors the existing ai-content/corpus/responses/
pattern). This module returns parsed dicts and lets the caller persist.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


# ────────────────────────── language config ──────────────────────────

# 11 target languages. `ar` is intentionally excluded — the lemma/surface
# slug IS the canonical Arabic form, and asking an Arabic LLM to
# paraphrase Arabic risks lemma echo with minimal UI payoff.
LANGUAGES = ["en", "fa", "ur", "tr", "id", "bn", "es", "fr", "de", "ru", "zh"]

LANG_FULL_NAMES = {
    "en": "English",
    "fa": "Farsi/Persian",
    "ur": "Urdu",
    "tr": "Turkish",
    "id": "Indonesian",
    "bn": "Bengali",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "ru": "Russian",
    "zh": "Chinese (Simplified)",
}

# Per-language script families. Used by the validator to flag Latin-script
# garbage in glosses that should be in non-Latin scripts.
NON_LATIN_LANGS = {"fa", "ur", "bn", "ru", "zh"}

# Latin-script regex: catches A-Z/a-z. Some non-Latin scripts use Latin
# digits or punctuation, which is fine — we only flag letters.
_LATIN_LETTER_RE = re.compile(r"[A-Za-z]")

# Per-gloss character limit. The UI's Path C truncated at 80; we hold
# the same line so existing card layouts don't break.
MAX_GLOSS_CHARS = 80

# Output-token cap. ~25 tokens per language × 11 langs + JSON overhead
# rounds to ~280; 300 leaves slack for slightly verbose languages (Russian,
# Persian) without enabling the `}`-loop pathology.
MAX_OUTPUT_TOKENS = 300


# ────────────────────────── system prompt ──────────────────────────

SYSTEM_PROMPT = """You are a professional translator producing short multilingual glosses of Arabic vocabulary for an Islamic hadith study application.

Output requirements (must follow exactly):
  • Return a strict JSON object matching the schema. Do not add commentary, code fences, or trailing text.
  • Each gloss is a SHORT phrase (≤80 characters), suitable for display on a word card.
  • Verbs: use the infinitive form ("to say", "to pray", "decir", "گفتن").
  • Nouns: use the singular base form ("speech", "prayer", "وَقت" → "time").
  • Function words (prepositions, conjunctions, particles): keep glosses literal and minimal ("to/toward", "and", "indeed").
  • Proper nouns and loanwords: transliterate, do not translate (e.g. "Muhammad", "Allah", "Quran").
  • Do NOT include diacritics on Latin-script languages (English, French, Spanish, German, Turkish, Indonesian).
  • Do NOT translate honorifics (radiyallah anhu, alaihis salam) — drop them.
  • For surface forms with clitics (e.g. "and by the covenant"), translate the WHOLE surface as a coherent phrase, not just the stem. Anchor word choice to the provided lemma translations so all inflections of one lemma share root vocabulary across languages.

Languages to produce, in this exact order: English (en), Farsi/Persian (fa), Urdu (ur), Turkish (tr), Indonesian (id), Bengali (bn), Spanish (es), French (fr), German (de), Russian (ru), Chinese Simplified (zh)."""


# ────────────────────────── schema ──────────────────────────

def build_schema() -> dict:
    """Strict json_schema with one short string per language.

    All property names are ASCII (en, fa, ur, ...) so the vLLM
    Arabic-property-name corruption bug from Phase 4 doesn't apply.
    """
    return {
        "type": "object",
        "properties": {
            "glosses": {
                "type": "object",
                "properties": {lang: {"type": "string"} for lang in LANGUAGES},
                "required": LANGUAGES,
                "additionalProperties": False,
            }
        },
        "required": ["glosses"],
        "additionalProperties": False,
    }


# ────────────────────────── prompt builders ──────────────────────────

def build_lemma_user_message(item: dict, *, include_classical: bool = False) -> str:
    """Render the lemma-pass prompt.

    Expected `item` keys:
      - lemma_ar:           Arabic NFC-normalized lemma string
      - pos:                CAMeL POS code (e.g. "verb", "noun_prop")
      - pos_label:          optional human-readable POS (e.g. "Verb")
      - en_gloss:           Wiktextract first POS-aligned sense (or "")
      - lane_body:          Lane's Lexicon body rendered to text (or "")
      - classical_summary:  hawramani aggregator top-3 classical lexicon
                            entries, HTML-stripped, capped (or "")

    `include_classical` is the Round 2 A/B knob: Round 1 keeps the prompt
    light (Wiktextract + Lane's only), Round 2 may flip this on to test
    whether the hawramani classical entries lift quality on religious /
    Quranic terminology.
    """
    lemma = item["lemma_ar"]
    pos = item.get("pos_label") or item.get("pos") or "unknown"
    en_gloss = item.get("en_gloss") or ""
    lane_body = item.get("lane_body") or ""
    classical = item.get("classical_summary") or ""

    lines = [
        f"Lemma (Arabic, NFC): {lemma}",
        f"Part of speech: {pos}",
    ]
    if en_gloss:
        lines.append(f"English gloss (from Wiktionary): {en_gloss}")
    if lane_body:
        # Trim Lane's to first ~1500 chars in v1 — full body sometimes
        # exceeds 10K chars, which inflates input tokens without
        # improving output quality. Round 2 may tune this.
        snippet = lane_body[:1500].rstrip()
        lines.append(f"Lane's Lexicon (classical):\n{snippet}")
    if include_classical and classical:
        lines.append(f"Other classical lexicons (top 3):\n{classical}")

    lines += [
        "",
        "Produce a short ≤80-character gloss in each of the 11 target languages.",
        "Verbs → infinitive form. Nouns → singular base form. Function words → literal/minimal.",
        "Return strict JSON matching the schema. No extra text.",
    ]
    return "\n".join(lines)


def build_surface_user_message(item: dict) -> str:
    """Render the surface-pass prompt.

    Expected `item` keys:
      - surface_ar:         Arabic NFC-normalized surface string
      - pos:                CAMeL POS of the stem
      - pos_label:          optional human-readable POS
      - lemma_ar:           the lemma's Arabic form (for context)
      - clitic_breakdown:   pre-rendered clitic line from
                            `app.words.clitic_labels.render_clitics`
                            (empty string when no clitics)
      - lemma_translations: dict of 11-lang lemma glosses from Phase A
      - en_gloss:           lemma's English gloss
      - lane_body:          lemma's Lane's body, or ""
      - corpus_contexts:    optional list of {path, window} dicts with
                            ±10-word context windows (Round 4+)
    """
    surface = item["surface_ar"]
    lemma = item.get("lemma_ar") or ""
    pos = item.get("pos_label") or item.get("pos") or "unknown"
    clitic_line = item.get("clitic_breakdown") or ""
    lemma_trans = item.get("lemma_translations") or {}
    en_gloss = item.get("en_gloss") or ""
    lane_body = item.get("lane_body") or ""
    contexts = item.get("corpus_contexts") or []

    lines = [
        f"Surface form (Arabic, NFC): {surface}",
        f"Underlying lemma: {lemma}" if lemma else "",
        f"Stem part of speech: {pos}",
    ]
    if clitic_line:
        lines.append(f"Clitic decomposition: {clitic_line}")
    if lemma_trans:
        # Compact one-line render so the LLM sees consistency anchor without
        # spending many input tokens.
        anchor = ", ".join(
            f"{lang}={lemma_trans.get(lang, '')!r}"
            for lang in LANGUAGES if lemma_trans.get(lang)
        )
        lines.append(f"Lemma translations (anchor — surface must compose from these): {anchor}")
    if en_gloss:
        lines.append(f"Lemma English gloss: {en_gloss}")
    if lane_body:
        snippet = lane_body[:1000].rstrip()
        lines.append(f"Lemma Lane's Lexicon (classical):\n{snippet}")
    if contexts:
        lines.append("Corpus usage examples (surface is the focal word):")
        for ctx in contexts[:3]:
            window = ctx.get("window") or ""
            if window:
                lines.append(f"  • {window}")

    lines += [
        "",
        f"Produce a short ≤80-character gloss of the WHOLE surface form '{surface}' in each of the 11 target languages.",
        "Translate the surface as a coherent phrase (including any clitics), not just the stem.",
        "Anchor word choice to the lemma translations above so this form composes consistently.",
        "Return strict JSON matching the schema. No extra text.",
    ]
    return "\n".join(line for line in lines if line)


# ────────────────────────── validation ──────────────────────────

def validate_translations(parsed: dict) -> list[str]:
    """Inspect a parsed `{glosses: {lang: text}}` payload.

    Returns a list of human-readable issue strings (empty list = clean).
    Used by the runner to decide whether to retry or quarantine; also
    surfaced in the per-round score in PATH_B_SPARK_LOG.md.

    Checks:
      • All 11 langs present and non-empty
      • Each gloss ≤80 chars
      • Non-Latin-script languages contain no A-Z/a-z characters
    """
    issues: list[str] = []

    glosses = (parsed or {}).get("glosses") if isinstance(parsed, dict) else None
    if not isinstance(glosses, dict):
        return ["glosses field missing or not a dict"]

    for lang in LANGUAGES:
        text = glosses.get(lang)
        if not isinstance(text, str) or not text.strip():
            issues.append(f"{lang}: missing or empty")
            continue
        if len(text) > MAX_GLOSS_CHARS:
            issues.append(f"{lang}: gloss exceeds {MAX_GLOSS_CHARS} chars ({len(text)})")
        if lang in NON_LATIN_LANGS and _LATIN_LETTER_RE.search(text):
            issues.append(f"{lang}: contains Latin letters (should be {lang} script)")

    return issues


# ────────────────────────── call wrappers ──────────────────────────

def _strip_code_fences(raw: str) -> str:
    """Tolerate ```json …``` wrappers — Spark occasionally adds them
    despite strict json_schema mode."""
    s = (raw or "").strip()
    if s.startswith("```"):
        s = s.split("\n", 1)[-1] if "\n" in s else s
        s = s.rsplit("```", 1)[0] if s.endswith("```") else s
    return s.strip()


async def _call_with_retry(
    user_message: str,
    *,
    model: str,
    schema_name: str,
    schema: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    """One Spark call with one retry on parse failure.

    Returns the raw `call_openai` result dict; caller is responsible for
    parsing the JSON. The retry guards against Qwen's `}`-loop pathology
    (cap helps too, but doesn't fully prevent it).
    """
    from app.pipeline_cli.openai_backend import call_openai

    last: dict = {}
    for attempt in range(2):
        async with semaphore:
            last = await call_openai(
                SYSTEM_PROMPT,
                user_message,
                model=model,
                max_output_tokens=MAX_OUTPUT_TOKENS,
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": schema_name,
                        "schema": schema,
                        "strict": True,
                    },
                },
            )
        if "error" in last:
            if attempt == 0:
                continue
            return last
        try:
            json.loads(_strip_code_fences(last.get("result", "")))
            return last
        except (json.JSONDecodeError, ValueError):
            if attempt == 0:
                logger.info("spark_translation retry: parse fail on %s", schema_name)
                continue
            return last
    return last


async def translate_lemma(
    item: dict,
    *,
    model: str = "qwen36-fast",
    semaphore: Optional[asyncio.Semaphore] = None,
) -> dict:
    """Translate one lemma. Returns `{slug, response, parsed, issues, meta}`.

    `parsed` is None on parse failure; `issues` lists validation problems.
    `meta` carries `elapsed`/`input_tokens`/`output_tokens` for telemetry.
    """
    sem = semaphore or asyncio.Semaphore(1)
    user = build_lemma_user_message(item)
    schema = build_schema()

    cr = await _call_with_retry(
        user, model=model, schema_name="word_translations",
        schema=schema, semaphore=sem,
    )

    parsed: Optional[dict] = None
    issues: list[str] = []
    if "error" in cr:
        issues.append(f"call_error: {cr.get('error', '?')[:200]}")
    else:
        try:
            parsed = json.loads(_strip_code_fences(cr.get("result", "")))
            issues = validate_translations(parsed)
        except (json.JSONDecodeError, ValueError) as e:
            issues.append(f"parse_error: {e}")

    return {
        "slug": item.get("slug") or item.get("lemma_ar"),
        "kind": "lemma",
        "parsed": parsed,
        "issues": issues,
        "meta": {
            "elapsed": cr.get("elapsed", 0.0),
            "input_tokens": cr.get("input_tokens", 0),
            "output_tokens": cr.get("output_tokens", 0),
            "model": cr.get("model", model),
            "backend": cr.get("backend", "spark"),
        },
        "raw": cr.get("result", "") if not parsed else None,
    }


async def translate_surface(
    item: dict,
    *,
    model: str = "qwen36-fast",
    semaphore: Optional[asyncio.Semaphore] = None,
) -> dict:
    """Translate one surface form. Same return shape as `translate_lemma`."""
    sem = semaphore or asyncio.Semaphore(1)
    user = build_surface_user_message(item)
    schema = build_schema()

    cr = await _call_with_retry(
        user, model=model, schema_name="word_translations",
        schema=schema, semaphore=sem,
    )

    parsed: Optional[dict] = None
    issues: list[str] = []
    if "error" in cr:
        issues.append(f"call_error: {cr.get('error', '?')[:200]}")
    else:
        try:
            parsed = json.loads(_strip_code_fences(cr.get("result", "")))
            issues = validate_translations(parsed)
        except (json.JSONDecodeError, ValueError) as e:
            issues.append(f"parse_error: {e}")

    return {
        "slug": item.get("slug") or item.get("surface_ar"),
        "kind": "surface",
        "parsed": parsed,
        "issues": issues,
        "meta": {
            "elapsed": cr.get("elapsed", 0.0),
            "input_tokens": cr.get("input_tokens", 0),
            "output_tokens": cr.get("output_tokens", 0),
            "model": cr.get("model", model),
            "backend": cr.get("backend", "spark"),
        },
        "raw": cr.get("result", "") if not parsed else None,
    }


async def run_lemma_batch(
    items: list[dict],
    *,
    model: str = "qwen36-fast",
    workers: int = 8,
    progress_cb=None,
) -> list[dict]:
    """Translate a batch of lemmas with bounded concurrency.

    `progress_cb(done, total, result)` is invoked after each item; pass
    a callable that emits to a logger or status bar.
    """
    sem = asyncio.Semaphore(workers)
    results: list[dict] = [None] * len(items)  # type: ignore[list-item]
    done = 0

    async def _one(idx: int, it: dict) -> None:
        nonlocal done
        r = await translate_lemma(it, model=model, semaphore=sem)
        results[idx] = r
        done += 1
        if progress_cb is not None:
            try:
                progress_cb(done, len(items), r)
            except Exception:
                logger.exception("progress_cb raised; ignoring")

    await asyncio.gather(*(_one(i, it) for i, it in enumerate(items)))
    return results


async def run_surface_batch(
    items: list[dict],
    *,
    model: str = "qwen36-fast",
    workers: int = 8,
    progress_cb=None,
) -> list[dict]:
    """Surface counterpart of `run_lemma_batch`."""
    sem = asyncio.Semaphore(workers)
    results: list[dict] = [None] * len(items)  # type: ignore[list-item]
    done = 0

    async def _one(idx: int, it: dict) -> None:
        nonlocal done
        r = await translate_surface(it, model=model, semaphore=sem)
        results[idx] = r
        done += 1
        if progress_cb is not None:
            try:
                progress_cb(done, len(items), r)
            except Exception:
                logger.exception("progress_cb raised; ignoring")

    await asyncio.gather(*(_one(i, it) for i, it in enumerate(items)))
    return results
