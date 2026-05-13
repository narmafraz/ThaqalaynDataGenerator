"""Round 4: Phase 4 per-chunk-per-language-tight calls instead of all-10-langs in one call.

Hypothesis: instead of one big call producing 10 languages × N chunks of output
(~3K completion tokens per batch, slow), do N×10 separate small calls (each
~200-400 tokens). Smaller calls:
  - Decode faster individually
  - Saturate Spark `max-num-seqs=16` better (16 small calls in flight at once)
  - Allow per-language retry on failure
  - Reduce JSON malformation risk further (each schema is tiny)

Caveat: more total prompt tokens (10× the system prompt). But Spark is free.

Saves to benchmark/phase4_qwen_round_d/.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import sys
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "app"))

from app.pipeline_cli.translation_phase import (  # noqa: E402
    NON_EN_LANGUAGES,
    _fill_empty_translations,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("phase4_r4")

SOURCE_DATA_DIR = Path(os.environ.get(
    "SOURCE_DATA_DIR",
    REPO_ROOT.parent / "ThaqalaynDataSources",
))
RESPONSES_DIR = SOURCE_DATA_DIR / "ai-content" / "corpus" / "responses"
SAMPLE_PATH = REPO_ROOT / "benchmark" / "phase4_qwen" / "sample.json"
BENCH_DIR = REPO_ROOT / "benchmark" / "phase4_qwen_round_d"
RESULTS_DIR = BENCH_DIR / "results"

QWEN_BASE_URL = "http://192.168.0.66:8000/v1"
QWEN_MODEL = "qwen36-fast"
WORKERS = 16  # Spark max-num-seqs ceiling

LANG_NAMES = {
    "ur": "Urdu", "tr": "Turkish", "fa": "Farsi/Persian",
    "id": "Indonesian", "bn": "Bengali", "es": "Spanish",
    "fr": "French", "de": "German", "ru": "Russian", "zh": "Chinese (Simplified)",
}

SYSTEM_PROMPT = """You are a professional translator specializing in Islamic religious texts.
Preserve Islamic terminology (salat, wudu, zakat) unless the target language has established equivalents.
Transliterate narrator names — do not translate proper nouns.
Preserve honorifics (peace be upon him, etc.) in each language's convention.
Be faithful — do not add commentary.
For Chinese: do not use spaces between words.
Output valid JSON only."""


def build_chunk_lang_schema() -> dict:
    """Schema for translating ONE chunk to ONE language."""
    return {
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
        "additionalProperties": False,
    }


def build_meta_lang_schema() -> dict:
    """Schema for translating summary + seo_question to ONE language."""
    return {
        "type": "object",
        "properties": {
            "summary": {"type": "string"},
            "seo_question": {"type": "string"},
        },
        "required": ["summary", "seo_question"],
        "additionalProperties": False,
    }


def build_chunk_user(en_text: str, lang_code: str, lang_name: str, arabic_text: str = "") -> str:
    parts = []
    if arabic_text:
        parts.append(
            f"Original Arabic (for context, translate from the English):\n{arabic_text}\n"
        )
    parts.append(f"Translate the following English passage into {lang_name}.")
    parts.append(f'Output JSON of the form: {{"text": "..."}}')
    parts.append(f"\nEnglish: {en_text}")
    return "\n".join(parts)


def build_meta_user(en_summary: str, en_seo: str, lang_code: str, lang_name: str) -> str:
    return (
        f"Translate the following English texts into {lang_name}.\n"
        f'Output JSON of the form: {{"summary": "...", "seo_question": "..."}}\n\n'
        f"English summary: {en_summary}\n"
        f"English SEO question: {en_seo}"
    )


@dataclass
class CallResult:
    ok: bool
    text: str
    parsed: Optional[dict]
    prompt_tokens: int
    completion_tokens: int
    elapsed: float
    error: Optional[str] = None


async def call_qwen(client, system, user, schema, max_tokens=600) -> CallResult:
    start = time.time()
    try:
        response = await client.chat.completions.create(
            model=QWEN_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_tokens=max_tokens,
            temperature=0,
            response_format={
                "type": "json_schema",
                "json_schema": {"name": "translation_unit", "schema": schema, "strict": True},
            },
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        )
        elapsed = round(time.time() - start, 2)
        text = response.choices[0].message.content or ""
        usage = response.usage
        pt = usage.prompt_tokens if usage else 0
        ct = usage.completion_tokens if usage else 0
        try:
            return CallResult(True, text, json.loads(text), pt, ct, elapsed)
        except json.JSONDecodeError as e:
            return CallResult(False, text, None, pt, ct, elapsed, error=f"json_parse: {e}")
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        return CallResult(False, "", None, 0, 0, elapsed, error=f"{type(e).__name__}: {str(e)[:200]}")


async def translate_one_verse(client, sem, verse: dict, baseline_record: dict) -> dict:
    """Spin up small calls per (chunk, lang) + (meta, lang) and gather."""
    verse_path = verse["verse_path"]
    result = copy.deepcopy(baseline_record["result"])
    for c in result.get("chunks", []) or []:
        c["translations"] = {"en": (c.get("translations") or {}).get("en", "")}
    en_block = (result.get("translations") or {}).get("en") or {}
    result["translations"] = {"en": {
        "summary": en_block.get("summary", ""),
        "seo_question": en_block.get("seo_question", ""),
        "key_terms": en_block.get("key_terms", {}),
    }}
    chunks = result.get("chunks", [])
    arabic_text = "\n".join(c.get("arabic_text", "") for c in chunks)

    chunk_schema = build_chunk_lang_schema()
    meta_schema = build_meta_lang_schema()

    tasks = []

    async def do_chunk_lang(chunk_idx, lang):
        async with sem:
            user = build_chunk_user(
                chunks[chunk_idx]["translations"]["en"], lang, LANG_NAMES[lang],
                arabic_text=arabic_text if chunk_idx == 0 else "",
            )
            return ("chunk", chunk_idx, lang, await call_qwen(client, SYSTEM_PROMPT, user, chunk_schema, max_tokens=600))

    async def do_meta_lang(lang):
        async with sem:
            user = build_meta_user(en_block.get("summary", ""), en_block.get("seo_question", ""),
                                   lang, LANG_NAMES[lang])
            return ("meta", None, lang, await call_qwen(client, SYSTEM_PROMPT, user, meta_schema, max_tokens=400))

    for lang in NON_EN_LANGUAGES:
        for i in range(len(chunks)):
            tasks.append(do_chunk_lang(i, lang))
        tasks.append(do_meta_lang(lang))

    overall_start = time.time()
    call_results = await asyncio.gather(*tasks)
    elapsed_total = round(time.time() - overall_start, 2)

    n_ok = sum(1 for _, _, _, cr in call_results if cr.ok)
    n_total = len(call_results)
    total_pt = sum(cr.prompt_tokens for _, _, _, cr in call_results)
    total_ct = sum(cr.completion_tokens for _, _, _, cr in call_results)

    # Merge results back into the verse structure
    for kind, idx, lang, cr in call_results:
        if not cr.ok or not cr.parsed:
            continue
        if kind == "chunk":
            chunks[idx]["translations"][lang] = cr.parsed.get("text", "")
        elif kind == "meta":
            tr = result.get("translations") or {}
            if lang not in tr:
                tr[lang] = {}
            tr[lang]["summary"] = cr.parsed.get("summary", "")
            tr[lang]["seo_question"] = cr.parsed.get("seo_question", "")
            tr[lang].setdefault("key_terms", {})

    _fill_empty_translations(result)

    return {
        "verse_path": verse_path,
        "round": "D",
        "approach": "per-chunk-per-language-tight",
        "elapsed_s": elapsed_total,
        "calls_total": n_total,
        "calls_ok": n_ok,
        "parse_rate_pct": round(100 * n_ok / n_total, 2),
        "prompt_tokens_total": total_pt,
        "completion_tokens_total": total_ct,
        "result": result,
        "stratum": verse.get("stratum"),
        "chunk_count": verse.get("chunk_count"),
        "ar_word_count": verse.get("ar_word_count"),
    }


async def main_async() -> None:
    from openai import AsyncOpenAI
    sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    verses = sample["verses"]
    logger.info("round 4: per-chunk-per-language-tight calls, workers=%d", WORKERS)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    client = AsyncOpenAI(api_key="not-needed", base_url=QWEN_BASE_URL,
                        timeout=600.0, max_retries=2)
    sem = asyncio.Semaphore(WORKERS)

    overall_start = time.time()
    out = []
    for verse in verses:
        baseline = json.loads((RESPONSES_DIR / verse["file"]).read_text(encoding="utf-8"))
        rec = await translate_one_verse(client, sem, verse, baseline)
        verse_id = rec["verse_path"].removeprefix("/books/").replace("/", "_").replace(":", "_")
        (RESULTS_DIR / f"{verse_id}.qwen.json").write_text(
            json.dumps(rec, indent=2, ensure_ascii=False), encoding="utf-8")
        out.append(rec)
        logger.info("%s: %.1fs, %d/%d calls ok (%.1f%%)",
                    rec["verse_path"], rec["elapsed_s"],
                    rec["calls_ok"], rec["calls_total"], rec["parse_rate_pct"])

    overall_elapsed = round(time.time() - overall_start, 2)
    total_calls = sum(r["calls_total"] for r in out)
    total_ok = sum(r["calls_ok"] for r in out)
    summary = {
        "round": "D",
        "approach": "per-chunk-per-language-tight",
        "model": QWEN_MODEL,
        "workers": WORKERS,
        "verse_count": len(out),
        "overall_wall_seconds": overall_elapsed,
        "calls_total": total_calls,
        "calls_ok": total_ok,
        "call_parse_rate_pct": round(100 * total_ok / total_calls, 2),
        "prompt_tokens_total": sum(r["prompt_tokens_total"] for r in out),
        "completion_tokens_total": sum(r["completion_tokens_total"] for r in out),
    }
    (BENCH_DIR / "qwen_run_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("done — %d verses in %.1fs, %d/%d calls ok (%.1f%%)",
                len(out), overall_elapsed, total_ok, total_calls, summary["call_parse_rate_pct"])


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
