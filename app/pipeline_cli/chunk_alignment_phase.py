"""Scraped-translation chunk alignment (the `align-scraped` command).

Re-segments existing *scraped* (non-AI) translations to the AI chunk
boundaries so the frontend can render them in the same interleaved
Arabic-segment ↔ translation-segment view it uses for AI translations.

This is EXTRACTIVE segmentation, not re-translation: the model only chooses
split points — concatenating the parts must reproduce the original
translation verbatim. It runs on the DGX Spark / Qwen at $0, reusing the
generation pipeline's durability model:

- Input: a *built* ThaqalaynData verse-detail file, which holds both
  ``verse.translations[id]`` (scraped) and ``verse.ai.chunks[].arabic_text``
  (post AI-merge — base chunks keep arabic_text; only per-chunk translations
  are stripped to sister files).
- Output artifact: DataSources ``ai-content/{subdir}/chunk_alignment/
  {verse_id}.json`` — resumable via skip-if-exists, exactly like
  ``responses/``.
- Build merge: ``ai_content_merger.merge_chunk_alignment()`` injects
  ``verse.chunk_translations`` into Data at ``add_data`` time.

No circular dependency: Data is a rebuildable output; the committed source of
truth is the DataSources artifact (mirrors ``ai-content/responses/``). The
producer may read a transient built Data as scratch — the same thing the AI
pipeline does via ``extract_pipeline_request(..., data_dir=DESTINATION_DIR)``.

The parts array is aligned positionally to the FULL ``ai.chunks`` array (same
length + order), because the frontend resolves a chunk's slot by its index in
``ai.chunks`` (``getChunkTranslation`` → ``chunks.indexOf(chunk)``).
"""

import asyncio
import json
import logging
import os
import re
import time
import unicodedata
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from app.config import AI_ALIGNMENT_DIR, AI_RESPONSES_DIR, DEFAULT_DESTINATION_DIR
from app.pipeline_cli.openai_backend import (
    archive_raw_response,
    call_openai,
    is_spark_model,
)
from app.pipeline_cli.translation_phase import _strip_code_fences
from app.pipeline_cli.verse_processor import verse_path_to_id

logger = logging.getLogger(__name__)

# Accept an alignment only when the re-joined parts reproduce the original
# scraped text EXACTLY, modulo whitespace. Segmentation is extractive — the
# model only chooses cut points — so anything short of verbatim (a paraphrase,
# a dropped word, added commentary, reordering) means the model rewrote the
# text and the alignment must be quarantined. This matters because the merged
# sister parts become the sole copy of the scraped translation (the flat base
# text is removed): a fuzzy threshold would silently lose words forever.

_SYSTEM = """You split one existing English translation of a hadith into consecutive parts, one per segment, guided by a reference that tells you what each segment means.

You are given N segments in order. Each segment has a REFERENCE (its meaning in English) and its ARABIC source. You are given one TRANSLATION (a different English rendering of the whole hadith). Cut the TRANSLATION into N consecutive parts so that part i covers the SAME content as segment i's reference.

RULES:
- Match by MEANING to each segment's reference, not by length or position. Part i must correspond to the same portion of the hadith that reference i describes. NEVER just cut the TRANSLATION into N similar-sized pieces — every cut must be justified by the references.
- Preserve the translation's wording EXACTLY — do not paraphrase, translate, reorder, add, or drop any words or markup (e.g. <sup>…</sup>). Concatenating your parts in order must reproduce the TRANSLATION verbatim.
- The cuts are the only choice you make. Every character of the TRANSLATION belongs to exactly one part, in order.
- The narrator chain / isnad at the start (e.g. "A number of our companions, from …") belongs to the first isnad segment.
- The TRANSLATION may OMIT or ABRIDGE segments — translators often skip the narrator chain or shorten the text. If a segment's content is absent from the TRANSLATION, return an empty string for that part and place the existing text under the segments it actually matches. Do NOT stretch neighboring text into an omitted segment's slot, and do NOT dump the whole text into part 1 when it belongs to a later segment.
- If a segment has no matching text in the TRANSLATION, return an empty string for it (and give its text to no other segment).
- Output valid JSON only."""


# A leading hadith-number marker ("1. ", " 5. ", "10 - "). It belongs to no
# segment's meaning, so models drop it when segmenting — 4 of the 5 July-pilot
# quarantines. Handled deterministically: stripped before the call,
# re-attached to the first non-empty part after (split_leading_number /
# attach_prefix), with the strict validator checking the reassembled whole.
_LEADING_NUM_RE = re.compile(r"^\s*\d{1,4}\s*[.)\-–:]\s+")

# Attempts per (verse, translation) call. The strict verbatim validator
# rejects marginal samples the old fuzzy check passed; fresh samples are the
# cheapest cure on Spark ($0), so allow one more than the historical 2.
MAX_ATTEMPTS = 3


def split_leading_number(text: str) -> Tuple[str, str]:
    """Split a leading hadith-number prefix off the text.

    Returns (prefix, rest); prefix is "" when the text doesn't start with a
    number marker. Lossless: callers re-attach the prefix after alignment.
    """
    m = _LEADING_NUM_RE.match(text or "")
    if not m:
        return "", text or ""
    return text[:m.end()], text[m.end():]


def attach_prefix(parts: List[str], prefix: str) -> List[str]:
    """Prepend the split-off number prefix to the first non-empty part."""
    if not prefix or not parts:
        return parts
    for i, p in enumerate(parts):
        if (p or "").strip():
            parts[i] = prefix + p
            return parts
    parts[0] = prefix + (parts[0] or "")
    return parts


def _squash(text: str) -> str:
    """The text's non-whitespace characters, in order, NFC-normalized.

    NFC matters for inline Arabic: models legally re-emit combining marks in
    canonical order (e.g. kasra+shadda for shadda+kasra) — identical text per
    Unicode, so it must not fail the verbatim check.
    """
    return unicodedata.normalize("NFC", "".join((text or "").split()))


def validate_alignment(parts: List[str], scraped_text: str) -> Tuple[bool, str]:
    """Return (ok, reason). Strict extractive check: the parts, concatenated
    in order, must equal the original scraped text with whitespace collapsed.
    """
    orig = _squash(scraped_text)
    got = _squash(" ".join(p or "" for p in parts))
    if orig == got:
        return True, ""
    # First divergence point, for a diagnostic the quarantine record can keep.
    i = 0
    limit = min(len(orig), len(got))
    while i < limit and orig[i] == got[i]:
        i += 1
    return False, (
        f"not verbatim: rejoined parts diverge at char {i} "
        f"(orig {len(orig)} chars, got {len(got)}): "
        f"{orig[max(0, i - 15):i + 25]!r} vs {got[max(0, i - 15):i + 25]!r}"
    )


# ── placement accuracy gate (uses the AI's own per-chunk English refs) ──────
#
# Strict validation proves the text survived intact; it cannot see MISPLACED
# cuts. Pilot2 accuracy analysis (2026-09-02) found two real classes:
#  - "single dump": abridged translations (esp. Sarwar, which omits isnads)
#    put their whole text in part 0 even when it belongs to a later segment;
#  - "off-by-one": parts lag/lead their segments by one slot (e.g.
#    al-amali-mufid 18:6 — every part from slot 1 rendered under the wrong
#    chunk). Both are detectable by comparing each part against the AI's own
#    English rendering of the same chunk (`en_ref`) — two English renderings
#    of the same Arabic share content words.

_CONTENT_WORD_RE = re.compile(r"[a-z']+")
_STOPWORDS = frozenset("""a an the and or of to in on for from with by at as
is are was were be been has have had he she it they we you i his her its
their this that these those who whom which what said says say then when
while not no nor so does do did but if than there here upon unto shall will
would may might""".split())

# A neighboring reference must fit this much better before we call a part
# misplaced — two independent translations legitimately word things apart.
_SHIFT_MARGIN = 0.25
_MOVE_MARGIN = 0.10


def _content_words(text: str) -> frozenset:
    return frozenset(w for w in _CONTENT_WORD_RE.findall((text or "").lower())
                     if w not in _STOPWORDS and len(w) > 2)


def _jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def fix_single_dump(parts: List[str], chunks: List[dict]) -> List[str]:
    """If exactly one part holds all the text, move it to the slot whose
    reference matches best. Deterministic and lossless (all other parts are
    empty, so the concatenation is unchanged). Handles abridged translations
    that omit the isnad: text dumped at slot 0 belongs at the matn slot.
    """
    nonempty = [i for i, p in enumerate(parts) if (p or "").strip()]
    if len(parts) < 2 or len(nonempty) != 1:
        return parts
    placed = nonempty[0]
    refs = [_content_words(c.get("en_ref") or "") for c in chunks]
    if len(refs) != len(parts) or len({frozenset(r) for r in refs if r}) < 2:
        return parts  # refs missing or degenerate — nothing to judge by
    pw = _content_words(parts[placed])
    if not pw:
        return parts
    sims = [_jaccard(pw, r) for r in refs]
    best = max(range(len(sims)), key=lambda i: sims[i])
    if best != placed and sims[best] > sims[placed] + _MOVE_MARGIN:
        moved = ["" for _ in parts]
        moved[best] = parts[placed]
        return moved
    return parts


def placement_suspect(parts: List[str], chunks: List[dict]) -> Optional[str]:
    """Return a reason when a part clearly matches a NEIGHBORING segment's
    reference better than its own (the off-by-one class); None when placement
    looks fine or the references are unusable (missing, degenerate, or the
    text has no English content words, e.g. transliterations).
    """
    refs = [_content_words(c.get("en_ref") or "") for c in chunks]
    if len(refs) != len(parts) or len({frozenset(r) for r in refs if r}) < 2:
        return None
    # Heavily-abridged translations (Sarwar summarises and omits isnads) end
    # up as one part that matches NO reference well — there is no faithful
    # per-chunk mapping, so the flat block view is the honest rendering.
    nonempty = [i for i, p in enumerate(parts) if (p or "").strip()]
    if len(parts) >= 2 and len(nonempty) == 1:
        placed = nonempty[0]
        pw = _content_words(parts[placed])
        if len(pw) >= 5 and refs[placed] and _jaccard(pw, refs[placed]) < 0.10:
            return ("placement suspect: single part matches its own segment "
                    f"weakly ({_jaccard(pw, refs[placed]):.2f}) — abridged "
                    "translation with no faithful chunk mapping")
    # Global mismatch: no part matches ANY reference — the scraped text isn't
    # a translation of this verse at all (seen in round 2: Sarwar texts
    # attached with an off-by-one hadith number, and title-stub "translations"
    # like nahj 1:110's two-word entry). Interleaving mismatched text under
    # the Arabic would be actively misleading; keep the flat view.
    all_pw = [_content_words(p) for p in parts]
    if any(all_pw):
        best_any = max(
            (_jaccard(pw, r) for pw in all_pw if pw for r in refs if r),
            default=0.0)
        if best_any < 0.05:
            return ("placement suspect: no part matches any segment reference "
                    f"(best {best_any:.2f}) — translation likely does not "
                    "belong to this verse (source misattachment or stub)")

    suspects = []
    for i, p in enumerate(parts):
        pw = _content_words(p)
        if len(pw) < 5 or not refs[i]:
            continue
        own = _jaccard(pw, refs[i])
        for j in (i - 1, i + 1):
            if 0 <= j < len(refs) and refs[j]:
                other = _jaccard(pw, refs[j])
                if other > own + _SHIFT_MARGIN:
                    suspects.append(
                        f"part {i} fits reference {j} better "
                        f"({other:.2f} vs {own:.2f})")
                    break
    if suspects:
        return "placement suspect: " + "; ".join(suspects[:3])
    return None


# ── best-effort split (deterministic, for placement-suspect alignments) ─────
#
# When the model's placement fails the gate (off-by-one shift, or an abridged
# translation dumped into one slot), don't give up: sentence-split the
# translation and assign contiguous sentence runs to chunks by reference
# similarity with an order-preserving DP. Chunks with no matching text (an
# omitted isnad, abridged-away passages) stay EMPTY. Parts are literal slices
# of the original, so the strict validator passes by construction. Returns
# None only when there is no similarity signal at all to place anything by.

# Sentence boundary: terminal punctuation (optionally + closing quote), then
# whitespace. Fixed-width lookbehind branches (Python `re` requirement).
_SENT_BREAK = re.compile(
    r'(?:(?<=[.!?؟…])|(?<=[.!?؟…]["”’]))\s+')


def _sentence_spans(text: str) -> List[Tuple[int, int]]:
    """Spans covering the whole text (slices rejoin to it exactly)."""
    spans = []
    start = 0
    for m in _SENT_BREAK.finditer(text):
        spans.append((start, m.end()))
        start = m.end()
    if start < len(text):
        spans.append((start, len(text)))
    return spans


def best_effort_split(scraped_text: str, chunks: List[dict]) -> Optional[List[str]]:
    """Order-preserving optimal assignment of sentences to chunks.

    dp[j][i] = best score with the first i sentences distributed over the
    first j chunks; each chunk takes a (possibly empty) contiguous run.
    Score = Σ jaccard(sentence words, chunk reference words).
    """
    n = len(chunks)
    refs = [_content_words(c.get("en_ref") or "") for c in chunks]
    if n < 2 or len({frozenset(r) for r in refs if r}) < 2:
        return None
    spans = _sentence_spans(scraped_text)
    s = len(spans)
    if s == 0:
        return None
    sent_words = [_content_words(scraped_text[a:b]) for a, b in spans]
    sim = [[_jaccard(w, r) for r in refs] for w in sent_words]
    # Signal requirement: a single incidentally-shared word is not evidence of
    # placement — some sentence must share >= 2 content words with some ref.
    if not any(len(w & r) >= 2 for w in sent_words for r in refs):
        return None  # nothing to place anything by

    NEG = float("-inf")
    dp = [[NEG] * (s + 1) for _ in range(n + 1)]
    dp[0][0] = 0.0
    back: Dict[Tuple[int, int], int] = {}
    for j in range(1, n + 1):
        for i in range(s + 1):
            best, arg = NEG, -1
            run = 0.0
            for k in range(i, -1, -1):  # chunk j-1 takes sentences [k, i)
                if k < i:
                    run += sim[k][j - 1]
                if dp[j - 1][k] > NEG and dp[j - 1][k] + run > best:
                    best, arg = dp[j - 1][k] + run, k
            if arg >= 0:
                dp[j][i] = best
                back[(j, i)] = arg
    if dp[n][s] <= 0.0:
        return None

    cuts = [s]
    i = s
    for j in range(n, 0, -1):
        i = back[(j, i)]
        cuts.append(i)
    cuts.reverse()  # cuts[j] = first sentence of chunk j
    parts = []
    for j in range(n):
        a, b = cuts[j], cuts[j + 1]
        parts.append(scraped_text[spans[a][0]:spans[b - 1][1]] if a < b else "")
    return parts


# ── cut-point salvage (reslice the ORIGINAL at model-implied boundaries) ────
#
# Pilot2 taxonomy (2026-08-20, 95/1189 quarantined): the model reliably finds
# the right cut points but mangles characters while re-typing the text —
# dropping "..." quote marks (63), duplicating a few words at a boundary (18),
# swapping curly↔straight quotes (11). All three vanish if we use the model's
# parts only to LOCATE boundaries and then slice the original string directly:
# the result is extractive by construction, so the strict validator passes.

# Quote-family characters the model substitutes freely; folded 1:1 for the
# boundary matching only (never in the output text, which is a literal slice).
_QUOTE_FOLD = str.maketrans({
    "‘": "'", "’": "'", "‚": "'", "‛": "'",
    "`": "'", "´": "'",
    "“": '"', "”": '"', "„": '"', "«": '"', "»": '"',
    "–": "-", "—": "-",
})

# A boundary falling inside an unmatched run longer than this is too uncertain
# to place — fail salvage rather than risk cutting mid-clause.
_MAX_BOUNDARY_GAP = 60


def _fold_indexed(text: str) -> Tuple[str, List[int]]:
    """(folded squashed string, per-char index into the original text)."""
    chars, idx = [], []
    for i, ch in enumerate(text):
        if ch.isspace():
            continue
        chars.append(ch.translate(_QUOTE_FOLD))
        idx.append(i)
    return "".join(chars), idx


def reslice_from_original(parts: List[str], original: str) -> Optional[List[str]]:
    """Re-derive the parts as literal slices of `original`, using the model's
    (possibly character-mangled) parts only for the boundary positions.

    Returns None when a boundary cannot be placed confidently.
    """
    from difflib import SequenceMatcher

    n = len(parts)
    if n == 0:
        return None
    a, a_idx = _fold_indexed(original)          # original (folded)
    b, _ = _fold_indexed("".join(p or "" for p in parts))  # model text (folded)
    if not a:
        return None

    # Part boundaries in b-coordinates (start of each part i >= 1).
    bounds = []
    pos = 0
    for p in parts[:-1]:
        pos += len(_fold_indexed(p or "")[0])
        bounds.append(pos)

    sm = SequenceMatcher(None, a, b, autojunk=False)
    blocks = sm.get_matching_blocks()  # ends with zero-size sentinel
    if sum(bl.size for bl in blocks) < 0.8 * max(len(a), len(b)):
        return None  # texts too dissimilar — not a character-mangling case

    def b_to_a(bpos: int) -> Optional[int]:
        """Map a b-coordinate to an a-coordinate; None if too uncertain."""
        prev_end_a = 0
        prev_end_b = 0
        for bl in blocks:
            if bl.b <= bpos < bl.b + bl.size:
                return bl.a + (bpos - bl.b)
            if bpos < bl.b:  # falls in the unmatched gap before this block
                gap = bl.b - prev_end_b
                if gap > _MAX_BOUNDARY_GAP or (bl.a - prev_end_a) > _MAX_BOUNDARY_GAP:
                    return None
                return bl.a  # snap to the next matched region's start
            prev_end_a, prev_end_b = bl.a + bl.size, bl.b + bl.size
        return len(a)

    cuts = [0]
    prev_apos = 0  # monotonicity must be checked in fold coordinates
    for bpos in bounds:
        apos = b_to_a(bpos)
        if apos is None or apos < prev_apos:
            return None
        prev_apos = apos
        # a-coordinate → index into the original string (start of that char).
        cuts.append(a_idx[apos] if apos < len(a_idx) else len(original))
    cuts.append(len(original))

    return [original[cuts[i]:cuts[i + 1]] for i in range(n)]


def _alignment_schema(n: int) -> dict:
    """Strict JSON schema locking the output to exactly n parts."""
    return {
        "type": "object",
        "properties": {
            "parts": {
                "type": "array",
                "minItems": n,
                "maxItems": n,
                "items": {
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["parts"],
        "additionalProperties": False,
    }


def build_alignment_prompt(chunks: List[dict], scraped_text: str) -> Tuple[str, str]:
    n = len(chunks)
    lines = [f"There are {n} segments, in order:\n"]
    for i, c in enumerate(chunks, 1):
        lines.append(f"Segment {i} [{c.get('chunk_type', 'body')}]:")
        ref = (c.get("en_ref") or "").strip()
        if ref:
            lines.append(f"  REFERENCE (meaning): {ref}")
        ar = (c.get("arabic_text") or "").strip()
        if ar:
            lines.append(f"  ARABIC: {ar}")
        lines.append("")
    lines.append("TRANSLATION to split (cut into exactly "
                 f"{n} consecutive parts, matching each segment by meaning):")
    lines.append(scraped_text)
    lines.append(
        f'\nOutput JSON: {{"parts": [{{"text": "..."}}, ...]}} with exactly {n} items.'
    )
    return _SYSTEM, "\n".join(lines)


# ── verse loading (from built ThaqalaynData) ──────────────────────────────

def _verse_file(verse_path: str, data_dir: str) -> str:
    p = verse_path.replace(":", "/")
    if p.startswith("/"):
        p = p[1:]
    return os.path.join(data_dir, p + ".json")


def load_built_verse(verse_path: str, data_dir: str) -> Optional[dict]:
    """Load the verse object from a built ThaqalaynData verse-detail file."""
    fp = _verse_file(verse_path, data_dir)
    if not os.path.exists(fp):
        return None
    try:
        with open(fp, "r", encoding="utf-8") as f:
            doc = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    data = doc.get("data", doc)
    return data.get("verse", data)


def get_chunks(verse: dict) -> List[dict]:
    ai = verse.get("ai") or {}
    chunks = ai.get("chunks") or []
    return [c for c in chunks if isinstance(c, dict)]


def _wa_word(entry) -> str:
    if isinstance(entry, dict):
        return entry.get("word", "") or ""
    return str(entry or "")


def prepared_chunks(verse: dict, response_result: Optional[dict] = None) -> List[dict]:
    """Return one {chunk_type, arabic_text} per chunk, in ai.chunks order.

    The built base file strips chunks[].arabic_text (v4 lean), so recover the
    Arabic anchor by priority: the chunk's own arabic_text → the DataSources
    response's chunks[i].arabic_text (full diacritized Phase-1 text) →
    reconstruct from word_analysis[word_start:word_end] (what the frontend
    itself renders). Length always == len(ai.chunks) so the produced parts
    stay index-aligned with what merge_chunk_alignment validates against.
    """
    chunks = get_chunks(verse)
    wa = (verse.get("ai") or {}).get("word_analysis") or []
    resp_chunks = (response_result or {}).get("chunks") or []
    out = []
    for i, c in enumerate(chunks):
        rc = resp_chunks[i] if i < len(resp_chunks) and isinstance(resp_chunks[i], dict) else {}
        ar = (c.get("arabic_text") or "").strip() or (rc.get("arabic_text") or "").strip()
        if not ar:
            ws, we = c.get("word_start"), c.get("word_end")
            if isinstance(ws, int) and isinstance(we, int) and wa:
                ar = " ".join(_wa_word(w) for w in wa[ws:we]).strip()
        # The AI's own English rendering of this chunk — a same-language
        # anchor that makes aligning the scraped English far more accurate
        # than matching it against Arabic.
        en_ref = ((rc.get("translations") or {}).get("en") or "").strip()
        out.append({
            "chunk_type": c.get("chunk_type", "body"),
            "arabic_text": ar,
            "en_ref": en_ref,
        })
    return out


def load_response_result(verse_id: str, responses_dir: Optional[str] = None) -> Optional[dict]:
    """Load result{} from a DataSources AI response file, if present."""
    rp = os.path.join(responses_dir or AI_RESPONSES_DIR, f"{verse_id}.json")
    if not os.path.exists(rp):
        return None
    try:
        with open(rp, "r", encoding="utf-8") as f:
            return json.load(f).get("result")
    except (json.JSONDecodeError, OSError):
        return None


def is_eligible(chunks: List[dict]) -> bool:
    """Eligible = ≥2 chunks and ≥2 of them carry arabic_text to segment on.

    Single-chunk verses need no alignment (the whole scraped text is the one
    chunk; the block fallback renders identically), so they are skipped.
    """
    if len(chunks) < 2:
        return False
    with_ar = sum(1 for c in chunks if (c.get("arabic_text") or "").strip())
    return with_ar >= 2


def eligible_scraped_ids(verse: dict, langs: Optional[set]) -> List[str]:
    """Scraped translation IDs on this verse for the requested languages.

    Excludes AI translations (``*.ai`` — already chunked) and empty entries.
    """
    out = []
    for tid, val in (verse.get("translations") or {}).items():
        if tid.endswith(".ai") or not val:
            continue
        if langs and tid.split(".")[0] not in langs:
            continue
        out.append(tid)
    return sorted(out)


def scraped_text_for(verse: dict, tid: str) -> str:
    return "\n".join(s for s in (verse["translations"][tid] or []) if s)


# ── the per-(verse, translation) alignment call ───────────────────────────

async def _align_one(
    chunks: List[dict],
    scraped_text: str,
    model: str,
    verse_id: str,
    tid: str,
    raw_archive_dir: Optional[str],
) -> Tuple[Optional[List[str]], str]:
    """Align one scraped translation to the chunks. Returns (parts, reason).

    parts is index-aligned to the full chunks list; None means quarantine.
    One retry on parse / validation failure (Qwen's occasional degenerate
    output usually clears on a fresh call — same pattern as Phase 4).
    """
    n = len(chunks)
    schema = _alignment_schema(n)
    # The hadith-number prefix belongs to no segment, so keep it away from the
    # model entirely and re-attach it to the first non-empty part afterwards.
    prefix, body = split_leading_number(scraped_text)
    system, user = build_alignment_prompt(chunks, body)
    # Output ≈ the input text re-emitted as JSON. English ~3 chars/token; add
    # JSON overhead + per-part framing. Cap to keep degenerate loops bounded.
    est = int(len(body) / 3) + 256 + 40 * n
    max_out = max(1024, min(16000, est))
    response_format = {
        "type": "json_schema",
        "json_schema": {"name": "chunk_alignment", "schema": schema, "strict": True},
    }
    last_reason = "exhausted"
    for attempt in range(MAX_ATTEMPTS):
        cr = await call_openai(
            system, user, model=model,
            max_output_tokens=max_out,
            response_format=response_format,
        )
        if "error" in cr:
            last_reason = f"api error: {str(cr.get('error'))[:120]}"
            continue
        try:
            parsed = json.loads(_strip_code_fences(cr.get("result", "")))
        except (json.JSONDecodeError, ValueError) as e:
            archive_raw_response(raw_archive_dir, verse_id, f"align.{tid}", cr.get("result", ""))
            last_reason = f"parse fail: {e}"
            continue
        parts = [(p or {}).get("text", "") for p in parsed.get("parts", [])]
        if len(parts) != n:
            last_reason = f"wrong part count {len(parts)} != {n}"
            continue
        parts = attach_prefix(parts, prefix)
        # Validate against the FULL original text — this also proves the
        # prefix re-attachment reassembles losslessly.
        ok, reason = validate_alignment(parts, scraped_text)
        if not ok:
            # Cut-point salvage: the usual failure is character mangling
            # (dropped/substituted quote marks, boundary duplication), not bad
            # cuts. Reslice the original at model-implied boundaries, re-check.
            resliced = reslice_from_original(parts, scraped_text)
            if resliced is not None:
                ok2, reason2 = validate_alignment(resliced, scraped_text)
                if ok2:
                    parts, ok = resliced, True
                else:
                    reason = f"{reason}; reslice failed too: {reason2[:80]}"
            if not ok:
                # Last resort: the model dropped whole spans (long texts) or
                # the boundary neighborhood is unrecoverable. Re-split the
                # ORIGINAL by reference similarity instead.
                bev = best_effort_split(scraped_text, chunks)
                if bev is not None and validate_alignment(bev, scraped_text)[0]:
                    parts, ok = bev, True
        if ok:
            # Placement accuracy gate (skipped for transliterations — no
            # English content words to judge by).
            if "transliteration" not in tid:
                parts = fix_single_dump(parts, chunks)
                suspect = placement_suspect(parts, chunks)
                if suspect:
                    if "does not belong" in suspect:
                        # Wrong-verse text / stub: no split can fix this —
                        # quarantine so the flat view (only) shows it.
                        return None, suspect
                    # Best effort instead of giving up: re-split the original
                    # by reference similarity (omitted segments stay empty —
                    # e.g. an isnad the translator skipped).
                    bev = best_effort_split(scraped_text, chunks)
                    if bev is not None and validate_alignment(bev, scraped_text)[0]:
                        return bev, ""
                    last_reason = suspect
                    continue  # fresh sample sometimes places correctly
            return parts, ""
        last_reason = reason
    return None, last_reason


# ── orchestration ─────────────────────────────────────────────────────────

def _prevent_sleep():
    """Keep a Windows host awake for the duration (no-op elsewhere)."""
    try:
        import ctypes
        ES_CONTINUOUS = 0x80000000
        ES_SYSTEM_REQUIRED = 0x00000001
        ES_AWAYMODE_REQUIRED = 0x00000040
        ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED
        )
        return True
    except (AttributeError, OSError):
        return False


def _release_sleep():
    try:
        import ctypes
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
    except (AttributeError, OSError):
        pass


async def _process_verse(
    verse_path: str,
    *,
    data_dir: str,
    model: str,
    lang_set: Optional[set],
    id_filter: Optional[set],
    alignment_dir: str,
    raw_archive_dir: str,
    attempt_quarantined: bool,
    dry_run: bool,
    stats: dict,
) -> None:
    vid = verse_path_to_id(verse_path)
    out_path = os.path.join(alignment_dir, f"{vid}.json")

    verse = load_built_verse(verse_path, data_dir)
    if verse is None:
        return
    if len(get_chunks(verse)) < 2:
        return
    chunks = prepared_chunks(verse, load_response_result(vid))
    if not is_eligible(chunks):
        return
    ids = eligible_scraped_ids(verse, lang_set)
    if id_filter:
        ids = [i for i in ids if i in id_filter]
    if not ids:
        return

    # Resume: load any prior artifact so we keep good alignments and only
    # (re)do missing / quarantined ids.
    prior_aligned: Dict[str, List[str]] = {}
    prior_quarantined: Dict[str, str] = {}
    if os.path.exists(out_path):
        try:
            with open(out_path, "r", encoding="utf-8") as f:
                prev = json.load(f)
            prior_aligned = prev.get("aligned", {}) or {}
            prior_quarantined = prev.get("quarantined", {}) or {}
        except (json.JSONDecodeError, OSError):
            pass

    to_do = []
    for tid in ids:
        if tid in prior_aligned:
            continue
        if tid in prior_quarantined and not attempt_quarantined:
            continue
        to_do.append(tid)
    if not to_do:
        stats["skipped"] += 1
        return

    stats["eligible"] += 1
    if dry_run:
        stats["processed"] += 1
        stats["aligned_ids"] += len(to_do)
        return

    aligned = dict(prior_aligned)
    quarantined = dict(prior_quarantined)
    for tid in to_do:
        text = scraped_text_for(verse, tid)
        parts, reason = await _align_one(chunks, text, model, vid, tid, raw_archive_dir)
        if parts is not None:
            aligned[tid] = parts
            quarantined.pop(tid, None)
            stats["aligned_ids"] += 1
        else:
            quarantined[tid] = reason
            stats["quarantined_ids"] += 1
            logger.warning("Align quarantine %s [%s]: %s", vid, tid, reason)

    out = {
        "verse_path": verse_path,
        "model": model,
        "generated_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "num_chunks": len(chunks),
        "aligned": aligned,
        "quarantined": quarantined,
    }
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    os.replace(tmp, out_path)
    stats["processed"] += 1


async def _progress(stats: dict, total: int, interval: int = 30):
    while True:
        await asyncio.sleep(interval)
        print(
            f"  …aligned {stats['processed']}/{total} verses | "
            f"ids ok={stats['aligned_ids']} quarantined={stats['quarantined_ids']} "
            f"skipped={stats['skipped']}",
            flush=True,
        )


async def run_alignment(
    *,
    book: Optional[str],
    langs: Optional[List[str]],
    translation_ids: Optional[List[str]],
    workers: int,
    data_dir: str,
    model: str,
    dry_run: bool,
    attempt_quarantined: bool,
    max_verses: Optional[int],
    alignment_dir: Optional[str] = None,
) -> dict:
    """Run the scraped-translation alignment pass over the corpus."""
    from app.pipeline_cli.pipeline import load_corpus_manifest

    alignment_dir = alignment_dir or AI_ALIGNMENT_DIR
    os.makedirs(alignment_dir, exist_ok=True)
    raw_archive_dir = os.path.join(os.path.dirname(alignment_dir), "raw_responses")

    verse_paths = load_corpus_manifest()
    if not verse_paths:
        print("ERROR: No verses in corpus manifest.", flush=True)
        return {}
    books = [b.strip() for b in book.split(",")] if book else []
    if books:
        verse_paths = [
            vp for vp in verse_paths
            if any(vp.startswith(f"/books/{b}:") for b in books)
        ]

    lang_set = set(langs) if langs else None
    id_filter = set(translation_ids) if translation_ids else None

    # Coarse queue: drop verses whose artifact already exists (unless we're
    # retrying quarantined ids). Per-id resume happens inside _process_verse.
    queue = []
    for vp in verse_paths:
        out_path = os.path.join(alignment_dir, f"{verse_path_to_id(vp)}.json")
        if os.path.exists(out_path) and not attempt_quarantined:
            continue
        queue.append(vp)
    if max_verses:
        queue = queue[:max_verses]

    is_spark = is_spark_model(model)
    print(
        f"align-scraped: {len(queue)} verses queued | book={book or 'all'} | "
        f"langs={sorted(lang_set) if lang_set else 'all'} | model={model} "
        f"(backend={'spark' if is_spark else 'openai'}) | workers={workers}"
        + (" | DRY RUN" if dry_run else ""),
        flush=True,
    )
    if not queue:
        print("Nothing to do — all aligned or filtered out.", flush=True)
        return {}

    stats = {"processed": 0, "aligned_ids": 0, "quarantined_ids": 0,
             "skipped": 0, "eligible": 0}
    sem = asyncio.Semaphore(workers)
    slept = _prevent_sleep() if not dry_run else False
    started = time.time()

    async def worker(vp):
        async with sem:
            try:
                await _process_verse(
                    vp, data_dir=data_dir, model=model, lang_set=lang_set,
                    id_filter=id_filter, alignment_dir=alignment_dir,
                    raw_archive_dir=raw_archive_dir,
                    attempt_quarantined=attempt_quarantined,
                    dry_run=dry_run, stats=stats,
                )
            except Exception as e:  # never let one verse kill the run
                logger.warning("Align error on %s: %s", vp, e)

    progress_task = asyncio.create_task(_progress(stats, len(queue)))
    try:
        await asyncio.gather(*(worker(vp) for vp in queue), return_exceptions=True)
    finally:
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass
        if slept:
            _release_sleep()

    elapsed = (time.time() - started) / 60
    print(f"\n{'=' * 60}", flush=True)
    print(f"align-scraped complete ({elapsed:.1f} min)", flush=True)
    print(f"  Verses with eligible work: {stats['eligible']}", flush=True)
    print(f"  Verses written: {stats['processed']} | already-done skipped: {stats['skipped']}", flush=True)
    print(f"  Translation-ids aligned: {stats['aligned_ids']} | quarantined: {stats['quarantined_ids']}", flush=True)
    return stats
