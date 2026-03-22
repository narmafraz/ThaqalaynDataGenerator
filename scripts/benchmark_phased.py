#!/usr/bin/env python3
"""Benchmark the phased pipeline against Claude Sonnet baseline responses.

Modes:
  Offline (default): Simulates Phase 1 from existing Claude data, runs Phase 2
    for real, uses Claude translations as Phase 3/4 stand-in. Measures Phase 2
    quality without API calls.

  Live (--live): Runs actual Phase 1 (OpenAI) + Phase 2 (programmatic) + Phase 4
    (OpenAI translation). Measures real end-to-end quality and cost.
    Requires OPENAI_API_KEY. Use --live-verses N to limit (default: 5).

Usage:
    python scripts/benchmark_phased.py                    # offline, all 11 verses
    python scripts/benchmark_phased.py --live             # live, 5 verses
    python scripts/benchmark_phased.py --live --live-verses 3  # live, 3 verses
    python scripts/benchmark_phased.py --verbose          # show per-field details
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

# Fix Windows console encoding for Arabic text
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

sys.path.insert(0, str(PROJECT_ROOT))

from app.config import AI_PIPELINE_DATA_DIR, AI_CONTENT_DIR
from app.pipeline_cli.programmatic_enrichment import programmatic_enrich
from app.ai_pipeline import (
    load_key_phrases_dictionary,
    load_topic_taxonomy,
    load_word_dictionary,
    validate_result,
)
from app.narrator_registry import NarratorRegistry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BENCHMARK_DIR = Path(AI_CONTENT_DIR).parent / "ai-content" / "benchmarks" / "claude-sonnet" / "responses"
if not BENCHMARK_DIR.exists():
    BENCHMARK_DIR = PROJECT_ROOT.parent / "ThaqalaynDataSources" / "ai-content" / "benchmarks" / "claude-sonnet" / "responses"

PHASED_OUTPUT_DIR = Path(AI_CONTENT_DIR).parent / "ai-content" / "benchmarks" / "phased" / "responses"
if not PHASED_OUTPUT_DIR.parent.parent.exists():
    PHASED_OUTPUT_DIR = PROJECT_ROOT.parent / "ThaqalaynDataSources" / "ai-content" / "benchmarks" / "phased" / "responses"

BENCHMARK_FILES = [
    "al-amali-saduq_1_16.json",
    "al-kafi_1_2_19_11.json",
    "al-kafi_1_2_8_2.json",
    "al-kafi_1_3_1_1.json",
    "al-kafi_1_4_41_6.json",
    "al-kafi_2_1_1_1.json",
    "al-kafi_3_1_1_1.json",
    "al-kafi_4_1_1_1.json",
    "al-kafi_6_2_8_5.json",
    "al-kafi_7_4_2_6.json",
    "tahdhib-al-ahkam_1_11_5.json",
]

# Monolithic GPT-5.4 projected cost per verse (from docs/PIPELINE_OPTIMIZATION_PLAN.md)
MONOLITHIC_GPT54_COST_PER_VERSE = 0.11

# Total corpus size
TOTAL_CORPUS_SIZE = 58000


# ---------------------------------------------------------------------------
# Resource loading helpers (reused from benchmark_phase2.py)
# ---------------------------------------------------------------------------


def load_benchmark(path: Path) -> Optional[dict]:
    """Load a single benchmark response JSON file."""
    if not path.exists():
        print(f"  WARNING: benchmark file not found: {path}")
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_narrator_templates_dict() -> Optional[dict]:
    """Load narrator templates keyed by canonical_id string."""
    path = os.path.join(AI_PIPELINE_DATA_DIR, "narrator_templates.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_tag_topic_mapping() -> Optional[dict]:
    """Load tag_topic_mapping.json if it exists."""
    path = os.path.join(AI_PIPELINE_DATA_DIR, "tag_topic_mapping.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def load_word_dict() -> Optional[dict]:
    """Load word translations dict (v4) or cache."""
    path = os.path.join(AI_PIPELINE_DATA_DIR, "word_translations_dict_v4.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    path = os.path.join(AI_PIPELINE_DATA_DIR, "word_translations_cache.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return load_word_dictionary()


def extract_book_name(verse_path: str) -> str:
    """Extract book name slug from verse path like /books/al-kafi:1:2:8:2."""
    parts = verse_path.replace("/books/", "").split(":")
    return parts[0] if parts else ""


def word_analysis_to_word_tags(word_analysis: List[dict]) -> List[list]:
    """Convert v3 word_analysis format to v4 word_tags format."""
    tags = []
    for entry in word_analysis:
        if isinstance(entry, dict):
            word = entry.get("word", "")
            pos = entry.get("pos", "")
            tags.append([word, pos])
    return tags


def count_arabic_words(text: str) -> int:
    """Count words in Arabic text."""
    if not text:
        return 0
    return len(text.split())


# ---------------------------------------------------------------------------
# Phase 1 simulation (offline mode)
# ---------------------------------------------------------------------------


def simulate_phase1(ai_result: dict) -> dict:
    """Simulate Phase 1 output from an existing Claude baseline result.

    Produces the new 7-field Phase 1 format:
    - chunks with arabic_text + EN-only translations (no word_start/word_end)
    - tags, content_type, topics (LLM-generated)
    - translations.en (summary + seo_question + key_terms)
    - related_quran filtered to thematic only
    - has_chain (top-level boolean)
    """
    phase1 = {}

    # Reconstruct word_tags from source for arabic_text reconstruction
    word_tags = ai_result.get("word_tags", [])
    if not word_tags and "word_analysis" in ai_result:
        word_tags = word_analysis_to_word_tags(ai_result["word_analysis"])

    # Chunks with arabic_text + EN-only translations (no word_start/word_end)
    chunks = ai_result.get("chunks", [])
    phase1_chunks = []
    for chunk in chunks:
        c = {
            "chunk_type": chunk.get("chunk_type", "body"),
            "arabic_text": chunk.get("arabic_text", ""),
            "translations": {},
        }
        # If arabic_text missing (stripped format), reconstruct from word_tags
        if not c["arabic_text"] and word_tags:
            ws = chunk.get("word_start", 0)
            we = chunk.get("word_end", 0)
            c["arabic_text"] = " ".join(
                wt[0] if isinstance(wt, (list, tuple)) else str(wt)
                for wt in word_tags[ws:we]
            )
        # EN-only translations
        if "translations" in chunk and isinstance(chunk["translations"], dict):
            if "en" in chunk["translations"]:
                c["translations"]["en"] = chunk["translations"]["en"]
        phase1_chunks.append(c)
    phase1["chunks"] = phase1_chunks

    # Tags, content_type, topics
    phase1["tags"] = ai_result.get("tags", [])
    phase1["content_type"] = ai_result.get("content_type", "")
    phase1["topics"] = ai_result.get("topics", [])

    # Translations: EN with summary, seo_question, key_terms
    translations = ai_result.get("translations", {})
    en_data = translations.get("en", {}) if isinstance(translations, dict) else {}
    phase1["translations"] = {"en": {
        "summary": en_data.get("summary", "") if isinstance(en_data, dict) else "",
        "seo_question": en_data.get("seo_question", "") if isinstance(en_data, dict) else "",
        "key_terms": en_data.get("key_terms", {}) if isinstance(en_data, dict) else {},
    }}

    # Related Quran: thematic only
    related_quran = ai_result.get("related_quran", [])
    phase1["related_quran"] = [
        ref for ref in related_quran
        if ref.get("relationship") == "thematic"
    ]

    # has_chain: top-level boolean
    isnad = ai_result.get("isnad_matn", {})
    phase1["has_chain"] = isnad.get("has_chain", False)

    return phase1


def build_request(verse_path: str, ai_result: dict) -> SimpleNamespace:
    """Build a mock request object for programmatic_enrich."""
    book_name = extract_book_name(verse_path)

    arabic_text = ai_result.get("diacritized_text", "")
    if not arabic_text:
        isnad = ai_result.get("isnad_matn", {})
        parts = [isnad.get("isnad_ar", ""), isnad.get("matn_ar", "")]
        arabic_text = " ".join(p for p in parts if p).strip()

    english_text = ""
    en_trans = ai_result.get("translations", {}).get("en", {})
    if isinstance(en_trans, dict):
        english_text = en_trans.get("summary", "")

    existing_chain = ai_result.get("isnad_matn", {}).get("isnad_ar", "")
    chapter_title = ""

    return SimpleNamespace(
        arabic_text=arabic_text,
        english_text=english_text,
        book_name=book_name,
        chapter_title=chapter_title,
        existing_narrator_chain=existing_chain,
    )


def backfill_translations(phase2_result: dict, ai_result: dict) -> dict:
    """Copy non-EN translations from Claude baseline into Phase 2 result.

    Simulates a perfect Phase 3/4 by taking Claude's translations.
    """
    result = dict(phase2_result)
    ai_translations = ai_result.get("translations", {})
    result_translations = result.get("translations", {})

    for lang, lang_data in ai_translations.items():
        if lang == "en":
            continue
        if isinstance(lang_data, dict):
            result_translations[lang] = dict(lang_data)

    # Also backfill chunk translations for non-EN languages
    ai_chunks = ai_result.get("chunks", [])
    result_chunks = result.get("chunks", [])
    for i, chunk in enumerate(result_chunks):
        if i < len(ai_chunks):
            ai_chunk_trans = ai_chunks[i].get("translations", {})
            chunk_trans = chunk.get("translations", {})
            for lang, text in ai_chunk_trans.items():
                if lang != "en":
                    chunk_trans[lang] = text
            chunk["translations"] = chunk_trans

    result["translations"] = result_translations
    return result


# ---------------------------------------------------------------------------
# Comparison functions
# ---------------------------------------------------------------------------


def jaccard(set_a: set, set_b: set) -> float:
    """Compute Jaccard similarity between two sets."""
    if not set_a and not set_b:
        return 1.0
    union = set_a | set_b
    if not union:
        return 1.0
    return len(set_a & set_b) / len(union)


def compare_topics(phase_topics: List[str], ai_topics: List[str]) -> float:
    """Jaccard similarity between topics."""
    return jaccard(set(phase_topics or []), set(ai_topics or []))


def compare_tags(phase_tags: List[str], ai_tags: List[str]) -> float:
    """Jaccard similarity between tags."""
    return jaccard(set(phase_tags or []), set(ai_tags or []))


def compare_content_type(phase_ct: str, ai_ct: str) -> bool:
    """Exact match on content_type."""
    if not ai_ct:
        return True
    return phase_ct == ai_ct


def compare_key_phrases(
    phase_phrases: List[dict], ai_phrases: List[dict]
) -> Tuple[int, int]:
    """Recall of baseline key phrases found in phased result.

    Returns (found, total) tuple.
    """
    ai_set = set()
    for p in (ai_phrases or []):
        ar = p.get("phrase_ar", "")
        if ar:
            ai_set.add(ar)
    if not ai_set:
        return (0, 0)
    phase_set = set()
    for p in (phase_phrases or []):
        ar = p.get("phrase_ar", "")
        if ar:
            phase_set.add(ar)
    found = len(ai_set & phase_set)
    return (found, len(ai_set))


def compare_key_terms(
    phase_translations: dict, ai_translations: dict
) -> Tuple[int, int]:
    """Recall of baseline Arabic key terms (EN) found in phased result.

    Returns (found, total) tuple.
    """
    ai_en = ai_translations.get("en", {})
    ai_kt = set()
    if isinstance(ai_en, dict) and "key_terms" in ai_en:
        kt = ai_en["key_terms"]
        if isinstance(kt, dict):
            ai_kt = set(kt.keys())

    if not ai_kt:
        return (0, 0)

    p_en = phase_translations.get("en", {})
    p_kt = set()
    if isinstance(p_en, dict) and "key_terms" in p_en:
        kt = p_en["key_terms"]
        if isinstance(kt, dict):
            p_kt = set(kt.keys())

    found = len(ai_kt & p_kt)
    return (found, len(ai_kt))


def compare_narrators(
    phase_isnad: dict, ai_isnad: dict
) -> Tuple[str, int, int]:
    """Compare narrator counts and canonical_id match rate.

    Returns (count_str, matched, total) where count_str is "P/A" format.
    """
    ai_narrs = ai_isnad.get("narrators", [])
    p_narrs = phase_isnad.get("narrators", [])

    if not ai_narrs:
        return (f"{len(p_narrs)}/{len(ai_narrs)}", 0, 0)

    # Match by position: canonical_id or name overlap
    matched = 0
    ai_by_pos = {n.get("position", i): n for i, n in enumerate(ai_narrs)}
    p_by_pos = {n.get("position", i): n for i, n in enumerate(p_narrs)}

    for pos, ai_n in ai_by_pos.items():
        p_n = p_by_pos.get(pos)
        if p_n is None:
            continue
        ai_cid = ai_n.get("canonical_id")
        p_cid = p_n.get("canonical_id")
        ai_name = (ai_n.get("name_en") or "").lower().strip()
        p_name = (p_n.get("name_en") or "").lower().strip()
        if (ai_cid is not None and p_cid is not None and ai_cid == p_cid) or \
           (ai_name and p_name and (ai_name in p_name or p_name in ai_name)):
            matched += 1

    return (f"{len(p_narrs)}/{len(ai_narrs)}", matched, len(ai_narrs))


def compare_quran_refs(
    phase_refs: List[dict], ai_refs: List[dict]
) -> Tuple[int, int]:
    """Recall of explicit Quran refs from baseline.

    Returns (found, total) tuple.
    """
    ai_explicit = set()
    for r in (ai_refs or []):
        if r.get("relationship") == "explicit":
            ai_explicit.add(r.get("ref", ""))
    if not ai_explicit:
        return (0, 0)

    p_all = set()
    for r in (phase_refs or []):
        p_all.add(r.get("ref", ""))

    found = len(ai_explicit & p_all)
    return (found, len(ai_explicit))


def compare_diacritics_status(phase_status: str, ai_status: str) -> bool:
    """Exact match on diacritics_status."""
    if not ai_status:
        return True
    return phase_status == ai_status


# ---------------------------------------------------------------------------
# Result row structure
# ---------------------------------------------------------------------------


def build_result_row(
    verse_id: str,
    word_count: int,
    is_valid: bool,
    topics_jaccard: float,
    tags_jaccard: float,
    content_type_match: bool,
    phrases_found: int,
    phrases_total: int,
    terms_found: int,
    terms_total: int,
    narr_str: str,
    narr_matched: int,
    narr_total: int,
    qref_found: int,
    qref_total: int,
    diac_match: bool,
    p1_cost: float = 0.0,
    p4_cost: float = 0.0,
) -> dict:
    """Build a results dict for one verse."""
    return {
        "verse_id": verse_id,
        "word_count": word_count,
        "is_valid": is_valid,
        "topics_jaccard": topics_jaccard,
        "tags_jaccard": tags_jaccard,
        "content_type_match": content_type_match,
        "phrases_found": phrases_found,
        "phrases_total": phrases_total,
        "terms_found": terms_found,
        "terms_total": terms_total,
        "narr_str": narr_str,
        "narr_matched": narr_matched,
        "narr_total": narr_total,
        "qref_found": qref_found,
        "qref_total": qref_total,
        "diac_match": diac_match,
        "p1_cost": p1_cost,
        "p4_cost": p4_cost,
    }


# ---------------------------------------------------------------------------
# Print helpers
# ---------------------------------------------------------------------------


def print_table(rows: List[dict], show_cost: bool = False) -> None:
    """Print the summary table."""
    # Header
    header = (
        f"{'Verse':<32} | {'Words':>5} | {'Valid':>5} | {'Topics':>6} | "
        f"{'Tags':>6} | {'Type':>4} | {'Phrases':>7} | {'Terms':>5} | "
        f"{'Narrators':>9} | {'QuranRef':>8}"
    )
    if show_cost:
        header += f" | {'P1 Cost':>7} | {'P4 Cost':>7} | {'Total':>7}"
    print(header)
    print("-" * len(header))

    for r in rows:
        valid_str = "PASS" if r["is_valid"] else "FAIL"
        ct_str = "YES" if r["content_type_match"] else "NO"
        phrases_str = f"{r['phrases_found']}/{r['phrases_total']}"
        terms_str = f"{r['terms_found']}/{r['terms_total']}"
        narr_match_str = f"{r['narr_matched']}/{r['narr_total']}" if r["narr_total"] > 0 else r["narr_str"]
        qref_str = f"{r['qref_found']}/{r['qref_total']}"
        diac_label = "YES" if r["diac_match"] else "NO"

        line = (
            f"{r['verse_id']:<32} | {r['word_count']:>5} | {valid_str:>5} | "
            f"{r['topics_jaccard']:>6.2f} | {r['tags_jaccard']:>6.2f} | "
            f"{ct_str:>4} | {phrases_str:>7} | {terms_str:>5} | "
            f"{narr_match_str:>9} | {qref_str:>8}"
        )
        if show_cost:
            total_cost = r["p1_cost"] + r["p4_cost"]
            line += f" | ${r['p1_cost']:>6.3f} | ${r['p4_cost']:>6.3f} | ${total_cost:>6.3f}"
        print(line)


def print_averages(rows: List[dict], show_cost: bool = False) -> None:
    """Print aggregate averages."""
    n = len(rows)
    if n == 0:
        print("  No results to average.")
        return

    valid_count = sum(1 for r in rows if r["is_valid"])
    avg_topics = sum(r["topics_jaccard"] for r in rows) / n
    avg_tags = sum(r["tags_jaccard"] for r in rows) / n
    ct_count = sum(1 for r in rows if r["content_type_match"])

    phrases_found_total = sum(r["phrases_found"] for r in rows)
    phrases_total_total = sum(r["phrases_total"] for r in rows)
    phrases_recall = phrases_found_total / phrases_total_total if phrases_total_total > 0 else 1.0

    terms_found_total = sum(r["terms_found"] for r in rows)
    terms_total_total = sum(r["terms_total"] for r in rows)
    terms_recall = terms_found_total / terms_total_total if terms_total_total > 0 else 1.0

    narr_matched_total = sum(r["narr_matched"] for r in rows)
    narr_total_total = sum(r["narr_total"] for r in rows)
    narr_rate = narr_matched_total / narr_total_total if narr_total_total > 0 else 1.0

    qref_found_total = sum(r["qref_found"] for r in rows)
    qref_total_total = sum(r["qref_total"] for r in rows)
    qref_recall = qref_found_total / qref_total_total if qref_total_total > 0 else 1.0

    print()
    print("AVERAGES:")
    print(f"  Validation pass rate:  {valid_count * 100 / n:.0f}% ({valid_count}/{n})")
    print(f"  Topics Jaccard:        {avg_topics:.2f}")
    print(f"  Tags Jaccard:          {avg_tags:.2f}")
    print(f"  Content type match:    {ct_count * 100 / n:.0f}% ({ct_count}/{n})")
    print(f"  Key phrases recall:    {phrases_recall * 100:.0f}%")
    print(f"  Key terms recall:      {terms_recall * 100:.0f}%")
    print(f"  Narrator match rate:   {narr_rate * 100:.0f}%")
    print(f"  Quran ref recall:      {qref_recall * 100:.0f}%")

    if show_cost:
        total_p1 = sum(r["p1_cost"] for r in rows)
        total_p4 = sum(r["p4_cost"] for r in rows)
        total_all = total_p1 + total_p4
        avg_cost = total_all / n if n > 0 else 0
        projected = avg_cost * TOTAL_CORPUS_SIZE
        monolithic_projected = MONOLITHIC_GPT54_COST_PER_VERSE * TOTAL_CORPUS_SIZE
        savings = (1 - projected / monolithic_projected) * 100 if monolithic_projected > 0 else 0

        print()
        print("COST SUMMARY:")
        print(f"  Avg cost/verse:     ${avg_cost:.3f}")
        print(f"  Projected {TOTAL_CORPUS_SIZE // 1000}K:      ${projected:,.0f}")
        print(f"  vs monolithic GPT-5.4: ${monolithic_projected:,.0f} ({savings:.0f}% savings)")


# ---------------------------------------------------------------------------
# Verbose detail printer
# ---------------------------------------------------------------------------


def print_verbose_details(
    verse_id: str,
    phase_result: dict,
    ai_result: dict,
    validation_errors: List[str],
) -> None:
    """Print detailed per-field comparison for a single verse."""
    print(f"\n    --- Detailed comparison for {verse_id} ---")

    if validation_errors:
        print(f"    Validation errors: {validation_errors}")

    # Topics
    ai_topics = ai_result.get("topics", [])
    p_topics = phase_result.get("topics", [])
    print(f"    Topics:       AI={ai_topics}")
    print(f"                  P ={p_topics}")

    # Tags
    ai_tags = ai_result.get("tags", [])
    p_tags = phase_result.get("tags", [])
    print(f"    Tags:         AI={ai_tags}")
    print(f"                  P ={p_tags}")

    # Content type
    ai_ct = ai_result.get("content_type", "")
    p_ct = phase_result.get("content_type", "")
    print(f"    Content type: AI={ai_ct} P={p_ct}")

    # Narrators
    ai_narrs = ai_result.get("isnad_matn", {}).get("narrators", [])
    p_narrs = phase_result.get("isnad_matn", {}).get("narrators", [])
    for i, ai_n in enumerate(ai_narrs):
        p_n = p_narrs[i] if i < len(p_narrs) else None
        ai_cid = ai_n.get("canonical_id")
        p_cid = p_n.get("canonical_id") if p_n else None
        ai_name = ai_n.get("name_en", "")
        p_name = p_n.get("name_en", "") if p_n else "MISSING"
        match = "OK" if (ai_cid is not None and p_cid is not None and ai_cid == p_cid) else "MISS"
        print(f"    Narrator {i}: AI=({ai_cid}, {ai_name}) P=({p_cid}, {p_name}) [{match}]")


# ---------------------------------------------------------------------------
# Offline benchmark
# ---------------------------------------------------------------------------


def run_offline_benchmark(
    verbose: bool,
    narrator_templates: Optional[dict],
    registry: Optional[NarratorRegistry],
    word_dict: Optional[dict],
    phrases_dict: Optional[dict],
    taxonomy: Optional[dict],
) -> List[dict]:
    """Run offline benchmark on all 11 benchmark verses. Returns result rows."""
    print(f"Loading benchmark files from: {BENCHMARK_DIR}")
    benchmarks = []
    for fname in BENCHMARK_FILES:
        path = BENCHMARK_DIR / fname
        data = load_benchmark(path)
        if data:
            benchmarks.append((fname, data))
    print(f"  Loaded {len(benchmarks)}/{len(BENCHMARK_FILES)} benchmark files")
    print()

    if not benchmarks:
        print("ERROR: No benchmark files loaded. Aborting.")
        return []

    rows = []
    for fname, data in benchmarks:
        verse_path = data.get("verse_path", fname)
        verse_id = fname.replace(".json", "")
        ai_result = data.get("result", {})

        # Count words
        arabic_text = ai_result.get("diacritized_text", "")
        if not arabic_text:
            isnad = ai_result.get("isnad_matn", {})
            parts = [isnad.get("isnad_ar", ""), isnad.get("matn_ar", "")]
            arabic_text = " ".join(p for p in parts if p).strip()
        word_count = count_arabic_words(arabic_text)

        # Step 1: Simulate Phase 1
        phase1 = simulate_phase1(ai_result)

        # Step 2: Run Phase 2
        request = build_request(verse_path, ai_result)
        phase2_result = programmatic_enrich(
            phase1_result=phase1,
            request=request,
            narrator_templates=narrator_templates,
            registry=registry,
            word_dict=word_dict,
            phrases_dict=phrases_dict,
            taxonomy=taxonomy,
        )

        # Step 3: Backfill Phase 3/4 (copy Claude translations)
        combined = backfill_translations(phase2_result, ai_result)

        # Step 4: Validate
        errors = validate_result(combined)
        is_valid = len(errors) == 0

        # Step 5: Compare field-by-field
        topics_j = compare_topics(combined.get("topics", []), ai_result.get("topics", []))
        tags_j = compare_tags(combined.get("tags", []), ai_result.get("tags", []))
        ct_match = compare_content_type(
            combined.get("content_type", ""), ai_result.get("content_type", "")
        )
        phr_found, phr_total = compare_key_phrases(
            combined.get("key_phrases", []), ai_result.get("key_phrases", [])
        )
        trm_found, trm_total = compare_key_terms(
            combined.get("translations", {}), ai_result.get("translations", {})
        )
        narr_str, narr_matched, narr_total = compare_narrators(
            combined.get("isnad_matn", {}), ai_result.get("isnad_matn", {})
        )
        qref_found, qref_total = compare_quran_refs(
            combined.get("related_quran", []), ai_result.get("related_quran", [])
        )
        diac_match = compare_diacritics_status(
            combined.get("diacritics_status", ""), ai_result.get("diacritics_status", "")
        )

        row = build_result_row(
            verse_id=verse_id,
            word_count=word_count,
            is_valid=is_valid,
            topics_jaccard=topics_j,
            tags_jaccard=tags_j,
            content_type_match=ct_match,
            phrases_found=phr_found,
            phrases_total=phr_total,
            terms_found=trm_found,
            terms_total=trm_total,
            narr_str=narr_str,
            narr_matched=narr_matched,
            narr_total=narr_total,
            qref_found=qref_found,
            qref_total=qref_total,
            diac_match=diac_match,
        )
        rows.append(row)

        if verbose:
            print_verbose_details(verse_id, combined, ai_result, errors)

    return rows


# ---------------------------------------------------------------------------
# Live benchmark
# ---------------------------------------------------------------------------


def select_live_verses(benchmarks: List[Tuple[str, dict]], count: int) -> List[Tuple[str, dict]]:
    """Select a spread of verses across word count ranges.

    Sorts by word count and picks indices [0, 2, 5, 8, 10] for 5 verses,
    or evenly spaced for other counts.
    """
    # Sort by word count
    def get_wc(item):
        ai = item[1].get("result", {})
        text = ai.get("diacritized_text", "")
        if not text:
            isnad = ai.get("isnad_matn", {})
            parts = [isnad.get("isnad_ar", ""), isnad.get("matn_ar", "")]
            text = " ".join(p for p in parts if p).strip()
        return count_arabic_words(text)

    sorted_benchmarks = sorted(benchmarks, key=get_wc)

    if count >= len(sorted_benchmarks):
        return sorted_benchmarks

    if count == 5 and len(sorted_benchmarks) == 11:
        indices = [0, 2, 5, 8, 10]
    else:
        # Evenly spaced
        step = (len(sorted_benchmarks) - 1) / max(count - 1, 1)
        indices = [round(i * step) for i in range(count)]

    return [sorted_benchmarks[i] for i in indices]


async def run_live_benchmark(
    verbose: bool,
    live_verses: int,
    phase1_model: str,
    phase4_model: str,
    narrator_templates: Optional[dict],
    registry: Optional[NarratorRegistry],
    word_dict: Optional[dict],
    phrases_dict: Optional[dict],
    taxonomy: Optional[dict],
) -> List[dict]:
    """Run live benchmark with actual API calls. Returns result rows."""
    from app.pipeline_cli.openai_backend import call_openai
    from app.pipeline_cli.phased_prompts import (
        build_phase1_system_prompt,
        build_phase1_user_message,
        parse_phase1_response,
    )
    from app.pipeline_cli.translation_phase import translate_chunks
    from app.ai_pipeline import extract_pipeline_request, PipelineRequest

    # Check for API key
    if not os.environ.get("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY not set. Live mode requires an API key.")
        return []

    # Load benchmark files
    print(f"Loading benchmark files from: {BENCHMARK_DIR}")
    benchmarks = []
    for fname in BENCHMARK_FILES:
        path = BENCHMARK_DIR / fname
        data = load_benchmark(path)
        if data:
            benchmarks.append((fname, data))
    print(f"  Loaded {len(benchmarks)}/{len(BENCHMARK_FILES)} benchmark files")

    # Select verses
    selected = select_live_verses(benchmarks, live_verses)
    print(f"  Selected {len(selected)} verses for live benchmark")
    print()

    # Prepare output directory
    PHASED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build Phase 1 system prompt (shared across verses)
    phase1_system = build_phase1_system_prompt()

    rows = []
    for fname, data in selected:
        verse_path = data.get("verse_path", fname)
        verse_id = fname.replace(".json", "")
        ai_result = data.get("result", {})

        arabic_text = ai_result.get("diacritized_text", "")
        if not arabic_text:
            isnad = ai_result.get("isnad_matn", {})
            parts = [isnad.get("isnad_ar", ""), isnad.get("matn_ar", "")]
            arabic_text = " ".join(p for p in parts if p).strip()
        word_count = count_arabic_words(arabic_text)

        print(f"  Processing {verse_id} ({word_count} words)...")

        # Try to load verse data for a proper PipelineRequest
        pipeline_request = None
        try:
            pipeline_request = extract_pipeline_request(verse_path)
        except Exception:
            pass

        # Build Phase 1 user message
        if pipeline_request is not None:
            phase1_user = build_phase1_user_message(pipeline_request)
        else:
            # Fall back to mock request
            mock_req = build_request(verse_path, ai_result)
            # Build a minimal PipelineRequest-like object
            phase1_user = (
                f"Arabic text: {mock_req.arabic_text}\n"
                f"English reference translation: {mock_req.english_text}\n"
                f"Book: {mock_req.book_name}\n"
                f"Chapter: {mock_req.chapter_title}\n"
                f"Existing narrator chain: {mock_req.existing_narrator_chain or 'null'}\n"
            )

        # --- Phase 1: OpenAI call ---
        p1_cost = 0.0
        print(f"    Phase 1 ({phase1_model})...", end="", flush=True)
        cr = await call_openai(phase1_system, phase1_user, model=phase1_model)

        if "error" in cr:
            print(f" ERROR: {cr['error']}")
            continue

        p1_cost = cr.get("cost", 0.0)
        print(f" done (${p1_cost:.4f})")

        # Parse Phase 1 response
        raw_text = cr.get("result", "")
        try:
            # Strip code fences
            cleaned = raw_text.strip()
            if cleaned.startswith("```"):
                first_nl = cleaned.index("\n") if "\n" in cleaned else 3
                cleaned = cleaned[first_nl + 1:]
                if cleaned.rstrip().endswith("```"):
                    cleaned = cleaned.rstrip()[:-3].rstrip()
            phase1_raw = json.loads(cleaned)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"    Phase 1 JSON parse failed: {e}")
            continue

        phase1 = parse_phase1_response(phase1_raw)

        # --- Phase 2: Programmatic enrichment ---
        print("    Phase 2 (programmatic)...", end="", flush=True)
        request = build_request(verse_path, ai_result)
        phase2_result = programmatic_enrich(
            phase1_result=phase1,
            request=request,
            narrator_templates=narrator_templates,
            registry=registry,
            word_dict=word_dict,
            phrases_dict=phrases_dict,
            taxonomy=taxonomy,
        )
        print(" done")

        # --- Phase 4: Translation ---
        p4_cost = 0.0
        print(f"    Phase 4 ({phase4_model})...", end="", flush=True)
        phase2_result = await translate_chunks(
            phase2_result,
            model=phase4_model,
            arabic_text=request.arabic_text,
        )
        p4_cost = phase2_result.pop("_phase4_cost", 0.0)
        phase2_result.pop("_phase4_tokens", None)
        print(f" done (${p4_cost:.4f})")

        # --- Validate ---
        errors = validate_result(phase2_result)
        is_valid = len(errors) == 0

        # --- Compare against Claude baseline ---
        topics_j = compare_topics(
            phase2_result.get("topics", []), ai_result.get("topics", [])
        )
        tags_j = compare_tags(
            phase2_result.get("tags", []), ai_result.get("tags", [])
        )
        ct_match = compare_content_type(
            phase2_result.get("content_type", ""), ai_result.get("content_type", "")
        )
        phr_found, phr_total = compare_key_phrases(
            phase2_result.get("key_phrases", []), ai_result.get("key_phrases", [])
        )
        trm_found, trm_total = compare_key_terms(
            phase2_result.get("translations", {}), ai_result.get("translations", {})
        )
        narr_str, narr_matched, narr_total = compare_narrators(
            phase2_result.get("isnad_matn", {}), ai_result.get("isnad_matn", {})
        )
        qref_found, qref_total = compare_quran_refs(
            phase2_result.get("related_quran", []), ai_result.get("related_quran", [])
        )
        diac_match = compare_diacritics_status(
            phase2_result.get("diacritics_status", ""),
            ai_result.get("diacritics_status", ""),
        )

        row = build_result_row(
            verse_id=verse_id,
            word_count=word_count,
            is_valid=is_valid,
            topics_jaccard=topics_j,
            tags_jaccard=tags_j,
            content_type_match=ct_match,
            phrases_found=phr_found,
            phrases_total=phr_total,
            terms_found=trm_found,
            terms_total=trm_total,
            narr_str=narr_str,
            narr_matched=narr_matched,
            narr_total=narr_total,
            qref_found=qref_found,
            qref_total=qref_total,
            diac_match=diac_match,
            p1_cost=p1_cost,
            p4_cost=p4_cost,
        )
        rows.append(row)

        # Save phased response
        output_path = PHASED_OUTPUT_DIR / fname
        output_data = {
            "verse_path": verse_path,
            "ai_attribution": {
                "model": f"phased:{phase1_model}+{phase4_model}",
                "generated_date": __import__("datetime").date.today().isoformat(),
                "pipeline_version": "4.0.0-phased",
                "generation_method": "openai_api_phased",
            },
            "phase1_model": phase1_model,
            "phase4_model": phase4_model,
            "phase1_cost": p1_cost,
            "phase4_cost": p4_cost,
            "total_cost": p1_cost + p4_cost,
            "result": phase2_result,
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        print(f"    Saved to {output_path}")

        if verbose:
            print_verbose_details(verse_id, phase2_result, ai_result, errors)

        print()

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark the phased pipeline against Claude Sonnet baseline responses."
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Run live mode with actual OpenAI API calls (requires OPENAI_API_KEY)",
    )
    parser.add_argument(
        "--live-verses", type=int, default=5,
        help="Number of verses to process in live mode (default: 5)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Show detailed per-field comparisons",
    )
    parser.add_argument(
        "--phase1-model", type=str, default="gpt-5.4",
        help="OpenAI model for Phase 1 structure generation (default: gpt-5.4)",
    )
    parser.add_argument(
        "--phase4-model", type=str, default="gpt-5-mini",
        help="OpenAI model for Phase 4 translation (default: gpt-5-mini)",
    )
    args = parser.parse_args()

    mode_label = "Live" if args.live else "Offline"
    print("=" * 78)
    print(f"=== Phased Pipeline Benchmark ({mode_label}) ===")
    print("=" * 78)
    print()

    # Load shared resources
    print("Loading resources...")
    narrator_templates = load_narrator_templates_dict()
    print(f"  Narrator templates: {len(narrator_templates) if narrator_templates else 0} entries")

    registry = None
    try:
        registry = NarratorRegistry()
        print(f"  Narrator registry: loaded ({registry._last_id} narrators)")
    except Exception as e:
        print(f"  Narrator registry: FAILED ({e})")

    phrases_dict = load_key_phrases_dictionary()
    print(f"  Key phrases dict: {len(phrases_dict.get('phrases', [])) if phrases_dict else 0} phrases")

    word_dict = load_word_dict()
    print(f"  Word dictionary: {'loaded' if word_dict else 'NOT FOUND'}")

    taxonomy = load_tag_topic_mapping()
    print(f"  Tag/topic mapping: {'loaded' if taxonomy else 'NOT FOUND (will use defaults)'}")

    print()

    # Run the appropriate mode
    if args.live:
        print(f"Phase 1 model: {args.phase1_model}")
        print(f"Phase 4 model: {args.phase4_model}")
        print(f"Live verses: {args.live_verses}")
        print()
        rows = asyncio.run(run_live_benchmark(
            verbose=args.verbose,
            live_verses=args.live_verses,
            phase1_model=args.phase1_model,
            phase4_model=args.phase4_model,
            narrator_templates=narrator_templates,
            registry=registry,
            word_dict=word_dict,
            phrases_dict=phrases_dict,
            taxonomy=taxonomy,
        ))
    else:
        rows = run_offline_benchmark(
            verbose=args.verbose,
            narrator_templates=narrator_templates,
            registry=registry,
            word_dict=word_dict,
            phrases_dict=phrases_dict,
            taxonomy=taxonomy,
        )

    if not rows:
        print("\nNo results to display.")
        sys.exit(1)

    # Print summary table
    print()
    print("=" * 78)
    print(f"Results ({mode_label} mode, {len(rows)} verses):")
    print("=" * 78)
    print()
    print_table(rows, show_cost=args.live)
    print_averages(rows, show_cost=args.live)
    print()

    # Exit code: 0 if >80% validation pass rate
    valid_count = sum(1 for r in rows if r["is_valid"])
    pass_rate = valid_count / len(rows) if rows else 0
    sys.exit(0 if pass_rate >= 0.80 else 1)


if __name__ == "__main__":
    main()
