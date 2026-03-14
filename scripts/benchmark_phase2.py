"""Benchmark Phase 2 (programmatic enrichment) against AI-generated benchmark responses.

Loads benchmark verse responses from the Claude Sonnet benchmark set, simulates
Phase 1 output, runs ``programmatic_enrich()`` on it, and compares to the full
AI-generated fields.

Usage:
    cd ThaqalaynDataGenerator
    source .venv/Scripts/activate
    PYTHONPATH="$PWD:$PWD/app" SOURCE_DATA_DIR="../ThaqalaynDataSources/" \
        python scripts/benchmark_phase2.py

    # Verbose per-verse details:
    python scripts/benchmark_phase2.py --verbose
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple

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
from app.ai_pipeline import load_key_phrases_dictionary, load_topic_taxonomy, load_word_dictionary
from app.narrator_registry import NarratorRegistry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BENCHMARK_DIR = Path(AI_CONTENT_DIR).parent / "ai-content" / "benchmarks" / "claude-sonnet" / "responses"
# Fallback: resolve relative to project root
if not BENCHMARK_DIR.exists():
    BENCHMARK_DIR = PROJECT_ROOT.parent / "ThaqalaynDataSources" / "ai-content" / "benchmarks" / "claude-sonnet" / "responses"

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

# Acceptance criteria thresholds
NARRATOR_MATCH_THRESHOLD = 0.80
EXPLICIT_QURAN_RECALL_THRESHOLD = 0.90
TOPICS_TAGS_OVERLAP_THRESHOLD = 0.60
KEY_TERMS_OVERLAP_THRESHOLD = 0.70


# ---------------------------------------------------------------------------
# Helpers
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
    """Load tag_topic_mapping.json if it exists, otherwise return None.

    The programmatic_enrichment module expects a taxonomy dict with keys like
    ``keyword_to_topics``, ``keyword_to_tags``, ``chapter_to_content_type``,
    and ``default_content_type_by_book``. If no such file exists, return None
    (the enrichment will use defaults).
    """
    # Try tag_topic_mapping.json first
    path = os.path.join(AI_PIPELINE_DATA_DIR, "tag_topic_mapping.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def load_word_dict() -> Optional[dict]:
    """Load word translations dict (v4) or cache."""
    # Try v4 dict first
    path = os.path.join(AI_PIPELINE_DATA_DIR, "word_translations_dict_v4.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    # Fall back to word_translations_cache
    path = os.path.join(AI_PIPELINE_DATA_DIR, "word_translations_cache.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    # Fall back to word_dictionary.json
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


def simulate_phase1(ai_result: dict) -> dict:
    """Simulate Phase 1 output: take only what Phase 1 (AI) would produce.

    Phase 1 provides:
    - diacritized_text
    - diacritics_changes
    - word_tags (or reconstructed from word_analysis)
    - chunks with EN-only translations
    - translations.en (summary, seo_question)
    - related_quran (thematic only)
    - isnad_matn (basic: isnad_ar, matn_ar, has_chain, but no narrators)
    """
    phase1 = {}

    # Diacritized text and changes
    phase1["diacritized_text"] = ai_result.get("diacritized_text", "")
    phase1["diacritics_changes"] = ai_result.get("diacritics_changes", [])

    # Word tags: use word_tags if present, else reconstruct from word_analysis
    if "word_tags" in ai_result:
        phase1["word_tags"] = ai_result["word_tags"]
    elif "word_analysis" in ai_result:
        phase1["word_tags"] = word_analysis_to_word_tags(ai_result["word_analysis"])

    # Chunks with EN-only translations (strip other languages)
    chunks = ai_result.get("chunks", [])
    phase1_chunks = []
    for chunk in chunks:
        c = dict(chunk)
        if "translations" in c and isinstance(c["translations"], dict):
            en_only = {}
            if "en" in c["translations"]:
                en_only["en"] = c["translations"]["en"]
            c["translations"] = en_only
        phase1_chunks.append(c)
    phase1["chunks"] = phase1_chunks

    # Translations: EN only (summary, seo_question -- but not key_terms)
    translations = ai_result.get("translations", {})
    if "en" in translations:
        en_data = translations["en"]
        phase1_en = {}
        if "summary" in en_data:
            phase1_en["summary"] = en_data["summary"]
        if "seo_question" in en_data:
            phase1_en["seo_question"] = en_data["seo_question"]
        # Intentionally omit key_terms -- Phase 2 derives them
        phase1["translations"] = {"en": phase1_en}
    else:
        phase1["translations"] = {}

    # Related Quran: thematic only (Phase 2 adds explicit ones)
    related_quran = ai_result.get("related_quran", [])
    phase1["related_quran"] = [
        ref for ref in related_quran
        if ref.get("relationship") == "thematic"
    ]

    # isnad_matn: basic (no narrators -- Phase 2 enriches narrators)
    isnad = ai_result.get("isnad_matn", {})
    phase1["isnad_matn"] = {
        "isnad_ar": isnad.get("isnad_ar", ""),
        "matn_ar": isnad.get("matn_ar", ""),
        "has_chain": isnad.get("has_chain", False),
        "narrators": [],  # Phase 2 fills this
    }

    # Intentionally omit: topics, tags, content_type, key_phrases -- Phase 2 derives them

    return phase1


def build_request(verse_path: str, ai_result: dict) -> SimpleNamespace:
    """Build a mock request object for programmatic_enrich."""
    book_name = extract_book_name(verse_path)

    # Reconstruct arabic_text from diacritized_text or isnad_matn
    arabic_text = ai_result.get("diacritized_text", "")
    if not arabic_text:
        isnad = ai_result.get("isnad_matn", {})
        parts = [isnad.get("isnad_ar", ""), isnad.get("matn_ar", "")]
        arabic_text = " ".join(p for p in parts if p).strip()

    # English text from translations.en.summary or first chunk
    english_text = ""
    en_trans = ai_result.get("translations", {}).get("en", {})
    if isinstance(en_trans, dict):
        english_text = en_trans.get("summary", "")

    # Existing narrator chain from isnad_matn.isnad_ar
    existing_chain = ai_result.get("isnad_matn", {}).get("isnad_ar", "")

    # Chapter title -- not available in benchmark files, derive from path
    chapter_title = ""

    return SimpleNamespace(
        arabic_text=arabic_text,
        english_text=english_text,
        book_name=book_name,
        chapter_title=chapter_title,
        existing_narrator_chain=existing_chain,
    )


# ---------------------------------------------------------------------------
# Comparison functions
# ---------------------------------------------------------------------------


def compare_narrators(
    phase2_isnad: dict, ai_isnad: dict, verbose: bool = False
) -> Dict[str, Any]:
    """Compare narrator enrichment results."""
    ai_narrators = ai_isnad.get("narrators", [])
    p2_narrators = phase2_isnad.get("narrators", [])

    if not ai_narrators:
        return {
            "has_chain": ai_isnad.get("has_chain", False),
            "ai_count": 0,
            "p2_count": len(p2_narrators),
            "canonical_id_matches": 0,
            "name_en_overlaps": 0,
            "match_rate": 1.0,  # No narrators to match = pass
            "details": "No narrators in AI response",
        }

    # Build lookup of AI narrators by position
    ai_by_pos = {n.get("position", i): n for i, n in enumerate(ai_narrators)}
    p2_by_pos = {n.get("position", i): n for i, n in enumerate(p2_narrators)}

    canonical_id_matches = 0
    name_en_overlaps = 0
    total_comparable = 0

    details_lines = []

    for pos, ai_n in ai_by_pos.items():
        total_comparable += 1
        p2_n = p2_by_pos.get(pos)
        if p2_n is None:
            if verbose:
                details_lines.append(
                    f"  pos={pos}: AI={ai_n.get('name_ar', '?')} | P2=MISSING"
                )
            continue

        # Canonical ID match
        ai_cid = ai_n.get("canonical_id")
        p2_cid = p2_n.get("canonical_id")
        if ai_cid is not None and p2_cid is not None and ai_cid == p2_cid:
            canonical_id_matches += 1

        # Name EN overlap (case-insensitive substring)
        ai_name = (ai_n.get("name_en") or "").lower().strip()
        p2_name = (p2_n.get("name_en") or "").lower().strip()
        if ai_name and p2_name and (ai_name in p2_name or p2_name in ai_name):
            name_en_overlaps += 1

        if verbose:
            cid_mark = "OK" if (ai_cid == p2_cid and ai_cid is not None) else "MISS"
            name_mark = "OK" if (ai_name and p2_name and (ai_name in p2_name or p2_name in ai_name)) else "MISS"
            details_lines.append(
                f"  pos={pos}: AI_cid={ai_cid} P2_cid={p2_cid} [{cid_mark}] | "
                f"AI_name='{ai_n.get('name_en', '')}' P2_name='{p2_n.get('name_en', '')}' [{name_mark}]"
            )

    # Match rate: fraction of AI narrators matched by canonical_id or name
    matched = 0
    for pos, ai_n in ai_by_pos.items():
        p2_n = p2_by_pos.get(pos)
        if p2_n is None:
            continue
        ai_cid = ai_n.get("canonical_id")
        p2_cid = p2_n.get("canonical_id")
        ai_name = (ai_n.get("name_en") or "").lower().strip()
        p2_name = (p2_n.get("name_en") or "").lower().strip()
        if (ai_cid is not None and p2_cid is not None and ai_cid == p2_cid) or \
           (ai_name and p2_name and (ai_name in p2_name or p2_name in ai_name)):
            matched += 1

    match_rate = matched / total_comparable if total_comparable > 0 else 1.0

    return {
        "has_chain": ai_isnad.get("has_chain", False),
        "ai_count": len(ai_narrators),
        "p2_count": len(p2_narrators),
        "canonical_id_matches": canonical_id_matches,
        "name_en_overlaps": name_en_overlaps,
        "match_rate": match_rate,
        "details": "\n".join(details_lines) if details_lines else "",
    }


def compare_quran_refs(
    phase2_refs: List[dict], ai_refs: List[dict], verbose: bool = False
) -> Dict[str, Any]:
    """Compare Quran reference recall (explicit refs)."""
    ai_explicit = {r["ref"] for r in ai_refs if r.get("relationship") == "explicit"}
    p2_explicit = {r["ref"] for r in phase2_refs if r.get("relationship") == "explicit"}
    p2_all_refs = {r["ref"] for r in phase2_refs}

    if not ai_explicit:
        return {
            "ai_explicit_count": 0,
            "p2_explicit_count": len(p2_explicit),
            "recall": 1.0,  # Nothing to recall = pass
            "details": "No explicit refs in AI response",
        }

    recalled = ai_explicit & p2_all_refs
    recall = len(recalled) / len(ai_explicit) if ai_explicit else 1.0

    details = ""
    if verbose:
        missed = ai_explicit - p2_all_refs
        details = f"  AI explicit: {ai_explicit} | P2 found: {recalled} | Missed: {missed}"

    return {
        "ai_explicit_count": len(ai_explicit),
        "p2_explicit_count": len(p2_explicit),
        "recall": recall,
        "details": details,
    }


def compare_topics_tags(
    phase2_topics: List[str],
    phase2_tags: List[str],
    phase2_ct: str,
    ai_topics: List[str],
    ai_tags: List[str],
    ai_ct: str,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Compare topics, tags, and content_type overlap."""
    # Topics overlap
    ai_topics_set = set(ai_topics) if ai_topics else set()
    p2_topics_set = set(phase2_topics) if phase2_topics else set()

    if ai_topics_set:
        topic_overlap = len(ai_topics_set & p2_topics_set) / len(ai_topics_set)
    else:
        topic_overlap = 1.0 if not p2_topics_set else 0.0

    # Tags overlap
    ai_tags_set = set(ai_tags) if ai_tags else set()
    p2_tags_set = set(phase2_tags) if phase2_tags else set()

    if ai_tags_set:
        tag_overlap = len(ai_tags_set & p2_tags_set) / len(ai_tags_set)
    else:
        tag_overlap = 1.0 if not p2_tags_set else 0.0

    # Content type match
    ct_match = (phase2_ct == ai_ct) if ai_ct else True

    # Combined overlap (average of topics and tags)
    combined_overlap = (topic_overlap + tag_overlap) / 2.0

    details = ""
    if verbose:
        details = (
            f"  Topics: AI={ai_topics} P2={phase2_topics} overlap={topic_overlap:.0%}\n"
            f"  Tags: AI={ai_tags} P2={phase2_tags} overlap={tag_overlap:.0%}\n"
            f"  Content type: AI={ai_ct} P2={phase2_ct} match={ct_match}"
        )

    return {
        "topic_overlap": topic_overlap,
        "tag_overlap": tag_overlap,
        "combined_overlap": combined_overlap,
        "content_type_match": ct_match,
        "details": details,
    }


def compare_key_terms(
    phase2_translations: dict,
    ai_translations: dict,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Compare key_terms overlap (Arabic terms) across languages."""
    # Extract Arabic key terms from AI translations.en.key_terms
    ai_en = ai_translations.get("en", {})
    ai_key_terms = set()
    if isinstance(ai_en, dict) and "key_terms" in ai_en:
        ai_kt = ai_en["key_terms"]
        if isinstance(ai_kt, dict):
            ai_key_terms = set(ai_kt.keys())

    # Extract Arabic key terms from Phase 2 translations.en.key_terms
    p2_en = phase2_translations.get("en", {})
    p2_key_terms = set()
    if isinstance(p2_en, dict) and "key_terms" in p2_en:
        p2_kt = p2_en["key_terms"]
        if isinstance(p2_kt, dict):
            p2_key_terms = set(p2_kt.keys())

    if not ai_key_terms:
        return {
            "ai_count": 0,
            "p2_count": len(p2_key_terms),
            "overlap": 1.0,
            "details": "No key_terms in AI response",
        }

    # Overlap based on Arabic term presence
    matched = ai_key_terms & p2_key_terms
    overlap = len(matched) / len(ai_key_terms) if ai_key_terms else 1.0

    details = ""
    if verbose:
        missed = ai_key_terms - p2_key_terms
        details = (
            f"  AI terms: {ai_key_terms}\n"
            f"  P2 terms: {p2_key_terms}\n"
            f"  Matched: {matched} | Missed: {missed}"
        )

    return {
        "ai_count": len(ai_key_terms),
        "p2_count": len(p2_key_terms),
        "overlap": overlap,
        "details": details,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_benchmark(verbose: bool = False) -> bool:
    """Run the Phase 2 benchmark. Returns True if all criteria pass."""
    print("=" * 72)
    print("Phase 2 Programmatic Enrichment Benchmark")
    print("=" * 72)
    print()

    # Load resources
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

    topic_taxonomy = load_topic_taxonomy()
    print(f"  Topic taxonomy: {'loaded' if topic_taxonomy else 'NOT FOUND'}")

    print()

    # Load benchmark files
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
        return False

    # Per-verse results
    narrator_results = []
    quran_ref_results = []
    topics_tags_results = []
    key_terms_results = []

    print("-" * 72)
    print("Per-verse results:")
    print("-" * 72)

    for fname, data in benchmarks:
        verse_path = data.get("verse_path", fname)
        ai_result = data.get("result", {})

        print(f"\n  {verse_path}")

        # Simulate Phase 1 output
        phase1 = simulate_phase1(ai_result)

        # Build mock request
        request = build_request(verse_path, ai_result)

        # Run Phase 2 enrichment
        phase2_result = programmatic_enrich(
            phase1_result=phase1,
            request=request,
            narrator_templates=narrator_templates,
            registry=registry,
            word_dict=word_dict,
            phrases_dict=phrases_dict,
            taxonomy=taxonomy,
        )

        # --- Compare narrators ---
        nr = compare_narrators(
            phase2_result.get("isnad_matn", {}),
            ai_result.get("isnad_matn", {}),
            verbose=verbose,
        )
        narrator_results.append(nr)
        chain_label = "CHAIN" if nr["has_chain"] else "no chain"
        print(f"    Narrators [{chain_label}]: AI={nr['ai_count']} P2={nr['p2_count']} "
              f"cid_match={nr['canonical_id_matches']} name_match={nr['name_en_overlaps']} "
              f"rate={nr['match_rate']:.0%}")
        if verbose and nr["details"]:
            print(nr["details"])

        # --- Compare Quran refs ---
        qr = compare_quran_refs(
            phase2_result.get("related_quran", []),
            ai_result.get("related_quran", []),
            verbose=verbose,
        )
        quran_ref_results.append(qr)
        print(f"    Quran refs: AI_explicit={qr['ai_explicit_count']} "
              f"P2_explicit={qr['p2_explicit_count']} recall={qr['recall']:.0%}")
        if verbose and qr["details"]:
            print(qr["details"])

        # --- Compare topics/tags/content_type ---
        tt = compare_topics_tags(
            phase2_result.get("topics", []),
            phase2_result.get("tags", []),
            phase2_result.get("content_type", ""),
            ai_result.get("topics", []),
            ai_result.get("tags", []),
            ai_result.get("content_type", ""),
            verbose=verbose,
        )
        topics_tags_results.append(tt)
        print(f"    Topics/tags: topic_overlap={tt['topic_overlap']:.0%} "
              f"tag_overlap={tt['tag_overlap']:.0%} "
              f"ct_match={tt['content_type_match']}")
        if verbose and tt["details"]:
            print(tt["details"])

        # --- Compare key terms ---
        kt = compare_key_terms(
            phase2_result.get("translations", {}),
            ai_result.get("translations", {}),
            verbose=verbose,
        )
        key_terms_results.append(kt)
        print(f"    Key terms: AI={kt['ai_count']} P2={kt['p2_count']} "
              f"overlap={kt['overlap']:.0%}")
        if verbose and kt["details"]:
            print(kt["details"])

    # ---------------------------------------------------------------------------
    # Aggregate statistics
    # ---------------------------------------------------------------------------

    print()
    print("=" * 72)
    print("Aggregate Statistics")
    print("=" * 72)

    # Narrators (only verses with chains)
    chain_results = [r for r in narrator_results if r["has_chain"]]
    if chain_results:
        avg_narrator_rate = sum(r["match_rate"] for r in chain_results) / len(chain_results)
    else:
        avg_narrator_rate = 1.0
    narrator_pass = avg_narrator_rate >= NARRATOR_MATCH_THRESHOLD

    print(f"\n  Narrator extraction ({len(chain_results)} verses with chains):")
    print(f"    Average match rate: {avg_narrator_rate:.1%}")
    print(f"    Threshold: {NARRATOR_MATCH_THRESHOLD:.0%}")
    print(f"    Result: {'PASS' if narrator_pass else 'FAIL'}")

    # Explicit Quran refs
    refs_with_explicit = [r for r in quran_ref_results if r["ai_explicit_count"] > 0]
    if refs_with_explicit:
        avg_recall = sum(r["recall"] for r in refs_with_explicit) / len(refs_with_explicit)
    else:
        avg_recall = 1.0
    quran_pass = avg_recall >= EXPLICIT_QURAN_RECALL_THRESHOLD

    print(f"\n  Explicit Quran refs ({len(refs_with_explicit)} verses with explicit refs):")
    print(f"    Average recall: {avg_recall:.1%}")
    print(f"    Threshold: {EXPLICIT_QURAN_RECALL_THRESHOLD:.0%}")
    print(f"    Result: {'PASS' if quran_pass else 'FAIL'}")

    # Topics/tags
    avg_combined = sum(r["combined_overlap"] for r in topics_tags_results) / len(topics_tags_results)
    ct_matches = sum(1 for r in topics_tags_results if r["content_type_match"])
    topics_pass = avg_combined >= TOPICS_TAGS_OVERLAP_THRESHOLD

    print(f"\n  Topics/tags ({len(topics_tags_results)} verses):")
    print(f"    Average combined overlap: {avg_combined:.1%}")
    print(f"    Content type matches: {ct_matches}/{len(topics_tags_results)}")
    print(f"    Threshold: {TOPICS_TAGS_OVERLAP_THRESHOLD:.0%}")
    print(f"    Result: {'PASS' if topics_pass else 'FAIL'}")

    # Key terms
    terms_with_ai = [r for r in key_terms_results if r["ai_count"] > 0]
    if terms_with_ai:
        avg_terms_overlap = sum(r["overlap"] for r in terms_with_ai) / len(terms_with_ai)
    else:
        avg_terms_overlap = 1.0
    terms_pass = avg_terms_overlap >= KEY_TERMS_OVERLAP_THRESHOLD

    print(f"\n  Key terms ({len(terms_with_ai)} verses with AI key_terms):")
    print(f"    Average Arabic term overlap: {avg_terms_overlap:.1%}")
    print(f"    Threshold: {KEY_TERMS_OVERLAP_THRESHOLD:.0%}")
    print(f"    Result: {'PASS' if terms_pass else 'FAIL'}")

    # Overall
    all_pass = narrator_pass and quran_pass and topics_pass and terms_pass

    print()
    print("=" * 72)
    criteria = [
        ("Narrator extraction (80%+ on chain verses)", narrator_pass),
        ("Explicit Quran refs (90%+ recall)", quran_pass),
        ("Topics/tags (60%+ overlap)", topics_pass),
        ("Key terms (70%+ Arabic overlap)", terms_pass),
    ]
    print("ACCEPTANCE CRITERIA SUMMARY:")
    for label, passed in criteria:
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {label}")

    print()
    if all_pass:
        print("OVERALL: ALL CRITERIA PASSED")
    else:
        failed = [label for label, passed in criteria if not passed]
        print(f"OVERALL: FAILED ({len(failed)} criteria)")
        for label in failed:
            print(f"  - {label}")
    print("=" * 72)

    return all_pass


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Phase 2 programmatic enrichment against AI-generated responses."
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Show detailed per-narrator, per-ref comparisons",
    )
    args = parser.parse_args()

    passed = run_benchmark(verbose=args.verbose)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
