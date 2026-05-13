"""Pick a stratified sample of verses for the Phase 4 open-weight benchmark.

Reads existing phased pipeline responses, extracts structural features
(chunk count, has_chain, content_type, AR word count, baseline P4 model),
and writes a sample.json with 30 verse_ids — split evenly between the two
baseline Phase 4 models (gpt-4.1-mini and gpt-5.4-mini) and stratified by
verse shape within each group.

Run with the venv active:
    python scripts/benchmark_phase4_sample.py
"""
from __future__ import annotations

import json
import os
import random
import re
from collections import defaultdict
from pathlib import Path

SOURCE_DATA_DIR = Path(os.environ.get(
    "SOURCE_DATA_DIR",
    Path(__file__).resolve().parents[2] / "ThaqalaynDataSources",
))
RESPONSES_DIR = SOURCE_DATA_DIR / "ai-content" / "corpus" / "responses"

BENCHMARK_DIR = Path(__file__).resolve().parents[1] / "benchmark" / "phase4_qwen"
SAMPLE_PATH = BENCHMARK_DIR / "sample.json"

TARGET_MODELS = {
    "phased_gpt-5.4+gpt-4.1-mini": "gpt-4.1-mini",
    "phased_gpt-5.4+gpt-5.4-mini": "gpt-5.4-mini",
}

SEED = 20260512


def ar_word_count(text: str) -> int:
    if not text:
        return 0
    return len(re.findall(r"\S+", text))


def classify_shape(record: dict) -> str:
    """Return a stratum key for this verse."""
    r = record["result"]
    chunks = r.get("chunks", []) or []
    chunk_types = [c.get("chunk_type", "") for c in chunks]
    content_type = r.get("content_type", "")
    has_chain = bool(r.get("has_chain", False))
    total_words = sum(ar_word_count(c.get("arabic_text", "")) for c in chunks)

    if "quran_quote" in chunk_types:
        return "quran_quoting"
    if content_type == "supplication" or "dua" in r.get("topics", []):
        return "dua"
    if len(chunks) >= 4:
        return "long_multi_chunk"
    if has_chain and 2 <= len(chunks) <= 3:
        return "medium_isnad_matn"
    if len(chunks) == 1 and total_words < 50:
        return "short_matn_only"
    return "other"


def main() -> None:
    if not RESPONSES_DIR.exists():
        raise SystemExit(f"responses dir not found: {RESPONSES_DIR}")

    by_model_book_stratum: dict[str, dict[str, dict[str, list[dict]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    total_seen = 0

    for fp in RESPONSES_DIR.glob("*.json"):
        # Cheap pre-filter so we don't json.load 60K files
        with fp.open("rb") as fh:
            head = fh.read(512).decode("utf-8", errors="replace")
        if '"phased_gpt-5.4+gpt-' not in head:
            continue
        try:
            record = json.loads(fp.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        attr = record.get("ai_attribution", {})
        model_str = attr.get("model", "")
        if model_str not in TARGET_MODELS:
            continue
        baseline = TARGET_MODELS[model_str]
        verse_path = record.get("verse_path", "")
        # /books/al-kafi:1:2:3 -> al-kafi
        m = re.match(r"^/books/([^:]+):", verse_path)
        if not m:
            continue
        book = m.group(1)
        stratum = classify_shape(record)
        r = record["result"]
        chunks = r.get("chunks", []) or []
        total_words = sum(ar_word_count(c.get("arabic_text", "")) for c in chunks)
        by_model_book_stratum[baseline][book][stratum].append({
            "verse_path": verse_path,
            "file": fp.name,
            "chunk_count": len(chunks),
            "has_chain": bool(r.get("has_chain", False)),
            "content_type": r.get("content_type", ""),
            "ar_word_count": total_words,
            "stratum": stratum,
            "baseline_p4": baseline,
            "book": book,
        })
        total_seen += 1

    print(f"scanned files matching target models: {total_seen}")
    for baseline, books in by_model_book_stratum.items():
        print(f"  {baseline}:")
        for book, strata in books.items():
            counts = {k: len(v) for k, v in strata.items()}
            total = sum(counts.values())
            print(f"    {book}: {total}  {counts}")

    rng = random.Random(SEED)

    # Per-baseline target: 15 verses total
    # Stratum quotas (best effort — if a stratum is short, spill into "other")
    stratum_quota = {
        "short_matn_only": 3,
        "medium_isnad_matn": 4,
        "long_multi_chunk": 4,
        "dua": 2,
        "quran_quoting": 2,
    }

    selected: list[dict] = []
    for baseline in ("gpt-4.1-mini", "gpt-5.4-mini"):
        books = by_model_book_stratum.get(baseline, {})
        # Flatten across books per stratum so we can stratify first, then
        # try to keep book diversity.
        per_stratum: dict[str, list[dict]] = defaultdict(list)
        for book, strata in books.items():
            for stratum, items in strata.items():
                per_stratum[stratum].extend(items)

        picked_for_baseline: list[dict] = []
        used_files: set[str] = set()
        for stratum, quota in stratum_quota.items():
            pool = [v for v in per_stratum.get(stratum, [])
                    if v["file"] not in used_files]
            rng.shuffle(pool)
            # Round-robin by book so we don't get all 4 from one book if avoidable
            by_book = defaultdict(list)
            for v in pool:
                by_book[v["book"]].append(v)
            books_cycle = list(by_book.keys())
            rng.shuffle(books_cycle)
            picks: list[dict] = []
            while len(picks) < quota and any(by_book[b] for b in books_cycle):
                for b in books_cycle:
                    if by_book[b] and len(picks) < quota:
                        picks.append(by_book[b].pop(0))
            for v in picks:
                used_files.add(v["file"])
            picked_for_baseline.extend(picks)

        # Fill any shortfall from "other"
        target = sum(stratum_quota.values())  # 15
        if len(picked_for_baseline) < target:
            spill = [v for v in per_stratum.get("other", [])
                    if v["file"] not in used_files]
            rng.shuffle(spill)
            need = target - len(picked_for_baseline)
            picked_for_baseline.extend(spill[:need])

        selected.extend(picked_for_baseline)

    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "sample_size": len(selected),
        "seed": SEED,
        "strata_quota_per_baseline": stratum_quota,
        "responses_dir": str(RESPONSES_DIR),
        "verses": selected,
    }
    SAMPLE_PATH.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nwrote {SAMPLE_PATH} with {len(selected)} verses")

    # Print a human-readable summary
    print("\n=== sample ===")
    for baseline in ("gpt-4.1-mini", "gpt-5.4-mini"):
        rows = [v for v in selected if v["baseline_p4"] == baseline]
        print(f"\nBaseline P4: {baseline}  ({len(rows)} verses)")
        for v in rows:
            print(f"  [{v['stratum']:<18}] {v['verse_path']:<55} "
                  f"chunks={v['chunk_count']} words={v['ar_word_count']} "
                  f"book={v['book']}")


if __name__ == "__main__":
    main()
