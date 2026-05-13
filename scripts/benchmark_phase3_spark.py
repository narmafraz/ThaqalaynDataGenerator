"""Phase 3 Spark scholarly enrichment benchmark.

Takes verses that already have Phase 1 + Phase 4 output (from spark_e2e),
runs `enrich_scholarly()` via Spark on each, and dumps a side-by-side
comparison of:
  - the original Phase 1 EN summary
  - the Phase 3-enriched summary
  - related_quran refs before/after

So I can read the pairs and judge whether Qwen's Phase 3 adds real
scholarly value vs just paraphrasing.
"""
from __future__ import annotations

import asyncio
import copy
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "app"))

os.environ.setdefault("SOURCE_DATA_DIR", str(REPO_ROOT.parent / "ThaqalaynDataSources"))

from app.pipeline_cli.scholarly_phase import enrich_scholarly  # noqa: E402
from app.ai_pipeline import extract_pipeline_request  # noqa: E402

SOURCE_DATA_DIR = Path(os.environ["SOURCE_DATA_DIR"])
E2E_RESPONSES = SOURCE_DATA_DIR / "ai-content" / "spark_e2e" / "responses"
BENCH_DIR = REPO_ROOT / "benchmark" / "phase3_spark"
BENCH_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR = REPO_ROOT.parent / "ThaqalaynData"

MODEL = "qwen36-fast"
N_VERSES = 10

# Pick 10 from spark_e2e — varied in length / chunk types
PICKS = [
    "al-tawhid_1_1_1",     # book opening
    "al-tawhid_2_1_1",     # short isnad+matn
    "al-tawhid_2_1_2",
    "al-tawhid_2_1_5",
    "al-tawhid_2_1_8",
    "al-tawhid_2_1_11",    # medium
    "al-tawhid_2_1_13",
    "al-tawhid_2_1_15",
    "al-tawhid_2_1_16",
    "al-tawhid_2_1_17",
]


async def run_one(verse_id: str) -> dict:
    src_path = E2E_RESPONSES / f"{verse_id}.json"
    if not src_path.exists():
        return {"verse_id": verse_id, "error": "source not in spark_e2e"}

    src = json.loads(src_path.read_text(encoding="utf-8"))
    verse_path = src["verse_path"]
    original_result = copy.deepcopy(src["result"])

    # Reload AR text + book/chapter from ThaqalaynData
    request = extract_pipeline_request(verse_path, data_dir=str(DATA_DIR))
    if request is None:
        return {"verse_id": verse_id, "error": "could not load verse from data dir"}

    p1_summary = (
        original_result.get("translations", {}).get("en", {}).get("summary", "")
    )
    p1_refs = list(original_result.get("related_quran", []))

    # Deep copy for P3 run so the original stays clean
    enriched = copy.deepcopy(original_result)
    enriched = await enrich_scholarly(
        enriched,
        arabic_text=request.arabic_text,
        book_name=request.book_name,
        chapter_title=request.chapter_title,
        backend="spark",
        model=MODEL,
        verse_id=verse_id,
    )

    p3_summary = enriched.get("translations", {}).get("en", {}).get("summary", "")
    p3_refs = enriched.get("related_quran", [])

    return {
        "verse_id": verse_id,
        "verse_path": verse_path,
        "ar_text": request.arabic_text,
        "en_translation": request.english_text[:600],
        "p1_summary": p1_summary,
        "p3_summary": p3_summary,
        "p1_refs": p1_refs,
        "p3_refs": p3_refs,
        "p3_added_refs": [r for r in p3_refs if r not in p1_refs],
        "p3_cost": enriched.get("_phase3_cost"),
        "p3_tokens": enriched.get("_phase3_tokens"),
    }


async def main_async() -> None:
    results = []
    for verse_id in PICKS[:N_VERSES]:
        print(f"Processing {verse_id}...")
        r = await run_one(verse_id)
        results.append(r)
        if "error" in r:
            print(f"  ERROR: {r['error']}")

    (BENCH_DIR / "results.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Markdown pairs file for manual reading
    lines = ["# Phase 3 Spark Benchmark — Side-by-side\n"]
    for r in results:
        if "error" in r:
            lines.append(f"\n---\n\n## `{r['verse_id']}` — ERROR: {r['error']}")
            continue
        lines.append(f"\n---\n\n## `{r['verse_path']}`")
        lines.append(f"\n**AR**: {r['ar_text'][:300]}...")
        lines.append(f"\n**EN reference (from ThaqalaynData)**: {r['en_translation'][:300]}...")
        lines.append(f"\n### Phase 1 summary (baseline)")
        lines.append(r['p1_summary'])
        lines.append(f"\n### Phase 3 ENRICHED summary (Spark/Qwen)")
        lines.append(r['p3_summary'])
        lines.append(f"\n### related_quran")
        lines.append(f"- Before: {[ref['ref'] for ref in r['p1_refs']]}")
        lines.append(f"- After:  {[ref['ref'] for ref in r['p3_refs']]}")
        lines.append(f"- Added by P3: {[ref['ref'] for ref in r['p3_added_refs']]}")
    (BENCH_DIR / "pairs.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {BENCH_DIR / 'results.json'} and pairs.md")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
