#!/usr/bin/env python3
"""Benchmark Phase 1 prompt: Claude Sonnet vs GPT-5.4.

Runs the same verses through both backends and compares quality + cost.

Usage:
    # Both backends (requires OPENAI_API_KEY)
    python scripts/benchmark_phase1.py --verses 5

    # Claude only
    python scripts/benchmark_phase1.py --verses 3 --backend claude

    # GPT only
    python scripts/benchmark_phase1.py --verses 3 --backend openai

    # Specific verses
    python scripts/benchmark_phase1.py --single /books/al-kafi:2:1:106:1 /books/al-kafi:2:1:106:2
"""

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "app"))

from app.ai_pipeline import (
    extract_pipeline_request,
    validate_result,
    VALID_TOPICS,
    VALID_TAGS,
    VALID_CONTENT_TYPES,
)
from app.pipeline_cli.phased_prompts import (
    build_phase1_system_prompt,
    build_phase1_user_message,
    parse_phase1_response,
)
from app.pipeline_cli.programmatic_enrichment import (
    programmatic_enrich,
    reconstruct_from_chunks,
)
from app.pipeline_cli.pipeline import call_claude, call_llm
from app.narrator_registry import NarratorRegistry
from app.config import AI_RESPONSES_DIR


def find_unprocessed_verses(n: int, book: str = "al-kafi", volume: int = 2) -> List[str]:
    """Find verse paths that don't have responses yet."""
    from app.pipeline_cli.pipeline import load_corpus_manifest
    manifest = load_corpus_manifest()
    responses_dir = AI_RESPONSES_DIR

    queue = []
    for vp in manifest:
        if not vp.startswith(f"/books/{book}:{volume}:"):
            continue
        vid = vp.replace("/books/", "").replace(":", "_")
        resp_path = os.path.join(responses_dir, f"{vid}.json")
        if not os.path.exists(resp_path):
            queue.append(vp)
        if len(queue) >= n * 3:  # grab extra in case some fail to load
            break
    return queue[:n]


async def run_phase1(verse_path: str, backend: str, model: str,
                     system_prompt: str, registry: NarratorRegistry) -> dict:
    """Run Phase 1 on a single verse and return results."""
    from app.pipeline_cli.verse_processor import strip_code_fences, repair_json_quotes

    request = extract_pipeline_request(verse_path)
    if not request:
        return {"error": f"Could not load verse: {verse_path}"}

    user_message = build_phase1_user_message(request)

    start = time.time()
    cr = await call_llm(system_prompt, user_message, model=model, backend=backend)
    elapsed = time.time() - start

    if "error" in cr:
        return {"error": cr["error"], "elapsed": elapsed, "cost": cr.get("cost", 0)}

    # Parse response
    raw = cr.get("result", "").strip()
    try:
        cleaned = strip_code_fences(raw)
        cleaned = repair_json_quotes(cleaned)
        phase1_dict = json.loads(cleaned)
    except (json.JSONDecodeError, ValueError) as e:
        return {"error": f"JSON parse: {e}", "elapsed": elapsed, "cost": cr.get("cost", 0),
                "raw": raw[:500]}

    phase1 = parse_phase1_response(phase1_dict)

    # Run Phase 2 enrichment
    enriched = programmatic_enrich(
        phase1_result=phase1,
        request=request,
        registry=registry,
    )

    # Validate
    errors = validate_result(enriched)

    # Quality metrics
    topics = enriched.get("topics", [])
    invalid_topics = [t for t in topics if t not in VALID_TOPICS]
    tags = enriched.get("tags", [])
    invalid_tags = [t for t in tags if t not in VALID_TAGS]
    ct = enriched.get("content_type", "")
    chunks = enriched.get("chunks", [])
    key_terms = enriched.get("translations", {}).get("en", {}).get("key_terms", {})
    summary = enriched.get("translations", {}).get("en", {}).get("summary", "")
    quran_refs = enriched.get("related_quran", [])
    narrators = enriched.get("isnad_matn", {}).get("narrators", [])
    has_chain = enriched.get("isnad_matn", {}).get("has_chain", False)

    # Word count from chunks
    total_words = sum(len(c.get("arabic_text", "").split()) for c in chunks)

    return {
        "verse_path": verse_path,
        "backend": backend,
        "model": cr.get("model", model),
        "cost": cr.get("cost", 0),
        "output_tokens": cr.get("output_tokens", 0),
        "input_tokens": cr.get("input_tokens", 0),
        "cache_creation_tokens": cr.get("cache_creation_tokens", 0),
        "cache_read_tokens": cr.get("cache_read_tokens", 0),
        "elapsed": round(elapsed, 1),
        "valid": len(errors) == 0,
        "validation_errors": errors[:5],
        "word_count": total_words,
        "chunks": len(chunks),
        "topics": topics,
        "invalid_topics": invalid_topics,
        "tags": tags,
        "invalid_tags": invalid_tags,
        "content_type": ct,
        "key_terms_count": len(key_terms),
        "summary_len": len(summary),
        "quran_refs": len(quran_refs),
        "narrators": len(narrators),
        "has_chain": has_chain,
        "summary": summary[:200],
        "key_terms_sample": dict(list(key_terms.items())[:3]),
        # Full results for manual inspection
        "full_result": enriched,
    }



def print_summary(all_results: dict, verse_count: int):
    """Print aggregate summary for all models."""
    print(f"\n{'='*70}")
    print(f"  SUMMARY ({verse_count} verses)")
    print(f"{'='*70}")

    for label, results in all_results.items():
        if not results:
            continue
        valid = sum(1 for r in results if r.get("valid"))
        api_errors = sum(1 for r in results if "error" in r)
        invalid_topics_count = sum(len(r.get("invalid_topics", [])) for r in results)
        total_cost = sum(r.get("cost", 0) for r in results)
        avg_cost = total_cost / len(results)
        avg_time = sum(r.get("elapsed", 0) for r in results) / len(results)
        avg_terms = sum(r.get("key_terms_count", 0) for r in results) / len(results)
        avg_refs = sum(r.get("quran_refs", 0) for r in results) / len(results)
        avg_summary_len = sum(r.get("summary_len", 0) for r in results) / len(results)

        print(f"\n  {label}:")
        print(f"    Pass: {valid}/{len(results)} | API Errors: {api_errors}")
        print(f"    Cost: ${total_cost:.4f} total, ${avg_cost:.4f}/verse")
        print(f"    Time: {avg_time:.1f}s avg/verse")
        print(f"    Invalid topics: {invalid_topics_count}")
        print(f"    Avg key terms: {avg_terms:.1f} | Avg Quran refs: {avg_refs:.1f}")
        print(f"    Avg summary length: {avg_summary_len:.0f} chars")
        print(f"    Projected 58K: ${avg_cost * 58000:.0f}")


async def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Phase 1 across multiple models",
        epilog="Default: tests Claude Sonnet, GPT-5.4, and GPT-5.4-mini on 5 verses",
    )
    parser.add_argument("--verses", type=int, default=5, help="Number of verses to test")
    parser.add_argument("--models", nargs="+", default=None,
                        help="Models to test as backend:model pairs (e.g., claude:sonnet openai:gpt-5.4 openai:gpt-5.4-mini)")
    parser.add_argument("--single", nargs="+", help="Specific verse paths")
    parser.add_argument("--book", default="al-kafi")
    parser.add_argument("--volume", type=int, default=2)
    args = parser.parse_args()

    os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")

    # Default model set: all three
    if args.models:
        models = []
        for m in args.models:
            if ":" in m:
                backend, model = m.split(":", 1)
            else:
                # Assume openai for gpt-*, claude for others
                backend = "openai" if m.startswith("gpt-") else "claude"
                model = m
            models.append((backend, model))
    else:
        models = [
            ("claude", "sonnet"),
            ("openai", "gpt-5.4"),
            ("openai", "gpt-5.4-mini"),
        ]

    # Build system prompt once
    system_prompt = build_phase1_system_prompt()
    registry = NarratorRegistry()

    # Get verse paths
    if args.single:
        verse_paths = args.single
    else:
        verse_paths = find_unprocessed_verses(args.verses, args.book, args.volume)

    if not verse_paths:
        print("No unprocessed verses found. Use --single to specify verses.")
        return

    model_labels = [f"{b}:{m}" for b, m in models]
    print(f"Benchmarking Phase 1: {len(verse_paths)} verses x {len(models)} models")
    for b, m in models:
        print(f"  - {b}:{m}")
    print()

    # Results keyed by model label
    all_results = {label: [] for label in model_labels}

    for vp in verse_paths:
        vid = vp.replace("/books/", "").replace(":", "_")
        print(f"Processing {vid}...")

        verse_results = {}
        for backend, model in models:
            label = f"{backend}:{model}"
            print(f"  {label}...", end="", flush=True)
            r = await run_phase1(vp, backend, model, system_prompt, registry)
            cost = r.get("cost", 0)
            if "error" in r:
                print(f" FAIL ${cost:.4f} — {r['error'][:60]}")
            else:
                status = "PASS" if r.get("valid") else "ERR"
                print(f" {status} ${cost:.4f} {r.get('elapsed', 0)}s")
            all_results[label].append(r)
            verse_results[label] = r

        # Per-verse comparison
        print(f"\n  {'':30s}", end="")
        for label in model_labels:
            print(f" {label:>20s}", end="")
        print()

        for field, fmt in [
            ("cost", "${:.4f}"),
            ("elapsed", "{:.0f}s"),
            ("word_count", "{}"),
            ("chunks", "{}"),
            ("key_terms_count", "{}"),
            ("quran_refs", "{}"),
            ("summary_len", "{} chars"),
            ("content_type", "{}"),
        ]:
            print(f"  {field:30s}", end="")
            for label in model_labels:
                r = verse_results.get(label, {})
                val = r.get(field, "—")
                if "error" in r:
                    print(f" {'—':>20s}", end="")
                else:
                    print(f" {fmt.format(val):>20s}", end="")
            print()

        # Topics row (special — show the actual list)
        print(f"  {'topics':30s}", end="")
        for label in model_labels:
            r = verse_results.get(label, {})
            topics = r.get("topics", [])
            t_str = ",".join(t[:12] for t in topics[:3])
            if len(topics) > 3:
                t_str += f"+{len(topics)-3}"
            print(f" {t_str:>20s}", end="")
        print()
        print()

    # Aggregate summary
    print_summary(all_results, len(verse_paths))

    # Save results
    out_dir = PROJECT_ROOT / "benchmarks" / "phase1"
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    def strip_full(r):
        return {k: v for k, v in r.items() if k != "full_result"}

    summary_path = out_dir / f"benchmark_{timestamp}.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": timestamp,
            "verses": len(verse_paths),
            "verse_paths": verse_paths,
            "models": model_labels,
            "results": {label: [strip_full(r) for r in results]
                        for label, results in all_results.items()},
        }, f, ensure_ascii=False, indent=2)
    print(f"\n  Summary saved: {summary_path}")

    # Save per-verse full results for manual inspection
    verses_dir = out_dir / f"responses_{timestamp}"
    verses_dir.mkdir(exist_ok=True)
    for label, results in all_results.items():
        safe_label = label.replace(":", "_").replace(".", "_")
        for r in results:
            if "full_result" not in r:
                continue
            vid = r["verse_path"].replace("/books/", "").replace(":", "_")
            vpath = verses_dir / f"{vid}__{safe_label}.json"
            with open(vpath, "w", encoding="utf-8") as f:
                json.dump(r["full_result"], f, ensure_ascii=False, indent=2)
    print(f"  Full responses: {verses_dir}/")


if __name__ == "__main__":
    asyncio.run(main())
