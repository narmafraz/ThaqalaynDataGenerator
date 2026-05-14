"""Async runner for the Path B Spark translation passes (lemmas + surfaces).

Reads the JSONL prompt file produced by
`extract_{lemma,surface}_translation_prompts.py`, calls Spark Qwen36
with bounded concurrency, persists each response to a per-slug JSON
file under `ThaqalaynWordSources/translation/{lemma,surface}_responses/`,
and prints summary stats (parse rate, latency, issue counts).

The runner is **resumable**: on each call it skips slugs whose response
file already exists unless `--force` is passed. Combined with the
pre-cached prompt JSONL, this means partial runs (Ctrl-C, machine sleep,
Spark hiccup) can be picked up trivially.

Round labelling (for experiment A/B): pass `--round R` to write outputs
to `lemma_responses/round-R/{slug}.json` instead of the top-level dir.
The merger only reads the top-level dir; rounds are scratch space.

Usage:

    # Pilot — Round 1 lemma baseline (100 lemmas)
    python scripts/run_path_b_translations.py --pass lemma \\
        --prompts ../ThaqalaynWordSources/translation/lemma_prompts.jsonl \\
        --pilot-set ../ThaqalaynWordSources/translation/pilot_set.json \\
        --round 1

    # Full lemma pass (13K lemmas)
    python scripts/run_path_b_translations.py --pass lemma --workers 8

    # Full surface pass (102K surfaces)
    python scripts/run_path_b_translations.py --pass surface --workers 8
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Iterable, List, Optional

logger = logging.getLogger("run_path_b_translations")


def load_prompts(path: Path) -> List[dict]:
    items: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            items.append(json.loads(line))
    return items


def filter_items(
    items: List[dict],
    *,
    pilot_set: Optional[dict] = None,
    which_pass: str = "lemma",
    limit: Optional[int] = None,
) -> List[dict]:
    if pilot_set is not None:
        if which_pass == "lemma":
            allowed = set(pilot_set.get("lemmas") or [])
        else:
            allowed = set(pilot_set.get("surfaces") or [])
        items = [it for it in items if it.get("slug") in allowed]
    if limit is not None:
        items = items[:limit]
    return items


def existing_response_slugs(out_dir: Path) -> set:
    if not out_dir.is_dir():
        return set()
    return {p.stem for p in out_dir.glob("*.json")}


def make_progress_cb(total: int):
    last_print = [time.monotonic()]
    started_at = time.monotonic()

    def cb(done: int, _total: int, _result: dict) -> None:
        now = time.monotonic()
        if now - last_print[0] >= 5.0 or done == total:
            elapsed = now - started_at
            rate = done / max(0.001, elapsed)
            eta = (total - done) / max(0.001, rate)
            logger.info(
                "  %d/%d done (%.1f/s, %.0f s elapsed, ~%.0f s remaining)",
                done, total, rate, elapsed, eta,
            )
            last_print[0] = now

    return cb


def persist_result(result: dict, out_dir: Path) -> None:
    """Write one response JSON, replacing the slug filename's reserved chars."""
    slug = result.get("slug") or "_unknown"
    # Slug is Arabic NFC by design but may contain characters that
    # interact badly with filesystem (e.g. "/" or "\0"). The existing
    # ThaqalaynWords lemmas/ uses raw Arabic slugs successfully, so we
    # just sanitize the bare minimum.
    safe = slug.replace("/", "_").replace("\\", "_").replace("\0", "_")
    out_path = out_dir / f"{safe}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)


def summarise(results: Iterable[dict]) -> None:
    total = 0
    parse_ok = 0
    issue_counts: Counter[str] = Counter()
    latencies: List[float] = []
    input_tok = 0
    output_tok = 0
    for r in results:
        total += 1
        meta = r.get("meta") or {}
        latencies.append(meta.get("elapsed", 0.0))
        input_tok += meta.get("input_tokens", 0) or 0
        output_tok += meta.get("output_tokens", 0) or 0
        if r.get("parsed"):
            parse_ok += 1
        for iss in r.get("issues") or []:
            kind = iss.split(":")[0].strip()
            issue_counts[kind] += 1
    if total == 0:
        logger.warning("no results to summarise")
        return
    avg_lat = sum(latencies) / len(latencies)
    logger.info("─" * 60)
    logger.info("summary: %d items", total)
    logger.info("  parsed cleanly: %d (%.1f%%)", parse_ok, 100 * parse_ok / total)
    logger.info("  avg latency:    %.2f s", avg_lat)
    logger.info("  total tokens:   %d in / %d out", input_tok, output_tok)
    if issue_counts:
        logger.info("  issue counts:")
        for kind, n in issue_counts.most_common():
            logger.info("    %s: %d", kind, n)


async def run_async(args: argparse.Namespace) -> int:
    from app.words.spark_translation import (
        run_lemma_batch, run_surface_batch,
    )

    prompts = load_prompts(args.prompts)
    logger.info("loaded %d %s prompts from %s",
                len(prompts), args.pass_, args.prompts)

    pilot = None
    if args.pilot_set is not None:
        with open(args.pilot_set, "r", encoding="utf-8") as f:
            pilot = json.load(f)
        logger.info(
            "pilot filter: %d lemmas / %d surfaces in pilot_set",
            len(pilot.get("lemmas") or []), len(pilot.get("surfaces") or []),
        )

    items = filter_items(
        prompts, pilot_set=pilot, which_pass=args.pass_, limit=args.limit,
    )
    logger.info("after filtering: %d items to translate", len(items))

    if args.round is not None:
        out_dir = (args.word_sources_dir / "translation" /
                   f"{args.pass_}_responses" / f"round-{args.round}")
    else:
        out_dir = (args.word_sources_dir / "translation" /
                   f"{args.pass_}_responses")
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("writing responses to %s", out_dir)

    existing = set() if args.force else existing_response_slugs(out_dir)
    if existing:
        logger.info("resume: skipping %d already-completed slugs",
                    len(existing))
        items = [it for it in items if it.get("slug") not in existing]
        logger.info("after resume filter: %d items remain", len(items))

    if not items:
        logger.info("nothing to do — exiting")
        return 0

    cb = make_progress_cb(len(items))
    if args.pass_ == "lemma":
        results = await run_lemma_batch(
            items, model=args.model, workers=args.workers, progress_cb=cb,
        )
    else:
        results = await run_surface_batch(
            items, model=args.model, workers=args.workers, progress_cb=cb,
        )

    # Persist each one. Sequential write — async I/O contention buys
    # nothing here since the calls were the bottleneck.
    for r in results:
        if r is None:
            continue
        persist_result(r, out_dir)
    logger.info("persisted %d response files", sum(1 for r in results if r))

    summarise(r for r in results if r)
    return 0


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pass", dest="pass_", required=True, choices=["lemma", "surface"],
    )
    parser.add_argument("--prompts", type=Path, default=None,
                        help="Path to prompts JSONL (default: derived from --pass)")
    parser.add_argument(
        "--word-sources-dir", type=Path,
        default=Path("../ThaqalaynWordSources"),
    )
    parser.add_argument("--pilot-set", type=Path, default=None)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--model", default="qwen36-fast")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap items for debugging")
    parser.add_argument(
        "--round", type=int, default=None,
        help="Optional round label — outputs go to round-N/ subdir (scratch)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-translate items whose response file already exists",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.prompts is None:
        args.prompts = (args.word_sources_dir / "translation" /
                        f"{args.pass_}_prompts.jsonl")

    if not args.prompts.exists():
        logger.error("prompts file not found: %s — run extractor first",
                     args.prompts)
        return 2

    return asyncio.run(run_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
