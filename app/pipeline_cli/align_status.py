"""Progress/status for the scraped-translation alignment pass.

Runnable from this machine while an `align-scraped` run grinds — either
locally (counts artifacts on disk against the eligible corpus) or against a
headless Spark-hosted run via ssh (mirrors the Hawramani health-checks).

Usage:
    python -m app.pipeline_cli.align_status [--book B] [--langs en] [--data-dir ../ThaqalaynData/]
    python -m app.pipeline_cli.align_status --spark [--spark-remote-dir DIR] [--spark-log FILE]

Local mode walks the corpus manifest, so scope with --book for a quick check;
a full-corpus tally loads every eligible verse file and takes a bit.
"""

import argparse
import json
import os
import subprocess
from collections import defaultdict
from typing import Optional

from app.config import AI_ALIGNMENT_DIR, DEFAULT_DESTINATION_DIR
from app.pipeline_cli.chunk_alignment_phase import (
    eligible_scraped_ids,
    get_chunks,
    is_eligible,
    load_built_verse,
    load_response_result,
    prepared_chunks,
)
from app.pipeline_cli.verse_processor import verse_path_to_id

SPARK_HOST = os.environ.get("SPARK_SSH", "pino@192.168.0.66")
SPARK_KEY = os.path.expanduser("~/.ssh/spark_key")


def _book_of(verse_path: str) -> str:
    return verse_path.replace("/books/", "").split(":")[0]


def local_status(book: Optional[str], langs: Optional[list], data_dir: str,
                 alignment_dir: str) -> None:
    from app.pipeline_cli.pipeline import load_corpus_manifest

    verse_paths = load_corpus_manifest()
    books = [b.strip() for b in book.split(",")] if book else []
    if books:
        verse_paths = [vp for vp in verse_paths
                       if any(vp.startswith(f"/books/{b}:") for b in books)]
    lang_set = set(langs) if langs else None

    # Per-book tallies: eligible verses, done (artifact present), quarantined ids.
    per_book = defaultdict(lambda: {"eligible": 0, "done": 0, "aligned_ids": 0,
                                    "quarantined_ids": 0})
    for vp in verse_paths:
        bk = _book_of(vp)
        verse = load_built_verse(vp, data_dir)
        if verse is None or len(get_chunks(verse)) < 2:
            continue
        chunks = prepared_chunks(verse, load_response_result(verse_path_to_id(vp)))
        if not is_eligible(chunks):
            continue
        ids = eligible_scraped_ids(verse, lang_set)
        if not ids:
            continue
        per_book[bk]["eligible"] += 1

        art_path = os.path.join(alignment_dir, f"{verse_path_to_id(vp)}.json")
        if os.path.exists(art_path):
            per_book[bk]["done"] += 1
            try:
                with open(art_path, "r", encoding="utf-8") as f:
                    art = json.load(f)
                per_book[bk]["aligned_ids"] += len(art.get("aligned", {}))
                per_book[bk]["quarantined_ids"] += len(art.get("quarantined", {}))
            except (json.JSONDecodeError, OSError):
                pass

    if not per_book:
        print("No eligible verses found (need ≥2 chunks + a scraped translation).")
        return

    tot_elig = tot_done = tot_aln = tot_q = 0
    print(f"{'book':<28} {'done/eligible':>16} {'aligned':>9} {'quar':>6}")
    print("-" * 64)
    for bk in sorted(per_book):
        s = per_book[bk]
        pct = (100 * s["done"] / s["eligible"]) if s["eligible"] else 0
        print(f"{bk:<28} {s['done']:>6}/{s['eligible']:<6} ({pct:>3.0f}%) "
              f"{s['aligned_ids']:>9} {s['quarantined_ids']:>6}")
        tot_elig += s["eligible"]; tot_done += s["done"]
        tot_aln += s["aligned_ids"]; tot_q += s["quarantined_ids"]
    print("-" * 64)
    pct = (100 * tot_done / tot_elig) if tot_elig else 0
    print(f"{'TOTAL':<28} {tot_done:>6}/{tot_elig:<6} ({pct:>3.0f}%) "
          f"{tot_aln:>9} {tot_q:>6}")


def _ssh(remote_cmd: str) -> str:
    cmd = ["ssh", "-i", SPARK_KEY, SPARK_HOST, remote_cmd]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return (out.stdout or "") + (out.stderr or "")
    except (subprocess.SubprocessError, OSError) as e:
        return f"<ssh failed: {e}>"


def spark_status(remote_dir: str, log_file: Optional[str]) -> None:
    print(f"Spark host: {SPARK_HOST}")
    print(f"Alignment artifacts in {remote_dir}:")
    print("  " + _ssh(f"ls {remote_dir}/*.json 2>/dev/null | wc -l").strip())
    print("Still running (align-scraped process):")
    print("  " + _ssh("pgrep -af align.scraped || pgrep -af pipeline || echo '(not found)'").strip())
    if log_file:
        print(f"Last log lines ({log_file}):")
        print(_ssh(f"tail -n 12 {log_file}"))


def main():
    ap = argparse.ArgumentParser(description="Status for the scraped chunk-alignment pass")
    ap.add_argument("--book", type=str, help="Filter to book(s), comma-separated")
    ap.add_argument("--langs", type=str, default="en",
                    help="Languages to count (default: en; 'all' for every scraped lang)")
    ap.add_argument("--data-dir", default=DEFAULT_DESTINATION_DIR, help="Built ThaqalaynData dir")
    ap.add_argument("--alignment-dir", default=AI_ALIGNMENT_DIR, help="Local artifact dir")
    ap.add_argument("--spark", action="store_true",
                    help="Query a Spark-hosted run via ssh instead of local disk")
    ap.add_argument("--spark-remote-dir",
                    default="~/thaqalayn-align/ThaqalaynDataSources/ai-content/corpus/chunk_alignment",
                    help="Remote alignment artifact dir on the Spark")
    ap.add_argument("--spark-log", default="~/thaqalayn-align/align.log",
                    help="Remote log file to tail")
    args = ap.parse_args()

    os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")
    if args.spark:
        spark_status(args.spark_remote_dir, args.spark_log)
        return
    langs = None
    if args.langs and args.langs.lower() != "all":
        langs = [l.strip() for l in args.langs.split(",") if l.strip()]
    local_status(args.book, langs, args.data_dir, args.alignment_dir)


if __name__ == "__main__":
    main()
