"""Pipeline status and monitoring tool.

Usage:
    python -m app.pipeline_cli.pipeline_status
    python -m app.pipeline_cli.pipeline_status --audit
    python -m app.pipeline_cli.pipeline_status --responses-dir path/to/responses
"""

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import AI_PIPELINE_DATA_DIR, AI_RESPONSES_DIR
from app.pipeline_cli.verse_processor import verse_path_to_id


def _load_corpus_manifest() -> dict:
    """Load corpus manifest and return {book: [paths]} mapping."""
    manifest_path = os.path.join(AI_PIPELINE_DATA_DIR, "corpus_manifest.json")
    if not os.path.exists(manifest_path):
        return {}
    with open(manifest_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    verses = data.get("verses", [])
    by_book = defaultdict(list)
    for v in verses:
        path = v["path"] if isinstance(v, dict) else v
        book = path.replace("/books/", "").split(":")[0]
        by_book[book].append(path)
    return dict(by_book)


def _count_responses(responses_dir: str) -> dict:
    """Count response files per book."""
    by_book = Counter()
    total = 0
    if not os.path.exists(responses_dir):
        return {"total": 0, "by_book": dict(by_book)}
    for fname in os.listdir(responses_dir):
        if not fname.endswith(".json"):
            continue
        book = fname.split("_")[0]
        by_book[book] += 1
        total += 1
    return {"total": total, "by_book": dict(by_book)}


def _load_quarantine(responses_dir: str) -> list:
    """Load quarantined verse info."""
    quarantine_dir = os.path.join(os.path.dirname(responses_dir), "quarantine")
    if not os.path.exists(quarantine_dir):
        return []
    entries = []
    for fname in os.listdir(quarantine_dir):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(quarantine_dir, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            entries.append({
                "verse_id": data.get("verse_id", fname.replace(".json", "")),
                "error": data.get("error", "unknown")[:80],
            })
        except (json.JSONDecodeError, OSError):
            entries.append({"verse_id": fname.replace(".json", ""), "error": "unreadable"})
    return entries


def _load_session_logs(tmp_dir: str) -> list:
    """Load session logs from pipeline tmp dir."""
    logs = []
    session_path = os.path.join(tmp_dir, "pipeline_session.json")
    if os.path.exists(session_path):
        try:
            with open(session_path, "r", encoding="utf-8") as f:
                logs.append(json.load(f))
        except (json.JSONDecodeError, OSError):
            pass
    return logs


def _count_stale_work_dirs(tmp_dir: str, responses_dir: str) -> int:
    """Count orphaned work directories."""
    if not os.path.exists(tmp_dir):
        return 0
    count = 0
    for entry in os.listdir(tmp_dir):
        work_dir = os.path.join(tmp_dir, entry)
        if not os.path.isdir(work_dir) or entry == "pipeline_session.json":
            continue
        resp_path = os.path.join(responses_dir, f"{entry}.json")
        if not os.path.exists(resp_path):
            count += 1
    return count


def _load_session_history(sessions_dir: str) -> list:
    """Load all session history files, sorted by date."""
    if not os.path.exists(sessions_dir):
        return []
    sessions = []
    for fname in sorted(os.listdir(sessions_dir)):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(sessions_dir, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                sessions.append(json.load(f))
        except (json.JSONDecodeError, OSError):
            pass
    return sessions


def _aggregate_verse_stats(stats_dir: str) -> dict:
    """Aggregate per-verse stats files for summary metrics."""
    if not os.path.exists(stats_dir):
        return {}
    total_cost = 0.0
    total_fix_cost = 0.0
    total_tokens = 0
    total_gen_time = 0.0
    total_fix_time = 0.0
    count = 0
    by_status = Counter()
    by_content_type = Counter()
    by_model = Counter()
    warnings_total = {"high": 0, "medium": 0, "low": 0}

    for fname in os.listdir(stats_dir):
        if not fname.endswith(".stats.json"):
            continue
        path = os.path.join(stats_dir, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                s = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        count += 1
        by_status[s.get("status", "unknown")] += 1
        gen = s.get("generation", {})
        total_cost += gen.get("cost_usd", 0)
        total_tokens += gen.get("output_tokens", 0)
        total_gen_time += gen.get("elapsed_s", 0)
        by_model[s.get("model", "unknown")] += 1
        fix = s.get("fix", {})
        if fix.get("needed"):
            total_fix_cost += fix.get("cost_usd", 0)
            total_fix_time += fix.get("elapsed_s", 0)
        ct = s.get("content", {}).get("content_type", "")
        if ct:
            by_content_type[ct] += 1
        q = s.get("quality", {})
        warnings_total["high"] += q.get("warnings_high", 0)
        warnings_total["medium"] += q.get("warnings_medium", 0)
        warnings_total["low"] += q.get("warnings_low", 0)

    return {
        "count": count,
        "total_cost": total_cost,
        "total_fix_cost": total_fix_cost,
        "total_tokens": total_tokens,
        "total_gen_time": total_gen_time,
        "total_fix_time": total_fix_time,
        "by_status": dict(by_status),
        "by_content_type": dict(by_content_type.most_common(10)),
        "by_model": dict(by_model),
        "warnings": warnings_total,
    }


def print_status(responses_dir: str, tmp_dir: str):
    """Print the pipeline status summary."""
    content_dir = os.path.dirname(responses_dir)

    # Corpus manifest
    manifest = _load_corpus_manifest()
    total_corpus = sum(len(v) for v in manifest.values())

    # Response counts
    resp = _count_responses(responses_dir)
    complete = resp["total"]
    remaining = total_corpus - complete

    # Quarantine
    quarantined = _load_quarantine(responses_dir)

    # Session logs (legacy + new history)
    sessions = _load_session_logs(tmp_dir)
    sessions_dir = os.path.join(content_dir, "sessions")
    session_history = _load_session_history(sessions_dir)

    # Per-verse stats
    stats_dir = os.path.join(content_dir, "stats")
    verse_agg = _aggregate_verse_stats(stats_dir)

    # Stale work dirs
    stale = _count_stale_work_dirs(tmp_dir, responses_dir)

    # Print
    pct = (complete / total_corpus * 100) if total_corpus > 0 else 0
    print(f"=== Pipeline Status ===")
    print()
    print(f"Progress: {complete:,} / {total_corpus:,} ({pct:.1f}%)  |  Remaining: {remaining:,}")

    # Per-book progress
    book_parts = []
    for book in sorted(manifest.keys()):
        book_total = len(manifest[book])
        book_done = resp["by_book"].get(book, 0)
        book_parts.append(f"  {book}: {book_done}/{book_total}")
    if book_parts:
        print("\n".join(book_parts))

    # Cumulative stats from per-verse stats files
    if verse_agg.get("count"):
        va = verse_agg
        print()
        print(f"Cumulative stats ({va['count']:,} verses with stats):")
        print(f"  Gen cost:  ${va['total_cost']:.2f}  |  Fix cost: ${va['total_fix_cost']:.2f}  |  Total: ${va['total_cost'] + va['total_fix_cost']:.2f}")
        avg_cost = (va['total_cost'] + va['total_fix_cost']) / va['count']
        print(f"  Avg cost/verse: ${avg_cost:.4f}  |  Tokens: {va['total_tokens']:,}")
        avg_time = va['total_gen_time'] / va['count']
        print(f"  Avg gen time: {avg_time:.0f}s  |  Total gen: {va['total_gen_time'] / 3600:.1f}h  |  Total fix: {va['total_fix_time'] / 3600:.1f}h")
        print(f"  Status: {va['by_status']}")
        w = va['warnings']
        print(f"  Warnings: high={w['high']}  medium={w['medium']}  low={w['low']}")
        if remaining > 0:
            projected = remaining * avg_cost
            print(f"  Projected remaining: ${projected:,.0f} ({remaining:,} x ${avg_cost:.4f})")

    # Session history
    if session_history:
        print()
        recent = session_history[-5:]  # show last 5 sessions
        print(f"Session history ({len(session_history)} total, showing last {len(recent)}):")
        for s in recent:
            sid = s.get("session_id", "?")
            completed = s.get("completed", 0)
            passed = s.get("passed", 0)
            fixed = s.get("fixed", 0)
            errors = s.get("errors", 0)
            cost = s.get("total_cost_usd", s.get("total_cost", 0))
            elapsed = s.get("elapsed_minutes", 0)
            rate = s.get("rate_per_hour", 0)
            print(f"  {sid}: {completed} done (p={passed} f={fixed} e={errors}) "
                  f"${cost:.2f} {elapsed:.0f}min {rate:.0f}/hr")
    elif sessions:
        # Fallback to legacy single session
        s = sessions[-1]
        print()
        total_cost = s.get("total_cost", 0)
        completed = s.get("completed", 0)
        avg_cost = total_cost / completed if completed > 0 else 0
        print(f"Last session: {s.get('started_at', '?')}")
        print(f"  Completed: {completed}  |  pass={s.get('passed', 0)}  fixed={s.get('fixed', 0)}  err={s.get('errors', 0)}")
        print(f"  Cost: ${total_cost:.2f} total  |  ${avg_cost:.2f}/verse")

    # Quarantine
    if quarantined:
        print()
        print(f"Quarantined ({len(quarantined)}):")
        for q in quarantined[:20]:
            print(f"  {q['verse_id']}: {q['error']}")
        if len(quarantined) > 20:
            print(f"  ... and {len(quarantined) - 20} more")

    # Stale work dirs
    if stale > 0:
        print()
        print(f"Stale work dirs: {stale} (run pipeline to auto-recover)")

    print()


def run_audit(responses_dir: str):
    """Re-validate all responses and report quality metrics."""
    from app.ai_pipeline import PIPELINE_VERSION, validate_result, reconstruct_fields
    from app.ai_pipeline_review import review_result
    from app.pipeline_cli.verse_processor import id_to_verse_path

    if not os.path.exists(responses_dir):
        print("No responses directory found.")
        return

    files = [f for f in os.listdir(responses_dir) if f.endswith(".json")]
    if not files:
        print("No response files found.")
        return

    print(f"Auditing {len(files)} response files...")
    print()

    validation_error_counts = Counter()
    warning_counts = Counter()
    version_counts = Counter()
    total_pass = 0
    total_fail = 0
    total_skipped = 0

    for fname in sorted(files):
        path = os.path.join(responses_dir, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                wrapper = json.load(f)
        except (json.JSONDecodeError, OSError):
            total_fail += 1
            validation_error_counts["unreadable_file"] += 1
            continue

        version = wrapper.get("ai_attribution", {}).get("pipeline_version", "unknown")
        version_counts[version] += 1

        # Only audit current version
        if version != PIPELINE_VERSION:
            total_skipped += 1
            continue

        result = wrapper.get("result", {})
        if not result:
            total_fail += 1
            validation_error_counts["empty_result"] += 1
            continue

        # Validate
        errors = validate_result(result)
        if errors:
            total_fail += 1
            for e in errors:
                # Categorize error
                category = e.split(":")[0] if ":" in e else e.split(" ")[0]
                validation_error_counts[category] += 1
        else:
            total_pass += 1

    print(f"Audit Results ({len(files)} files):")
    print(f"  Pass: {total_pass}  |  Fail: {total_fail}  |  Skipped (old version): {total_skipped}")
    print()

    if version_counts:
        print("Pipeline versions:")
        for ver, count in version_counts.most_common():
            print(f"  {ver}: {count}")
        print()

    if validation_error_counts:
        print(f"Top validation errors ({sum(validation_error_counts.values())} total):")
        for err, count in validation_error_counts.most_common(15):
            print(f"  {err}: {count}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Pipeline v3 status and monitoring")
    parser.add_argument("--responses-dir", default=None, help="Override responses directory")
    parser.add_argument("--tmp-dir", default="tmp/pipeline", help="Pipeline temp directory")
    parser.add_argument("--audit", action="store_true", help="Re-validate all responses (slow)")
    args = parser.parse_args()

    responses_dir = args.responses_dir or AI_RESPONSES_DIR
    os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")

    print_status(responses_dir, args.tmp_dir)

    if args.audit:
        run_audit(responses_dir)


if __name__ == "__main__":
    main()
