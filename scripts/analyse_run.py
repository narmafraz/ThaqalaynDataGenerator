"""Analyse a pipeline run and produce a structured report for LLM-driven improvement.

Usage:
    # Analyse latest session
    python scripts/analyse_run.py

    # Analyse specific session
    python scripts/analyse_run.py --session 20260307T225947Z

    # Analyse and output for LLM consumption (machine-readable)
    python scripts/analyse_run.py --format llm

    # Batch-100 workflow: run, analyse, improve
    python scripts/analyse_run.py --batch-report

Output: Structured analysis to stdout (human or LLM-consumable).
"""

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Fix Windows console encoding for Arabic text
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Resolve paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
SOURCE_DATA_DIR = PROJECT_ROOT.parent / "ThaqalaynDataSources"


def find_content_dir(subdir: str = "corpus") -> Path:
    return SOURCE_DATA_DIR / "ai-content" / subdir


def load_session(session_id: Optional[str], content_dir: Path) -> Optional[dict]:
    """Load a session file. If no ID given, load the latest."""
    sessions_dir = content_dir / "sessions"
    if not sessions_dir.exists():
        return None

    if session_id:
        path = sessions_dir / f"{session_id}.json"
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return None

    # Find latest
    sessions = sorted(sessions_dir.glob("*.json"), reverse=True)
    if not sessions:
        return None
    return json.loads(sessions[0].read_text(encoding="utf-8"))


def load_all_stats(content_dir: Path) -> List[dict]:
    """Load all per-verse stats files."""
    stats_dir = content_dir / "stats"
    if not stats_dir.exists():
        return []
    stats = []
    for f in stats_dir.glob("*.stats.json"):
        try:
            stats.append(json.loads(f.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError):
            pass
    return stats


def load_session_stats(session: dict, all_stats: List[dict]) -> List[dict]:
    """Filter stats to those generated during a session's time window."""
    if not session:
        return all_stats
    start = session.get("started_at", "")
    end = session.get("completed_at", "")
    if not start or not end:
        return all_stats
    return [s for s in all_stats if start <= s.get("generated_at", "") <= end]


def load_log_events(content_dir: Path, session_id: Optional[str] = None) -> List[dict]:
    """Load JSONL event log entries, optionally filtered to a session."""
    log_path = content_dir / "logs" / "pipeline.jsonl"
    if not log_path.exists():
        return []
    events = []
    for line in log_path.read_text(encoding="utf-8").strip().split("\n"):
        if not line.strip():
            continue
        try:
            evt = json.loads(line)
            if session_id and "session_id" in evt and evt["session_id"] != session_id:
                continue  # skip events from other sessions
            events.append(evt)
        except json.JSONDecodeError:
            pass
    return events


def analyse_errors(stats: List[dict]) -> dict:
    """Categorize and count errors."""
    error_types = Counter()
    error_details = defaultdict(list)
    total_error_cost = 0.0

    for s in stats:
        if s.get("status") != "error":
            continue
        total_error_cost += s.get("generation", {}).get("cost_usd", 0)

        # Categorize by validation errors
        val_errors = s.get("quality", {}).get("validation_errors", [])
        error_str = s.get("error", "")

        if val_errors:
            for ve in val_errors:
                # Extract error category
                if "ambiguity_note" in ve:
                    error_types["missing_ambiguity_note"] += 1
                elif "invalid topic" in ve:
                    error_types["invalid_topic"] += 1
                elif "invalid tag" in ve:
                    error_types["invalid_tag"] += 1
                elif "invalid content_type" in ve:
                    error_types["invalid_content_type"] += 1
                elif "missing required field" in ve:
                    error_types["missing_required_field"] += 1
                elif "word_analysis" in ve:
                    error_types["word_analysis_error"] += 1
                elif "invalid pos" in ve:
                    error_types["invalid_pos"] += 1
                else:
                    error_types[f"other_validation: {ve[:60]}"] += 1
                error_details[s["verse_id"]].append(ve)
        elif "Timed out" in error_str:
            error_types["timeout"] += 1
            error_details[s["verse_id"]].append(error_str)
        elif "JSON parse" in error_str:
            error_types["json_parse_error"] += 1
            error_details[s["verse_id"]].append(error_str)
        elif "malformed" in error_str.lower():
            error_types["malformed_response"] += 1
            error_details[s["verse_id"]].append(error_str)
        else:
            error_types[f"other: {error_str[:60]}"] += 1
            error_details[s["verse_id"]].append(error_str)

    return {
        "error_types": dict(error_types.most_common()),
        "error_details": dict(error_details),
        "total_error_cost": round(total_error_cost, 2),
        "total_errors": sum(1 for s in stats if s.get("status") == "error"),
    }


def analyse_costs(stats: List[dict]) -> dict:
    """Break down costs by outcome."""
    pass_costs = []
    fix_costs = []
    error_costs = []

    for s in stats:
        gen_cost = s.get("generation", {}).get("cost_usd", 0)
        fix_cost = s.get("fix", {}).get("cost_usd", 0)
        total = gen_cost + fix_cost
        status = s.get("status", "")

        if status == "pass":
            if s.get("fix", {}).get("applied"):
                fix_costs.append(total)
            else:
                pass_costs.append(total)
        elif status == "error":
            error_costs.append(total)

    def _summary(costs):
        if not costs:
            return {"count": 0, "total": 0, "avg": 0, "min": 0, "max": 0}
        return {
            "count": len(costs),
            "total": round(sum(costs), 2),
            "avg": round(sum(costs) / len(costs), 2),
            "min": round(min(costs), 2),
            "max": round(max(costs), 2),
        }

    return {
        "pass_direct": _summary(pass_costs),
        "pass_via_fix": _summary(fix_costs),
        "errors": _summary(error_costs),
        "total_cost": round(sum(pass_costs) + sum(fix_costs) + sum(error_costs), 2),
        "wasted_cost": round(sum(error_costs), 2),
        "waste_pct": round(sum(error_costs) / (sum(pass_costs) + sum(fix_costs) + sum(error_costs)) * 100, 1)
        if (pass_costs or fix_costs or error_costs) else 0,
    }


def analyse_timing(stats: List[dict]) -> dict:
    """Analyse timing patterns."""
    gen_times = []
    fix_times = []
    timeout_verses = []

    for s in stats:
        gen_elapsed = s.get("generation", {}).get("elapsed_s", 0)
        if gen_elapsed > 0:
            gen_times.append((s["verse_id"], s.get("word_count", 0), gen_elapsed))
        if s.get("fix", {}).get("elapsed_s", 0) > 0:
            fix_times.append((s["verse_id"], s.get("fix", {}).get("elapsed_s", 0)))
        if gen_elapsed >= 1799:
            timeout_verses.append(s["verse_id"])

    gen_times.sort(key=lambda x: x[2], reverse=True)

    return {
        "avg_gen_time": round(sum(t[2] for t in gen_times) / len(gen_times), 1) if gen_times else 0,
        "avg_fix_time": round(sum(t[1] for t in fix_times) / len(fix_times), 1) if fix_times else 0,
        "slowest_5": [{"verse": t[0], "words": t[1], "elapsed_s": t[2]} for t in gen_times[:5]],
        "timeout_verses": timeout_verses,
    }


def analyse_quality(stats: List[dict]) -> dict:
    """Analyse warning patterns in passing verses."""
    warning_counts = {"high": 0, "medium": 0, "low": 0}
    fix_success_rate = {"attempted": 0, "succeeded": 0}

    for s in stats:
        q = s.get("quality", {})
        warning_counts["high"] += q.get("warnings_high", 0)
        warning_counts["medium"] += q.get("warnings_medium", 0)
        warning_counts["low"] += q.get("warnings_low", 0)

        if s.get("fix", {}).get("needed"):
            fix_success_rate["attempted"] += 1
            if s.get("fix", {}).get("applied"):
                fix_success_rate["succeeded"] += 1

    return {
        "warnings": warning_counts,
        "fix_pass": {
            **fix_success_rate,
            "rate": round(fix_success_rate["succeeded"] / fix_success_rate["attempted"] * 100, 1)
            if fix_success_rate["attempted"] else 0,
        },
    }


def analyse_failure_patterns(all_stats: List[dict]) -> dict:
    """Find chronic failers across all runs."""
    failure_counts = Counter()
    for s in all_stats:
        fc = s.get("failure_count", 0)
        if fc > 0:
            failure_counts[s["verse_id"]] = max(failure_counts[s["verse_id"]], fc)

    chronic = {vid: count for vid, count in failure_counts.items() if count >= 2}
    return {
        "chronic_failers": dict(sorted(chronic.items(), key=lambda x: -x[1])[:20]),
        "total_with_failures": len(failure_counts),
    }


def generate_report(
    session: Optional[dict],
    session_stats: List[dict],
    all_stats: List[dict],
    content_dir: Path,
    format: str = "human",
) -> str:
    """Generate the full analysis report."""
    errors = analyse_errors(session_stats)
    costs = analyse_costs(session_stats)
    timing = analyse_timing(session_stats)
    quality = analyse_quality(session_stats)
    failures = analyse_failure_patterns(all_stats)

    # Count quarantined
    quarantine_dir = content_dir / "quarantine"
    quarantined = len(list(quarantine_dir.glob("*.json"))) if quarantine_dir.exists() else 0

    # Corpus totals
    total_complete = len(list((content_dir / "responses").glob("*.json"))) if (content_dir / "responses").exists() else 0

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "session": session,
        "corpus_status": {
            "total_complete": total_complete,
            "total_quarantined": quarantined,
            "total_stats_files": len(all_stats),
        },
        "session_summary": {
            "verses_processed": len(session_stats),
            "pass_rate": round(sum(1 for s in session_stats if s.get("status") == "pass") / len(session_stats) * 100, 1)
            if session_stats else 0,
            "effective_pass_rate": round(
                sum(1 for s in session_stats if s.get("status") == "pass") /
                (len(session_stats) - sum(1 for s in session_stats if s.get("status") == "skipped"))
                * 100, 1
            ) if sum(1 for s in session_stats if s.get("status") != "skipped") else 0,
        },
        "errors": errors,
        "costs": costs,
        "timing": timing,
        "quality": quality,
        "failure_patterns": failures,
    }

    if format == "llm":
        return _format_llm(report)
    elif format == "json":
        return json.dumps(report, indent=2, ensure_ascii=False)
    else:
        return _format_human(report)


def _format_human(r: dict) -> str:
    """Human-readable report."""
    lines = []
    lines.append("=" * 70)
    lines.append("PIPELINE RUN ANALYSIS")
    lines.append("=" * 70)

    if r["session"]:
        s = r["session"]
        lines.append(f"Session: {s.get('session_id', 'unknown')}")
        lines.append(f"Duration: {s.get('elapsed_minutes', 0)} min | Workers: {s.get('config', {}).get('workers', '?')}")
        lines.append(f"Model: {s.get('config', {}).get('model', '?')} | Fix: {s.get('config', {}).get('fix_model', '?')}")

    cs = r["corpus_status"]
    lines.append(f"\nCorpus: {cs['total_complete']} complete, {cs['total_quarantined']} quarantined")

    ss = r["session_summary"]
    lines.append(f"\nThis Run: {ss['verses_processed']} verses, {ss['pass_rate']}% pass rate")

    # Costs
    c = r["costs"]
    lines.append(f"\n--- Costs ---")
    lines.append(f"Total: ${c['total_cost']} | Wasted: ${c['wasted_cost']} ({c['waste_pct']}%)")
    lines.append(f"Pass (direct): {c['pass_direct']['count']} @ ${c['pass_direct']['avg']} avg")
    lines.append(f"Pass (via fix): {c['pass_via_fix']['count']} @ ${c['pass_via_fix']['avg']} avg")
    lines.append(f"Errors: {c['errors']['count']} @ ${c['errors']['avg']} avg")

    # Errors
    e = r["errors"]
    lines.append(f"\n--- Errors ({e['total_errors']} total, ${e['total_error_cost']} wasted) ---")
    for etype, count in e["error_types"].items():
        lines.append(f"  {count}x {etype}")

    # Error details
    if e["error_details"]:
        lines.append(f"\n--- Error Details ---")
        for vid, errs in list(e["error_details"].items())[:10]:
            lines.append(f"  {vid}:")
            for err in errs[:3]:
                lines.append(f"    - {err}")

    # Timing
    t = r["timing"]
    lines.append(f"\n--- Timing ---")
    lines.append(f"Avg gen: {t['avg_gen_time']}s | Avg fix: {t['avg_fix_time']}s")
    if t["timeout_verses"]:
        lines.append(f"Timeouts: {', '.join(t['timeout_verses'])}")
    if t["slowest_5"]:
        lines.append(f"Slowest:")
        for s in t["slowest_5"]:
            lines.append(f"  {s['verse']}: {s['elapsed_s']}s ({s['words']} words)")

    # Quality
    q = r["quality"]
    lines.append(f"\n--- Quality ---")
    lines.append(f"Warnings: {q['warnings']['high']} high, {q['warnings']['medium']} med, {q['warnings']['low']} low")
    lines.append(f"Fix pass: {q['fix_pass']['succeeded']}/{q['fix_pass']['attempted']} ({q['fix_pass']['rate']}%)")

    # Chronic failers
    f = r["failure_patterns"]
    if f["chronic_failers"]:
        lines.append(f"\n--- Chronic Failers ---")
        for vid, count in f["chronic_failers"].items():
            lines.append(f"  {vid}: {count} failures")

    lines.append("\n" + "=" * 70)
    return "\n".join(lines)


def _format_llm(r: dict) -> str:
    """LLM-consumable report for automated improvement workflow."""
    lines = []
    lines.append("# Pipeline Run Analysis Report")
    lines.append("")
    lines.append("You are analysing a pipeline run to identify improvements. Below is structured data from the run.")
    lines.append("Your task: identify the top 3-5 actionable improvements, ranked by impact (cost savings × frequency).")
    lines.append("")
    lines.append("## Session Overview")
    lines.append(json.dumps(r["session"], indent=2) if r["session"] else "No session data")
    lines.append("")
    lines.append("## Corpus Status")
    lines.append(json.dumps(r["corpus_status"], indent=2))
    lines.append("")
    lines.append("## Error Analysis")
    lines.append(f"Total errors: {r['errors']['total_errors']}")
    lines.append(f"Wasted cost: ${r['errors']['total_error_cost']}")
    lines.append("")
    lines.append("### Error Types (most frequent first)")
    lines.append(json.dumps(r["errors"]["error_types"], indent=2))
    lines.append("")
    lines.append("### Error Details (per verse)")
    lines.append(json.dumps(r["errors"]["error_details"], indent=2))
    lines.append("")
    lines.append("## Cost Breakdown")
    lines.append(json.dumps(r["costs"], indent=2))
    lines.append("")
    lines.append("## Timing Analysis")
    lines.append(json.dumps(r["timing"], indent=2))
    lines.append("")
    lines.append("## Quality Metrics")
    lines.append(json.dumps(r["quality"], indent=2))
    lines.append("")
    lines.append("## Chronic Failure Patterns")
    lines.append(json.dumps(r["failure_patterns"], indent=2))
    lines.append("")
    lines.append("## Instructions")
    lines.append("")
    lines.append("Based on this data:")
    lines.append("1. Identify the top error categories and their root causes")
    lines.append("2. Propose specific code changes (file, function, what to change)")
    lines.append("3. Estimate the impact of each change (% error reduction, $ saved per 100 verses)")
    lines.append("4. Flag any new patterns not seen in previous runs")
    lines.append("5. Check if any previously-applied fixes (in PIPELINE_CHANGELOG.md) need adjustment")
    lines.append("")
    lines.append("Key files to modify:")
    lines.append("- `app/pipeline_cli/verse_processor.py` — postprocessing, auto-fix, validation routing")
    lines.append("- `app/pipeline_cli/pipeline.py` — orchestrator, retry logic, error handling")
    lines.append("- `app/ai_pipeline.py` — system prompt (build_system_prompt), validation (validate_result)")
    lines.append("- `app/ai_pipeline_review.py` — quality review checks")
    lines.append("- `PIPELINE_CHANGELOG.md` — document all changes with rationale")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Analyse pipeline run and produce improvement report")
    parser.add_argument("--session", type=str, help="Session ID to analyse (default: latest)")
    parser.add_argument("--subdir", default="corpus", help="AI content subdirectory (default: corpus)")
    parser.add_argument("--format", choices=["human", "llm", "json"], default="human",
                        help="Output format (default: human)")
    parser.add_argument("--batch-report", action="store_true",
                        help="Generate report suitable for batch-100 improvement workflow")
    args = parser.parse_args()

    content_dir = find_content_dir(args.subdir)
    if not content_dir.exists():
        print(f"ERROR: Content dir not found: {content_dir}", file=sys.stderr)
        sys.exit(1)

    # Load data
    session = load_session(args.session, content_dir)
    all_stats = load_all_stats(content_dir)

    if session:
        session_stats = load_session_stats(session, all_stats)
        print(f"Loaded session {session.get('session_id', '?')} with {len(session_stats)} verse stats", file=sys.stderr)
    else:
        session_stats = all_stats
        print(f"No session found, analysing all {len(all_stats)} stats files", file=sys.stderr)

    fmt = "llm" if args.batch_report else args.format
    report = generate_report(session, session_stats, all_stats, content_dir, fmt)
    print(report)


if __name__ == "__main__":
    main()
