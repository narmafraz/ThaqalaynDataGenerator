"""Batch-improve pipeline: run N verses, analyse, improve, repeat.

Orchestrates the full self-improving loop:
  1. Run a batch of --batch-size verses via pipeline.py (subprocess)
  2. Analyse the batch results via analyse_run.py
  3. Spin up a Claude agent to apply improvements
  4. Run tests to verify improvements don't break anything
  5. Repeat until --total-verses reached or queue exhausted

Each batch runs as a fresh subprocess so code changes from the improvement
agent take effect on the next batch.

Usage:
    # Run 1000 verses in batches of 100, auto-improving between batches
    python scripts/batch_improve.py --total-verses 1000 --batch-size 100 --workers 20

    # Dry run to see what would happen
    python scripts/batch_improve.py --total-verses 200 --batch-size 100 --dry-run

    # Skip improvement step (just batched execution)
    python scripts/batch_improve.py --total-verses 500 --batch-size 100 --no-improve

    # Custom improvement model
    python scripts/batch_improve.py --total-verses 1000 --improve-model opus
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Fix Windows console encoding
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
SOURCE_DATA_DIR = PROJECT_ROOT.parent / "ThaqalaynDataSources"
CLAUDE_EXE = shutil.which("claude") or str(Path.home() / ".local" / "bin" / "claude.exe")
PYTHON_EXE = str(PROJECT_ROOT / ".venv" / "Scripts" / "python.exe")

IMPROVEMENT_SYSTEM_PROMPT = """\
You are an AI pipeline engineer improving an Islamic scripture content generation pipeline.
You will receive an analysis report from the latest batch of processed verses.

Your job:
1. Read PIPELINE_CHANGELOG.md FIRST — understand what changes have already been made and why.
   Do NOT re-apply a change that was already tried and reverted. Do NOT revert a change that
   was made deliberately unless you have strong data showing it made things worse.
2. Read the analysis report carefully
3. Identify the top 3-5 most impactful improvements (ranked by: error_count × cost_per_error)
4. For each improvement, make the actual code change
5. Update PIPELINE_CHANGELOG.md with what you changed and why (include batch number, error counts,
   and cost data that motivated the change)
6. Run the test suite to verify nothing is broken
7. If all tests pass, commit your changes with a descriptive message

RULES:
- ALWAYS read PIPELINE_CHANGELOG.md before making any changes — it is the single source of truth
  for what has been tried, what works, and what was reverted. Respect prior decisions.
- Only make changes that are clearly supported by the data in the report
- Be conservative — a small targeted fix is better than a large refactor
- If an error type has <2 occurrences, skip it (not statistically significant)
- Focus on the postprocessing/validation layer (verse_processor.py, ai_pipeline.py) and prompts
- Do NOT change the pipeline orchestrator (pipeline.py) or test infrastructure
- Do NOT change the analyse_run.py or batch_improve.py scripts
- After making changes, run tests:
  PYTHONPATH="$PWD:$PWD/app" SOURCE_DATA_DIR="../ThaqalaynDataSources/" .venv/Scripts/python.exe -m pytest --no-cov -q
- If tests fail, revert your changes and explain what went wrong
- If all tests pass, commit with:
  git add -A && git commit -m "Pipeline improvement: <short description of changes>

  Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
- If there are no actionable improvements (all errors are timeouts, <2 occurrences, or already
  handled by existing code), say "No improvements needed" and exit without changes

Key files:
- PIPELINE_CHANGELOG.md — READ FIRST, then document ALL changes with rationale and data
- app/pipeline_cli/verse_processor.py — postprocessing, auto-fix logic, validation routing
- app/ai_pipeline.py — system prompt (build_system_prompt), validation (validate_result), enums
- app/ai_pipeline_review.py — quality review checks (review_result)
"""


def run_batch(args, batch_num: int, batch_size: int, completed_so_far: int) -> dict:
    """Run a single batch of verses via pipeline.py subprocess."""
    cmd = [
        PYTHON_EXE, "-m", "app.pipeline_cli.pipeline",
        "--workers", str(args.workers),
        "--model", args.model,
        "--fix-model", args.fix_model,
        "--max-verses", str(batch_size),
    ]
    if args.max_words:
        cmd.extend(["--max-words", str(args.max_words)])
    if args.book:
        cmd.extend(["--book", args.book])
    if args.volume is not None:
        cmd.extend(["--volume", str(args.volume)])
    if args.dry_run:
        cmd.append("--dry-run")

    env = {
        **os.environ,
        "PYTHONPATH": f"{PROJECT_ROOT}{os.pathsep}{PROJECT_ROOT / 'app'}",
        "SOURCE_DATA_DIR": str(SOURCE_DATA_DIR) + "/",
    }
    if args.content_subdir:
        env["AI_CONTENT_SUBDIR"] = args.content_subdir
    # Remove CLAUDECODE to allow nested claude calls within pipeline
    env.pop("CLAUDECODE", None)

    print(f"\n{'='*60}", flush=True)
    print(f"BATCH {batch_num}: Processing up to {batch_size} verses "
          f"({completed_so_far} completed so far)", flush=True)
    print(f"{'='*60}\n", flush=True)

    start = time.time()
    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=env,
        capture_output=False,  # let output stream to terminal
    )
    elapsed = time.time() - start

    return {
        "batch_num": batch_num,
        "returncode": result.returncode,
        "elapsed_s": round(elapsed, 1),
    }


def get_latest_session(content_dir: Path) -> dict | None:
    """Find the most recent session file."""
    sessions_dir = content_dir / "sessions"
    if not sessions_dir.exists():
        return None
    sessions = sorted(sessions_dir.glob("*.json"), reverse=True)
    if not sessions:
        return None
    return json.loads(sessions[0].read_text(encoding="utf-8"))


def run_analysis(args, content_dir: Path) -> str:
    """Run analyse_run.py and return the LLM-format report."""
    cmd = [
        PYTHON_EXE, str(SCRIPT_DIR / "analyse_run.py"),
        "--subdir", args.content_subdir or "corpus",
        "--format", "llm",
    ]
    env = {
        **os.environ,
        "PYTHONPATH": f"{PROJECT_ROOT}{os.pathsep}{PROJECT_ROOT / 'app'}",
        "SOURCE_DATA_DIR": str(SOURCE_DATA_DIR) + "/",
    }
    env.pop("CLAUDECODE", None)

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        print(f"WARNING: analyse_run.py failed: {result.stderr[:500]}", flush=True)
        return ""
    return result.stdout


def run_improvement_agent(report: str, args) -> dict:
    """Spin up a Claude agent to apply improvements based on the analysis."""
    print(f"\n{'='*60}", flush=True)
    print("IMPROVEMENT AGENT: Analysing batch and applying fixes...", flush=True)
    print(f"{'='*60}\n", flush=True)

    user_prompt = f"""Here is the analysis report from the latest pipeline batch:

{report}

Analyse this report and apply any improvements to the pipeline code.
Remember to update PIPELINE_CHANGELOG.md and run tests after changes.
Working directory: {PROJECT_ROOT}
"""

    cmd = [
        CLAUDE_EXE, "-p",
        "--model", args.improve_model,
        "--output-format", "json",
        "--no-session-persistence",
        "--setting-sources", "",
        "--max-turns", "30",
        "--tools", "Read,Edit,Write,Bash,Grep,Glob",
        "--dangerously-skip-permissions",
        "--system-prompt", IMPROVEMENT_SYSTEM_PROMPT,
    ]

    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            env=env,
            input=user_prompt,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=1800,  # 30 min max for improvement agent
        )
        elapsed = time.time() - start

        if result.returncode != 0:
            print(f"WARNING: Improvement agent failed (rc={result.returncode})", flush=True)
            if result.stderr:
                print(f"  stderr: {result.stderr[:500]}", flush=True)
            return {"status": "error", "error": result.stderr[:500], "elapsed_s": round(elapsed, 1)}

        # Parse JSON output
        try:
            data = json.loads(result.stdout)
            agent_response = data.get("result", "")
            cost = data.get("total_cost_usd", 0)
            turns = data.get("num_turns", 0)
        except json.JSONDecodeError:
            agent_response = result.stdout[:2000]
            cost = 0
            turns = 0

        # Print summary
        print(f"\n--- Improvement Agent Summary ---", flush=True)
        print(f"  Turns: {turns} | Cost: ${cost:.4f} | Time: {elapsed:.0f}s", flush=True)
        # Print first 1000 chars of response
        preview = agent_response[:1000]
        if len(agent_response) > 1000:
            preview += "\n  ... (truncated)"
        print(f"  Response:\n  {preview}", flush=True)
        print(f"---\n", flush=True)

        return {
            "status": "ok",
            "response": agent_response[:5000],
            "cost_usd": cost,
            "turns": turns,
            "elapsed_s": round(elapsed, 1),
        }

    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        print(f"WARNING: Improvement agent timed out after 30 min", flush=True)
        return {"status": "timeout", "elapsed_s": round(elapsed, 1)}


def run_tests() -> bool:
    """Run the test suite and return True if all pass."""
    print("Running tests to verify improvements...", flush=True)
    env = {
        **os.environ,
        "PYTHONPATH": f"{PROJECT_ROOT}{os.pathsep}{PROJECT_ROOT / 'app'}",
        "SOURCE_DATA_DIR": str(SOURCE_DATA_DIR) + "/",
    }
    env.pop("CLAUDECODE", None)

    result = subprocess.run(
        [PYTHON_EXE, "-m", "pytest", "--no-cov", "-q"],
        cwd=str(PROJECT_ROOT),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=300,  # 5 min max
    )
    # Show last few lines of test output
    lines = result.stdout.strip().split("\n")
    for line in lines[-5:]:
        print(f"  {line}", flush=True)

    if result.returncode != 0:
        print(f"WARNING: Tests failed! Improvement may have introduced issues.", flush=True)
        return False
    return True


def count_completed(content_dir: Path) -> int:
    """Count completed response files."""
    responses_dir = content_dir / "responses"
    if not responses_dir.exists():
        return 0
    return len(list(responses_dir.glob("*.json")))


def main():
    parser = argparse.ArgumentParser(
        description="Batch-improve pipeline: run, analyse, improve, repeat"
    )
    parser.add_argument("--total-verses", type=int, required=True,
                        help="Total number of verses to process across all batches")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Verses per batch (default: 100)")
    parser.add_argument("--workers", type=int, default=20,
                        help="Concurrent workers per batch (default: 20)")
    parser.add_argument("--model", default="sonnet",
                        help="Model for generation (default: sonnet)")
    parser.add_argument("--fix-model", default="sonnet",
                        help="Model for fix pass (default: sonnet)")
    parser.add_argument("--improve-model", default="sonnet",
                        help="Model for improvement agent (default: sonnet)")
    parser.add_argument("--max-words", type=int, default=None,
                        help="Skip verses with more than N Arabic words")
    parser.add_argument("--book", type=str, default=None,
                        help="Filter to specific book(s)")
    parser.add_argument("--volume", type=int, default=None,
                        help="Filter to specific volume")
    parser.add_argument("--content-subdir", default="corpus",
                        help="AI content subdirectory (default: corpus)")
    parser.add_argument("--no-improve", action="store_true",
                        help="Skip improvement step (just batched execution)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Dry run (no Claude calls)")
    args = parser.parse_args()

    content_dir = SOURCE_DATA_DIR / "ai-content" / (args.content_subdir or "corpus")
    initial_completed = count_completed(content_dir)

    print(f"Batch-improve pipeline starting", flush=True)
    print(f"  Total target: {args.total_verses} verses", flush=True)
    print(f"  Batch size: {args.batch_size}", flush=True)
    print(f"  Workers: {args.workers}", flush=True)
    print(f"  Model: {args.model} | Fix: {args.fix_model} | Improve: {args.improve_model}", flush=True)
    print(f"  Already completed: {initial_completed}", flush=True)
    print(f"  Improvement: {'disabled' if args.no_improve else 'enabled'}", flush=True)

    # Track overall progress
    batch_log = []
    improvement_log = []
    total_cost = 0.0
    total_improvement_cost = 0.0
    overall_start = time.time()

    batch_num = 0
    verses_processed = 0

    while verses_processed < args.total_verses:
        batch_num += 1
        remaining = args.total_verses - verses_processed
        this_batch = min(args.batch_size, remaining)

        # Step 1: Run batch
        batch_result = run_batch(args, batch_num, this_batch, verses_processed)
        batch_log.append(batch_result)

        if batch_result["returncode"] != 0:
            print(f"\nWARNING: Batch {batch_num} exited with code {batch_result['returncode']}", flush=True)

        # Count how many actually completed
        current_completed = count_completed(content_dir)
        new_this_batch = current_completed - initial_completed - verses_processed
        verses_processed += max(new_this_batch, 0)

        # Get session info for cost tracking
        session = get_latest_session(content_dir)
        if session:
            batch_cost = session.get("total_cost_usd", 0)
            total_cost += batch_cost
            batch_passed = session.get("passed", 0)
            batch_errors = session.get("errors", 0)
            print(f"\nBatch {batch_num} complete: "
                  f"{session.get('completed', 0)} processed, "
                  f"{batch_passed} passed, {batch_errors} errors, "
                  f"${batch_cost:.2f}", flush=True)

        # Check if queue is exhausted (batch produced 0 new completions)
        if new_this_batch <= 0 and not args.dry_run:
            print(f"\nQueue appears exhausted (0 new completions). Stopping.", flush=True)
            break

        # Step 2: Analyse + Improve (skip on last batch or if disabled)
        if not args.no_improve and not args.dry_run and verses_processed < args.total_verses:
            # Run analysis
            report = run_analysis(args, content_dir)
            if not report:
                print("WARNING: Analysis produced no report, skipping improvement", flush=True)
                continue

            # Run improvement agent
            improve_result = run_improvement_agent(report, args)
            improvement_log.append({
                "batch_num": batch_num,
                **improve_result,
            })
            if improve_result.get("cost_usd"):
                total_improvement_cost += improve_result["cost_usd"]

            # Verify tests pass after improvement
            if improve_result.get("status") == "ok":
                tests_ok = run_tests()
                if not tests_ok:
                    print("WARNING: Tests failed after improvement. "
                          "The improvement agent should have handled this. "
                          "Continuing to next batch.", flush=True)

    # Final summary
    overall_elapsed = (time.time() - overall_start) / 60
    final_completed = count_completed(content_dir)
    total_new = final_completed - initial_completed

    print(f"\n{'='*60}", flush=True)
    print(f"BATCH-IMPROVE COMPLETE", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Batches: {batch_num}", flush=True)
    print(f"  New completions: {total_new}", flush=True)
    print(f"  Pipeline cost: ${total_cost:.2f}", flush=True)
    print(f"  Improvement cost: ${total_improvement_cost:.2f}", flush=True)
    print(f"  Total cost: ${total_cost + total_improvement_cost:.2f}", flush=True)
    print(f"  Elapsed: {overall_elapsed:.1f} min", flush=True)
    print(f"  Corpus total: {final_completed} completed", flush=True)

    # Save run log
    run_log = {
        "started_at": datetime.fromtimestamp(overall_start, timezone.utc).isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_minutes": round(overall_elapsed, 1),
        "batches": batch_num,
        "total_new_completions": total_new,
        "pipeline_cost_usd": round(total_cost, 4),
        "improvement_cost_usd": round(total_improvement_cost, 4),
        "total_cost_usd": round(total_cost + total_improvement_cost, 4),
        "batch_log": batch_log,
        "improvement_log": improvement_log,
        "config": {
            "total_verses": args.total_verses,
            "batch_size": args.batch_size,
            "workers": args.workers,
            "model": args.model,
            "fix_model": args.fix_model,
            "improve_model": args.improve_model,
            "max_words": args.max_words,
            "book": args.book,
            "volume": args.volume,
        },
    }

    log_dir = content_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"batch_improve_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    log_path.write_text(json.dumps(run_log, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Run log: {log_path}", flush=True)


if __name__ == "__main__":
    main()
