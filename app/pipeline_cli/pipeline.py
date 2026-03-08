"""Pipeline v3 orchestrator — asyncio-based parallel verse processing.

Usage:
    python -m app.pipeline_cli.pipeline --workers 5
    python -m app.pipeline_cli.pipeline --workers 5 --dry-run
    python -m app.pipeline_cli.pipeline --single /books/al-kafi:1:1:1:1
    python -m app.pipeline_cli.pipeline --workers 5 --resume
    python -m app.pipeline_cli.pipeline --workers 5 --book al-kafi --volume 1
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import AI_PIPELINE_DATA_DIR, AI_RESPONSES_DIR
from app.pipeline_cli.verse_processor import (
    VersePlan,
    VerseResult,
    apply_fix,
    is_complete,
    load_narrator_templates,
    load_word_dictionary,
    postprocess_verse,
    prepare_fix,
    prepare_verse,
    verse_path_to_id,
)

logger = logging.getLogger(__name__)

CLAUDE_EXE = shutil.which("claude") or r"C:\Users\TrainingGR03\.local\bin\claude.exe"
DEFAULT_DATA_DIR = "../ThaqalaynData/"
DEFAULT_TMP_DIR = "tmp/pipeline"
DEFAULT_WORKERS = 5


# ---------------------------------------------------------------------------
# Persistent event log (JSONL append-only)
# ---------------------------------------------------------------------------

class EventLog:
    """Append-only JSONL event log for pipeline history."""

    def __init__(self, log_dir: str):
        os.makedirs(log_dir, exist_ok=True)
        self._path = os.path.abspath(os.path.join(log_dir, "pipeline.jsonl"))
        self._f = open(self._path, "a", encoding="utf-8")
        logger.info("OPENED %s", self._path)

    def log(self, event: str, **data):
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event,
            **data,
        }
        self._f.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._f.flush()

    def close(self):
        self._f.close()


class NullEventLog:
    """No-op event log for dry runs."""
    def log(self, event: str, **data): pass
    def close(self): pass


class TeeWriter:
    """Duplicates stdout writes to a log file so print() output is captured."""

    def __init__(self, original, log_file):
        self._original = original
        self._log_file = log_file

    def write(self, data):
        self._original.write(data)
        try:
            self._log_file.write(data)
            self._log_file.flush()
        except Exception:
            pass

    def flush(self):
        self._original.flush()
        try:
            self._log_file.flush()
        except Exception:
            pass

    def reconfigure(self, **kwargs):
        if hasattr(self._original, "reconfigure"):
            self._original.reconfigure(**kwargs)

    def __getattr__(self, name):
        return getattr(self._original, name)


# ---------------------------------------------------------------------------
# Per-verse stats persistence
# ---------------------------------------------------------------------------

def save_verse_stats(
    verse_id: str,
    verse_path: str,
    stats_dir: str,
    *,
    status: str,
    word_count: int = 0,
    mode: str = "single",
    model: str = "",
    fix_model: Optional[str] = None,
    gen_cost: float = 0,
    gen_output_tokens: int = 0,
    gen_elapsed: float = 0,
    gen_turns: int = 1,
    fix_cost: float = 0,
    fix_output_tokens: int = 0,
    fix_elapsed: float = 0,
    fix_needed: bool = False,
    fix_applied: bool = False,
    validation_errors: Optional[List[str]] = None,
    warnings_high: int = 0,
    warnings_medium: int = 0,
    warnings_low: int = 0,
    content_type: str = "",
    has_chain: bool = False,
    system_prompt_hash: str = "",
    pipeline_version: str = "3.0.0",
    error: Optional[str] = None,
    false_positive_accepted: bool = False,
):
    """Write per-verse stats to stats/{verse_id}.stats.json."""
    os.makedirs(stats_dir, exist_ok=True)
    stats_path = os.path.join(stats_dir, f"{verse_id}.stats.json")

    # Read previous failure count for cumulative tracking
    prev_failure_count = 0
    if os.path.exists(stats_path):
        try:
            with open(stats_path, "r", encoding="utf-8") as f:
                prev_data = json.load(f)
            prev_failure_count = prev_data.get("failure_count", 0)
        except (json.JSONDecodeError, OSError):
            pass
    failure_count = prev_failure_count + (1 if status == "error" else 0)

    data = {
        "verse_id": verse_id,
        "verse_path": verse_path,
        "pipeline_version": pipeline_version,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "word_count": word_count,
        "mode": mode,
        "model": model,
        "generation": {
            "cost_usd": round(gen_cost, 6),
            "output_tokens": gen_output_tokens,
            "elapsed_s": round(gen_elapsed, 2),
            "turns": gen_turns,
        },
        "fix": {
            "needed": fix_needed,
            "applied": fix_applied,
            "model": fix_model or "",
            "cost_usd": round(fix_cost, 6),
            "output_tokens": fix_output_tokens,
            "elapsed_s": round(fix_elapsed, 2),
        },
        "quality": {
            "warnings_high": warnings_high,
            "warnings_medium": warnings_medium,
            "warnings_low": warnings_low,
            "validation_errors": validation_errors or [],
        },
        "content": {
            "content_type": content_type,
            "has_chain": has_chain,
        },
        "system_prompt_hash": system_prompt_hash,
        "failure_count": failure_count,
        "false_positive_accepted": false_positive_accepted,
    }
    if error:
        data["error"] = error[:500]
    stats_path = os.path.abspath(stats_path)
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("WROTE %s", stats_path)


@dataclass
class PipelineConfig:
    """Pipeline configuration."""
    workers: int = DEFAULT_WORKERS
    model: str = "sonnet"
    fix_model: str = "sonnet"
    data_dir: str = DEFAULT_DATA_DIR
    tmp_dir: str = DEFAULT_TMP_DIR
    responses_dir: Optional[str] = None
    max_retries: int = 2
    max_fix_attempts: int = 1
    max_failures: int = 3
    attempt_quarantined: bool = False
    dry_run: bool = False
    max_words: Optional[int] = None
    max_verses: Optional[int] = None
    progress_interval: int = 30
    # Derived paths (set by run_pipeline)
    stats_dir: str = ""
    logs_dir: str = ""
    sessions_dir: str = ""
    prompts_dir: str = ""
    system_prompt_hash: str = ""
    session_id: str = ""
    event_log: object = field(default_factory=NullEventLog)


@dataclass
class SessionStats:
    """Tracks stats for the current pipeline session."""
    started_at: float = field(default_factory=time.time)
    total_queued: int = 0
    completed: int = 0
    skipped: int = 0
    passed: int = 0
    needs_fix: int = 0
    fixed: int = 0
    errors: int = 0
    total_cost: float = 0.0
    total_output_tokens: int = 0
    total_elapsed: float = 0.0


# Global shutdown event for graceful Ctrl+C handling
shutdown_event = asyncio.Event()


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""
    def handler(sig, frame):
        print("\nShutting down after current verses finish...", flush=True)
        shutdown_event.set()

    signal.signal(signal.SIGINT, handler)
    # SIGTERM not available on Windows but try anyway
    try:
        signal.signal(signal.SIGTERM, handler)
    except (OSError, AttributeError):
        pass


def load_corpus_manifest() -> List[str]:
    """Load verse paths from corpus manifest."""
    manifest_path = os.path.join(AI_PIPELINE_DATA_DIR, "corpus_manifest.json")
    if not os.path.exists(manifest_path):
        logger.error("Corpus manifest not found: %s", manifest_path)
        return []
    with open(manifest_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    verses = data.get("verses", [])
    # Manifest entries can be strings or dicts with "path" key
    return [v["path"] if isinstance(v, dict) else v for v in verses]


def is_quarantined(verse_id: str, responses_dir: str) -> bool:
    """Check if a verse is quarantined (too many failures)."""
    quarantine_dir = os.path.join(os.path.dirname(responses_dir), "quarantine")
    return os.path.exists(os.path.join(quarantine_dir, f"{verse_id}.json"))


def get_failure_count(verse_id: str, stats_dir: str) -> int:
    """Read cumulative failure count from stats file."""
    stats_path = os.path.join(stats_dir, f"{verse_id}.stats.json")
    if not os.path.exists(stats_path):
        return 0
    try:
        with open(stats_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("failure_count", 0)
    except (json.JSONDecodeError, OSError):
        return 0


def build_queue(
    verse_paths: List[str],
    responses_dir: str,
    book: Optional[str] = None,
    volume: Optional[int] = None,
    attempt_quarantined: bool = False,
) -> List[str]:
    """Filter verse paths to those not yet completed or quarantined."""
    books = [b.strip() for b in book.split(",")] if book else []
    queue = []
    skipped_quarantine = 0
    for vp in verse_paths:
        # Filter by book(s)
        if books and not any(vp.startswith(f"/books/{b}:") for b in books):
            continue
        # Filter by volume
        if volume is not None:
            parts = vp.replace("/books/", "").split(":")
            if len(parts) >= 2 and parts[1] != str(volume):
                continue
        # Skip completed
        vid = verse_path_to_id(vp)
        if is_complete(vid, responses_dir):
            continue
        # Skip quarantined (unless --attempt-quarantined)
        if not attempt_quarantined and is_quarantined(vid, responses_dir):
            skipped_quarantine += 1
            continue
        queue.append(vp)
    if skipped_quarantine:
        logger.info("Skipped %d quarantined verses (use --attempt-quarantined to retry)", skipped_quarantine)
    return queue


async def call_claude(
    system_prompt: str,
    user_message: str,
    model: str = "sonnet",
    max_retries: int = 2,
    fallback_model: Optional[str] = "haiku",
    max_budget_usd: Optional[float] = 5.0,
) -> dict:
    """Call claude -p with retry logic. Returns parsed response metadata."""
    cmd = [
        CLAUDE_EXE, "-p", "--model", model, "--output-format", "json",
        "--no-session-persistence", "--setting-sources", "",
        "--max-turns", "1",
        "--system-prompt", system_prompt,
    ]
    if fallback_model:
        cmd.extend(["--fallback-model", fallback_model])
    if max_budget_usd:
        cmd.extend(["--max-budget-usd", str(max_budget_usd)])
    # Filter CLAUDECODE from env to avoid nested session issues
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

    for attempt in range(max_retries + 1):
        start = time.time()
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(input=user_message.encode("utf-8")),
                    timeout=1800,  # 30 minute timeout per claude -p call
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                if attempt < max_retries:
                    wait = 10
                    logger.warning("Claude call timed out (attempt %d/%d, 30m). Retrying in %ds...",
                                   attempt + 1, max_retries + 1, wait)
                    await asyncio.sleep(wait)
                    continue
                return {"error": "Timed out after 30 minutes", "elapsed": 1800.0}
        except OSError as e:
            if attempt < max_retries:
                wait = 5 * (2 ** attempt)
                logger.warning("Claude call OSError (attempt %d/%d): %s. Retrying in %ds...",
                               attempt + 1, max_retries + 1, e, wait)
                await asyncio.sleep(wait)
                continue
            return {"error": f"OSError: {e}", "elapsed": round(time.time() - start, 2)}

        elapsed = time.time() - start

        if proc.returncode != 0 or not stdout:
            error_msg = stderr.decode("utf-8", errors="replace")[:500] if stderr else "no output"
            if attempt < max_retries and ("rate" in error_msg.lower() or "overloaded" in error_msg.lower()):
                wait = 5 * (2 ** attempt)
                logger.warning("Claude call failed (attempt %d/%d): %s. Retrying in %ds...",
                               attempt + 1, max_retries + 1, error_msg[:80], wait)
                await asyncio.sleep(wait)
                continue
            return {"error": error_msg, "elapsed": round(elapsed, 2)}

        try:
            data = json.loads(stdout.decode("utf-8", errors="replace"))
        except json.JSONDecodeError as e:
            return {"error": f"CLI JSON parse error: {e}", "elapsed": round(elapsed, 2)}

        return {
            "result": data.get("result", ""),
            "cost": data.get("total_cost_usd", 0),
            "output_tokens": data.get("usage", {}).get("output_tokens", 0),
            "elapsed": round(elapsed, 2),
            "stop_reason": data.get("stop_reason"),
            "num_turns": data.get("num_turns", 1),
        }

    return {"error": "max retries exceeded", "elapsed": 0}


def quarantine_verse(verse_id: str, error: str, responses_dir: str) -> None:
    """Move a failed verse to the quarantine directory."""
    quarantine_dir = os.path.join(os.path.dirname(responses_dir), "quarantine")
    os.makedirs(quarantine_dir, exist_ok=True)
    quarantine_path = os.path.join(quarantine_dir, f"{verse_id}.json")
    quarantine_path = os.path.abspath(quarantine_path)
    with open(quarantine_path, "w", encoding="utf-8") as f:
        json.dump({
            "verse_id": verse_id,
            "error": error,
            "quarantined_at": datetime.now(timezone.utc).isoformat(),
        }, f, indent=2)
    logger.info("WROTE %s", quarantine_path)


def recover_stale_work_dirs(tmp_dir: str, responses_dir: str) -> List[str]:
    """Find orphaned work dirs from crashed sessions and return verse paths to requeue."""
    requeue = []
    if not os.path.exists(tmp_dir):
        return requeue
    for entry in os.listdir(tmp_dir):
        work_dir = os.path.join(tmp_dir, entry)
        if not os.path.isdir(work_dir) or entry == "pipeline_session.json":
            continue
        # Check if this verse is already complete
        if is_complete(entry, responses_dir):
            shutil.rmtree(work_dir, ignore_errors=True)
            logger.info("Cleaned stale work dir (already complete): %s", entry)
        else:
            # Re-add to queue
            from app.pipeline_cli.verse_processor import id_to_verse_path
            verse_path = id_to_verse_path(entry)
            requeue.append(verse_path)
            shutil.rmtree(work_dir, ignore_errors=True)
            logger.info("Recovered stale work dir: %s -> requeued", entry)
    return requeue


async def process_verse(
    verse_path: str,
    config: PipelineConfig,
    semaphore: asyncio.Semaphore,
    stats: SessionStats,
    word_dict: Optional[dict],
    narrator_tmpl: Optional[dict],
) -> VerseResult:
    """Process a single verse through the full pipeline."""
    verse_id = verse_path_to_id(verse_path)
    responses_dir = config.responses_dir or AI_RESPONSES_DIR
    work_dir = os.path.join(config.tmp_dir, verse_id)

    # Skip if already complete
    if is_complete(verse_id, responses_dir):
        stats.skipped += 1
        return VerseResult(verse_id=verse_id, status="skipped")

    # Check shutdown
    if shutdown_event.is_set():
        return VerseResult(verse_id=verse_id, status="skipped")

    # Track per-verse timing/cost for stats persistence
    fix_cr = None
    fix_needed = False
    fix_applied = False

    try:
        # Step 1: Prepare (0 tokens)
        plan = prepare_verse(verse_path, work_dir, data_dir=config.data_dir)
        if plan is None:
            stats.errors += 1
            config.event_log.log("VERSE_ERROR", verse_id=verse_id, error="verse not found")
            return VerseResult(verse_id=verse_id, status="error", error="verse not found")

        # Skip if word count exceeds limit
        if config.max_words and plan.word_count > config.max_words:
            stats.skipped += 1
            return VerseResult(verse_id=verse_id, status="skipped")

        if config.dry_run:
            logger.info("[DRY-RUN] %s: mode=%s, words=%d", verse_id, plan.mode, plan.word_count)
            stats.skipped += 1
            return VerseResult(verse_id=verse_id, status="skipped")

        config.event_log.log("VERSE_START", verse_id=verse_id,
                             words=plan.word_count, mode=plan.mode)

        # Step 2: Generate (acquire semaphore for Claude call, retry on malformed)
        cr = None
        for gen_attempt in range(2):  # 1 retry on malformed response
            async with semaphore:
                if shutdown_event.is_set():
                    return VerseResult(verse_id=verse_id, status="skipped")

                logger.info("GEN %s (%d words, %s)%s...", verse_id, plan.word_count, plan.mode,
                            " [retry]" if gen_attempt > 0 else "")
                cr = await call_claude(plan.system_prompt, plan.user_message, config.model)

            if "error" in cr:
                break  # real error, don't retry

            # Check for truncated/malformed response
            raw = cr.get("result", "").strip()
            # First check: raw response should exist and look like JSON or fenced JSON
            if not raw or (not raw.startswith("{") and not raw.startswith("`")):
                if gen_attempt == 0:
                    logger.warning("GEN %s: malformed response (starts with %r), retrying...",
                                   verse_id, raw[:50] if raw else "<empty>")
                    stats.total_cost += cr.get("cost", 0)
                    stats.total_output_tokens += cr.get("output_tokens", 0)
                    continue
            # Second check: if fenced, content inside fences must start with {
            elif raw.startswith("`"):
                from pipeline_cli.verse_processor import strip_code_fences
                inner = strip_code_fences(raw).strip()
                if not inner.startswith("{"):
                    if gen_attempt == 0:
                        logger.warning("GEN %s: non-JSON inside code fences (starts with %r), retrying...",
                                       verse_id, inner[:50] if inner else "<empty>")
                        stats.total_cost += cr.get("cost", 0)
                        stats.total_output_tokens += cr.get("output_tokens", 0)
                        continue
            break  # valid-looking response or final attempt

        if "error" in cr:
            stats.errors += 1
            logger.error("GEN %s FAILED: %s", verse_id, cr["error"][:100])
            config.event_log.log("VERSE_ERROR", verse_id=verse_id,
                                 error=cr["error"][:200], elapsed=cr.get("elapsed", 0))
            # Persist error stats
            save_verse_stats(
                verse_id, verse_path, config.stats_dir,
                status="error", word_count=plan.word_count, mode=plan.mode,
                model=config.model, gen_elapsed=cr.get("elapsed", 0),
                system_prompt_hash=config.system_prompt_hash,
                error=cr["error"],
            )
            # Quarantine if too many failures
            failure_count = get_failure_count(verse_id, config.stats_dir)
            if failure_count >= config.max_failures:
                quarantine_verse(verse_id, cr["error"], responses_dir)
                logger.warning("QUARANTINED %s after %d failures", verse_id, failure_count)
                config.event_log.log("VERSE_QUARANTINED", verse_id=verse_id,
                                     failure_count=failure_count)
            return VerseResult(verse_id=verse_id, status="error", error=cr["error"])

        raw_response_str = cr["result"]

        # Save raw response (work dir + permanent archive)
        os.makedirs(work_dir, exist_ok=True)  # defensive re-create in case of race
        raw_path = os.path.abspath(os.path.join(work_dir, "raw_response.txt"))
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(raw_response_str)
        logger.info("WROTE %s", raw_path)
        # Archive to permanent dir
        raw_archive_dir = os.path.join(os.path.dirname(responses_dir), "raw_responses")
        os.makedirs(raw_archive_dir, exist_ok=True)
        raw_archive_path = os.path.abspath(os.path.join(raw_archive_dir, f"{verse_id}.raw.txt"))
        with open(raw_archive_path, "w", encoding="utf-8") as f:
            f.write(raw_response_str)
        logger.info("WROTE %s", raw_archive_path)

        # Track costs
        stats.total_cost += cr.get("cost", 0)
        stats.total_output_tokens += cr.get("output_tokens", 0)
        stats.total_elapsed += cr.get("elapsed", 0)

        # Step 3: Postprocess (0 tokens)
        result = postprocess_verse(
            plan=plan,
            raw_response=raw_response_str,
            word_dict_data=word_dict,
            narrator_templates=narrator_tmpl,
            responses_dir=responses_dir,
            parsed_dict=None,
        )
        result.token_usage = {
            "output_tokens": cr.get("output_tokens", 0),
            "cost": cr.get("cost", 0),
            "elapsed": cr.get("elapsed", 0),
            "num_turns": cr.get("num_turns", 1),
        }

        # Step 4: Fix pass if needed
        if result.status == "needs_fix" and config.max_fix_attempts > 0:
            stats.needs_fix += 1
            fix_needed = True
            fix_system, fix_user = prepare_fix(plan, result)

            config.event_log.log("FIX_START", verse_id=verse_id,
                                 warnings=len([w for w in result.warnings
                                               if w.severity in ("high", "medium")]))

            async with semaphore:
                if shutdown_event.is_set():
                    return result

                logger.info("FIX %s (%d warnings)...", verse_id,
                            len([w for w in result.warnings if w.severity in ("high", "medium")]))
                fix_cr = await call_claude(
                    fix_system, fix_user, config.fix_model,
                    fallback_model="haiku",
                )

            if "error" not in fix_cr:
                # Archive fix raw response for debugging
                fix_raw_dir = os.path.join(os.path.dirname(responses_dir), "raw_responses")
                os.makedirs(fix_raw_dir, exist_ok=True)
                fix_raw_path = os.path.join(fix_raw_dir, f"{verse_id}.fix.raw.txt")
                with open(fix_raw_path, "w", encoding="utf-8") as f:
                    f.write(fix_cr.get("result", ""))

                # Pass original result for merge if fix returns partial corrections
                orig_result = result.result_dict
                if orig_result and "diacritized_text" not in orig_result:
                    from app.ai_pipeline import reconstruct_fields
                    orig_result = reconstruct_fields(orig_result)
                fix_result = apply_fix(
                    plan=plan,
                    fix_response=fix_cr["result"],
                    word_dict_data=word_dict,
                    narrator_templates=narrator_tmpl,
                    responses_dir=responses_dir,
                    original_result=orig_result,
                )
                stats.total_cost += fix_cr.get("cost", 0)
                stats.total_output_tokens += fix_cr.get("output_tokens", 0)

                if fix_result.status == "pass":
                    stats.fixed += 1
                    fix_applied = True
                    result = fix_result
                    if fix_result.false_positive_accepted:
                        logger.info("FIX %s -> PASS (false positives accepted)", verse_id)
                        config.event_log.log("FIX_DONE", verse_id=verse_id,
                                             outcome="false_positive_accepted",
                                             cost=fix_cr.get("cost", 0),
                                             elapsed=fix_cr.get("elapsed", 0))
                    else:
                        logger.info("FIX %s -> PASS", verse_id)
                        config.event_log.log("FIX_DONE", verse_id=verse_id, outcome="fixed",
                                             cost=fix_cr.get("cost", 0),
                                             elapsed=fix_cr.get("elapsed", 0))
                else:
                    fix_val_errs = len(fix_result.validation_errors)
                    fix_hm_warns = len([w for w in fix_result.warnings
                                        if w.severity in ("high", "medium")])
                    logger.warning("FIX %s -> %s (%d validation errors, %d high/med warnings)",
                                   verse_id, fix_result.status, fix_val_errs, fix_hm_warns)
                    config.event_log.log("FIX_DONE", verse_id=verse_id, outcome="still_failing",
                                         validation_errors=fix_val_errs, warnings_hm=fix_hm_warns)
            else:
                config.event_log.log("FIX_ERROR", verse_id=verse_id,
                                     error=fix_cr.get("error", "")[:200])

        # Update stats
        if result.status == "pass":
            stats.passed += 1
        elif result.status == "error":
            stats.errors += 1
        stats.completed += 1

        status_icon = {"pass": "OK", "needs_fix": "FIX", "error": "ERR"}.get(result.status, "??")
        logger.info("%s %s [%.0fs, $%.4f, %d tok]",
                    status_icon, verse_id,
                    cr.get("elapsed", 0), cr.get("cost", 0), cr.get("output_tokens", 0))

        # Extract content metadata from result for stats
        content_type = ""
        has_chain = False
        if result.result_dict:
            content_type = result.result_dict.get("content_type", "")
            has_chain = result.result_dict.get("isnad_matn", {}).get("has_chain", False)

        # Count warnings by severity
        w_high = len([w for w in result.warnings if w.severity == "high"])
        w_med = len([w for w in result.warnings if w.severity == "medium"])
        w_low = len([w for w in result.warnings if w.severity == "low"])

        # Persist per-verse stats
        save_verse_stats(
            verse_id, verse_path, config.stats_dir,
            status=result.status,
            word_count=plan.word_count,
            mode=plan.mode,
            model=config.model,
            fix_model=config.fix_model if fix_needed else None,
            gen_cost=cr.get("cost", 0),
            gen_output_tokens=cr.get("output_tokens", 0),
            gen_elapsed=cr.get("elapsed", 0),
            gen_turns=cr.get("num_turns", 1),
            fix_cost=fix_cr.get("cost", 0) if fix_cr and "error" not in fix_cr else 0,
            fix_output_tokens=fix_cr.get("output_tokens", 0) if fix_cr and "error" not in fix_cr else 0,
            fix_elapsed=fix_cr.get("elapsed", 0) if fix_cr and "error" not in fix_cr else 0,
            fix_needed=fix_needed,
            fix_applied=fix_applied,
            validation_errors=result.validation_errors,
            warnings_high=w_high,
            warnings_medium=w_med,
            warnings_low=w_low,
            content_type=content_type,
            has_chain=has_chain,
            system_prompt_hash=config.system_prompt_hash,
            false_positive_accepted=result.false_positive_accepted,
        )

        config.event_log.log(
            "VERSE_DONE", verse_id=verse_id, status=result.status,
            words=plan.word_count, cost=cr.get("cost", 0),
            elapsed=cr.get("elapsed", 0), tokens=cr.get("output_tokens", 0),
            fix=fix_needed, fixed=fix_applied,
            warnings_h=w_high, warnings_m=w_med, warnings_l=w_low,
        )

        return result

    except Exception as e:
        stats.errors += 1
        logger.exception("CRASH %s: %s", verse_id, e)
        config.event_log.log("VERSE_CRASH", verse_id=verse_id, error=str(e)[:300])
        return VerseResult(verse_id=verse_id, status="error", error=str(e))

    finally:
        # Clean up tmp dir
        if os.path.exists(work_dir) and not config.dry_run:
            try:
                shutil.rmtree(work_dir)
            except OSError:
                pass


async def progress_reporter(stats: SessionStats, config: PipelineConfig):
    """Periodically check progress and print only when something changes."""
    last_snapshot = (0, 0, 0, 0, 0)  # completed, passed, fixed, errors, skipped
    while not shutdown_event.is_set():
        await asyncio.sleep(config.progress_interval)
        current = (stats.completed, stats.passed, stats.fixed, stats.errors, stats.skipped)
        if current == last_snapshot:
            continue
        last_snapshot = current

        elapsed_min = (time.time() - stats.started_at) / 60
        rate = stats.completed / (elapsed_min / 60) if elapsed_min > 1 else 0
        remaining = stats.total_queued - stats.completed - stats.skipped
        eta_hours = remaining / rate if rate > 0 else 0

        print(
            f"\n--- Progress [{elapsed_min:.0f}m] ---\n"
            f"  Done: {stats.completed}/{stats.total_queued} "
            f"(pass={stats.passed}, fix={stats.needs_fix}, fixed={stats.fixed}, err={stats.errors})\n"
            f"  Rate: {rate:.0f}/hr | ETA: {eta_hours:.1f}h\n"
            f"  Cost: ${stats.total_cost:.2f} | Tokens: {stats.total_output_tokens:,}\n"
            f"---",
            flush=True,
        )


async def run_pipeline(config: PipelineConfig, verse_paths: List[str]):
    """Main pipeline entry point."""
    responses_dir = config.responses_dir or AI_RESPONSES_DIR
    content_dir = os.path.dirname(responses_dir)  # e.g. ai-content/corpus/

    # Set up persistence directories alongside responses/
    config.stats_dir = os.path.join(content_dir, "stats")
    config.logs_dir = os.path.join(content_dir, "logs")
    config.sessions_dir = os.path.join(content_dir, "sessions")
    config.prompts_dir = os.path.join(content_dir, "prompts")
    for d in (config.stats_dir, config.logs_dir, config.sessions_dir, config.prompts_dir):
        os.makedirs(d, exist_ok=True)

    # Session ID for this run
    config.session_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Per-run log file (human-readable, one file per session)
    # Both logging and print() output are captured to the same file.
    run_log_path = os.path.abspath(os.path.join(config.logs_dir, f"{config.session_id}.log"))
    run_log_file = open(run_log_path, "w", encoding="utf-8")
    # Use a StreamHandler pointing at the same file so logger + print share one file
    file_handler = logging.StreamHandler(run_log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
    logging.getLogger().addHandler(file_handler)
    # Tee stdout so print() output (progress, summary) also goes to the log file
    original_stdout = sys.stdout
    sys.stdout = TeeWriter(original_stdout, run_log_file)
    logger.info("Run log: %s", run_log_path)

    # Event log (append-only JSONL)
    if not config.dry_run:
        config.event_log = EventLog(config.logs_dir)
    else:
        config.event_log = NullEventLog()

    # Load dictionaries and schema once
    word_dict = load_word_dictionary()
    narrator_tmpl = load_narrator_templates()
    logger.info("Word dictionary: %d entries", len(word_dict.get("words", {})) if word_dict else 0)
    logger.info("Narrator templates: %d entries", len(narrator_tmpl.get("narrators", {})) if narrator_tmpl else 0)

    # Build system prompt once and archive it
    # (prepare_verse builds it per-call but it's identical for all — save once for audit)
    from app.ai_pipeline import build_system_prompt
    sample_prompt = build_system_prompt(few_shot_examples={"examples": []})
    prompt_hash = hashlib.sha256(sample_prompt.encode("utf-8")).hexdigest()[:16]
    config.system_prompt_hash = prompt_hash
    prompt_archive_path = os.path.join(config.prompts_dir, f"system_prompt_{prompt_hash}.txt")
    prompt_archive_path = os.path.abspath(prompt_archive_path)
    if not os.path.exists(prompt_archive_path):
        with open(prompt_archive_path, "w", encoding="utf-8") as f:
            f.write(sample_prompt)
        logger.info("WROTE %s (system prompt, %d chars)", prompt_archive_path, len(sample_prompt))

    # Recover stale work dirs from crashed sessions
    recovered = recover_stale_work_dirs(config.tmp_dir, responses_dir)
    if recovered:
        logger.info("Recovered %d verses from stale work dirs", len(recovered))
        # Add recovered paths to verse_paths (dedup)
        existing = set(verse_paths)
        for vp in recovered:
            if vp not in existing:
                verse_paths.append(vp)

    # Build queue (filters out already-completed verses)
    queue = build_queue(verse_paths, responses_dir,
                        attempt_quarantined=config.attempt_quarantined)
    if config.max_verses:
        queue = queue[:config.max_verses]
    if not queue:
        print("No verses to process — all complete or filtered out.", flush=True)
        config.event_log.close()
        logging.getLogger().removeHandler(file_handler)
        file_handler.close()
        sys.stdout = original_stdout
        run_log_file.close()
        print(f"Run log: {run_log_path}")
        return

    stats = SessionStats(total_queued=len(queue))
    semaphore = asyncio.Semaphore(config.workers)

    print(f"Pipeline v3 starting: {len(queue)} verses, {config.workers} workers, model={config.model}", flush=True)
    if config.dry_run:
        print("DRY RUN — no Claude calls will be made.", flush=True)

    config.event_log.log("SESSION_START",
                         session_id=config.session_id,
                         workers=config.workers, model=config.model,
                         fix_model=config.fix_model,
                         queue_size=len(queue),
                         max_words=config.max_words,
                         system_prompt_hash=prompt_hash)

    # Start progress reporter
    progress_task = asyncio.create_task(progress_reporter(stats, config))

    # Process all verses
    tasks = [
        process_verse(vp, config, semaphore, stats, word_dict, narrator_tmpl)
        for vp in queue
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Stop progress reporter
    progress_task.cancel()
    try:
        await progress_task
    except asyncio.CancelledError:
        pass

    # Final summary
    elapsed_min = (time.time() - stats.started_at) / 60
    print(f"\n{'=' * 60}", flush=True)
    print(f"Pipeline Complete ({elapsed_min:.1f} min)", flush=True)
    print(f"  Total: {stats.completed} processed, {stats.skipped} skipped", flush=True)
    print(f"  Pass: {stats.passed} | Fixed: {stats.fixed} | Errors: {stats.errors}", flush=True)
    print(f"  Cost: ${stats.total_cost:.2f} | Tokens: {stats.total_output_tokens:,}", flush=True)
    print(f"{'=' * 60}", flush=True)

    # Build session summary dict
    session_data = {
        "session_id": config.session_id,
        "started_at": datetime.fromtimestamp(stats.started_at, timezone.utc).isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_minutes": round(elapsed_min, 1),
        "total_queued": stats.total_queued,
        "completed": stats.completed,
        "skipped": stats.skipped,
        "passed": stats.passed,
        "needs_fix": stats.needs_fix,
        "fixed": stats.fixed,
        "errors": stats.errors,
        "total_cost_usd": round(stats.total_cost, 4),
        "total_output_tokens": stats.total_output_tokens,
        "total_elapsed_s": round(stats.total_elapsed, 1),
        "avg_cost_per_verse": round(stats.total_cost / stats.completed, 4) if stats.completed else 0,
        "avg_elapsed_per_verse": round(stats.total_elapsed / stats.completed, 1) if stats.completed else 0,
        "rate_per_hour": round(stats.completed / (elapsed_min / 60), 1) if elapsed_min > 1 else 0,
        "config": {
            "workers": config.workers,
            "model": config.model,
            "fix_model": config.fix_model,
            "max_words": config.max_words,
            "max_verses": config.max_verses,
            "dry_run": config.dry_run,
            "system_prompt_hash": config.system_prompt_hash,
        },
    }

    # Save to latest session pointer (for quick status checks)
    session_path = os.path.abspath(os.path.join(config.tmp_dir, "pipeline_session.json"))
    os.makedirs(config.tmp_dir, exist_ok=True)
    with open(session_path, "w", encoding="utf-8") as f:
        json.dump(session_data, f, indent=2)
    logger.info("WROTE %s", session_path)

    # Also save to sessions/ history (never overwritten)
    history_path = os.path.abspath(os.path.join(config.sessions_dir, f"{config.session_id}.json"))
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(session_data, f, indent=2)
    logger.info("WROTE %s", history_path)

    config.event_log.log("SESSION_END", session_id=config.session_id,
                         completed=stats.completed, passed=stats.passed,
                         fixed=stats.fixed, errors=stats.errors,
                         cost=round(stats.total_cost, 4),
                         elapsed_min=round(elapsed_min, 1))
    config.event_log.close()

    # Close per-run log file and restore stdout
    logger.info("Run log saved: %s", run_log_path)
    logging.getLogger().removeHandler(file_handler)
    file_handler.close()
    sys.stdout = original_stdout
    run_log_file.close()
    print(f"Run log: {run_log_path}")


def main():
    parser = argparse.ArgumentParser(description="Pipeline v3 — AI content generation orchestrator")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Concurrent Claude calls")
    parser.add_argument("--model", default="sonnet", help="Model for generation (default: sonnet)")
    parser.add_argument("--fix-model", default="sonnet", help="Model for fix pass (default: sonnet)")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR, help="ThaqalaynData directory")
    parser.add_argument("--tmp-dir", default=DEFAULT_TMP_DIR, help="Temp working directory")
    parser.add_argument("--responses-dir", default=None, help="Override responses output directory")
    parser.add_argument("--dry-run", action="store_true", help="Prepare only, no Claude calls")
    parser.add_argument("--single", type=str, help="Process a single verse path")
    parser.add_argument("--book", type=str, help="Filter to specific book(s), comma-separated (e.g., al-kafi,al-istibsar)")
    parser.add_argument("--volume", type=int, help="Filter to specific volume")
    parser.add_argument("--max-verses", type=int, help="Limit number of verses to process")
    parser.add_argument("--max-words", type=int, help="Skip verses with more than N Arabic words (filters out long hadiths)")
    parser.add_argument("--max-failures", type=int, default=3, help="Quarantine verse after N cumulative failures (default: 3)")
    parser.add_argument("--attempt-quarantined", action="store_true", help="Include quarantined verses in queue (default: skip them)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    sys.stdout.reconfigure(encoding="utf-8")

    # Setup signal handlers
    setup_signal_handlers()

    config = PipelineConfig(
        workers=args.workers,
        model=args.model,
        fix_model=args.fix_model,
        data_dir=args.data_dir,
        tmp_dir=args.tmp_dir,
        responses_dir=args.responses_dir,
        dry_run=args.dry_run,
        max_words=args.max_words,
        max_verses=args.max_verses,
        max_failures=args.max_failures,
        attempt_quarantined=args.attempt_quarantined,
    )

    # Load verse paths
    if args.single:
        verse_paths = [args.single]
    else:
        verse_paths = load_corpus_manifest()
        if not verse_paths:
            print("ERROR: No verses in corpus manifest.", flush=True)
            sys.exit(1)

    # Apply book/volume filter
    if args.book or args.volume is not None:
        books = [b.strip() for b in args.book.split(",")] if args.book else []
        verse_paths = [
            vp for vp in verse_paths
            if (not books or any(vp.startswith(f"/books/{b}:") for b in books))
            and (args.volume is None or vp.replace("/books/", "").split(":")[1] == str(args.volume)
                 if len(vp.replace("/books/", "").split(":")) >= 2 else True)
        ]

    print(f"Corpus: {len(verse_paths)} verses", flush=True)

    # Set env vars
    os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")

    asyncio.run(run_pipeline(config, verse_paths))


if __name__ == "__main__":
    main()
