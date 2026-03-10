"""OpenAI Batch API backend for the pipeline.

Provides submit/status/download commands for asynchronous batch processing
at 50% cost discount. State is persisted to disk so operations survive
machine restarts.

Security: OPENAI_API_KEY is read from environment only, never written to
any file (state, logs, JSONL). The key is required at submit and download
time but not for status checks (batch_id is sufficient).

Usage:
    # Phase 1: Submit generation batch
    python -m app.pipeline_cli.pipeline batch submit --book al-kafi --volume 1

    # Phase 2: Check status (no API key needed for display, key needed for API call)
    python -m app.pipeline_cli.pipeline batch status

    # Phase 3: Download results, postprocess, identify fixes
    python -m app.pipeline_cli.pipeline batch download

    # Phase 4: Submit fix batch (auto-detected from download results)
    python -m app.pipeline_cli.pipeline batch submit-fixes

    # Phase 5: Download fix results
    python -m app.pipeline_cli.pipeline batch download-fixes
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Batch API pricing is 50% of standard
BATCH_DISCOUNT = 0.5

# Default state directory — alongside responses
DEFAULT_BATCH_DIR = "batches"


def _get_sync_client():
    """Get synchronous OpenAI client. API key from environment only."""
    try:
        from openai import OpenAI
    except ImportError:
        print("ERROR: openai package not installed. Install with: pip install openai", flush=True)
        sys.exit(1)

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print(
            "ERROR: OPENAI_API_KEY environment variable not set.\n"
            "Set it for this session only (not persisted):\n"
            "  export OPENAI_API_KEY=sk-...   (bash)\n"
            "  $env:OPENAI_API_KEY='sk-...'   (PowerShell)\n"
            "\n"
            "Get your key from: https://platform.openai.com/api-keys\n"
            "SECURITY: Never put your API key in files that could be committed to git.",
            flush=True,
        )
        sys.exit(1)

    return OpenAI(api_key=api_key, max_retries=3, timeout=120.0)


def _get_batch_dir(responses_dir: str) -> str:
    """Get the batch state directory (sibling of responses/)."""
    content_dir = os.path.dirname(responses_dir)
    batch_dir = os.path.join(content_dir, DEFAULT_BATCH_DIR)
    os.makedirs(batch_dir, exist_ok=True)
    return batch_dir


def _get_state_path(batch_dir: str, phase: str = "generation") -> str:
    """Get the path to the active batch state file for a phase."""
    return os.path.join(batch_dir, f"batch_state_{phase}.json")


def _load_state(batch_dir: str, phase: str = "generation") -> Optional[dict]:
    """Load batch state from disk. Returns None if no active batch."""
    path = _get_state_path(batch_dir, phase)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_state(batch_dir: str, state: dict, phase: str = "generation") -> str:
    """Save batch state to disk. Returns path written."""
    path = os.path.abspath(_get_state_path(batch_dir, phase))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    logger.info("WROTE %s", path)
    return path


def _archive_state(batch_dir: str, state: dict, phase: str = "generation") -> str:
    """Archive completed batch state (never overwritten)."""
    archive_dir = os.path.join(batch_dir, "history")
    os.makedirs(archive_dir, exist_ok=True)
    batch_id = state.get("batch_id", "unknown")
    path = os.path.abspath(os.path.join(archive_dir, f"{batch_id}_{phase}.json"))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    logger.info("WROTE %s (archived)", path)
    return path


# ---------------------------------------------------------------------------
# Submit
# ---------------------------------------------------------------------------

def batch_submit(
    verse_paths: List[str],
    model: str,
    responses_dir: str,
    data_dir: str,
    use_v3: bool = False,
    max_verses: Optional[int] = None,
    max_words: Optional[int] = None,
    attempt_quarantined: bool = False,
) -> None:
    """Prepare all verses, write JSONL, upload to OpenAI, create batch."""
    from app.pipeline_cli.verse_processor import (
        is_complete,
        prepare_verse,
        verse_path_to_id,
    )
    from app.pipeline_cli.pipeline import is_quarantined

    batch_dir = _get_batch_dir(responses_dir)

    # Check for existing active batch
    existing = _load_state(batch_dir, "generation")
    if existing and existing.get("status") not in ("completed", "failed", "expired", "cancelled", "downloaded"):
        print(
            f"ERROR: Active generation batch already exists: {existing.get('batch_id')}\n"
            f"  Status: {existing.get('status')}\n"
            f"  Use 'batch status' to check progress, or 'batch download' when complete.\n"
            f"  Delete {_get_state_path(batch_dir, 'generation')} to force a new batch.",
            flush=True,
        )
        return

    # Filter verse paths
    queue = []
    for vp in verse_paths:
        vid = vp.replace("/books/", "").replace(":", "_")
        if is_complete(vid, responses_dir):
            continue
        if not attempt_quarantined and is_quarantined(vid, responses_dir):
            continue
        queue.append(vp)

    if max_verses:
        queue = queue[:max_verses]

    if not queue:
        print("No verses to process — all complete or filtered out.", flush=True)
        return

    print(f"Preparing {len(queue)} verses for batch submission...", flush=True)

    # Prepare all verse plans and build JSONL
    tmp_dir = os.path.join(batch_dir, "tmp_prepare")
    os.makedirs(tmp_dir, exist_ok=True)

    jsonl_lines = []
    verse_mapping = {}  # custom_id -> verse_path
    skipped = 0

    for i, vp in enumerate(queue):
        vid = vp.replace("/books/", "").replace(":", "_")
        work_dir = os.path.join(tmp_dir, vid)

        plan = prepare_verse(vp, work_dir, data_dir=data_dir, use_v3=use_v3)
        if plan is None:
            skipped += 1
            continue

        if max_words and plan.word_count > max_words:
            skipped += 1
            continue

        custom_id = f"gen-{vid}"
        verse_mapping[custom_id] = vp

        # Reasoning models (gpt-5, o3, o4) use different API parameters
        is_reasoning = model.startswith(("gpt-5", "o3", "o4"))
        if is_reasoning:
            request_body = {
                "model": model,
                "max_completion_tokens": 40000,
                "messages": [
                    {"role": "developer", "content": plan.system_prompt},
                    {"role": "user", "content": plan.user_message},
                ],
            }
        else:
            request_body = {
                "model": model,
                "temperature": 0.0,
                "max_tokens": 40000,
                "messages": [
                    {"role": "system", "content": plan.system_prompt},
                    {"role": "user", "content": plan.user_message},
                ],
            }

        jsonl_lines.append(json.dumps({
            "custom_id": custom_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": request_body,
        }, ensure_ascii=False))

        if (i + 1) % 100 == 0:
            print(f"  Prepared {i + 1}/{len(queue)}...", flush=True)

    if not jsonl_lines:
        print(f"No verses to submit (skipped {skipped}).", flush=True)
        return

    print(f"Prepared {len(jsonl_lines)} requests ({skipped} skipped).", flush=True)

    # Write JSONL file
    jsonl_path = os.path.abspath(os.path.join(batch_dir, "batch_generation.jsonl"))
    with open(jsonl_path, "w", encoding="utf-8") as f:
        f.write("\n".join(jsonl_lines) + "\n")
    jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
    print(f"Wrote {jsonl_path} ({jsonl_size_mb:.1f} MB, {len(jsonl_lines)} requests)", flush=True)

    # Upload to OpenAI
    print("Uploading to OpenAI...", flush=True)
    client = _get_sync_client()

    with open(jsonl_path, "rb") as f:
        file_obj = client.files.create(file=f, purpose="batch")
    print(f"Uploaded: file_id={file_obj.id}", flush=True)

    # Create batch
    batch = client.batches.create(
        input_file_id=file_obj.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"pipeline": "thaqalayn", "phase": "generation", "model": model},
    )
    print(f"Batch created: batch_id={batch.id}", flush=True)

    # Save state
    state = {
        "batch_id": batch.id,
        "input_file_id": file_obj.id,
        "phase": "generation",
        "model": model,
        "use_v3": use_v3,
        "data_dir": data_dir,
        "responses_dir": responses_dir,
        "status": batch.status,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "request_count": len(jsonl_lines),
        "verse_mapping": verse_mapping,
        "jsonl_path": jsonl_path,
    }
    state_path = _save_state(batch_dir, state, "generation")

    # Clean up tmp prepare dir
    import shutil
    shutil.rmtree(tmp_dir, ignore_errors=True)

    print(f"\nBatch submitted successfully!", flush=True)
    print(f"  Batch ID: {batch.id}", flush=True)
    print(f"  Requests: {len(jsonl_lines)}", flush=True)
    print(f"  Model: {model}", flush=True)
    print(f"  State: {state_path}", flush=True)
    print(f"\nOpenAI will process within 24 hours at 50% cost discount.", flush=True)
    print(f"Check progress: python -m app.pipeline_cli.pipeline batch status", flush=True)


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def batch_status(responses_dir: str, phase: Optional[str] = None) -> None:
    """Check and display batch status. Queries OpenAI API if key is available."""
    batch_dir = _get_batch_dir(responses_dir)

    phases = [phase] if phase else ["generation", "fix"]
    found_any = False

    for ph in phases:
        state = _load_state(batch_dir, ph)
        if not state:
            continue
        found_any = True

        print(f"\n{'=' * 50}", flush=True)
        print(f"Batch: {ph.upper()}", flush=True)
        print(f"  Batch ID:  {state.get('batch_id', 'N/A')}", flush=True)
        print(f"  Model:     {state.get('model', 'N/A')}", flush=True)
        print(f"  Requests:  {state.get('request_count', 'N/A')}", flush=True)
        print(f"  Created:   {state.get('created_at', 'N/A')}", flush=True)
        print(f"  Status:    {state.get('status', 'unknown')}", flush=True)

        # Show download stats if available
        dl = state.get("download_stats")
        if dl:
            print(f"  Downloaded: {dl.get('downloaded_at', 'N/A')}", flush=True)
            print(f"  Passed: {dl.get('passed', 0)} | Needs fix: {dl.get('needs_fix', 0)} | "
                  f"Errors: {dl.get('errors', 0)}", flush=True)
            print(f"  Cost: ${dl.get('total_cost', 0):.4f} (batch 50% discount)", flush=True)

        # Try to query OpenAI for live status
        api_key = os.environ.get("OPENAI_API_KEY")
        if api_key and state.get("status") not in ("downloaded", "completed_and_processed"):
            try:
                from openai import OpenAI
                client = OpenAI(api_key=api_key, timeout=30.0)
                batch = client.batches.retrieve(state["batch_id"])

                # Update local state
                state["status"] = batch.status
                rc = batch.request_counts
                state["request_counts"] = {
                    "total": rc.total if rc else 0,
                    "completed": rc.completed if rc else 0,
                    "failed": rc.failed if rc else 0,
                }
                if batch.output_file_id:
                    state["output_file_id"] = batch.output_file_id
                if batch.error_file_id:
                    state["error_file_id"] = batch.error_file_id

                _save_state(batch_dir, state, ph)

                print(f"  [Live] Status: {batch.status}", flush=True)
                if rc:
                    print(f"  [Live] Progress: {rc.completed}/{rc.total} completed, "
                          f"{rc.failed} failed", flush=True)

                if batch.status == "completed":
                    print(f"\n  Ready to download! Run:", flush=True)
                    dl_cmd = "download-fixes" if ph == "fix" else "download"
                    print(f"    python -m app.pipeline_cli.pipeline batch {dl_cmd}", flush=True)
                elif batch.status in ("failed", "expired", "cancelled"):
                    print(f"\n  Batch {batch.status}. Check OpenAI dashboard for details.", flush=True)

            except Exception as e:
                print(f"  [API check failed: {e}]", flush=True)
        elif not api_key:
            print(f"  (Set OPENAI_API_KEY to get live status from API)", flush=True)

    if not found_any:
        print("No active batches found.", flush=True)
        print(f"  State dir: {batch_dir}", flush=True)
        print(f"  Submit with: python -m app.pipeline_cli.pipeline batch submit --book ...", flush=True)


# ---------------------------------------------------------------------------
# Download generation results
# ---------------------------------------------------------------------------

def batch_download(responses_dir: str) -> None:
    """Download generation batch results, postprocess, identify fixes needed."""
    from app.pipeline_cli.openai_backend import compute_cost, BATCH_DISCOUNT
    from app.pipeline_cli.verse_processor import (
        VersePlan,
        postprocess_verse,
        prepare_verse,
        verse_path_to_id,
        load_word_dictionary as load_word_dict_vp,
        load_narrator_templates as load_narrator_tmpl_vp,
    )

    batch_dir = _get_batch_dir(responses_dir)
    state = _load_state(batch_dir, "generation")
    if not state:
        print("ERROR: No generation batch state found. Submit a batch first.", flush=True)
        return

    # Refresh status from API
    client = _get_sync_client()
    batch = client.batches.retrieve(state["batch_id"])
    state["status"] = batch.status

    if batch.status != "completed":
        rc = batch.request_counts
        if rc:
            print(f"Batch not ready. Status: {batch.status} "
                  f"({rc.completed}/{rc.total} completed, {rc.failed} failed)", flush=True)
        else:
            print(f"Batch not ready. Status: {batch.status}", flush=True)

        if batch.status in ("failed", "expired", "cancelled"):
            print(f"Batch {batch.status}. You may need to resubmit.", flush=True)
            state["status"] = batch.status
            _save_state(batch_dir, state, "generation")
        return

    # Download output file
    output_file_id = batch.output_file_id
    if not output_file_id:
        print("ERROR: Batch completed but no output file available.", flush=True)
        return

    print(f"Downloading results from {output_file_id}...", flush=True)
    output_content = client.files.content(output_file_id).content
    output_path = os.path.abspath(os.path.join(batch_dir, "batch_generation_output.jsonl"))
    with open(output_path, "wb") as f:
        f.write(output_content)
    print(f"Wrote {output_path} ({len(output_content) / 1024:.0f} KB)", flush=True)

    # Download error file if present
    error_file_id = batch.error_file_id
    errors_data = []
    if error_file_id:
        error_content = client.files.content(error_file_id).content
        error_path = os.path.abspath(os.path.join(batch_dir, "batch_generation_errors.jsonl"))
        with open(error_path, "wb") as f:
            f.write(error_content)
        for line in error_content.decode("utf-8", errors="replace").strip().split("\n"):
            if line.strip():
                errors_data.append(json.loads(line))
        print(f"Wrote {error_path} ({len(errors_data)} errors)", flush=True)

    # Parse output
    results = []
    with open(output_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                results.append(json.loads(line))

    print(f"Processing {len(results)} results...", flush=True)

    # Load verse mapping and dictionaries
    verse_mapping = state.get("verse_mapping", {})
    word_dict = load_word_dict_vp()
    narrator_tmpl = load_narrator_tmpl_vp()

    # Process each result
    model = state.get("model", "gpt-4.1-mini")
    data_dir = state.get("data_dir", "../ThaqalaynData/")
    use_v3 = state.get("use_v3", False)
    total_cost = 0.0
    total_input_tokens = 0
    total_output_tokens = 0
    passed = 0
    needs_fix = 0
    processing_errors = 0
    fix_candidates = {}  # custom_id -> {verse_path, warnings}

    for res in results:
        custom_id = res.get("custom_id", "")
        verse_path = verse_mapping.get(custom_id)
        if not verse_path:
            logger.warning("Unknown custom_id in output: %s", custom_id)
            processing_errors += 1
            continue

        verse_id = verse_path_to_id(verse_path)
        response = res.get("response", {})
        status_code = response.get("status_code", 0)
        body = response.get("body", {})

        if status_code != 200:
            logger.warning("API error for %s: status=%d, body=%s",
                           verse_id, status_code, str(body)[:200])
            processing_errors += 1
            continue

        # Extract usage for cost tracking
        usage = body.get("usage", {})
        input_tokens = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)
        cost = compute_cost(model, input_tokens, output_tokens) * BATCH_DISCOUNT
        total_cost += cost
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens

        # Extract response text
        choices = body.get("choices", [])
        if not choices:
            logger.warning("No choices for %s", verse_id)
            processing_errors += 1
            continue
        raw_response = choices[0].get("message", {}).get("content", "")

        # Prepare a minimal VersePlan for postprocessing
        work_dir = os.path.join(batch_dir, "tmp_process", verse_id)
        os.makedirs(work_dir, exist_ok=True)
        plan = prepare_verse(verse_path, work_dir, data_dir=data_dir, use_v3=use_v3)
        if plan is None:
            logger.warning("Cannot prepare verse for postprocessing: %s", verse_path)
            processing_errors += 1
            continue

        # Tag plan with backend info
        plan.backend = "openai"
        plan.model = model

        # Save raw response for archive
        raw_archive_dir = os.path.join(os.path.dirname(responses_dir), "raw_responses")
        os.makedirs(raw_archive_dir, exist_ok=True)
        raw_path = os.path.abspath(os.path.join(raw_archive_dir, f"{verse_id}.raw.txt"))
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(raw_response)

        # Postprocess
        result = postprocess_verse(
            plan=plan,
            raw_response=raw_response,
            word_dict_data=word_dict,
            narrator_templates=narrator_tmpl,
            responses_dir=responses_dir,
        )

        if result.status == "pass":
            passed += 1
        elif result.status == "needs_fix":
            needs_fix += 1
            fix_candidates[custom_id] = {
                "verse_path": verse_path,
                "warnings": [
                    {"field": w.field, "category": w.category,
                     "severity": w.severity, "message": w.message}
                    for w in result.warnings if w.severity in ("high", "medium")
                ],
            }
        else:
            processing_errors += 1

        # Clean up work dir
        import shutil
        shutil.rmtree(work_dir, ignore_errors=True)

    # Handle API-level errors
    for err in errors_data:
        processing_errors += 1
        cid = err.get("custom_id", "unknown")
        err_body = err.get("error") or err.get("response", {}).get("body", {}).get("error", {})
        logger.warning("Batch error for %s: %s", cid, str(err_body)[:200])

    # Clean up tmp_process dir
    tmp_process = os.path.join(batch_dir, "tmp_process")
    if os.path.exists(tmp_process):
        import shutil
        shutil.rmtree(tmp_process, ignore_errors=True)

    # Update state
    state["status"] = "downloaded"
    state["output_file_id"] = output_file_id
    state["download_stats"] = {
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "total_results": len(results),
        "passed": passed,
        "needs_fix": needs_fix,
        "errors": processing_errors,
        "total_cost": round(total_cost, 6),
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
    }
    if fix_candidates:
        state["fix_candidates"] = fix_candidates
    _save_state(batch_dir, state, "generation")
    _archive_state(batch_dir, state, "generation")

    # Summary
    print(f"\n{'=' * 60}", flush=True)
    print(f"Batch Download Complete", flush=True)
    print(f"  Results:  {len(results)} downloaded", flush=True)
    print(f"  Passed:   {passed}", flush=True)
    print(f"  Needs fix: {needs_fix}", flush=True)
    print(f"  Errors:   {processing_errors}", flush=True)
    print(f"  Cost:     ${total_cost:.4f} (batch 50% discount applied)", flush=True)
    print(f"  Tokens:   In: {total_input_tokens:,} | Out: {total_output_tokens:,}", flush=True)
    print(f"{'=' * 60}", flush=True)

    if fix_candidates:
        print(f"\n{len(fix_candidates)} verses need fixes. Submit fix batch with:", flush=True)
        print(f"  python -m app.pipeline_cli.pipeline batch submit-fixes", flush=True)


# ---------------------------------------------------------------------------
# Submit fixes
# ---------------------------------------------------------------------------

def batch_submit_fixes(responses_dir: str) -> None:
    """Submit a fix batch for verses that need fixes from generation download."""
    from app.pipeline_cli.verse_processor import (
        prepare_verse,
        prepare_fix,
        postprocess_verse,
        verse_path_to_id,
        load_word_dictionary as load_word_dict_vp,
        load_narrator_templates as load_narrator_tmpl_vp,
    )

    batch_dir = _get_batch_dir(responses_dir)

    # Check for existing active fix batch
    existing_fix = _load_state(batch_dir, "fix")
    if existing_fix and existing_fix.get("status") not in ("completed", "failed", "expired", "cancelled", "downloaded"):
        print(
            f"ERROR: Active fix batch already exists: {existing_fix.get('batch_id')}\n"
            f"  Status: {existing_fix.get('status')}\n"
            f"  Use 'batch status' to check progress, or 'batch download-fixes' when complete.",
            flush=True,
        )
        return

    # Load generation state to get fix candidates
    gen_state = _load_state(batch_dir, "generation")
    if not gen_state:
        print("ERROR: No generation batch state found. Run batch download first.", flush=True)
        return

    fix_candidates = gen_state.get("fix_candidates", {})
    if not fix_candidates:
        print("No verses need fixes. Generation batch fully passed.", flush=True)
        return

    model = gen_state.get("model", "gpt-4.1-mini")
    data_dir = gen_state.get("data_dir", "../ThaqalaynData/")
    use_v3 = gen_state.get("use_v3", False)

    print(f"Preparing {len(fix_candidates)} fix requests...", flush=True)

    word_dict = load_word_dict_vp()
    narrator_tmpl = load_narrator_tmpl_vp()

    jsonl_lines = []
    verse_mapping = {}
    skipped = 0

    for custom_id, info in fix_candidates.items():
        verse_path = info["verse_path"]
        verse_id = verse_path_to_id(verse_path)

        # Re-prepare the verse to get the plan
        work_dir = os.path.join(batch_dir, "tmp_fix_prepare", verse_id)
        os.makedirs(work_dir, exist_ok=True)
        plan = prepare_verse(verse_path, work_dir, data_dir=data_dir, use_v3=use_v3)
        if plan is None:
            skipped += 1
            continue

        plan.backend = "openai"
        plan.model = model

        # Load the raw response to re-postprocess and get the result for fix prompt
        raw_archive_dir = os.path.join(os.path.dirname(responses_dir), "raw_responses")
        raw_path = os.path.join(raw_archive_dir, f"{verse_id}.raw.txt")
        if not os.path.exists(raw_path):
            logger.warning("Raw response not found for %s, skipping fix", verse_id)
            skipped += 1
            continue

        with open(raw_path, "r", encoding="utf-8") as f:
            raw_response = f.read()

        # Re-postprocess to get the VerseResult with warnings
        result = postprocess_verse(
            plan=plan,
            raw_response=raw_response,
            word_dict_data=word_dict,
            narrator_templates=narrator_tmpl,
            responses_dir=responses_dir,
        )

        if result.status != "needs_fix":
            # May have passed on reprocessing (e.g. dictionary updates)
            skipped += 1
            continue

        # Build fix prompt
        fix_system, fix_user = prepare_fix(plan, result)

        fix_custom_id = f"fix-{verse_id}"
        verse_mapping[fix_custom_id] = verse_path

        # Reasoning models (gpt-5, o3, o4) use different API parameters
        is_reasoning = model.startswith(("gpt-5", "o3", "o4"))
        if is_reasoning:
            request_body = {
                "model": model,
                "max_completion_tokens": 40000,
                "messages": [
                    {"role": "developer", "content": fix_system},
                    {"role": "user", "content": fix_user},
                ],
            }
        else:
            request_body = {
                "model": model,
                "temperature": 0.0,
                "max_tokens": 40000,
                "messages": [
                    {"role": "system", "content": fix_system},
                    {"role": "user", "content": fix_user},
                ],
            }

        jsonl_lines.append(json.dumps({
            "custom_id": fix_custom_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": request_body,
        }, ensure_ascii=False))

    # Clean up tmp dir
    import shutil
    tmp_fix = os.path.join(batch_dir, "tmp_fix_prepare")
    if os.path.exists(tmp_fix):
        shutil.rmtree(tmp_fix, ignore_errors=True)

    if not jsonl_lines:
        print(f"No fix requests to submit ({skipped} skipped/resolved).", flush=True)
        return

    print(f"Prepared {len(jsonl_lines)} fix requests ({skipped} skipped).", flush=True)

    # Write JSONL
    jsonl_path = os.path.abspath(os.path.join(batch_dir, "batch_fix.jsonl"))
    with open(jsonl_path, "w", encoding="utf-8") as f:
        f.write("\n".join(jsonl_lines) + "\n")
    print(f"Wrote {jsonl_path}", flush=True)

    # Upload and create batch
    print("Uploading to OpenAI...", flush=True)
    client = _get_sync_client()

    with open(jsonl_path, "rb") as f:
        file_obj = client.files.create(file=f, purpose="batch")
    print(f"Uploaded: file_id={file_obj.id}", flush=True)

    batch = client.batches.create(
        input_file_id=file_obj.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"pipeline": "thaqalayn", "phase": "fix", "model": model},
    )
    print(f"Fix batch created: batch_id={batch.id}", flush=True)

    # Save state
    state = {
        "batch_id": batch.id,
        "input_file_id": file_obj.id,
        "phase": "fix",
        "model": model,
        "use_v3": use_v3,
        "data_dir": data_dir,
        "responses_dir": responses_dir,
        "status": batch.status,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "request_count": len(jsonl_lines),
        "verse_mapping": verse_mapping,
        "jsonl_path": jsonl_path,
        "generation_batch_id": gen_state.get("batch_id"),
    }
    _save_state(batch_dir, state, "fix")

    print(f"\nFix batch submitted!", flush=True)
    print(f"  Batch ID: {batch.id}", flush=True)
    print(f"  Requests: {len(jsonl_lines)}", flush=True)
    print(f"Check progress: python -m app.pipeline_cli.pipeline batch status", flush=True)


# ---------------------------------------------------------------------------
# Download fix results
# ---------------------------------------------------------------------------

def batch_download_fixes(responses_dir: str) -> None:
    """Download fix batch results, apply fixes, save final responses."""
    from app.pipeline_cli.openai_backend import compute_cost, BATCH_DISCOUNT
    from app.pipeline_cli.verse_processor import (
        apply_fix,
        postprocess_verse,
        prepare_verse,
        prepare_fix,
        verse_path_to_id,
        load_word_dictionary as load_word_dict_vp,
        load_narrator_templates as load_narrator_tmpl_vp,
    )

    batch_dir = _get_batch_dir(responses_dir)
    state = _load_state(batch_dir, "fix")
    if not state:
        print("ERROR: No fix batch state found. Submit fixes first.", flush=True)
        return

    # Refresh status
    client = _get_sync_client()
    batch = client.batches.retrieve(state["batch_id"])
    state["status"] = batch.status

    if batch.status != "completed":
        rc = batch.request_counts
        if rc:
            print(f"Fix batch not ready. Status: {batch.status} "
                  f"({rc.completed}/{rc.total} completed, {rc.failed} failed)", flush=True)
        else:
            print(f"Fix batch not ready. Status: {batch.status}", flush=True)
        return

    # Download output
    output_file_id = batch.output_file_id
    if not output_file_id:
        print("ERROR: Fix batch completed but no output file.", flush=True)
        return

    print(f"Downloading fix results from {output_file_id}...", flush=True)
    output_content = client.files.content(output_file_id).content
    output_path = os.path.abspath(os.path.join(batch_dir, "batch_fix_output.jsonl"))
    with open(output_path, "wb") as f:
        f.write(output_content)
    print(f"Wrote {output_path}", flush=True)

    # Download errors if present
    error_file_id = batch.error_file_id
    if error_file_id:
        error_content = client.files.content(error_file_id).content
        error_path = os.path.abspath(os.path.join(batch_dir, "batch_fix_errors.jsonl"))
        with open(error_path, "wb") as f:
            f.write(error_content)

    # Parse results
    results = []
    with open(output_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                results.append(json.loads(line))

    print(f"Processing {len(results)} fix results...", flush=True)

    verse_mapping = state.get("verse_mapping", {})
    model = state.get("model", "gpt-4.1-mini")
    data_dir = state.get("data_dir", "../ThaqalaynData/")
    use_v3 = state.get("use_v3", False)
    word_dict = load_word_dict_vp()
    narrator_tmpl = load_narrator_tmpl_vp()

    total_cost = 0.0
    total_input_tokens = 0
    total_output_tokens = 0
    fixed = 0
    still_failing = 0
    fix_errors = 0

    for res in results:
        custom_id = res.get("custom_id", "")
        verse_path = verse_mapping.get(custom_id)
        if not verse_path:
            fix_errors += 1
            continue

        verse_id = verse_path_to_id(verse_path)
        response = res.get("response", {})
        status_code = response.get("status_code", 0)
        body = response.get("body", {})

        if status_code != 200:
            fix_errors += 1
            continue

        # Cost tracking
        usage = body.get("usage", {})
        input_tokens = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)
        cost = compute_cost(model, input_tokens, output_tokens) * BATCH_DISCOUNT
        total_cost += cost
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens

        fix_response = body.get("choices", [{}])[0].get("message", {}).get("content", "")

        # Re-prepare original verse plan
        work_dir = os.path.join(batch_dir, "tmp_fix_apply", verse_id)
        os.makedirs(work_dir, exist_ok=True)
        plan = prepare_verse(verse_path, work_dir, data_dir=data_dir, use_v3=use_v3)
        if plan is None:
            fix_errors += 1
            continue

        plan.backend = "openai"
        plan.model = model

        # Load original raw response for context
        raw_archive_dir = os.path.join(os.path.dirname(responses_dir), "raw_responses")
        raw_path = os.path.join(raw_archive_dir, f"{verse_id}.raw.txt")
        if not os.path.exists(raw_path):
            fix_errors += 1
            continue

        with open(raw_path, "r", encoding="utf-8") as f:
            raw_response = f.read()

        # Postprocess original to get the result dict for merging
        orig_result = postprocess_verse(
            plan=plan,
            raw_response=raw_response,
            word_dict_data=word_dict,
            narrator_templates=narrator_tmpl,
            responses_dir=responses_dir,
        )
        original_dict = orig_result.result_dict
        if original_dict:
            from app.ai_pipeline import reconstruct_fields
            if "diacritized_text" not in original_dict:
                original_dict = reconstruct_fields(original_dict)

        # Apply fix
        fix_result = apply_fix(
            plan=plan,
            fix_response=fix_response,
            word_dict_data=word_dict,
            narrator_templates=narrator_tmpl,
            responses_dir=responses_dir,
            original_result=original_dict,
        )

        # Archive fix raw response
        fix_raw_path = os.path.join(raw_archive_dir, f"{verse_id}.fix.raw.txt")
        with open(fix_raw_path, "w", encoding="utf-8") as f:
            f.write(fix_response)

        if fix_result.status == "pass":
            fixed += 1
        else:
            still_failing += 1

        # Clean up
        import shutil
        shutil.rmtree(work_dir, ignore_errors=True)

    # Clean up tmp dir
    tmp_apply = os.path.join(batch_dir, "tmp_fix_apply")
    if os.path.exists(tmp_apply):
        import shutil
        shutil.rmtree(tmp_apply, ignore_errors=True)

    # Update state
    state["status"] = "downloaded"
    state["download_stats"] = {
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "total_results": len(results),
        "fixed": fixed,
        "still_failing": still_failing,
        "errors": fix_errors,
        "total_cost": round(total_cost, 6),
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
    }
    _save_state(batch_dir, state, "fix")
    _archive_state(batch_dir, state, "fix")

    # Combine with generation cost
    gen_state = _load_state(batch_dir, "generation")
    gen_cost = gen_state.get("download_stats", {}).get("total_cost", 0) if gen_state else 0

    print(f"\n{'=' * 60}", flush=True)
    print(f"Fix Batch Download Complete", flush=True)
    print(f"  Fixed:         {fixed}", flush=True)
    print(f"  Still failing: {still_failing}", flush=True)
    print(f"  Errors:        {fix_errors}", flush=True)
    print(f"  Fix cost:      ${total_cost:.4f}", flush=True)
    print(f"  Total cost:    ${gen_cost + total_cost:.4f} (gen + fix, batch discount)", flush=True)
    print(f"  Tokens:        In: {total_input_tokens:,} | Out: {total_output_tokens:,}", flush=True)
    print(f"{'=' * 60}", flush=True)


# ---------------------------------------------------------------------------
# CLI handler
# ---------------------------------------------------------------------------

def handle_batch_command(args) -> None:
    """Route batch subcommands."""
    from app.config import AI_RESPONSES_DIR

    os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")
    responses_dir = args.responses_dir or AI_RESPONSES_DIR
    subcmd = args.subcommand

    if subcmd == "submit":
        # Load verse paths
        from app.pipeline_cli.pipeline import load_corpus_manifest
        if args.single:
            verse_paths = [args.single]
        else:
            verse_paths = load_corpus_manifest()
            if not verse_paths:
                print("ERROR: No verses in corpus manifest.", flush=True)
                sys.exit(1)

        # Apply book/volume filters
        if args.book or args.volume is not None:
            books = [b.strip() for b in args.book.split(",")] if args.book else []
            verse_paths = [
                vp for vp in verse_paths
                if (not books or any(vp.startswith(f"/books/{b}:") for b in books))
                and (args.volume is None or
                     (len(vp.replace("/books/", "").split(":")) >= 2
                      and vp.replace("/books/", "").split(":")[1] == str(args.volume)))
            ]

        # Determine model
        model = args.openai_model
        if args.model != "sonnet":
            model = args.model

        batch_submit(
            verse_paths=verse_paths,
            model=model,
            responses_dir=responses_dir,
            data_dir=args.data_dir,
            use_v3=args.v3,
            max_verses=args.max_verses,
            max_words=args.max_words,
            attempt_quarantined=args.attempt_quarantined,
        )

    elif subcmd == "status":
        batch_status(responses_dir)

    elif subcmd == "download":
        batch_download(responses_dir)

    elif subcmd == "submit-fixes":
        batch_submit_fixes(responses_dir)

    elif subcmd == "download-fixes":
        batch_download_fixes(responses_dir)

    else:
        print("Usage: python -m app.pipeline_cli.pipeline batch <subcommand>", flush=True)
        print("", flush=True)
        print("Subcommands:", flush=True)
        print("  submit          Prepare verses and submit generation batch to OpenAI", flush=True)
        print("  status          Check batch status (generation and fix)", flush=True)
        print("  download        Download generation results, postprocess", flush=True)
        print("  submit-fixes    Submit fix batch for verses needing corrections", flush=True)
        print("  download-fixes  Download fix results, apply, save final responses", flush=True)
        print("", flush=True)
        print("Workflow:", flush=True)
        print("  1. batch submit --book al-kafi --volume 1", flush=True)
        print("  2. batch status  (repeat until completed)", flush=True)
        print("  3. batch download", flush=True)
        print("  4. batch submit-fixes  (if any verses need fixes)", flush=True)
        print("  5. batch status  (repeat until completed)", flush=True)
        print("  6. batch download-fixes", flush=True)
        print("", flush=True)
        print("API key: Set OPENAI_API_KEY environment variable (never stored on disk).", flush=True)
        print("  export OPENAI_API_KEY=sk-...   (bash)", flush=True)
        print("  $env:OPENAI_API_KEY='sk-...'   (PowerShell)", flush=True)
        sys.exit(1)
