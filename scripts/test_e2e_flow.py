"""End-to-end test of v3 pipeline: prepare → claude -p (sonnet) → postprocess.

Tests the full single-verse flow on one hadith to validate the pipeline works.
"""

import asyncio
import json
import os
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))
sys.stdout.reconfigure(encoding="utf-8")
os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")

from app.pipeline_cli.verse_processor import (
    prepare_verse, postprocess_verse, strip_code_fences,
    load_word_dictionary, load_narrator_templates,
)

CLAUDE_EXE = r"C:\Users\TrainingGR03\.local\bin\claude.exe"
DATA_DIR = "../ThaqalaynData/"
TMP_DIR = "tmp/e2e_test"
TEST_RESPONSES_DIR = "tmp/e2e_test_responses"

# Short hadith (14 words) to validate flow quickly
TEST_VERSE = "/books/al-kafi:1:1:1:3"


async def call_claude(
    system_prompt: str,
    user_message: str,
    model: str = "sonnet",
) -> dict:
    """Call claude -p and return parsed response."""
    cmd = [
        CLAUDE_EXE, "-p", "--model", model, "--output-format", "json",
        "--no-session-persistence", "--setting-sources", "",
        "--max-turns", "1",
        "--system-prompt", system_prompt,
    ]
    cmd.extend(["--fallback-model", "haiku"])
    cmd.extend(["--max-budget-usd", "5.00"])

    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    start = time.time()
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE, env=env,
    )
    stdout, stderr = await proc.communicate(input=user_message.encode("utf-8"))
    elapsed = time.time() - start

    if proc.returncode != 0 or not stdout:
        return {"error": stderr.decode("utf-8", errors="replace")[:200] if stderr else "no output"}

    data = json.loads(stdout.decode("utf-8", errors="replace"))
    return {
        "result": data.get("result", ""),
        "cost": data.get("total_cost_usd", 0),
        "output_tokens": data.get("usage", {}).get("output_tokens", 0),
        "num_turns": data.get("num_turns", 1),
        "elapsed": round(elapsed, 2),
        "stop_reason": data.get("stop_reason"),
    }


async def main():
    print("=" * 60, flush=True)
    print("E2E Pipeline Test: prepare → claude -p → postprocess", flush=True)
    print(f"Verse: {TEST_VERSE}", flush=True)
    print("=" * 60, flush=True)
    print(flush=True)

    # Clean up
    for d in [TMP_DIR, TEST_RESPONSES_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)

    # Step 1: Prepare
    print("Step 1: prepare_verse()...", flush=True)
    plan = prepare_verse(TEST_VERSE, TMP_DIR, data_dir=DATA_DIR, include_few_shot=False)
    if plan is None:
        print("  FAILED: verse not found", flush=True)
        return
    print(f"  verse_id: {plan.verse_id}", flush=True)
    print(f"  mode: {plan.mode}", flush=True)
    print(f"  word_count: {plan.word_count}", flush=True)
    print(f"  system_prompt: {len(plan.system_prompt)} chars", flush=True)
    print(f"  user_message: {len(plan.user_message)} chars", flush=True)
    print(flush=True)

    # Step 2: Call Claude
    print("Step 2: claude -p --model sonnet --max-turns 1...", flush=True)
    cr = await call_claude(plan.system_prompt, plan.user_message, "sonnet")
    if "error" in cr:
        print(f"  FAILED: {cr['error']}", flush=True)
        return
    print(f"  elapsed: {cr['elapsed']}s", flush=True)
    print(f"  cost: ${cr['cost']:.4f}", flush=True)
    print(f"  output_tokens: {cr['output_tokens']}", flush=True)
    print(f"  num_turns: {cr['num_turns']}", flush=True)
    print(f"  stop_reason: {cr['stop_reason']}", flush=True)
    print(f"  result length: {len(cr['result'])} chars", flush=True)

    # Save raw response
    raw_path = os.path.join(TMP_DIR, "raw_response.txt")
    with open(raw_path, "w", encoding="utf-8") as f:
        f.write(cr["result"])
    print(f"  raw saved to: {raw_path}", flush=True)
    print(flush=True)

    # Step 3: Postprocess
    print("Step 3: postprocess_verse()...", flush=True)
    word_dict = load_word_dictionary()
    narrator_tmpl = load_narrator_templates()
    print(f"  word_dictionary: {len(word_dict.get('words', {})) if word_dict else 0} entries", flush=True)
    print(f"  narrator_templates: {len(narrator_tmpl.get('narrators', {})) if narrator_tmpl else 0} entries", flush=True)

    result = postprocess_verse(
        plan=plan,
        raw_response=cr["result"],
        word_dict_data=word_dict,
        narrator_templates=narrator_tmpl,
        responses_dir=TEST_RESPONSES_DIR,
    )

    print(f"  status: {result.status}", flush=True)
    print(f"  validation_errors: {len(result.validation_errors)}", flush=True)
    if result.validation_errors:
        for e in result.validation_errors[:5]:
            print(f"    ERR: {e[:80]}", flush=True)
    print(f"  warnings: {len(result.warnings)}", flush=True)
    for w in result.warnings:
        print(f"    {w.severity.upper()}: [{w.category}] {w.message[:60]}", flush=True)
    if result.error:
        print(f"  error: {result.error}", flush=True)
    print(flush=True)

    # Check if response was saved
    if result.status == "pass":
        resp_file = os.path.join(TEST_RESPONSES_DIR, f"{plan.verse_id}.json")
        if os.path.exists(resp_file):
            size = os.path.getsize(resp_file)
            print(f"Response saved: {resp_file} ({size:,} bytes)", flush=True)
        else:
            print("WARNING: status=pass but response file not saved!", flush=True)

    # Check audit log
    audit_file = os.path.join(TMP_DIR, "audit.json")
    if os.path.exists(audit_file):
        with open(audit_file, "r", encoding="utf-8") as f:
            audit = json.load(f)
        print(f"Audit: word_overrides={audit.get('word_overrides_count', 0)}, narrator_overrides={audit.get('narrator_overrides_count', 0)}", flush=True)

    print(flush=True)
    print("=" * 60, flush=True)
    if result.status == "pass":
        print("E2E TEST PASSED", flush=True)
    elif result.status == "needs_fix":
        print(f"E2E TEST: needs fix ({len([w for w in result.warnings if w.severity in ('high', 'medium')])} high/med warnings)", flush=True)
    else:
        print(f"E2E TEST FAILED: {result.status} - {result.error}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
