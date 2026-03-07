"""Compare --agent vs plain --system-prompt for token efficiency.
Writes results to stdout with flush. Run with: .venv/Scripts/python.exe scripts/test_agents_vs_plain2.py
"""
import asyncio
import json
import os
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")

CLAUDE_EXE = r"C:\Users\TrainingGR03\.local\bin\claude.exe"
MODEL = "haiku"
PROJECT_DIR = r"C:\Users\TrainingGR03\Documents\Projects\scripture"

SYSTEM_PROMPT = (
    "You are a translator. Given Arabic text, translate it to English. "
    "Respond with ONLY the English translation, nothing else."
)
TEST_PROMPT = "Translate: بسم الله الرحمن الرحيم"


async def call_claude(label, extra_args, prompt, use_stdin=False):
    start = time.time()
    cmd = [
        CLAUDE_EXE, "-p",
        "--model", MODEL,
        "--output-format", "json",
        "--no-session-persistence",
    ] + extra_args

    if not use_stdin:
        cmd.append(prompt)

    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

    if use_stdin:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env, cwd=PROJECT_DIR,
        )
        stdout, stderr = await proc.communicate(input=prompt.encode("utf-8"))
    else:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env, cwd=PROJECT_DIR,
        )
        stdout, stderr = await proc.communicate()

    elapsed = time.time() - start
    r = {"label": label, "elapsed": round(elapsed, 2), "rc": proc.returncode}

    if proc.returncode == 0 and stdout:
        try:
            data = json.loads(stdout.decode("utf-8", errors="replace"))
            r["response"] = data.get("result", "")[:80]
            usage = data.get("usage", {})
            r["input"] = usage.get("input_tokens", 0)
            r["cache_create"] = usage.get("cache_creation_input_tokens", 0)
            r["cache_read"] = usage.get("cache_read_input_tokens", 0)
            r["output"] = usage.get("output_tokens", 0)
            r["total_ctx"] = r["input"] + r["cache_create"] + r["cache_read"]
            r["cost"] = data.get("total_cost_usd", "?")
        except json.JSONDecodeError:
            r["error"] = "JSON parse failed"
    else:
        r["error"] = (stderr.decode("utf-8", errors="replace")[:200] if stderr else "no output")

    return r


def pr(r):
    if "error" in r:
        print(f"  ERROR: {r['error']}", flush=True)
    else:
        print(
            f"  ctx={r['total_ctx']} "
            f"(in={r['input']}, cc={r['cache_create']}, cr={r['cache_read']}), "
            f"out={r['output']}, cost=${r['cost']}, {r['elapsed']}s",
            flush=True,
        )
        print(f"  Response: {r.get('response', '')}", flush=True)


async def main():
    print("=" * 60, flush=True)
    print("Agent vs Plain System Prompt Comparison", flush=True)
    print(f"Model: {MODEL}", flush=True)
    print("=" * 60, flush=True)
    print(flush=True)

    results = []

    # Test 1
    print("--- Test 1: Plain --system-prompt (no project settings) ---", flush=True)
    r = await call_claude(
        "plain_no_settings",
        ["--setting-sources", "", "--system-prompt", SYSTEM_PROMPT],
        TEST_PROMPT,
    )
    pr(r)
    results.append(r)
    print(flush=True)

    # Test 2
    print("--- Test 2: Plain --system-prompt (with project settings) ---", flush=True)
    r = await call_claude(
        "plain_with_settings",
        ["--system-prompt", SYSTEM_PROMPT],
        TEST_PROMPT,
    )
    pr(r)
    results.append(r)
    print(flush=True)

    # Test 3
    print("--- Test 3: --append-system-prompt (no project settings) ---", flush=True)
    r = await call_claude(
        "append_no_settings",
        ["--setting-sources", "", "--append-system-prompt", SYSTEM_PROMPT],
        TEST_PROMPT,
    )
    pr(r)
    results.append(r)
    print(flush=True)

    # Test 4
    print("--- Test 4: Inline --agents definition ---", flush=True)
    agent_def = json.dumps({
        "translator": {
            "description": "Translates Arabic to English",
            "prompt": SYSTEM_PROMPT,
        }
    })
    r = await call_claude(
        "inline_agent",
        ["--setting-sources", "", "--agents", agent_def, "--agent", "translator"],
        TEST_PROMPT,
    )
    pr(r)
    results.append(r)
    print(flush=True)

    # Test 5
    print("--- Test 5: stdin piping (no project settings) ---", flush=True)
    r = await call_claude(
        "stdin_pipe",
        ["--setting-sources", "", "--system-prompt", SYSTEM_PROMPT],
        TEST_PROMPT,
        use_stdin=True,
    )
    pr(r)
    results.append(r)
    print(flush=True)

    # Summary
    print("=" * 60, flush=True)
    print("SUMMARY (lower context = more efficient)", flush=True)
    print("=" * 60, flush=True)
    for r in results:
        ctx = r.get("total_ctx", "ERR")
        cost = r.get("cost", "?")
        t = r.get("elapsed", "?")
        print(f"  {r['label']:30s}: ctx={str(ctx):>6}, cost=${cost}, time={t}s", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
