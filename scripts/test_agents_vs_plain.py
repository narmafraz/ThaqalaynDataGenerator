"""Compare --agent vs plain --system-prompt for token efficiency.

Tests:
1. Plain --system-prompt + stdin (our planned approach)
2. --agent flag referencing a .claude/agents/ file
3. --agents inline JSON definition

Measures token usage and cost for each approach.

Usage:
    python scripts/test_agents_vs_plain.py
"""
import asyncio
import json
import os
import time

CLAUDE_EXE = r"C:\Users\TrainingGR03\.local\bin\claude.exe"
MODEL = "haiku"
PROJECT_DIR = r"C:\Users\TrainingGR03\Documents\Projects\scripture"

SYSTEM_PROMPT = """You are a translator. Given Arabic text, translate it to English.
Respond with ONLY the English translation, nothing else."""

TEST_PROMPT = "Translate: بسم الله الرحمن الرحيم"


async def call_claude(label: str, extra_args: list, prompt: str, use_stdin: bool = False):
    """Call claude -p and return timing + token info."""
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
            env=env,
            cwd=PROJECT_DIR,
        )
        stdout, stderr = await proc.communicate(input=prompt.encode("utf-8"))
    else:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=PROJECT_DIR,
        )
        stdout, stderr = await proc.communicate()

    elapsed = time.time() - start
    result = {"label": label, "elapsed_s": round(elapsed, 2), "rc": proc.returncode}

    if proc.returncode == 0 and stdout:
        try:
            data = json.loads(stdout.decode("utf-8", errors="replace"))
            result["response"] = data.get("result", "")[:100]
            result["duration_ms"] = data.get("duration_ms")
            result["total_cost_usd"] = data.get("total_cost_usd")
            result["num_turns"] = data.get("num_turns")
            usage = data.get("usage", {})
            result["input_tokens"] = usage.get("input_tokens", 0)
            result["cache_creation"] = usage.get("cache_creation_input_tokens", 0)
            result["cache_read"] = usage.get("cache_read_input_tokens", 0)
            result["output_tokens"] = usage.get("output_tokens", 0)
            result["total_context"] = result["cache_creation"] + result["cache_read"] + result["input_tokens"]
        except json.JSONDecodeError:
            result["raw_stdout"] = stdout.decode("utf-8", errors="replace")[:300]
    else:
        result["stderr"] = stderr.decode("utf-8", errors="replace")[:300]
        if stdout:
            result["stdout"] = stdout.decode("utf-8", errors="replace")[:300]

    return result


def print_result(r):
    """Pretty print a result dict."""
    print(f"  [{r['label']}]")
    print(f"    RC: {r['rc']}, Elapsed: {r['elapsed_s']}s")
    if "stderr" in r:
        print(f"    ERROR: {r['stderr']}")
    if "raw_stdout" in r:
        print(f"    RAW: {r['raw_stdout']}")
    if "response" in r:
        print(f"    Response: {r['response']}")
        print(f"    Turns: {r.get('num_turns', '?')}, Cost: ${r.get('total_cost_usd', '?')}")
        print(f"    Context: {r.get('total_context', '?')} (create={r.get('cache_creation', '?')}, read={r.get('cache_read', '?')}, input={r.get('input_tokens', '?')})")
        print(f"    Output: {r.get('output_tokens', '?')} tokens")


async def main():
    print("=" * 60)
    print("Agent vs Plain System Prompt Comparison")
    print("=" * 60)
    print(f"Model: {MODEL}")
    print()

    # Test 1: Plain --system-prompt + --setting-sources ""
    print("--- Test 1: Plain --system-prompt (no project settings) ---")
    r1 = await call_claude(
        "plain_no_settings",
        ["--setting-sources", "", "--system-prompt", SYSTEM_PROMPT],
        TEST_PROMPT,
    )
    print_result(r1)
    print()

    # Test 2: Plain --system-prompt WITH project settings
    print("--- Test 2: Plain --system-prompt (with project settings) ---")
    r2 = await call_claude(
        "plain_with_settings",
        ["--system-prompt", SYSTEM_PROMPT],
        TEST_PROMPT,
    )
    print_result(r2)
    print()

    # Test 3: --append-system-prompt (adds to default, not replaces)
    print("--- Test 3: --append-system-prompt (no project settings) ---")
    r3 = await call_claude(
        "append_no_settings",
        ["--setting-sources", "", "--append-system-prompt", SYSTEM_PROMPT],
        TEST_PROMPT,
    )
    print_result(r3)
    print()

    # Test 4: Inline agent via --agents
    print("--- Test 4: Inline --agents definition ---")
    agent_def = json.dumps({
        "translator": {
            "description": "Translates Arabic to English",
            "prompt": SYSTEM_PROMPT,
        }
    })
    r4 = await call_claude(
        "inline_agent",
        ["--setting-sources", "", "--agents", agent_def, "--agent", "translator"],
        TEST_PROMPT,
    )
    print_result(r4)
    print()

    # Test 5: stdin piping with --system-prompt
    print("--- Test 5: stdin piping (no project settings) ---")
    r5 = await call_claude(
        "stdin_pipe",
        ["--setting-sources", "", "--system-prompt", SYSTEM_PROMPT],
        TEST_PROMPT,
        use_stdin=True,
    )
    print_result(r5)
    print()

    # Summary
    print("=" * 60)
    print("SUMMARY: Total context tokens (lower is better)")
    print("=" * 60)
    for r in [r1, r2, r3, r4, r5]:
        ctx = r.get("total_context", "ERROR")
        cost = r.get("total_cost_usd", "ERROR")
        turns = r.get("num_turns", "?")
        print(f"  {r['label']:30s}: context={ctx:>8}, cost=${cost}, turns={turns}")


if __name__ == "__main__":
    asyncio.run(main())
