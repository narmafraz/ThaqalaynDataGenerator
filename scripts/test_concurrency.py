"""Test claude -p concurrency and measure token overhead.

Runs N parallel claude -p calls and measures:
- Whether they run concurrently or serialize
- Token usage per call (base system prompt overhead)
- Total wall time vs sequential baseline

Usage:
    python scripts/test_concurrency.py
"""
import asyncio
import json
import os
import sys
import time

CLAUDE_EXE = r"C:\Users\TrainingGR03\.local\bin\claude.exe"
MODEL = "haiku"

async def call_claude(label: str, prompt: str, use_setting_sources_empty: bool = False):
    """Call claude -p and return timing + token info."""
    start = time.time()

    cmd = [
        CLAUDE_EXE, "-p",
        "--model", MODEL,
        "--output-format", "json",
        "--no-session-persistence",
    ]
    if use_setting_sources_empty:
        cmd.extend(["--setting-sources", ""])
    cmd.append(prompt)

    # Remove CLAUDECODE env var to allow nested calls
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    stdout, stderr = await proc.communicate()
    elapsed = time.time() - start

    result = {"label": label, "elapsed_s": round(elapsed, 2), "rc": proc.returncode}

    if proc.returncode == 0 and stdout:
        try:
            data = json.loads(stdout.decode("utf-8", errors="replace"))
            result["response"] = data.get("result", "")[:80]
            result["duration_ms"] = data.get("duration_ms")
            result["total_cost_usd"] = data.get("total_cost_usd")
            usage = data.get("usage", {})
            result["input_tokens"] = usage.get("input_tokens", 0)
            result["cache_creation"] = usage.get("cache_creation_input_tokens", 0)
            result["cache_read"] = usage.get("cache_read_input_tokens", 0)
            result["output_tokens"] = usage.get("output_tokens", 0)
            result["total_context"] = result["cache_creation"] + result["cache_read"] + result["input_tokens"]
        except json.JSONDecodeError:
            result["raw"] = stdout.decode("utf-8", errors="replace")[:200]
    else:
        result["stderr"] = stderr.decode("utf-8", errors="replace")[:200]

    return result


async def run_parallel(n: int, use_setting_sources_empty: bool):
    """Run N concurrent claude -p calls."""
    tasks = [
        call_claude(f"parallel_{i+1}", f"Respond with exactly: OK_{i+1}", use_setting_sources_empty)
        for i in range(n)
    ]
    start = time.time()
    results = await asyncio.gather(*tasks)
    wall_time = time.time() - start
    return results, round(wall_time, 2)


async def run_sequential(n: int, use_setting_sources_empty: bool):
    """Run N sequential claude -p calls."""
    results = []
    start = time.time()
    for i in range(n):
        r = await call_claude(f"sequential_{i+1}", f"Respond with exactly: SEQ_{i+1}", use_setting_sources_empty)
        results.append(r)
    wall_time = time.time() - start
    return results, round(wall_time, 2)


async def main():
    print("=" * 60)
    print("Claude -p Concurrency Test")
    print("=" * 60)
    print(f"Model: {MODEL}")
    print(f"Claude exe: {CLAUDE_EXE}")
    print()

    # Test 1: Sequential baseline (2 calls)
    print("--- Test 1: Sequential baseline (2 calls) ---")
    seq_results, seq_wall = await run_sequential(2, use_setting_sources_empty=True)
    for r in seq_results:
        print(f"  {r['label']}: {r['elapsed_s']}s, context={r.get('total_context', '?')}, cost=${r.get('total_cost_usd', '?')}")
    print(f"  Wall time: {seq_wall}s")
    print()

    # Test 2: 3 parallel calls
    print("--- Test 2: 3 parallel calls ---")
    par3_results, par3_wall = await run_parallel(3, use_setting_sources_empty=True)
    for r in par3_results:
        print(f"  {r['label']}: {r['elapsed_s']}s, context={r.get('total_context', '?')}, cost=${r.get('total_cost_usd', '?')}")
    print(f"  Wall time: {par3_wall}s")
    print()

    # Test 3: 5 parallel calls
    print("--- Test 3: 5 parallel calls ---")
    par5_results, par5_wall = await run_parallel(5, use_setting_sources_empty=True)
    for r in par5_results:
        print(f"  {r['label']}: {r['elapsed_s']}s, context={r.get('total_context', '?')}, cost=${r.get('total_cost_usd', '?')}")
    print(f"  Wall time: {par5_wall}s")
    print()

    # Test 4: 5 parallel WITHOUT --setting-sources "" (to see CLAUDE.md overhead)
    print("--- Test 4: 5 parallel WITH project settings (CLAUDE.md loaded) ---")
    par5_settings, par5s_wall = await run_parallel(5, use_setting_sources_empty=False)
    for r in par5_settings:
        print(f"  {r['label']}: {r['elapsed_s']}s, context={r.get('total_context', '?')}, cost=${r.get('total_cost_usd', '?')}")
    print(f"  Wall time: {par5s_wall}s")
    print()

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    avg_seq = sum(r["elapsed_s"] for r in seq_results) / len(seq_results)
    avg_par3 = par3_wall / 3 if par3_wall else 0
    avg_par5 = par5_wall / 5 if par5_wall else 0

    print(f"Sequential avg per call: {avg_seq:.1f}s")
    print(f"Parallel-3 wall time:    {par3_wall}s (effective: {par3_wall/3:.1f}s/call)")
    print(f"Parallel-5 wall time:    {par5_wall}s (effective: {par5_wall/5:.1f}s/call)")
    print()

    if par5_wall < seq_wall * 3:
        print("RESULT: Concurrency WORKS (parallel is faster than sequential * N)")
    else:
        print("RESULT: Concurrency may be LIMITED (parallel took ~same as sequential)")

    # Token overhead analysis
    print()
    print("Token overhead per call (--setting-sources empty):")
    for r in par5_results[:1]:
        print(f"  cache_creation: {r.get('cache_creation', '?')}")
        print(f"  cache_read: {r.get('cache_read', '?')}")
        print(f"  input_tokens: {r.get('input_tokens', '?')}")
        print(f"  total_context: {r.get('total_context', '?')}")

    print()
    print("Token overhead per call (with project settings):")
    for r in par5_settings[:1]:
        print(f"  cache_creation: {r.get('cache_creation', '?')}")
        print(f"  cache_read: {r.get('cache_read', '?')}")
        print(f"  input_tokens: {r.get('input_tokens', '?')}")
        print(f"  total_context: {r.get('total_context', '?')}")


if __name__ == "__main__":
    asyncio.run(main())
