"""Phase 1 Quality Comparison: Haiku vs Sonnet generation quality.

Tests whether Haiku can match Sonnet quality for hadith generation.
Uses --system-prompt (cheap, no tool overhead) for all calls.

Few-shot comparison is NOT included because:
- Few-shot prompt (50K chars) exceeds Windows --system-prompt limit (32K)
- Using @file triggers multi-turn tool use (~$0.70-1.00/call vs $0.03-0.05/call)
- We're removing few-shot in v3 regardless

Test verses: 8 diverse hadiths from 6 Al-Kafi volumes, 14-281 words.

Usage:
    cd ThaqalaynDataGenerator
    .venv/Scripts/python.exe scripts/test_quality_comparison.py [--quick]
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))
sys.stdout.reconfigure(encoding="utf-8")

os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")

from app.ai_pipeline import (
    PipelineRequest,
    build_system_prompt,
    build_user_message,
    extract_pipeline_request,
    validate_result,
)
from app.ai_pipeline_review import review_result
from app.pipeline_cli.verse_processor import expand_compact_words, strip_code_fences

CLAUDE_EXE = r"C:\Users\TrainingGR03\.local\bin\claude.exe"
DATA_DIR = "../ThaqalaynData/"

# 8 diverse hadiths: 6 volumes, range of lengths and content types
TEST_VERSES = [
    "/books/al-kafi:6:3:2:1",   # 14w  - tiny, food rulings (Vol 6)
    "/books/al-kafi:2:2:3:1",   # 15w  - tiny, faith/belief (Vol 2)
    "/books/al-kafi:7:3:1:1",   # 20w  - small, inheritance law (Vol 7)
    "/books/al-kafi:1:4:16:3",  # 34w  - short, imamate/hujjah (Vol 1)
    "/books/al-kafi:4:2:2:5",   # 99w  - medium, zakat rulings (Vol 4)
    "/books/al-kafi:3:3:2:1",   # 114w - medium, prayer/tahara (Vol 3)
    "/books/al-kafi:4:1:2:3",   # 142w - long, charity/giving (Vol 4)
    "/books/al-kafi:6:6:1:1",   # 281w - very long, animal rulings (Vol 6)
]

QUICK_VERSES = TEST_VERSES[:4]  # just the short ones

CONDITIONS = [
    {"label": "sonnet", "model": "sonnet"},
    {"label": "haiku",  "model": "haiku"},
]

COMPACT_WORD_INSTRUCTIONS = """
COMPACT WORD FORMAT: For word_analysis, you MAY use either format:
A) Standard JSON objects (as described above)
B) Compact arrays: ["word","POS","en","ur","tr","fa","id","bn","es","fr","de","ru","zh"]
   Language order is fixed: en, ur, tr, fa, id, bn, es, fr, de, ru, zh.
Both formats are accepted."""


async def call_claude(system_prompt: str, user_message: str, model: str) -> dict:
    """Call claude -p with --system-prompt and stdin for user message."""
    start = time.time()
    cmd = [
        CLAUDE_EXE, "-p",
        "--model", model,
        "--output-format", "json",
        "--no-session-persistence",
        "--setting-sources", "",
        "--system-prompt", system_prompt,
    ]
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    stdout, stderr = await proc.communicate(input=user_message.encode("utf-8"))
    elapsed = time.time() - start

    result = {"elapsed": round(elapsed, 2), "rc": proc.returncode}

    if proc.returncode == 0 and stdout:
        try:
            data = json.loads(stdout.decode("utf-8", errors="replace"))
            result["ai_result"] = data.get("result", "")
            result["cost"] = data.get("total_cost_usd", 0)
            usage = data.get("usage", {})
            result["input_tokens"] = usage.get("input_tokens", 0)
            result["cache_create"] = usage.get("cache_creation_input_tokens", 0)
            result["cache_read"] = usage.get("cache_read_input_tokens", 0)
            result["output_tokens"] = usage.get("output_tokens", 0)
            result["total_ctx"] = result["input_tokens"] + result["cache_create"] + result["cache_read"]
        except json.JSONDecodeError:
            result["error"] = "JSON parse failed from claude output"
    else:
        result["error"] = stderr.decode("utf-8", errors="replace")[:300] if stderr else "no output"

    return result


def evaluate_response(ai_result_str: str, request: PipelineRequest) -> dict:
    """Parse, validate, and review AI output."""
    ev = {
        "parse_ok": False, "validation_errors": [], "error_details": [],
        "high": 0, "medium": 0, "low": 0, "warning_details": [], "word_count": 0,
    }

    try:
        result = json.loads(strip_code_fences(ai_result_str))
        ev["parse_ok"] = True
    except (json.JSONDecodeError, TypeError):
        ev["error_details"] = ["JSON parse failed"]
        return ev

    if "word_analysis" in result:
        result["word_analysis"] = expand_compact_words(result["word_analysis"])
        ev["word_count"] = len(result["word_analysis"])

    errors = validate_result(result)
    ev["validation_errors"] = errors
    ev["error_details"] = errors[:5]
    if errors:
        return ev

    try:
        warnings = review_result(result, request)
        ev["warning_details"] = [
            {"check": w.check, "severity": w.severity, "msg": w.message[:80]}
            for w in warnings
        ]
        ev["high"] = sum(1 for w in warnings if w.severity == "high")
        ev["medium"] = sum(1 for w in warnings if w.severity == "medium")
        ev["low"] = sum(1 for w in warnings if w.severity == "low")
    except Exception as e:
        ev["warning_details"] = [{"check": "error", "severity": "high", "msg": str(e)[:80]}]
        ev["high"] = 1

    return ev


async def main():
    quick = "--quick" in sys.argv
    verses = QUICK_VERSES if quick else TEST_VERSES

    print("=" * 70, flush=True)
    print("Phase 1: Haiku vs Sonnet Quality Comparison", flush=True)
    print(f"Verses: {len(verses)}, Models: {len(CONDITIONS)}", flush=True)
    print(f"Total Claude calls: {len(verses) * len(CONDITIONS)}", flush=True)
    print("=" * 70, flush=True)
    print(flush=True)

    # Build system prompt (no few-shot)
    system_prompt = build_system_prompt(few_shot_examples={"examples": []})
    system_prompt += "\n" + COMPACT_WORD_INSTRUCTIONS
    print(f"System prompt: {len(system_prompt)} chars (~{len(system_prompt)//4} tokens est)", flush=True)
    print(flush=True)

    all_results = {}
    for cond in CONDITIONS:
        label = cond["label"]
        model = cond["model"]
        print(f"--- {label} ---", flush=True)
        results = []

        for verse_path in verses:
            request = extract_pipeline_request(verse_path, DATA_DIR)
            if request is None:
                results.append({"verse": verse_path, "error": "not found"})
                continue

            user_message = build_user_message(request)
            wc = len(request.arabic_text.split())

            print(f"  {verse_path} ({wc}w) ...", end="", flush=True)
            cr = await call_claude(system_prompt, user_message, model)

            if "error" in cr:
                print(f" ERROR: {cr['error'][:50]}", flush=True)
                results.append({"verse": verse_path, "wc": wc, "error": cr["error"][:200], "elapsed": cr.get("elapsed")})
                continue

            ev = evaluate_response(cr["ai_result"], request)
            v_err = len(ev["validation_errors"])
            status = "PASS" if ev["high"] == 0 and v_err == 0 else "WARN" if v_err == 0 else "FAIL"
            print(f" {status} | {cr['elapsed']}s | ${cr.get('cost',0):.4f} | out={cr.get('output_tokens',0)} | H={ev['high']} M={ev['medium']} L={ev['low']}", flush=True)

            results.append({
                "verse": verse_path, "wc": wc, "status": status,
                "elapsed": cr["elapsed"], "cost": cr.get("cost", 0),
                "input_tokens": cr.get("input_tokens", 0),
                "cache_create": cr.get("cache_create", 0),
                "cache_read": cr.get("cache_read", 0),
                "output_tokens": cr.get("output_tokens", 0),
                "total_ctx": cr.get("total_ctx", 0),
                "parse_ok": ev["parse_ok"],
                "validation_errors": v_err,
                "high": ev["high"], "medium": ev["medium"], "low": ev["low"],
                "word_count_out": ev["word_count"],
                "error_details": ev["error_details"],
                "warning_details": ev["warning_details"],
            })

            # Print high/medium warnings
            for w in ev["warning_details"]:
                if w["severity"] in ("high", "medium"):
                    print(f"    {w['severity'].upper()}: [{w['check']}] {w['msg'][:60]}", flush=True)

        all_results[label] = results
        print(flush=True)

    # Summary
    print("=" * 70, flush=True)
    print("SUMMARY", flush=True)
    print("=" * 70, flush=True)
    print(flush=True)
    print(f"{'Model':<10} {'OK':<6} {'FAIL':<6} {'High':<6} {'Med':<6} {'Low':<6} {'Cost':>10} {'Out tok':>10} {'Avg time':>10}", flush=True)
    print("-" * 80, flush=True)

    for cond in CONDITIONS:
        label = cond["label"]
        rs = [r for r in all_results[label] if "error" not in r]
        ok = sum(1 for r in rs if r.get("status") == "PASS")
        fail = sum(1 for r in rs if r.get("status") == "FAIL")
        high = sum(r.get("high", 0) for r in rs)
        med = sum(r.get("medium", 0) for r in rs)
        low = sum(r.get("low", 0) for r in rs)
        cost = sum(r.get("cost", 0) for r in rs)
        out = sum(r.get("output_tokens", 0) for r in rs)
        t = sum(r.get("elapsed", 0) for r in all_results[label])
        avg = t / len(all_results[label]) if all_results[label] else 0
        n = len(all_results[label])
        print(f"{label:<10} {ok}/{n:<4} {fail:<6} {high:<6} {med:<6} {low:<6} ${cost:>9.4f} {out:>10} {avg:>8.1f}s", flush=True)

    # Per-verse comparison
    print(flush=True)
    print("PER-VERSE COMPARISON:", flush=True)
    print("-" * 70, flush=True)
    for verse_path in verses:
        print(f"\n  {verse_path}:", flush=True)
        for cond in CONDITIONS:
            r = next((x for x in all_results[cond["label"]] if x.get("verse") == verse_path), {})
            if "error" in r:
                print(f"    {cond['label']:<10}: ERROR", flush=True)
            else:
                print(
                    f"    {cond['label']:<10}: {r.get('status','?')} | "
                    f"out={r.get('output_tokens',0):>5} | "
                    f"${r.get('cost',0):.4f} | "
                    f"{r.get('elapsed',0):>5.1f}s | "
                    f"H={r.get('high',0)} M={r.get('medium',0)} L={r.get('low',0)}",
                    flush=True,
                )

    # Save results
    output_path = os.path.join(os.path.dirname(__file__), "quality_comparison_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nResults saved to: {output_path}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
