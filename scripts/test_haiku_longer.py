"""Test Haiku on medium and long hadiths (99-281 words).

Quick validation that Haiku quality holds for longer content.
Only tests Haiku (our chosen model) on 4 hadiths not yet tested.
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

from app.ai_pipeline import build_system_prompt, build_user_message, extract_pipeline_request, validate_result
from app.ai_pipeline_review import review_result
from app.pipeline_cli.verse_processor import expand_compact_words, strip_code_fences

CLAUDE_EXE = r"C:\Users\TrainingGR03\.local\bin\claude.exe"
DATA_DIR = "../ThaqalaynData/"

VERSES = [
    "/books/al-kafi:4:2:2:5",   # 99w  - medium, zakat
    "/books/al-kafi:3:3:2:1",   # 114w - medium, prayer
    "/books/al-kafi:4:1:2:3",   # 142w - long, charity
    "/books/al-kafi:6:6:1:1",   # 281w - very long, animals
]

COMPACT = "\nCOMPACT WORD FORMAT: For word_analysis, you MAY use either format:\nA) Standard JSON objects\nB) Compact arrays: [\"word\",\"POS\",\"en\",\"ur\",\"tr\",\"fa\",\"id\",\"bn\",\"es\",\"fr\",\"de\",\"ru\",\"zh\"]\nBoth accepted."


async def call_claude(system_prompt, user_message):
    cmd = [CLAUDE_EXE, "-p", "--model", "haiku", "--output-format", "json",
           "--no-session-persistence", "--setting-sources", "", "--system-prompt", system_prompt]
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    start = time.time()
    proc = await asyncio.create_subprocess_exec(*cmd, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env)
    stdout, stderr = await proc.communicate(input=user_message.encode("utf-8"))
    elapsed = time.time() - start
    if proc.returncode == 0 and stdout:
        data = json.loads(stdout.decode("utf-8", errors="replace"))
        return {"ai_result": data.get("result", ""), "cost": data.get("total_cost_usd", 0),
                "output_tokens": data.get("usage", {}).get("output_tokens", 0), "elapsed": round(elapsed, 2)}
    return {"error": stderr.decode("utf-8", errors="replace")[:200] if stderr else "no output", "elapsed": round(elapsed, 2)}


async def main():
    print("Haiku on medium/long hadiths (99-281 words)", flush=True)
    print("=" * 60, flush=True)
    sp = build_system_prompt(few_shot_examples={"examples": []}) + COMPACT

    for vp in VERSES:
        req = extract_pipeline_request(vp, DATA_DIR)
        if not req:
            print(f"{vp}: NOT FOUND", flush=True); continue
        wc = len(req.arabic_text.split())
        um = build_user_message(req)
        print(f"\n{vp} ({wc}w)...", end="", flush=True)
        r = await call_claude(sp, um)
        if "error" in r:
            print(f" ERROR: {r['error'][:50]}", flush=True); continue
        try:
            result = json.loads(strip_code_fences(r["ai_result"]))
            if "word_analysis" in result:
                result["word_analysis"] = expand_compact_words(result["word_analysis"])
            errors = validate_result(result)
            if errors:
                print(f" FAIL ({len(errors)} errors, {r['elapsed']}s, ${r['cost']:.4f})", flush=True)
                for e in errors[:3]: print(f"  ERR: {e[:80]}", flush=True)
                continue
            warnings = review_result(result, req)
            h = sum(1 for w in warnings if w.severity == "high")
            m = sum(1 for w in warnings if w.severity == "medium")
            l = sum(1 for w in warnings if w.severity == "low")
            status = "PASS" if h == 0 else "WARN"
            wa_count = len(result.get("word_analysis", []))
            print(f" {status} | {r['elapsed']}s | ${r['cost']:.4f} | out={r['output_tokens']} | words={wa_count} | H={h} M={m} L={l}", flush=True)
            for w in warnings:
                if w.severity in ("high", "medium"):
                    print(f"  {w.severity.upper()}: [{w.check}] {w.message[:70]}", flush=True)
        except Exception as e:
            print(f" PARSE ERROR: {str(e)[:60]}", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
