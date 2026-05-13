"""Round B: Phase 1 + few-shot examples to lift chunk segmentation quality.

Hypothesis: Qwen 3.6 under-segments chunks (merges body+quran_quote+closing
into a single body) in 12/29 verses (Round A baseline). Adding 2-3 worked
examples of fine-grained segmentation to the user message should bias Qwen
to emit more chunks at natural boundaries.

Saves to benchmark/phase1_qwen_round_c/. Reuses the 30-verse sample.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import sys
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "app"))

os.environ.setdefault("SOURCE_DATA_DIR", str(REPO_ROOT.parent / "ThaqalaynDataSources"))

from app.pipeline_cli.phased_prompts import (  # noqa: E402
    build_phase1_system_prompt,
    build_phase1_user_message,
    build_phase1_schema,
)
from app.ai_pipeline import extract_pipeline_request, load_topic_taxonomy  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("phase1_qwen_round_c")

BENCH_DIR = REPO_ROOT / "benchmark" / "phase1_qwen_round_c"
SAMPLE_PATH = REPO_ROOT / "benchmark" / "phase4_qwen" / "sample.json"
RESULTS_DIR = BENCH_DIR / "results"
RAW_DIR = BENCH_DIR / "raw_responses"
DATA_DIR = REPO_ROOT.parent / "ThaqalaynData"

QWEN_BASE_URL = "http://192.168.0.66:8000/v1"
QWEN_MODEL = "qwen36-fast"
WORKERS = 8


# Few-shot examples — two representative segmentations from gpt-5.4 baselines.
# Picked deliberately: one with isnad+body, one with isnad+body+quran_quote+closing.
# Format: each shows the Arabic input shape (no diacritics for brevity), then
# the expected chunk structure with EN translation.
FEW_SHOT_EXAMPLES = """

FEW-SHOT EXAMPLES (illustrating the expected chunking granularity):

Example 1 — short isnad + body, 2 chunks:
Input Arabic: "محمد بن يحيى، عن أحمد بن محمد، عن أبي عبد الله عليه السلام قال: الصلاة عمود الدين."
Expected chunks:
  [
    {"chunk_type": "isnad", "arabic_text": "<diacritized isnad>", "translations": {"en": "Muhammad ibn Yahya narrated from Ahmad ibn Muhammad, from Abu Abdillah (peace be upon him) who said:"}},
    {"chunk_type": "body", "arabic_text": "<diacritized matn>", "translations": {"en": "Salat is the pillar of religion."}}
  ]

Example 2 — isnad + body + Quran quote + closing, 4 chunks:
Input Arabic: "روى محمد بن الفضيل عن أبي عبد الله عليه السلام قال: إذا طلق الرجل امرأته قبل أن يدخل بها فلها نصف مهرها، فمتاع بالمعروف على الموسع قدره وعلى المقتر قدره، وليس لها عدة تتزوج من شاءت من ساعتها."
Expected chunks:
  [
    {"chunk_type": "isnad", "arabic_text": "<diacritized isnad>", "translations": {"en": "Muhammad ibn al-Fudayl narrated from Abu Abdillah (peace be upon him) who said:"}},
    {"chunk_type": "body", "arabic_text": "<diacritized body up to the Quran quote>", "translations": {"en": "If a man divorces his wife before consummating the marriage, she is entitled to half of her dowry."}},
    {"chunk_type": "quran_quote", "arabic_text": "فَمَتَاعٌ بِالْمَعْرُوفِ عَلَى الْمُوسِعِ قَدَرُهُ وَعَلَى الْمُقْتِرِ قَدَرُهُ", "translations": {"en": "Then provision should be made in a fair manner, according to the means of the wealthy and according to the means of the poor."}},
    {"chunk_type": "closing", "arabic_text": "<diacritized closing>", "translations": {"en": "And there is no waiting period for her; she may marry whomever she wishes immediately."}}
  ]

CHUNKING RULES (reinforce):
- Always separate isnad from matn into their own chunks
- Always make Quran verses (recognised by phrasing or known formulae) into their own chunk_type=quran_quote
- Closing formulae (e.g. independent legal-ruling statements after the main narration) get their own chunk_type=closing
- Prefer more chunks at finer boundaries over fewer big chunks
"""


@dataclass
class CallResult:
    ok: bool
    raw_text: str
    parsed: Optional[dict]
    prompt_tokens: int
    completion_tokens: int
    elapsed: float
    error: Optional[str] = None


def build_phase1_user_message_v2(request) -> str:
    """Phase 1 user message + few-shot examples appended."""
    base = build_phase1_user_message(request)
    return base + "\n" + FEW_SHOT_EXAMPLES


async def call_qwen(client, system: str, user: str, schema: dict, max_tokens: int = 12000) -> CallResult:
    start = time.time()
    try:
        response = await client.chat.completions.create(
            model=QWEN_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_tokens=max_tokens,
            temperature=0,
            response_format={
                "type": "json_schema",
                "json_schema": {"name": "phase1_response", "schema": schema, "strict": True},
            },
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        )
        elapsed = round(time.time() - start, 2)
        text = response.choices[0].message.content or ""
        usage = response.usage
        pt = usage.prompt_tokens if usage else 0
        ct = usage.completion_tokens if usage else 0
        try:
            return CallResult(True, text, json.loads(text), pt, ct, elapsed)
        except json.JSONDecodeError as e:
            return CallResult(False, text, None, pt, ct, elapsed, error=f"json_parse: {e}")
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        return CallResult(False, "", None, 0, 0, elapsed, error=f"{type(e).__name__}: {str(e)[:300]}")


async def run_one(client, verse: dict, taxonomy: dict, schema: dict) -> dict:
    verse_path = verse["verse_path"]
    request = extract_pipeline_request(verse_path, data_dir=str(DATA_DIR))
    if request is None:
        return {"verse_path": verse_path, "error": "could not load verse"}

    system = build_phase1_system_prompt(topic_taxonomy=taxonomy)
    user = build_phase1_user_message_v2(request)

    cr = await call_qwen(client, system, user, schema, max_tokens=12000)
    verse_id = verse_path.removeprefix("/books/").replace("/", "_").replace(":", "_")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (RAW_DIR / f"{verse_id}.raw.txt").write_text(cr.raw_text or "", encoding="utf-8")

    return {
        "verse_path": verse_path,
        "stratum": verse.get("stratum"),
        "ar_word_count": verse.get("ar_word_count"),
        "ok": cr.ok,
        "error": cr.error,
        "prompt_tokens": cr.prompt_tokens,
        "completion_tokens": cr.completion_tokens,
        "elapsed_s": cr.elapsed,
        "arabic_input": request.arabic_text,
        "english_reference": request.english_text,
        "qwen_p1": cr.parsed,
    }


async def main_async() -> None:
    from openai import AsyncOpenAI
    sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    verses = sample["verses"]
    logger.info("ROUND C (few-shot): %d verses, strict json_schema, workers=%d",
                len(verses), WORKERS)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    taxonomy = load_topic_taxonomy()
    schema = build_phase1_schema(topic_taxonomy=taxonomy)
    client = AsyncOpenAI(api_key="not-needed", base_url=QWEN_BASE_URL,
                        timeout=600.0, max_retries=2)
    sem = asyncio.Semaphore(WORKERS)

    async def worker(verse):
        async with sem:
            try:
                return await run_one(client, verse, taxonomy, schema)
            except Exception as e:
                return {"verse_path": verse["verse_path"], "error": str(e)}

    overall_start = time.time()
    out = await asyncio.gather(*[worker(v) for v in verses])
    overall_elapsed = round(time.time() - overall_start, 2)

    for rec in out:
        verse_id = rec["verse_path"].removeprefix("/books/").replace("/", "_").replace(":", "_")
        (RESULTS_DIR / f"{verse_id}.qwen-p1.json").write_text(
            json.dumps(rec, indent=2, ensure_ascii=False), encoding="utf-8")

    successes = [r for r in out if r.get("ok")]
    parse_rate = 100 * len(successes) / len(out) if out else 0
    total_pt = sum(r.get("prompt_tokens", 0) for r in successes)
    total_ct = sum(r.get("completion_tokens", 0) for r in successes)
    summary = {
        "round": "C",
        "approach": "phase1_with_few_shot",
        "verse_count": len(out),
        "ok_count": len(successes),
        "parse_rate_pct": round(parse_rate, 2),
        "overall_wall_seconds": overall_elapsed,
        "prompt_tokens_total": total_pt,
        "completion_tokens_total": total_ct,
    }
    (BENCH_DIR / "qwen_p1_run_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("done — %d verses in %.1fs, parse rate %.1f%%",
                len(out), overall_elapsed, parse_rate)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
