"""Run Phase 4 translation against Qwen 3.6-35B on the DGX Spark.

For each verse in benchmark/phase4_qwen/sample.json:
  1. Load the existing baseline response from ai-content/corpus/responses/.
  2. Strip non-EN translations (keep EN chunks + summary + seo_question — the
     same inputs Phase 4 saw originally).
  3. Re-run Phase 4 with model=qwen36-fast, base_url=http://192.168.0.66:8000/v1.
  4. Save the Qwen result + per-batch metadata side-by-side in
     benchmark/phase4_qwen/results/{verse_id}.qwen.json (NEVER modifies the
     baseline response file).

Uses the same prompt builder as production (`translation_phase._build_batch_prompt`)
so the comparison is apples-to-apples: only the model + base_url differ.

Run with venv active:
    python scripts/benchmark_phase4_qwen.py
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Ensure app imports work
import sys
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "app"))

from app.pipeline_cli.translation_phase import (  # noqa: E402
    CHUNK_BATCH_SIZE,
    NON_EN_LANGUAGES,
    _build_batch_prompt,
    _strip_code_fences,
    _fill_empty_translations,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("phase4_qwen")

SOURCE_DATA_DIR = Path(os.environ.get(
    "SOURCE_DATA_DIR",
    REPO_ROOT.parent / "ThaqalaynDataSources",
))
RESPONSES_DIR = SOURCE_DATA_DIR / "ai-content" / "corpus" / "responses"

BENCH_DIR = REPO_ROOT / "benchmark" / "phase4_qwen"
SAMPLE_PATH = BENCH_DIR / "sample.json"
RESULTS_DIR = BENCH_DIR / "results"
RAW_DIR = BENCH_DIR / "raw_responses"

QWEN_BASE_URL = "http://192.168.0.66:8000/v1"
QWEN_MODEL = "qwen36-fast"
WORKERS = 8


@dataclass
class CallResult:
    ok: bool
    raw_text: str
    parsed: Optional[dict]
    prompt_tokens: int
    completion_tokens: int
    elapsed: float
    error: Optional[str] = None


async def call_qwen(client, system: str, user: str, max_tokens: int = 8192) -> CallResult:
    """Single chat completion against the Spark vLLM endpoint."""
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
            extra_body={
                "chat_template_kwargs": {"enable_thinking": False},
            },
        )
        elapsed = round(time.time() - start, 2)
        text = response.choices[0].message.content or ""
        usage = response.usage
        pt = usage.prompt_tokens if usage else 0
        ct = usage.completion_tokens if usage else 0
        try:
            parsed = json.loads(_strip_code_fences(text))
            return CallResult(True, text, parsed, pt, ct, elapsed)
        except json.JSONDecodeError as e:
            return CallResult(False, text, None, pt, ct, elapsed,
                              error=f"json_parse: {e}")
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        return CallResult(False, "", None, 0, 0, elapsed,
                          error=f"{type(e).__name__}: {str(e)[:300]}")


def build_arabic_text(result: dict) -> str:
    """Concatenate chunk arabic_text for context."""
    parts = []
    for c in result.get("chunks", []) or []:
        t = c.get("arabic_text", "")
        if t:
            parts.append(t)
    return "\n".join(parts)


async def translate_one_verse(client, verse: dict, baseline_record: dict) -> dict:
    """Re-run Phase 4 against Qwen for a single verse. Returns a serializable
    record with the new translations plus per-batch metadata.
    """
    verse_path = verse["verse_path"]
    result = copy.deepcopy(baseline_record["result"])

    # Strip non-EN translations so we're not "leaking" the baseline to Qwen.
    for chunk in result.get("chunks", []) or []:
        trans = chunk.get("translations") or {}
        chunk["translations"] = {"en": trans.get("en", "")}
    en_block = (result.get("translations") or {}).get("en") or {}
    result["translations"] = {"en": {
        "summary": en_block.get("summary", ""),
        "seo_question": en_block.get("seo_question", ""),
        "key_terms": en_block.get("key_terms", {}),
    }}

    chunks = result.get("chunks", [])
    en_summary = en_block.get("summary", "")
    en_seo = en_block.get("seo_question", "")
    arabic_text = build_arabic_text(result)

    batches = [chunks[i:i + CHUNK_BATCH_SIZE]
               for i in range(0, len(chunks), CHUNK_BATCH_SIZE)] or [[]]

    per_batch_meta: list[dict] = []
    overall_start = time.time()

    for batch_idx, batch_chunks in enumerate(batches):
        include_metadata = (batch_idx == 0)
        if not batch_chunks and not include_metadata:
            continue
        system, user = _build_batch_prompt(
            batch_chunks, en_summary, en_seo, arabic_text,
            include_metadata, batch_idx,
        )
        cr = await call_qwen(client, system, user, max_tokens=8192)
        per_batch_meta.append({
            "batch_idx": batch_idx,
            "ok": cr.ok,
            "error": cr.error,
            "prompt_tokens": cr.prompt_tokens,
            "completion_tokens": cr.completion_tokens,
            "elapsed_s": cr.elapsed,
            "chunk_count": len(batch_chunks),
            "include_metadata": include_metadata,
        })
        # Persist raw text for inspection regardless of parse result
        verse_id = verse_path.removeprefix("/books/").replace("/", "_").replace(":", "_")
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        (RAW_DIR / f"{verse_id}.batch{batch_idx}.raw.txt").write_text(
            cr.raw_text or "", encoding="utf-8")
        if not cr.ok or cr.parsed is None:
            logger.warning("verse=%s batch=%d failed: %s",
                           verse_path, batch_idx, cr.error)
            continue

        trans_data = cr.parsed
        chunk_offset = batch_idx * CHUNK_BATCH_SIZE
        for j, chunk_trans in enumerate(trans_data.get("chunks", [])):
            abs_idx = chunk_offset + j
            if abs_idx < len(chunks):
                ct = chunk_trans.get("translations") or {}
                if "translations" not in chunks[abs_idx]:
                    chunks[abs_idx]["translations"] = {}
                for lang in NON_EN_LANGUAGES:
                    if lang in ct:
                        chunks[abs_idx]["translations"][lang] = ct[lang]

        if include_metadata:
            summary_trans = trans_data.get("summary") or {}
            seo_trans = trans_data.get("seo_question") or {}
            translations = result.get("translations") or {}
            for lang in NON_EN_LANGUAGES:
                if lang not in translations:
                    translations[lang] = {}
                if lang in summary_trans:
                    translations[lang]["summary"] = summary_trans[lang]
                if lang in seo_trans:
                    translations[lang]["seo_question"] = seo_trans[lang]
                translations[lang].setdefault("key_terms", {})
            result["translations"] = translations

    _fill_empty_translations(result)

    elapsed_total = round(time.time() - overall_start, 2)
    return {
        "verse_path": verse_path,
        "baseline_p4": verse["baseline_p4"],
        "baseline_file": verse["file"],
        "qwen_model": QWEN_MODEL,
        "qwen_base_url": QWEN_BASE_URL,
        "elapsed_s": elapsed_total,
        "batches": per_batch_meta,
        "result": result,
        "stratum": verse.get("stratum"),
        "book": verse.get("book"),
        "chunk_count": verse.get("chunk_count"),
        "ar_word_count": verse.get("ar_word_count"),
    }


async def main_async() -> None:
    try:
        from openai import AsyncOpenAI
    except ImportError:
        raise SystemExit("pip install openai")

    sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    verses = sample["verses"]
    logger.info("running %d verses against %s @ %s, workers=%d",
                len(verses), QWEN_MODEL, QWEN_BASE_URL, WORKERS)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    client = AsyncOpenAI(
        api_key="not-needed",
        base_url=QWEN_BASE_URL,
        timeout=600.0,
        max_retries=2,
    )

    sem = asyncio.Semaphore(WORKERS)

    async def worker(verse: dict) -> Optional[dict]:
        baseline_path = RESPONSES_DIR / verse["file"]
        try:
            baseline_record = json.loads(baseline_path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error("could not load baseline %s: %s", baseline_path, e)
            return None
        async with sem:
            try:
                return await translate_one_verse(client, verse, baseline_record)
            except Exception as e:
                logger.exception("verse %s failed: %s", verse["verse_path"], e)
                return {"verse_path": verse["verse_path"], "error": str(e)}

    overall_start = time.time()
    out_records = await asyncio.gather(*[worker(v) for v in verses])
    overall_elapsed = round(time.time() - overall_start, 2)

    successes = [r for r in out_records if r and "error" not in r]
    for rec in successes:
        verse_id = rec["verse_path"].removeprefix("/books/").replace("/", "_").replace(":", "_")
        out_path = RESULTS_DIR / f"{verse_id}.qwen.json"
        out_path.write_text(json.dumps(rec, indent=2, ensure_ascii=False),
                            encoding="utf-8")

    # Summary stats
    total_prompt = sum(b["prompt_tokens"] for r in successes for b in r["batches"])
    total_completion = sum(b["completion_tokens"] for r in successes for b in r["batches"])
    n_batches = sum(len(r["batches"]) for r in successes)
    n_failed_batches = sum(1 for r in successes for b in r["batches"] if not b["ok"])
    parse_rate = 100 * (n_batches - n_failed_batches) / n_batches if n_batches else 0

    summary = {
        "model": QWEN_MODEL,
        "base_url": QWEN_BASE_URL,
        "workers": WORKERS,
        "verse_count": len(successes),
        "overall_wall_seconds": overall_elapsed,
        "batches_total": n_batches,
        "batches_failed": n_failed_batches,
        "parse_rate_pct": round(parse_rate, 2),
        "prompt_tokens_total": total_prompt,
        "completion_tokens_total": total_completion,
    }
    (BENCH_DIR / "qwen_run_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info("done — wrote %d results in %.1fs", len(successes), overall_elapsed)
    logger.info("parse rate: %.1f%% (%d/%d batches)",
                parse_rate, n_batches - n_failed_batches, n_batches)
    logger.info("tokens: prompt=%d completion=%d", total_prompt, total_completion)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
