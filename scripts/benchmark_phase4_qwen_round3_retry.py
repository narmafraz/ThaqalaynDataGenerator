"""Round 3: retry the 2 verses that failed in round 2 with fixed parameters.

Round 2 failures:
  - al-kafi:1:4:125:4 batch 0 → APITimeout at 1801s (SDK timeout 600s; total was 30 min)
  - faqih:2:3:97:2 batch 0 → max_tokens (8192) exhausted mid-string

Fixes:
  - max_tokens 8192 → 16384
  - SDK timeout 600s → 1800s
  - workers 1 (these are the slow verses; we want full Spark headroom each)

Saves to benchmark/phase4_qwen_round3/.
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("phase4_r3")

SOURCE_DATA_DIR = Path(os.environ.get(
    "SOURCE_DATA_DIR",
    REPO_ROOT.parent / "ThaqalaynDataSources",
))
RESPONSES_DIR = SOURCE_DATA_DIR / "ai-content" / "corpus" / "responses"
SAMPLE_PATH = REPO_ROOT / "benchmark" / "phase4_qwen" / "sample.json"
BENCH_DIR = REPO_ROOT / "benchmark" / "phase4_qwen_round3"
RESULTS_DIR = BENCH_DIR / "results"
RAW_DIR = BENCH_DIR / "raw_responses"

QWEN_BASE_URL = "http://192.168.0.66:8000/v1"
QWEN_MODEL = "qwen36-fast"

# Verses to retry
RETRY_PATHS = {
    "/books/al-kafi:1:4:125:4",
    "/books/man-la-yahduruhu-al-faqih:2:3:97:2",
}


def build_schema(num_chunks: int, include_metadata: bool) -> dict:
    lang_obj = {
        "type": "object",
        "properties": {lang: {"type": "string"} for lang in NON_EN_LANGUAGES},
        "required": list(NON_EN_LANGUAGES),
        "additionalProperties": False,
    }
    chunk_obj = {
        "type": "object",
        "properties": {"translations": lang_obj},
        "required": ["translations"],
        "additionalProperties": False,
    }
    properties = {
        "chunks": {
            "type": "array",
            "items": chunk_obj,
            "minItems": num_chunks,
            "maxItems": num_chunks,
        }
    }
    required = ["chunks"]
    if include_metadata:
        properties["summary"] = lang_obj
        properties["seo_question"] = lang_obj
        required += ["summary", "seo_question"]
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


@dataclass
class CallResult:
    ok: bool
    raw_text: str
    parsed: Optional[dict]
    prompt_tokens: int
    completion_tokens: int
    elapsed: float
    error: Optional[str] = None


async def call_qwen(client, system, user, schema, max_tokens=16384) -> CallResult:
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
                "json_schema": {"name": "phase4", "schema": schema, "strict": True},
            },
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        )
        elapsed = round(time.time() - start, 2)
        text = response.choices[0].message.content or ""
        usage = response.usage
        pt = usage.prompt_tokens if usage else 0
        ct = usage.completion_tokens if usage else 0
        try:
            return CallResult(True, text, json.loads(_strip_code_fences(text)), pt, ct, elapsed)
        except json.JSONDecodeError as e:
            return CallResult(False, text, None, pt, ct, elapsed, error=f"json_parse: {e}")
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        return CallResult(False, "", None, 0, 0, elapsed, error=f"{type(e).__name__}: {str(e)[:300]}")


def build_arabic_text(result: dict) -> str:
    return "\n".join(c.get("arabic_text", "") for c in (result.get("chunks") or []) if c.get("arabic_text"))


async def translate_one_verse(client, verse: dict, baseline_record: dict) -> dict:
    verse_path = verse["verse_path"]
    result = copy.deepcopy(baseline_record["result"])
    for chunk in result.get("chunks", []) or []:
        chunk["translations"] = {"en": (chunk.get("translations") or {}).get("en", "")}
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

    per_batch_meta = []
    overall_start = time.time()

    for batch_idx, batch_chunks in enumerate(batches):
        include_metadata = (batch_idx == 0)
        if not batch_chunks and not include_metadata:
            continue
        system, user = _build_batch_prompt(
            batch_chunks, en_summary, en_seo, arabic_text, include_metadata, batch_idx,
        )
        schema = build_schema(len(batch_chunks), include_metadata)
        cr = await call_qwen(client, system, user, schema, max_tokens=16384)
        per_batch_meta.append({
            "batch_idx": batch_idx, "ok": cr.ok, "error": cr.error,
            "prompt_tokens": cr.prompt_tokens, "completion_tokens": cr.completion_tokens,
            "elapsed_s": cr.elapsed, "chunk_count": len(batch_chunks),
            "include_metadata": include_metadata,
        })
        verse_id = verse_path.removeprefix("/books/").replace("/", "_").replace(":", "_")
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        (RAW_DIR / f"{verse_id}.batch{batch_idx}.raw.txt").write_text(
            cr.raw_text or "", encoding="utf-8")
        if not cr.ok or cr.parsed is None:
            logger.warning("verse=%s batch=%d FAILED: %s", verse_path, batch_idx, cr.error)
            continue

        trans_data = cr.parsed
        chunk_offset = batch_idx * CHUNK_BATCH_SIZE
        for j, ct in enumerate(trans_data.get("chunks", [])):
            abs_idx = chunk_offset + j
            if abs_idx < len(chunks):
                tr = ct.get("translations") or {}
                if "translations" not in chunks[abs_idx]:
                    chunks[abs_idx]["translations"] = {}
                for lang in NON_EN_LANGUAGES:
                    if lang in tr:
                        chunks[abs_idx]["translations"][lang] = tr[lang]
        if include_metadata:
            for lang in NON_EN_LANGUAGES:
                tr = result.get("translations") or {}
                if lang not in tr:
                    tr[lang] = {}
                if lang in trans_data.get("summary", {}):
                    tr[lang]["summary"] = trans_data["summary"][lang]
                if lang in trans_data.get("seo_question", {}):
                    tr[lang]["seo_question"] = trans_data["seo_question"][lang]
                tr[lang].setdefault("key_terms", {})

    _fill_empty_translations(result)
    return {
        "verse_path": verse_path,
        "round": 3,
        "max_tokens": 16384,
        "timeout_s": 1800,
        "elapsed_s": round(time.time() - overall_start, 2),
        "batches": per_batch_meta,
        "result": result,
    }


async def main_async() -> None:
    from openai import AsyncOpenAI
    sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    targets = [v for v in sample["verses"] if v["verse_path"] in RETRY_PATHS]
    logger.info("round 3 retry: %d verses", len(targets))
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    # Use 1800s SDK timeout — much longer than round 2's 600s
    client = AsyncOpenAI(api_key="not-needed", base_url=QWEN_BASE_URL,
                        timeout=1800.0, max_retries=1)
    for verse in targets:
        baseline_path = RESPONSES_DIR / verse["file"]
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
        rec = await translate_one_verse(client, verse, baseline)
        verse_id = rec["verse_path"].removeprefix("/books/").replace("/", "_").replace(":", "_")
        (RESULTS_DIR / f"{verse_id}.qwen.json").write_text(
            json.dumps(rec, indent=2, ensure_ascii=False), encoding="utf-8")
        ok = all(b["ok"] for b in rec["batches"])
        logger.info("%s → %s (%.1fs, %d batches)",
                    rec["verse_path"], "OK" if ok else "FAIL", rec["elapsed_s"], len(rec["batches"]))


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
