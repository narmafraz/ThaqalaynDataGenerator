"""Benchmark Phase 1 (structural pass) on Qwen 3.6-35B with strict JSON-schema.

Phase 1 is the hard one — closed-vocabulary enums (topics, tags, content_type),
exact-coverage chunk segmentation, full Arabic diacritization, and `surah:ayah`
numeric Quran refs. Prior memory says gpt-4.1-mini fails Phase 1 instruction
following (5/5 quarantined on al-khisal diagnostic). Question: does strict
JSON-schema response_format + Qwen 3.6 do better?

For each verse in the existing 30-verse sample:
  1. Load the verse via extract_pipeline_request() from ThaqalaynData
  2. Build the production Phase 1 prompt
  3. Call qwen36-fast with strict JSON-schema enforcement
  4. Save the Qwen P1 output side-by-side with the existing gpt-5.4 P1 fields
     (from baseline responses)

Saves to benchmark/phase1_qwen/. Never writes to ai-content/.
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
)
from app.ai_pipeline import extract_pipeline_request, load_topic_taxonomy  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("phase1_qwen")

BENCH_DIR = REPO_ROOT / "benchmark" / "phase1_qwen"
SAMPLE_PATH = REPO_ROOT / "benchmark" / "phase4_qwen" / "sample.json"
RESULTS_DIR = BENCH_DIR / "results"
RAW_DIR = BENCH_DIR / "raw_responses"
DATA_DIR = REPO_ROOT.parent / "ThaqalaynData"

QWEN_BASE_URL = "http://192.168.0.66:8000/v1"
QWEN_MODEL = "qwen36-fast"
WORKERS = 8

# Closed enums from the production prompt
TAGS_ENUM = [
    "theology", "ethics", "jurisprudence", "worship", "quran_commentary",
    "prophetic_tradition", "family", "social_relations", "knowledge", "dua",
    "afterlife", "history", "economy", "governance",
]
CONTENT_TYPE_ENUM = [
    "legal_ruling", "ethical_teaching", "narrative", "prophetic_tradition",
    "quranic_commentary", "supplication", "creedal", "eschatological",
    "biographical", "theological", "exhortation", "cosmological",
]
CHUNK_TYPE_ENUM = ["isnad", "opening", "body", "quran_quote", "closing"]

# Topic enum taken straight from the production prompt's closed list
TOPICS_ENUM = [
    "abrogation", "ahlulbayt_virtues", "anger_control", "backbiting", "barzakh",
    "charity", "community", "companions", "consultation", "death_dying", "dhikr",
    "divine_attributes", "divine_decree", "divine_justice", "divine_knowledge",
    "etiquette", "etiquette_of_dua", "events", "fasting", "fasting_rulings",
    "financial_law", "forbidding_evil", "friendship", "ghadir", "gratitude",
    "hadith_sciences", "hajj", "halal_haram", "honesty", "hospitality",
    "humility", "ignorance", "imamate", "imams_biography", "inheritance",
    "intercession", "judicial_rulings", "justice_system", "karbala", "kinship",
    "leadership", "marriage_family_law", "miracles", "mosque_etiquette",
    "neighbors", "night_prayer", "occasions_of_revelation", "oppression",
    "orphans", "paradise_hell", "parenting", "patience", "poverty_wealth",
    "prayer_rulings", "prophethood", "prophetic_character", "prophets",
    "quran_interpretation_method", "quran_recitation", "quran_virtues",
    "reasoning", "reckoning", "religious_authority", "repentance",
    "resurrection", "rights_of_others", "rights_of_rulers", "ritual_purity",
    "salat", "scholars_virtues", "seeking_forgiveness", "seeking_knowledge",
    "seeking_refuge", "signs_of_end", "sincerity", "specific_supplications",
    "spousal_rights", "sunnah", "tafsir_specific_verse", "tawhid", "teaching",
    "times_for_dua", "trade_ethics", "trust", "usury", "womens_rights",
    "work_livelihood", "zakat_khums",
]


def build_phase1_schema() -> dict:
    """JSON schema for the Phase 1 response, matching phased_prompts.build_phase1_user_message."""
    return {
        "type": "object",
        "properties": {
            "has_chain": {"type": "boolean"},
            "tags": {
                "type": "array",
                "items": {"type": "string", "enum": TAGS_ENUM},
                "minItems": 2,
                "maxItems": 5,
            },
            "content_type": {"type": "string", "enum": CONTENT_TYPE_ENUM},
            "chunks": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "properties": {
                        "chunk_type": {"type": "string", "enum": CHUNK_TYPE_ENUM},
                        "arabic_text": {"type": "string"},
                        "translations": {
                            "type": "object",
                            "properties": {"en": {"type": "string"}},
                            "required": ["en"],
                            "additionalProperties": False,
                        },
                    },
                    "required": ["chunk_type", "arabic_text", "translations"],
                    "additionalProperties": False,
                },
            },
            "translations": {
                "type": "object",
                "properties": {
                    "en": {
                        "type": "object",
                        "properties": {
                            "summary": {"type": "string"},
                            "seo_question": {"type": "string"},
                            "key_terms": {
                                "type": "object",
                                "additionalProperties": {"type": "string"},
                            },
                        },
                        "required": ["summary", "seo_question", "key_terms"],
                        "additionalProperties": False,
                    }
                },
                "required": ["en"],
                "additionalProperties": False,
            },
            "related_quran": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "ref": {"type": "string", "pattern": "^[0-9]{1,3}:[0-9]{1,3}$"},
                        "relationship": {"type": "string"},
                    },
                    "required": ["ref", "relationship"],
                    "additionalProperties": False,
                },
            },
            "topics": {
                "type": "array",
                "minItems": 1,
                "maxItems": 5,
                "items": {"type": "string", "enum": TOPICS_ENUM},
            },
        },
        "required": [
            "has_chain", "tags", "content_type", "chunks",
            "translations", "related_quran", "topics",
        ],
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
                "json_schema": {
                    "name": "phase1_response",
                    "schema": schema,
                    "strict": True,
                },
            },
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        )
        elapsed = round(time.time() - start, 2)
        text = response.choices[0].message.content or ""
        usage = response.usage
        pt = usage.prompt_tokens if usage else 0
        ct = usage.completion_tokens if usage else 0
        try:
            parsed = json.loads(text)
            return CallResult(True, text, parsed, pt, ct, elapsed)
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
    user = build_phase1_user_message(request)

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
    try:
        from openai import AsyncOpenAI
    except ImportError:
        raise SystemExit("pip install openai")

    sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    verses = sample["verses"]
    logger.info("Phase 1 bench: %d verses against %s @ %s, strict json_schema, workers=%d",
                len(verses), QWEN_MODEL, QWEN_BASE_URL, WORKERS)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    taxonomy = load_topic_taxonomy()
    schema = build_phase1_schema()

    client = AsyncOpenAI(api_key="not-needed", base_url=QWEN_BASE_URL,
                        timeout=600.0, max_retries=2)
    sem = asyncio.Semaphore(WORKERS)

    async def worker(verse: dict) -> dict:
        async with sem:
            try:
                return await run_one(client, verse, taxonomy, schema)
            except Exception as e:
                logger.exception("verse %s failed", verse["verse_path"])
                return {"verse_path": verse["verse_path"], "error": str(e)}

    overall_start = time.time()
    out = await asyncio.gather(*[worker(v) for v in verses])
    overall_elapsed = round(time.time() - overall_start, 2)

    for rec in out:
        if not rec or "error" in rec and rec.get("error") and rec.get("qwen_p1") is None and rec.get("ok") is False:
            pass
        verse_id = rec["verse_path"].removeprefix("/books/").replace("/", "_").replace(":", "_")
        (RESULTS_DIR / f"{verse_id}.qwen-p1.json").write_text(
            json.dumps(rec, indent=2, ensure_ascii=False), encoding="utf-8")

    successes = [r for r in out if r.get("ok")]
    parse_rate = 100 * len(successes) / len(out) if out else 0
    total_pt = sum(r.get("prompt_tokens", 0) for r in successes)
    total_ct = sum(r.get("completion_tokens", 0) for r in successes)
    summary = {
        "model": QWEN_MODEL,
        "base_url": QWEN_BASE_URL,
        "workers": WORKERS,
        "verse_count": len(out),
        "ok_count": len(successes),
        "parse_rate_pct": round(parse_rate, 2),
        "overall_wall_seconds": overall_elapsed,
        "prompt_tokens_total": total_pt,
        "completion_tokens_total": total_ct,
        "response_format": "json_schema_strict",
    }
    (BENCH_DIR / "qwen_p1_run_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("done — wrote %d results in %.1fs", len(out), overall_elapsed)
    logger.info("parse rate: %.1f%% (%d/%d)", parse_rate, len(successes), len(out))
    logger.info("tokens: prompt=%d completion=%d", total_pt, total_ct)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
