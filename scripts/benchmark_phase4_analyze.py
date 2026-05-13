"""Compare Phase 4 Qwen results against the existing baseline responses.

For each sampled verse, computes per-language reliability metrics across
the three models (gpt-4.1-mini baseline, gpt-5.4-mini baseline, qwen36-fast).

Outputs:
  benchmark/phase4_qwen/analysis.json  -- raw aggregated metrics
  benchmark/phase4_qwen/analysis.md    -- human-readable summary

Metrics checked per (verse, lang, model):
  1. Translation present and non-empty
  2. Length ratio relative to EN translation (flag <0.4 or >2.5)
  3. Arabic echo in non-Arabic-script slot (flag if AR chars appear in
     European/CJK languages)
  4. Missing diacritics on European languages when EN has named/proper-noun
     style characters (heuristic only)

Aggregates per language and per baseline model.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent

_cli = argparse.ArgumentParser()
_cli.add_argument("--bench-dir", default="benchmark/phase4_qwen",
                  help="Path (relative to repo root) to the bench output dir")
_args = _cli.parse_args()
BENCH_DIR = REPO_ROOT / _args.bench_dir
RESULTS_DIR = BENCH_DIR / "results"
SOURCE_DATA_DIR = Path(REPO_ROOT.parent / "ThaqalaynDataSources")
RESPONSES_DIR = SOURCE_DATA_DIR / "ai-content" / "corpus" / "responses"

NON_EN_LANGUAGES = ["ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"]
LANG_NAMES = {
    "ur": "Urdu", "tr": "Turkish", "fa": "Farsi", "id": "Indonesian",
    "bn": "Bengali", "es": "Spanish", "fr": "French", "de": "German",
    "ru": "Russian", "zh": "Chinese",
}

ARABIC_RANGE = re.compile(r"[؀-ۿݐ-ݿࢠ-ࣿﭐ-﷿ﹰ-﻿]")
# Languages where Arabic-script presence would be a problem
LATIN_LANGS = {"tr", "id", "es", "fr", "de"}
# Languages whose script is NOT Arabic
NON_ARABIC_SCRIPT_LANGS = {"tr", "id", "bn", "es", "fr", "de", "ru", "zh"}
# Urdu and Farsi legitimately use Arabic script

EUROPEAN_DIACRITICS = re.compile(r"[áàâäãåçéèêëíìîïñóòôöõúùûüýÿÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕÚÙÛÜÝŸşçğıİÖÜşçğıİ]")


def load_baseline(verse: dict) -> dict:
    fp = RESPONSES_DIR / verse.get("file", verse.get("baseline_file", ""))
    return json.loads(fp.read_text(encoding="utf-8"))


def gather_translations(record: dict) -> dict:
    """Flatten record into {lang: {summary, seo_question, chunks: [str]}}."""
    result = record.get("result", {})
    out: dict[str, dict] = defaultdict(lambda: {"summary": "", "seo_question": "", "chunks": []})
    trans = result.get("translations", {}) or {}
    for lang in ["en"] + NON_EN_LANGUAGES:
        block = trans.get(lang, {}) or {}
        out[lang]["summary"] = block.get("summary", "") or ""
        out[lang]["seo_question"] = block.get("seo_question", "") or ""
    chunks = result.get("chunks", []) or []
    for c in chunks:
        ct = c.get("translations", {}) or {}
        for lang in ["en"] + NON_EN_LANGUAGES:
            v = ct.get(lang, "")
            if isinstance(v, str):
                out[lang]["chunks"].append(v)
            else:
                out[lang]["chunks"].append("")
    return out


def check_translation(en_text: str, target_text: str, lang: str) -> dict:
    """Return per-translation flags."""
    flags = {
        "empty": False,
        "length_ratio": None,
        "length_flag": False,
        "arabic_echo": False,
        "diacritics_flag": False,
    }
    if not target_text or not target_text.strip():
        flags["empty"] = True
        return flags
    if en_text:
        ratio = len(target_text) / max(1, len(en_text))
        flags["length_ratio"] = round(ratio, 2)
        if ratio < 0.3 or ratio > 3.5:
            flags["length_flag"] = True
    if lang in NON_ARABIC_SCRIPT_LANGS:
        if ARABIC_RANGE.search(target_text):
            flags["arabic_echo"] = True
    if lang in ("tr", "es", "fr", "de"):
        if len(target_text) > 30 and not EUROPEAN_DIACRITICS.search(target_text):
            flags["diacritics_flag"] = True
    return flags


def main() -> None:
    # sample.json always lives in the round 1 dir; rounds reuse the same sample
    sample_path = REPO_ROOT / "benchmark" / "phase4_qwen" / "sample.json"
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    verses = sample["verses"]
    summary_obj = json.loads((BENCH_DIR / "qwen_run_summary.json").read_text(encoding="utf-8"))

    per_verse: list[dict] = []
    # model_key -> lang -> counter dict
    agg: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    # totals for normalization
    totals_baseline: dict[str, int] = defaultdict(int)  # by baseline model name
    totals_qwen: int = 0

    for verse in verses:
        verse_id = verse["verse_path"].removeprefix("/books/").replace("/", "_").replace(":", "_")
        qwen_path = RESULTS_DIR / f"{verse_id}.qwen.json"
        if not qwen_path.exists():
            print(f"WARN: missing qwen result for {verse_id}", file=sys.stderr)
            continue
        qwen_rec = json.loads(qwen_path.read_text(encoding="utf-8"))
        baseline_rec = load_baseline(verse)
        baseline_model = verse["baseline_p4"]

        baseline_flat = gather_translations(baseline_rec)
        qwen_flat = gather_translations(qwen_rec)
        en_chunks = baseline_flat["en"]["chunks"]
        en_summary = baseline_flat["en"]["summary"]
        en_seo = baseline_flat["en"]["seo_question"]

        verse_record = {
            "verse": verse["verse_path"],
            "baseline_p4": baseline_model,
            "stratum": verse.get("stratum"),
            "chunk_count": verse.get("chunk_count"),
            "ar_word_count": verse.get("ar_word_count"),
            "qwen_elapsed_s": qwen_rec.get("elapsed_s"),
            "qwen_batches": qwen_rec.get("batches"),
            "by_lang": {},
        }
        totals_baseline[baseline_model] += 1
        totals_qwen += 1

        for lang in NON_EN_LANGUAGES:
            # Combine all translation surfaces (chunks + summary + seo)
            # so a missing translation anywhere counts.
            b = baseline_flat[lang]
            q = qwen_flat[lang]
            slot_data = []
            for label, en_text, b_text, q_text in [
                ("summary", en_summary, b["summary"], q["summary"]),
                ("seo_question", en_seo, b["seo_question"], q["seo_question"]),
            ]:
                slot_data.append((label, en_text, b_text, q_text))
            for i, en_chunk in enumerate(en_chunks):
                b_chunk = b["chunks"][i] if i < len(b["chunks"]) else ""
                q_chunk = q["chunks"][i] if i < len(q["chunks"]) else ""
                slot_data.append((f"chunk[{i}]", en_chunk, b_chunk, q_chunk))

            b_flags = []
            q_flags = []
            for label, en_text, b_text, q_text in slot_data:
                bf = check_translation(en_text, b_text, lang)
                qf = check_translation(en_text, q_text, lang)
                bf["slot"] = label
                qf["slot"] = label
                b_flags.append(bf)
                q_flags.append(qf)

            # Per-language summary for this verse
            def summarize(flags_list):
                return {
                    "empty_slots": sum(1 for f in flags_list if f["empty"]),
                    "length_flags": sum(1 for f in flags_list if f["length_flag"]),
                    "arabic_echo": sum(1 for f in flags_list if f["arabic_echo"]),
                    "diacritics_flags": sum(1 for f in flags_list if f["diacritics_flag"]),
                    "total_slots": len(flags_list),
                }

            verse_record["by_lang"][lang] = {
                "baseline": summarize(b_flags),
                "qwen": summarize(q_flags),
            }

            # Aggregate (treat the baseline's model name as the key)
            for key, flags_list in [(baseline_model, b_flags), ("qwen36-fast", q_flags)]:
                agg[key][lang]["empty"] += sum(1 for f in flags_list if f["empty"])
                agg[key][lang]["length_flag"] += sum(1 for f in flags_list if f["length_flag"])
                agg[key][lang]["arabic_echo"] += sum(1 for f in flags_list if f["arabic_echo"])
                agg[key][lang]["diacritics"] += sum(1 for f in flags_list if f["diacritics_flag"])
                agg[key][lang]["total"] += len(flags_list)

        per_verse.append(verse_record)

    out = {
        "qwen_run_summary": summary_obj,
        "verse_count": len(per_verse),
        "totals_baseline": dict(totals_baseline),
        "totals_qwen": totals_qwen,
        "per_model_per_lang": {
            m: {lang: dict(stats) for lang, stats in langs.items()}
            for m, langs in agg.items()
        },
        "per_verse": per_verse,
    }
    (BENCH_DIR / "analysis.json").write_text(
        json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")

    # Markdown summary
    lines = []
    lines.append("# Phase 4 Open-Weight Benchmark — Automated Metrics\n")
    lines.append(f"Verses sampled: {len(per_verse)}\n")
    lines.append(f"Baseline counts: {dict(totals_baseline)}\n")
    lines.append("\n## Qwen run summary\n")
    for k, v in summary_obj.items():
        lines.append(f"- {k}: {v}")
    lines.append("\n## Reliability flags per model per language\n")
    lines.append("Counts across all translation slots (per-verse summary + seo + N chunks).\n")
    lines.append("| Lang | Model | empty | length | ar-echo | diacritics | total |")
    lines.append("|------|-------|------:|-------:|--------:|-----------:|------:|")
    for lang in NON_EN_LANGUAGES:
        for model in sorted(agg.keys()):
            s = agg[model][lang]
            lines.append(
                f"| {LANG_NAMES[lang]} | {model} | {s['empty']} | {s['length_flag']} | "
                f"{s['arabic_echo']} | {s['diacritics']} | {s['total']} |"
            )

    lines.append("\n## Per-verse Qwen timing & failures\n")
    lines.append("| Verse | Stratum | Chunks | AR words | Elapsed | Failed batches |")
    lines.append("|-------|---------|-------:|---------:|--------:|---------------:|")
    per_verse.sort(key=lambda v: -v["qwen_elapsed_s"])
    for v in per_verse:
        failed = sum(1 for b in (v["qwen_batches"] or []) if not b["ok"])
        lines.append(f"| `{v['verse']}` | {v['stratum']} | {v['chunk_count']} | "
                     f"{v['ar_word_count']} | {v['qwen_elapsed_s']:.0f}s | {failed} |")

    (BENCH_DIR / "analysis.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote analysis.json and analysis.md")


if __name__ == "__main__":
    main()
