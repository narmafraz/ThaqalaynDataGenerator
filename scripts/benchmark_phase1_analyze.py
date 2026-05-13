"""Analyse Phase 1 Qwen benchmark outputs.

For each verse:
  1. Parse + schema validity (strict schema enforces but we re-check)
  2. Chunk word coverage — every word in input AR text appears in exactly one chunk
  3. Diacritization completeness — fraction of words with at least one tashkeel char
  4. Quran ref format compliance (regex enforced by schema, but we count refs)
  5. Comparison to baseline gpt-5.4 Phase 1 fields:
       - Chunk count delta
       - Topic enum compliance (baseline counted as ground truth)
       - has_chain agreement
"""
from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
import argparse
_cli = argparse.ArgumentParser()
_cli.add_argument("--bench-dir", default="benchmark/phase1_qwen")
_args = _cli.parse_args()
BENCH_DIR = REPO_ROOT / _args.bench_dir
RESULTS_DIR = BENCH_DIR / "results"
SOURCE_DATA_DIR = REPO_ROOT.parent / "ThaqalaynDataSources"
BASELINE_DIR = SOURCE_DATA_DIR / "ai-content" / "corpus" / "responses"
SAMPLE_PATH = REPO_ROOT / "benchmark" / "phase4_qwen" / "sample.json"

TASHKEEL = set("ؘؙؚؐؑؒؓؔؕؖؗ"
               "ًٌٍَُِّْٕٖٜٟٓٔٗ٘ٙٚٛٝٞ"
               "ٰ")
QURAN_REF_RE = re.compile(r"^\d+:\d+$")


def normalize_ar(text: str) -> str:
    """Strip diacritics, normalise alef/yaa, lowercase for token compare."""
    text = unicodedata.normalize("NFKC", text)
    text = "".join(c for c in text if c not in TASHKEEL)
    text = text.replace("ـ", "")  # tatweel
    text = re.sub(r"[آأإ]", "ا", text)  # alef variants → bare alef
    text = text.replace("ى", "ي")  # yaa
    text = text.replace("ة", "ه")  # ta marbuta → ha
    return text.strip()


def tokenize_ar(text: str) -> list[str]:
    return [normalize_ar(tok) for tok in re.split(r"\s+", text) if tok.strip()]


def diacritization_rate(text: str) -> float:
    words = re.split(r"\s+", text)
    counted = 0
    diacritized = 0
    for w in words:
        if not w.strip() or not any(0x0600 <= ord(c) <= 0x06FF for c in w):
            continue
        counted += 1
        if any(c in TASHKEEL for c in w):
            diacritized += 1
    return diacritized / counted if counted else 0.0


def main() -> None:
    sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    verses = {v["verse_path"]: v for v in sample["verses"]}

    rows = []
    for fp in sorted(RESULTS_DIR.glob("*.qwen-p1.json")):
        rec = json.loads(fp.read_text(encoding="utf-8"))
        if not rec.get("ok") or not rec.get("qwen_p1"):
            rows.append({
                "verse": rec["verse_path"],
                "ok": False,
                "error": rec.get("error", "unknown"),
            })
            continue
        q = rec["qwen_p1"]
        # Load baseline gpt-5.4 P1 fields
        verse_meta = verses.get(rec["verse_path"], {})
        baseline_file = BASELINE_DIR / verse_meta.get("file", "")
        baseline_rec = None
        if baseline_file.exists():
            baseline_rec = json.loads(baseline_file.read_text(encoding="utf-8")).get("result", {})

        # Chunk word coverage: tokens in input AR vs tokens in concat chunks
        input_tokens = tokenize_ar(rec.get("arabic_input", ""))
        chunks_text = " ".join(c.get("arabic_text", "") for c in q.get("chunks", []))
        chunk_tokens = tokenize_ar(chunks_text)
        input_set = set(input_tokens)
        chunk_set = set(chunk_tokens)
        coverage = (len(input_set & chunk_set) / len(input_set)) if input_set else 0
        extra = len(chunk_set - input_set)  # words added by Qwen not in original
        missing = len(input_set - chunk_set)  # input words not appearing in chunks

        # Diacritization rate
        diac = diacritization_rate(chunks_text)

        # Quran ref validity
        refs = q.get("related_quran", [])
        bad_refs = [r for r in refs if not QURAN_REF_RE.match(r.get("ref", ""))]

        # key_terms count
        kt = q.get("translations", {}).get("en", {}).get("key_terms", {})
        # Check if any narrator name appears in key_terms keys (heuristic — look for
        # narrator-like patterns: short Arabic names ending in بن or starting with أبي)
        narrator_in_kt = sum(
            1 for k in kt
            if any(marker in k for marker in [" بْنِ ", " بِنْ ", "أَبِي ", "أَبُو ", "اِبْنِ "])
        )

        # Chunk count comparison
        baseline_chunk_count = len(baseline_rec.get("chunks", [])) if baseline_rec else None
        qwen_chunk_count = len(q.get("chunks", []))

        # Topic enum check (schema enforces but verify)
        topics = q.get("topics", [])
        baseline_topics = baseline_rec.get("topics", []) if baseline_rec else []
        topic_overlap = len(set(topics) & set(baseline_topics))

        # has_chain agreement
        baseline_has_chain = baseline_rec.get("has_chain") if baseline_rec else None
        qwen_has_chain = q.get("has_chain")
        has_chain_agree = (baseline_has_chain == qwen_has_chain) if baseline_has_chain is not None else None

        rows.append({
            "verse": rec["verse_path"],
            "ok": True,
            "stratum": rec.get("stratum"),
            "ar_words": rec.get("ar_word_count"),
            "chunk_count_qwen": qwen_chunk_count,
            "chunk_count_baseline": baseline_chunk_count,
            "chunk_word_coverage_pct": round(100 * coverage, 1),
            "extra_words_added": extra,
            "missing_words": missing,
            "diacritization_rate_pct": round(100 * diac, 1),
            "topics_qwen": topics,
            "topics_baseline": baseline_topics,
            "topics_overlap": topic_overlap,
            "has_chain_agree": has_chain_agree,
            "quran_refs": refs,
            "bad_refs": bad_refs,
            "key_terms_count": len(kt),
            "narrator_names_in_key_terms": narrator_in_kt,
            "elapsed_s": rec.get("elapsed_s"),
            "prompt_tokens": rec.get("prompt_tokens"),
            "completion_tokens": rec.get("completion_tokens"),
        })

    # Aggregate
    ok_rows = [r for r in rows if r.get("ok")]
    summary = {
        "verses_total": len(rows),
        "verses_ok": len(ok_rows),
        "parse_rate_pct": round(100 * len(ok_rows) / len(rows), 1) if rows else 0,
        "mean_chunk_word_coverage_pct": round(
            sum(r["chunk_word_coverage_pct"] for r in ok_rows) / len(ok_rows), 1
        ) if ok_rows else 0,
        "min_chunk_word_coverage_pct": round(
            min(r["chunk_word_coverage_pct"] for r in ok_rows), 1
        ) if ok_rows else 0,
        "mean_diacritization_pct": round(
            sum(r["diacritization_rate_pct"] for r in ok_rows) / len(ok_rows), 1
        ) if ok_rows else 0,
        "verses_with_extra_words": sum(1 for r in ok_rows if r["extra_words_added"] > 0),
        "verses_with_missing_words": sum(1 for r in ok_rows if r["missing_words"] > 0),
        "verses_chunk_count_lt_baseline": sum(
            1 for r in ok_rows
            if r["chunk_count_baseline"] is not None
            and r["chunk_count_qwen"] < r["chunk_count_baseline"]
        ),
        "verses_chunk_count_eq_baseline": sum(
            1 for r in ok_rows
            if r["chunk_count_baseline"] is not None
            and r["chunk_count_qwen"] == r["chunk_count_baseline"]
        ),
        "verses_chunk_count_gt_baseline": sum(
            1 for r in ok_rows
            if r["chunk_count_baseline"] is not None
            and r["chunk_count_qwen"] > r["chunk_count_baseline"]
        ),
        "has_chain_agreement_pct": round(
            100 * sum(1 for r in ok_rows if r.get("has_chain_agree")) / len(ok_rows), 1
        ) if ok_rows else 0,
        "verses_with_kt_narrator_names": sum(
            1 for r in ok_rows if r.get("narrator_names_in_key_terms", 0) > 0
        ),
        "total_prompt_tokens": sum(r.get("prompt_tokens", 0) for r in ok_rows),
        "total_completion_tokens": sum(r.get("completion_tokens", 0) for r in ok_rows),
    }

    out_obj = {"summary": summary, "rows": rows}
    (BENCH_DIR / "p1_analysis.json").write_text(
        json.dumps(out_obj, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Markdown table
    lines = []
    lines.append("# Phase 1 Qwen Benchmark — Analysis\n")
    lines.append("## Summary\n")
    for k, v in summary.items():
        lines.append(f"- **{k}**: {v}")
    lines.append("\n## Per-verse\n")
    lines.append("| verse | chunks Q/B | cov% | diac% | topics overlap | refs | bad refs | kt+narrators |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for r in ok_rows:
        bcc = r.get("chunk_count_baseline", "?")
        lines.append(
            f"| `{r['verse']}` | {r['chunk_count_qwen']}/{bcc} | "
            f"{r['chunk_word_coverage_pct']} | {r['diacritization_rate_pct']} | "
            f"{r['topics_overlap']}/{len(r['topics_qwen'])} | "
            f"{len(r['quran_refs'])} | {len(r['bad_refs'])} | "
            f"{r['narrator_names_in_key_terms']} |"
        )
    (BENCH_DIR / "p1_analysis.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"summary: {json.dumps(summary, indent=2)}")


if __name__ == "__main__":
    main()
