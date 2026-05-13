"""Dump side-by-side translation comparisons to a single markdown file
that I (Claude) can read for manual quality scoring.

For each verse:
  - Arabic source (joined chunks)
  - English reference (summary + chunks + seo)
  - Baseline P4 model translations
  - Qwen36 translations

Output: benchmark/phase4_qwen/pairs.md
"""
from __future__ import annotations

import json
from pathlib import Path

import argparse
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
_cli = argparse.ArgumentParser()
_cli.add_argument("--bench-dir", default="benchmark/phase4_qwen")
_args = _cli.parse_args()
BENCH_DIR = REPO_ROOT / _args.bench_dir
RESULTS_DIR = BENCH_DIR / "results"
SAMPLE_PATH = REPO_ROOT / "benchmark" / "phase4_qwen" / "sample.json"
SOURCE_DATA_DIR = Path(REPO_ROOT.parent / "ThaqalaynDataSources")
RESPONSES_DIR = SOURCE_DATA_DIR / "ai-content" / "corpus" / "responses"

NON_EN_LANGUAGES = ["ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"]
LANG_NAMES = {
    "ur": "Urdu", "tr": "Turkish", "fa": "Farsi", "id": "Indonesian",
    "bn": "Bengali", "es": "Spanish", "fr": "French", "de": "German",
    "ru": "Russian", "zh": "Chinese",
}


def gather(record: dict) -> dict:
    """Flatten translations from a record."""
    result = record.get("result", {})
    trans = result.get("translations", {}) or {}
    chunks = result.get("chunks", []) or []
    out = {"summary": {}, "seo": {}, "chunks_text": {}}
    for lang in ["en"] + NON_EN_LANGUAGES:
        block = trans.get(lang, {}) or {}
        out["summary"][lang] = block.get("summary", "") or ""
        out["seo"][lang] = block.get("seo_question", "") or ""
        chunk_strs = []
        for c in chunks:
            ct = c.get("translations", {}) or {}
            v = ct.get(lang, "")
            if isinstance(v, str):
                chunk_strs.append(v)
        out["chunks_text"][lang] = chunk_strs
    out["arabic_chunks"] = [c.get("arabic_text", "") for c in chunks]
    out["chunk_types"] = [c.get("chunk_type", "") for c in chunks]
    return out


def main() -> None:
    sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    verses = sample["verses"]

    lines: list[str] = []
    lines.append("# Phase 4 Side-by-side translation comparison\n")
    lines.append(f"Generated for manual quality scoring. 30 verses, 10 non-EN languages each.\n")
    lines.append("Format per verse: AR source → EN reference → Baseline → Qwen36 per language.\n")

    for verse in verses:
        verse_path = verse["verse_path"]
        verse_id = verse_path.removeprefix("/books/").replace("/", "_").replace(":", "_")
        baseline_path = RESPONSES_DIR / verse.get("file", "")
        qwen_path = RESULTS_DIR / f"{verse_id}.qwen.json"
        if not baseline_path.exists() or not qwen_path.exists():
            continue
        baseline_rec = json.loads(baseline_path.read_text(encoding="utf-8"))
        qwen_rec = json.loads(qwen_path.read_text(encoding="utf-8"))
        b = gather(baseline_rec)
        q = gather(qwen_rec)

        lines.append(f"\n---\n\n## `{verse_path}`")
        lines.append(f"- Baseline P4: **{verse['baseline_p4']}**  |  stratum: {verse.get('stratum')}  "
                     f"|  chunks: {verse.get('chunk_count')}  |  AR words: {verse.get('ar_word_count')}")
        lines.append(f"- Qwen elapsed: {qwen_rec.get('elapsed_s')}s  |  "
                     f"failed batches: {sum(1 for x in qwen_rec.get('batches', []) if not x['ok'])}")
        lines.append("")
        lines.append("### Source (Arabic chunks)")
        for i, (ct, ar) in enumerate(zip(b["chunk_types"], b["arabic_chunks"])):
            lines.append(f"- chunk[{i}] ({ct}): {ar}")

        lines.append("\n### English reference")
        lines.append(f"- summary: {b['summary'].get('en','')}")
        lines.append(f"- seo: {b['seo'].get('en','')}")
        for i, t in enumerate(b["chunks_text"].get("en", [])):
            lines.append(f"- chunk[{i}]: {t}")

        for lang in NON_EN_LANGUAGES:
            lines.append(f"\n### {LANG_NAMES[lang]} ({lang})")
            lines.append("**Baseline:**")
            lines.append(f"- summary: {b['summary'].get(lang,'')}")
            lines.append(f"- seo: {b['seo'].get(lang,'')}")
            for i, t in enumerate(b["chunks_text"].get(lang, [])):
                lines.append(f"- chunk[{i}]: {t}")
            lines.append("")
            lines.append("**Qwen36:**")
            lines.append(f"- summary: {q['summary'].get(lang,'')}")
            lines.append(f"- seo: {q['seo'].get(lang,'')}")
            for i, t in enumerate(q["chunks_text"].get(lang, [])):
                lines.append(f"- chunk[{i}]: {t}")

    out_path = BENCH_DIR / "pairs.md"
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {out_path} ({len(lines)} lines)")


if __name__ == "__main__":
    main()
