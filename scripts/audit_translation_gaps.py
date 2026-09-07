"""Audit AI-content coverage gaps (SPARK_AI_CONTENT_ROADMAP item 1).

Two gap classes per manifest verse:
  - NO RESPONSE: verse never generated (needs a full --phased run)
  - PARTIAL: response exists but one or more of the 11 languages is missing
    or empty (needs `pipeline retranslate`, which re-runs Phase 4 only)

Usage: python scripts/audit_translation_gaps.py [--json OUT] [--responses DIR]
"""
import argparse
import json
import os
import sys
from collections import Counter, defaultdict

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import AI_RESPONSES_DIR  # noqa: E402
from app.pipeline_cli.verse_processor import verse_path_to_id  # noqa: E402

LANGS = ["en", "ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--responses", default=None)
    ap.add_argument("--json", default=None, help="write machine-readable gap lists")
    args = ap.parse_args()

    responses_dir = args.responses or AI_RESPONSES_DIR
    src = os.environ.get("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")
    manifest = json.load(open(os.path.join(src, "ai-pipeline-data",
                                           "corpus_manifest.json"), encoding="utf-8"))
    paths = [v["path"] if isinstance(v, dict) else v for v in manifest["verses"]]

    on_disk = {f[:-5] for f in os.listdir(responses_dir) if f.endswith(".json")}

    missing = defaultdict(list)          # book -> [paths] never generated
    partial = defaultdict(list)          # book -> [(path, missing_langs)]
    lang_gap_counts = Counter()
    complete = Counter()

    for vp in paths:
        book = vp.split("/")[2].split(":")[0]
        vid = verse_path_to_id(vp)
        if vid not in on_disk:
            missing[book].append(vp)
            continue
        try:
            with open(os.path.join(responses_dir, vid + ".json"), encoding="utf-8") as f:
                result = json.load(f).get("result") or {}
        except (json.JSONDecodeError, OSError):
            missing[book].append(vp)
            continue
        translations = result.get("translations") or {}
        gaps = []
        for lang in LANGS:
            ld = translations.get(lang)
            # summary is the per-language must-have (text is chunk-derived)
            if not isinstance(ld, dict) or not (ld.get("summary") or "").strip():
                gaps.append(lang)
        if gaps:
            partial[book].append((vp, gaps))
            for g in gaps:
                lang_gap_counts[g] += 1
        else:
            complete[book] += 1

    total = len(paths)
    n_missing = sum(len(v) for v in missing.values())
    n_partial = sum(len(v) for v in partial.values())
    n_complete = sum(complete.values())
    print(f"manifest verses: {total}")
    print(f"  complete (all {len(LANGS)} langs): {n_complete} ({n_complete/total:.1%})")
    print(f"  partial (some langs missing):     {n_partial}")
    print(f"  never generated:                  {n_missing}\n")

    print(f"{'book':30s} {'complete':>8s} {'partial':>8s} {'missing':>8s}")
    books = sorted(set(missing) | set(partial) | set(complete))
    for b in books:
        print(f"{b:30s} {complete[b]:8d} {len(partial[b]):8d} {len(missing[b]):8d}")

    if lang_gap_counts:
        print("\npartial gaps by language:",
              dict(lang_gap_counts.most_common()))

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump({
                "missing": {b: v for b, v in missing.items()},
                "partial": {b: [{"path": p, "langs": g} for p, g in v]
                            for b, v in partial.items()},
            }, f, ensure_ascii=False, indent=1)
        print(f"\ngap lists -> {args.json}")


if __name__ == "__main__":
    main()
