"""Post-merge integrity sweep for chunk-aligned scraped translations.

For every alignment artifact, verifies the merged ThaqalaynData state:
- aligned ids: sister file has chunk_translations[tid] equal to the artifact
  parts, part count == base ai.chunks count, base no longer carries the flat
  text, and the id is still discoverable via data.verse_translations;
- quarantined ids: base STILL carries the flat text (nothing merged/stripped).

Usage: python scripts/verify_alignment_merge.py [--data PATH] [--art PATH]
Exit code 1 when any check fails.
"""
import argparse
import json
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEF_ART = os.path.join(ROOT, "..", "ThaqalaynDataSources", "ai-content",
                       "corpus", "chunk_alignment")
DEF_DATA = os.path.join(ROOT, "..", "ThaqalaynData")


def verse_file(vp, data):
    return os.path.join(data, vp.lstrip("/").replace(":", "/") + ".json")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=DEF_DATA)
    ap.add_argument("--art", default=DEF_ART)
    ap.add_argument("--max-errors", type=int, default=30)
    args = ap.parse_args()

    stats = {"artifacts": 0, "aligned_ids": 0, "quarantined_ids": 0,
             "ok": 0, "errors": 0, "missing_verse_file": 0}
    errors = []

    def err(msg):
        stats["errors"] += 1
        if len(errors) < args.max_errors:
            errors.append(msg)

    for fname in sorted(os.listdir(args.art)):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(args.art, fname), encoding="utf-8") as f:
            art = json.load(f)
        vp = art.get("verse_path")
        stats["artifacts"] += 1
        bf = verse_file(vp, args.data)
        if not os.path.exists(bf):
            stats["missing_verse_file"] += 1
            continue
        doc = json.load(open(bf, encoding="utf-8"))
        data = doc.get("data", {})
        verse = data.get("verse", {})
        vtrans = data.get("verse_translations") or []
        chunks = ((verse.get("ai") or {}).get("chunks")) or []
        sisters = {}

        for tid, parts in (art.get("aligned") or {}).items():
            stats["aligned_ids"] += 1
            lang = tid.split(".")[0]
            if lang not in sisters:
                sp = bf[:-5] + f".{lang}.json"
                sisters[lang] = (json.load(open(sp, encoding="utf-8"))
                                 if os.path.exists(sp) else None)
            sis = sisters[lang]
            ct = (sis or {}).get("chunk_translations") or {}
            if tid not in ct:
                err(f"{vp} [{tid}]: aligned but sister lacks chunk_translations")
                continue
            if ct[tid] != parts:
                err(f"{vp} [{tid}]: sister parts differ from artifact")
                continue
            if chunks and len(parts) != len(chunks):
                err(f"{vp} [{tid}]: {len(parts)} parts vs {len(chunks)} chunks")
                continue
            if tid in (verse.get("translations") or {}):
                err(f"{vp} [{tid}]: flat text still in base (should be stripped)")
                continue
            if tid not in vtrans:
                err(f"{vp} [{tid}]: id missing from verse_translations")
                continue
            if not any((p or "").strip() for p in parts):
                err(f"{vp} [{tid}]: all parts empty")
                continue
            stats["ok"] += 1

        for tid in (art.get("quarantined") or {}):
            stats["quarantined_ids"] += 1
            if tid not in (verse.get("translations") or {}):
                err(f"{vp} [{tid}]: quarantined but flat text missing from base")
            else:
                stats["ok"] += 1

    print(json.dumps(stats, indent=2))
    for e in errors:
        print("ERROR:", e)
    if stats["errors"]:
        print(f"... ({stats['errors']} total errors)")
    sys.exit(1 if stats["errors"] else 0)


if __name__ == "__main__":
    main()
