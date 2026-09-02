"""Accuracy analysis of align-scraped artifacts (beyond lossless validation).

Strict validation proves the rejoined parts reproduce the scraped text; it says
nothing about whether the CUTS are in the right places. This report checks cut
placement by comparing each part against the AI's own English rendering of the
same chunk (``responses/{vid}.json`` → ``result.chunks[i].translations.en``):
two English renderings of the same Arabic should share content words.

Signals per aligned (verse, translation-id):
- slot similarity: Jaccard over content words between part i and en_ref i
- shift detection: part i matches a NEIGHBORING reference better than its own
- length-share divergence: part i's share of total chars vs reference share
- empty-part sanity: part empty while its reference carries real content

Usage: python scripts/alignment_accuracy_report.py [--dir PATH] [--samples N]
       [--show-worst N]
"""
import argparse
import json
import os
import re
import sys
from collections import Counter

sys.stdout.reconfigure(encoding="utf-8")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_ART = os.path.join(ROOT, "..", "ThaqalaynDataSources", "ai-content",
                           "corpus", "chunk_alignment")
RESPONSES = os.path.join(ROOT, "..", "ThaqalaynDataSources", "ai-content",
                         "corpus", "responses")

_WORD = re.compile(r"[a-z']+")
_STOP = set("""a an the and or of to in on for from with by at as is are was
were be been has have had he she it they we you i his her its their this that
these those who whom which what said says say then when while not no nor so
does do did but if than there here upon unto shall will would may might""".split())


def words(text: str) -> set:
    return {w for w in _WORD.findall((text or "").lower())
            if w not in _STOP and len(w) > 2}


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def load_refs(vid: str):
    rp = os.path.join(RESPONSES, f"{vid}.json")
    if not os.path.exists(rp):
        return None
    try:
        with open(rp, encoding="utf-8") as f:
            chunks = (json.load(f).get("result") or {}).get("chunks") or []
        return [((c.get("translations") or {}).get("en") or "").strip()
                for c in chunks]
    except (json.JSONDecodeError, OSError):
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=DEFAULT_ART)
    ap.add_argument("--show-worst", type=int, default=5)
    args = ap.parse_args()

    n_ids = 0
    no_refs = 0
    ref_mismatch = 0
    empty_ref_slots = 0
    total_slots = 0
    sims = []           # per-slot jaccard (both sides non-empty)
    flags = Counter()
    flagged_examples = {"shifted": [], "empty-part-with-content-ref": [],
                        "low-sim": [], "length-divergence": []}
    per_book_flagged = Counter()
    per_book_ids = Counter()
    worst = []          # (mean_sim, vid, tid, parts, refs)

    for fname in sorted(os.listdir(args.dir)):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(args.dir, fname), encoding="utf-8") as f:
            art = json.load(f)
        vid = fname[:-5]
        book = (art.get("verse_path") or "?").split("/")[2].split(":")[0]
        refs = load_refs(vid)
        aligned = art.get("aligned") or {}
        if not aligned:
            continue
        if refs is None:
            no_refs += len(aligned)
            continue
        for tid, parts in aligned.items():
            n_ids += 1
            per_book_ids[book] += 1
            if len(refs) != len(parts):
                ref_mismatch += 1
                continue
            pw = [words(p) for p in parts]
            rw = [words(r) for r in refs]
            id_flags = set()
            slot_sims = []
            plen = sum(len(p or "") for p in parts) or 1
            rlen = sum(len(r or "") for r in refs) or 1
            for i in range(len(parts)):
                total_slots += 1
                if not rw[i]:
                    empty_ref_slots += 1
                    continue
                if not pw[i]:
                    # part empty but the AI translated real content here
                    if len(rw[i]) >= 4:
                        id_flags.add("empty-part-with-content-ref")
                    continue
                s = jaccard(pw[i], rw[i])
                sims.append(s)
                slot_sims.append(s)
                # shift: neighbor reference fits this part clearly better
                for j in (i - 1, i + 1):
                    if 0 <= j < len(rw) and rw[j]:
                        if jaccard(pw[i], rw[j]) > s + 0.25:
                            id_flags.add("shifted")
                # length share divergence
                if abs(len(parts[i] or "") / plen - len(refs[i] or "") / rlen) > 0.35:
                    id_flags.add("length-divergence")
                if s < 0.08 and len(pw[i]) >= 5 and len(rw[i]) >= 5:
                    id_flags.add("low-sim")
            for fl in id_flags:
                flags[fl] += 1
                if len(flagged_examples[fl]) < 3:
                    flagged_examples[fl].append(f"{vid} [{tid}]")
            if id_flags:
                per_book_flagged[book] += 1
            if slot_sims:
                worst.append((sum(slot_sims) / len(slot_sims), vid, tid,
                              parts, refs))

    print(f"aligned ids analyzed: {n_ids} "
          f"(no response file: {no_refs}, chunk-count mismatch: {ref_mismatch})")
    if sims:
        sims.sort()
        mean = sum(sims) / len(sims)
        print(f"\nslot similarity (part vs its own reference), "
              f"{len(sims)} comparable slots of {total_slots} total "
              f"({empty_ref_slots} slots had empty references):")
        print(f"  mean {mean:.2f} | p10 {sims[len(sims)//10]:.2f} | "
              f"median {sims[len(sims)//2]:.2f} | p90 {sims[9*len(sims)//10]:.2f}")

    print(f"\nids with at least one flag ({sum(per_book_flagged.values())} "
          f"of {n_ids}):")
    for fl, n in flags.most_common():
        print(f"  {n:5d}  {fl}")
        for ex in flagged_examples[fl]:
            print(f"         e.g. {ex}")
    print("\nflagged ids per book:")
    for b in sorted(per_book_ids):
        print(f"  {b:24s} {per_book_flagged[b]:4d} / {per_book_ids[b]}")

    worst.sort()
    if args.show_worst and worst:
        print(f"\n=== {args.show_worst} lowest mean-similarity ids "
              f"(side-by-side, truncated) ===")
        for mean_s, vid, tid, parts, refs in worst[:args.show_worst]:
            print(f"\n--- {vid} [{tid}] mean sim {mean_s:.2f}")
            for i, (p, r) in enumerate(zip(parts, refs)):
                print(f"  [{i}] PART: {(p or '')[:110]!r}")
                print(f"      REF : {(r or '')[:110]!r}")


if __name__ == "__main__":
    main()
