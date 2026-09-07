"""Cross-corpus similar-narration candidate generation (SPARK item 8, stage 1).

Programmatic, $0, no LLM: MinHash/LSH over diacritics-stripped word
3-gram shingles of every hadith's matn (isnad excluded via AI chunk word
ranges where available). Emits cross-chapter candidate pairs with exact
shingle-Jaccard scores for the later Spark LLM verification stage.

Usage:
  python scripts/similar_candidates.py extract      # corpus -> matn shingle file
  python scripts/similar_candidates.py candidates   # MinHash/LSH -> pairs
  python scripts/similar_candidates.py report
"""
import argparse
import hashlib
import json
import os
import re
import sys
from collections import defaultdict

import numpy as np

sys.stdout.reconfigure(encoding="utf-8")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.environ.get("DESTINATION_DIR", os.path.join(ROOT, "..", "ThaqalaynData"))
SOURCES = os.environ.get("SOURCE_DATA_DIR", os.path.join(ROOT, "..", "ThaqalaynDataSources"))
RESPONSES = os.path.join(SOURCES, "ai-content", "corpus", "responses")
OUT_DIR = os.path.join(SOURCES, "ai-pipeline-data", "similar_narrations")
CORPUS = os.path.join(OUT_DIR, "matn_shingles.jsonl")
PAIRS = os.path.join(OUT_DIR, "candidate_pairs.json")

SHINGLE_N = 3
NUM_PERM = 128
BANDS = 32          # 32 bands x 4 rows: ~87% recall at J=0.5, ~98% at J=0.6
ROWS = NUM_PERM // BANDS
MIN_JACCARD = 0.35  # exact-verified floor for keeping a candidate pair
MIN_TOKENS = 8      # matn too short to fingerprint reliably
MAX_BUCKET = 150    # skip degenerate LSH buckets (boilerplate formulas)

_DIACRITICS = re.compile(r"[ً-ٰٟۖ-ۭـ]")
_NON_ARABIC = re.compile(r"[^ء-ي\s]")


def normalize(text):
    t = _DIACRITICS.sub("", text)
    t = _NON_ARABIC.sub(" ", t)
    t = (t.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
          .replace("ى", "ي").replace("ة", "ه").replace("ؤ", "و").replace("ئ", "ي"))
    return t.split()


def matn_tokens(verse, verse_id=None):
    """Normalized matn tokens. AI response chunks carry the exact Arabic
    text per chunk (the merged Data files only carry word ranges into the
    AI's own diacritized tokenization, which does NOT index the display
    text) — so isnad exclusion uses the response's chunks[].arabic_text,
    falling back to the full display text when no response exists."""
    if verse_id:
        fp = os.path.join(RESPONSES, verse_id + ".json")
        if os.path.exists(fp):
            try:
                result = json.load(open(fp, encoding="utf-8")).get("result") or {}
                chunks = [c for c in (result.get("chunks") or [])
                          if isinstance(c, dict) and (c.get("arabic_text") or "").strip()]
                matn = [c["arabic_text"] for c in chunks
                        if c.get("chunk_type") != "isnad"]
                if matn and len(matn) < len(chunks):  # isnad actually excluded
                    return normalize(" ".join(matn))
            except (json.JSONDecodeError, OSError):
                pass
    return normalize(" ".join(verse.get("text") or []))


def shingle_hashes(tokens):
    out = set()
    for i in range(len(tokens) - SHINGLE_N + 1):
        s = " ".join(tokens[i:i + SHINGLE_N]).encode("utf-8")
        out.add(int.from_bytes(hashlib.blake2b(s, digest_size=8).digest(), "big"))
    return sorted(out)


def cmd_extract(_args):
    from app.pipeline_cli.verse_processor import verse_path_to_id
    os.makedirs(OUT_DIR, exist_ok=True)
    n = skipped = 0
    with open(CORPUS, "w", encoding="utf-8") as out:
        for root, dirs, files in os.walk(os.path.join(DATA, "books")):
            parts = root.replace("\\", "/").split("/")
            if "complete" in parts or "quran" in parts:
                dirs[:] = []
                continue
            for f in files:
                if not f.endswith(".json") or "." in f[:-5]:
                    continue
                try:
                    doc = json.load(open(os.path.join(root, f), encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    continue
                if doc.get("kind") != "verse_detail":
                    continue
                verse = doc["data"]["verse"]
                if verse.get("part_type") not in ("Hadith", "Verse"):
                    continue
                vp = verse.get("path") or ""
                toks = matn_tokens(verse, verse_path_to_id(vp) if vp else None)
                if len(toks) < MIN_TOKENS:
                    skipped += 1
                    continue
                out.write(json.dumps(
                    {"path": verse.get("path"), "n_tokens": len(toks),
                     "shingles": shingle_hashes(toks)},
                    ensure_ascii=False) + "\n")
                n += 1
    print(f"extract: {n} narrations fingerprinted, {skipped} too short")


def chapter_of(path):
    return path.rsplit(":", 1)[0]


def cmd_candidates(_args):
    rng = np.random.default_rng(42)
    P = (1 << 61) - 1
    a = rng.integers(1, P, size=NUM_PERM, dtype=np.int64)
    b = rng.integers(0, P, size=NUM_PERM, dtype=np.int64)

    paths, shingle_arrays, sigs = [], [], []
    with open(CORPUS, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            h = np.array(r["shingles"], dtype=np.uint64)
            paths.append(r["path"])
            shingle_arrays.append(h)
            hv = (h % np.uint64(P)).astype(np.int64)
            sig = ((a[:, None] * hv[None, :] + b[:, None]) % P).min(axis=1)
            sigs.append(sig)
    sigs = np.stack(sigs)
    print(f"candidates: {len(paths)} signatures built")

    cand = set()
    skipped_buckets = 0
    for band in range(BANDS):
        buckets = defaultdict(list)
        seg = sigs[:, band * ROWS:(band + 1) * ROWS]
        for i in range(len(paths)):
            buckets[seg[i].tobytes()].append(i)
        for members in buckets.values():
            if len(members) < 2:
                continue
            if len(members) > MAX_BUCKET:
                skipped_buckets += 1
                continue
            for x in range(len(members)):
                for y in range(x + 1, len(members)):
                    i, j = members[x], members[y]
                    if chapter_of(paths[i]) != chapter_of(paths[j]):
                        cand.add((min(i, j), max(i, j)))
    print(f"candidates: {len(cand)} LSH pairs ({skipped_buckets} degenerate buckets skipped)")

    pairs = []
    for i, j in cand:
        inter = len(np.intersect1d(shingle_arrays[i], shingle_arrays[j],
                                   assume_unique=True))
        union = len(shingle_arrays[i]) + len(shingle_arrays[j]) - inter
        jac = inter / union if union else 0.0
        if jac >= MIN_JACCARD:
            pairs.append({"a": paths[i], "b": paths[j], "jaccard": round(jac, 4)})
    pairs.sort(key=lambda p: -p["jaccard"])
    with open(PAIRS, "w", encoding="utf-8") as f:
        json.dump({"count": len(pairs), "min_jaccard": MIN_JACCARD,
                   "shingle_n": SHINGLE_N, "pairs": pairs}, f, ensure_ascii=False, indent=1)
    print(f"candidates: {len(pairs)} pairs >= J{MIN_JACCARD} -> {PAIRS}")


def cmd_report(_args):
    d = json.load(open(PAIRS, encoding="utf-8"))
    pairs = d["pairs"]
    print(f"pairs: {len(pairs)}")
    def bucket(j):
        return f"{int(j * 10) / 10:.1f}"
    hist = defaultdict(int)
    cross_book = 0
    for p in pairs:
        hist[bucket(p["jaccard"])] += 1
        if p["a"].split(":")[0] != p["b"].split(":")[0]:
            cross_book += 1
    for k in sorted(hist, reverse=True):
        print(f"  J {k}+ : {hist[k]}")
    print(f"cross-book pairs: {cross_book}")
    for p in pairs[:5]:
        print(f"  {p['jaccard']:.3f} {p['a']} <-> {p['b']}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("extract")
    sub.add_parser("candidates")
    sub.add_parser("report")
    args = ap.parse_args()
    {"extract": cmd_extract, "candidates": cmd_candidates,
     "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    main()
