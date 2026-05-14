"""Lock the pilot set for Path B experiment rounds.

Samples 100 lemmas + 100 surfaces stratified by frequency band + part of
speech + topical category. The resulting `pilot_set.json` pins each
round to the same item set, so prompt-tweaks have attributable signal.

Strata (per WORDS_PROJECT_PLAN.md "Path B / Experiment rounds"):

  Lemmas (100 total):
    30 hi-freq content words   (top-50 by frequency, non-function-word)
    30 mid-freq content words  (rank 50-500)
    20 low-freq content words  (rank 500+, but >=1 occurrence)
    10 function words          (prep/conj/pron/particle)
     5 classical religious     (seed list — تقوى, إيمان, تسبيح…)
     5 proper nouns / loanwords (pos_camel=noun_prop, common ones)

  Surfaces (100 total):
    30 hi-freq                 (top-50 by occurrence_count)
    30 mid-freq                (rank 50-2,000)
    20 low-freq                (rank 2,000+)
    10 surfaces with clitics   (compounds — wa-bi-al-X, etc.)
     5 inflections of religious lemmas
     5 surfaces of proper nouns

Pinned via `random.seed(20260514)` for reproducibility. Persisted as
ThaqalaynWordSources/translation/pilot_set.json.
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("build_path_b_pilot_set")

PILOT_SEED = 20260514


# ────────────────── canonical seed sets ──────────────────

# Classical religious / Quranic-theological terms. Stored as
# diacritic-stripped keys; sampler picks any matching lemma slug from
# the corpus at run time (the corpus uses tanwin/case suffixes the
# undiacritized form doesn't anticipate).
CLASSICAL_RELIGIOUS_KEYS = [
    "تقوى",   # taqwā — God-consciousness, piety
    "ايمان",  # īmān — faith
    "تسبيح",  # tasbīḥ — glorification
    "زكاة",   # zakāh — obligatory alms
    "صلاة",   # ṣalāh — ritual prayer
    "جهاد",   # jihād — struggle
    "ولاية",  # wilāyah — guardianship (Shia-specific)
    "شهيد",   # shahīd — martyr/witness
    "إمام",   # imām — leader
    "رحمة",   # raḥmah — mercy
]


def _strip_diac(s: str) -> str:
    import unicodedata
    norm = unicodedata.normalize("NFKD", s)
    return "".join(c for c in norm if not unicodedata.combining(c)).replace("ـ", "")


# Function-word POS codes — covers every CAMeL pos_camel variant I saw
# in the corpus audit (prep / conj / pron / part / *_part / *_pseudo / interj).
FUNCTION_POS_CAMEL = {
    "prep",
    "conj", "conj_sub",
    "pron", "pron_rel", "pron_dem", "pron_interrog", "pron_exclam",
    "det",
    "part", "particle",
    "part_neg", "part_verb", "part_voc", "part_focus", "part_interrog",
    "part_restrict", "part_fut", "part_emph",
    "fut_part", "neg_part", "interrog_part", "focus_part",
    "prog_part", "voc_part",
    "interj",
    "adv_interrog",  # interrogative adverbs (e.g. كم, متى)
    "verb_pseudo",   # كَانَ-class auxiliary verbs — function-ish
}

# Content-word POS codes (verbs, nouns, adjectives, adverbs).
CONTENT_POS_CAMEL = {
    "verb",
    "noun", "noun_quant", "noun_num",
    "adj", "adj.act", "adj.pass", "adj_comp", "adj_num",
    "adv",
}

PROPER_NOUN_POS_CAMEL = {"noun_prop"}


# ────────────────── lemma sampler ──────────────────


def compute_lemma_frequencies(words_dir: Path) -> Counter[str]:
    """Walk surfaces and roll up occurrences per lemma slug.

    Uses `morphology.lemma_slug` on each surface; the index's `frequency`
    field is unreliable for non-verb POS.
    """
    surfaces_dir = words_dir / "surfaces"
    counts: Counter[str] = Counter()
    for p in surfaces_dir.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            continue
        ls = (d.get("morphology") or {}).get("lemma_slug")
        if ls:
            counts[ls] += d.get("occurrence_count", 0)
    return counts


def load_lemma_pos(words_dir: Path, slugs: List[str]) -> Dict[str, str]:
    """For a list of lemma slugs, return slug → pos_camel."""
    lemmas_dir = words_dir / "lemmas"
    out: Dict[str, str] = {}
    for slug in slugs:
        path = lemmas_dir / f"{slug}.json"
        if not path.exists():
            out[slug] = ""
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                d = json.load(f)
            out[slug] = d.get("pos_camel") or ""
        except Exception:
            out[slug] = ""
    return out


def pick_religious_seeds(
    lemma_freq: Counter[str], rng: random.Random, want: int = 5
) -> List[str]:
    """Pick `want` corpus-attested lemma slugs matching the religious keys.

    For each key (e.g. "تقوى"), finds any lemma slug whose diacritic-
    stripped form matches AND that has non-zero corpus frequency, then
    picks the most frequent variant.
    """
    out: List[str] = []
    by_key: Dict[str, List[tuple[str, int]]] = {}
    for slug, freq in lemma_freq.items():
        if freq <= 0:
            continue
        key = _strip_diac(slug)
        by_key.setdefault(key, []).append((slug, freq))

    keys_in_order = list(CLASSICAL_RELIGIOUS_KEYS)
    rng.shuffle(keys_in_order)
    for k in keys_in_order:
        if len(out) >= want:
            break
        if k in by_key:
            # Pick the most-frequent matching variant
            best = max(by_key[k], key=lambda t: t[1])
            out.append(best[0])
    return out


def sample_lemmas(
    words_dir: Path, rng: random.Random
) -> Dict[str, List[str]]:
    logger.info("computing lemma frequencies from surfaces…")
    counts = compute_lemma_frequencies(words_dir)
    logger.info("found %d distinct lemmas referenced by surfaces", len(counts))

    by_freq = counts.most_common()  # [(slug, count), …] descending
    all_slugs = [s for s, _ in by_freq]

    poses = load_lemma_pos(words_dir, all_slugs)

    def filter_pos(pool: List[str], allowed: set, exclude_seeds: set = set()) -> List[str]:
        return [
            s for s in pool
            if poses.get(s) in allowed and s not in exclude_seeds
        ]

    # Religious seeds first — picked by diacritic-stripped key match.
    religious_seeds = pick_religious_seeds(counts, rng, want=5)
    seeds_set = set(religious_seeds)
    logger.info("classical religious seeds: %s", religious_seeds)

    # Tier source pools — wider than the per-stratum sample so we always
    # have enough content words to draw from (top-50 only has ~14 nouns +
    # 6 verbs, so we widen to top-200 for the hi-freq pool to get 30 picks).
    top200 = all_slugs[:200]
    rank_200_1000 = all_slugs[200:1000]
    rank_1000_plus = all_slugs[1000:]

    def sample_n(pool: List[str], n: int) -> List[str]:
        if len(pool) <= n:
            logger.warning("pool size %d < requested %d; taking all", len(pool), n)
            return list(pool)
        return rng.sample(pool, n)

    sample = {
        "hi_freq_content": sample_n(filter_pos(top200, CONTENT_POS_CAMEL, seeds_set), 30),
        "mid_freq_content": sample_n(filter_pos(rank_200_1000, CONTENT_POS_CAMEL, seeds_set), 30),
        "low_freq_content": sample_n(filter_pos(rank_1000_plus, CONTENT_POS_CAMEL, seeds_set), 20),
        "function_words": sample_n(filter_pos(all_slugs, FUNCTION_POS_CAMEL, seeds_set), 10),
        "classical_religious": religious_seeds,
        "proper_nouns": sample_n(filter_pos(all_slugs, PROPER_NOUN_POS_CAMEL, seeds_set), 5),
    }
    return sample


# ────────────────── surface sampler ──────────────────


def sample_surfaces(
    words_dir: Path,
    rng: random.Random,
    religious_lemma_slugs: List[str],
    proper_noun_lemma_slugs: List[str],
) -> Dict[str, List[str]]:
    """Stratified surface sample.

    Reads each surface's JSON once; uses occurrence_count for ranking
    and morphology.clitics to identify compound surfaces.
    """
    surfaces_dir = words_dir / "surfaces"
    records: List[dict] = []
    for p in surfaces_dir.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            continue
        records.append({
            "slug": d.get("slug"),
            "count": d.get("occurrence_count", 0),
            "lemma_slug": (d.get("morphology") or {}).get("lemma_slug"),
            "clitics": (d.get("morphology") or {}).get("clitics") or {},
            "pos": (d.get("morphology") or {}).get("pos"),
        })
    logger.info("loaded %d surface records", len(records))

    by_freq = sorted(records, key=lambda r: (-r["count"], r["slug"] or ""))
    top50 = by_freq[:50]
    rank_50_2000 = by_freq[50:2000]
    rank_2000_plus = by_freq[2000:]

    compounds = [r for r in records if r["clitics"]]
    religious_set = set(religious_lemma_slugs)
    religious_surfaces = [
        r for r in records if r["lemma_slug"] in religious_set
    ]
    proper_set = set(proper_noun_lemma_slugs)
    proper_surfaces = [
        r for r in records if r["lemma_slug"] in proper_set
    ]

    def slugs(pool: List[dict], n: int) -> List[str]:
        if len(pool) <= n:
            return [r["slug"] for r in pool]
        return [r["slug"] for r in rng.sample(pool, n)]

    return {
        "hi_freq": slugs(top50, 30),
        "mid_freq": slugs(rank_50_2000, 30),
        "low_freq": slugs(rank_2000_plus, 20),
        "compounds_with_clitics": slugs(compounds, 10),
        "inflections_of_religious": slugs(religious_surfaces, 5),
        "proper_noun_surfaces": slugs(proper_surfaces, 5),
    }


# ────────────────── CLI ──────────────────


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--words-dir", type=Path, default=Path("../ThaqalaynWords"),
    )
    parser.add_argument(
        "--out", type=Path,
        default=Path("../ThaqalaynWordSources/translation/pilot_set.json"),
    )
    parser.add_argument("--seed", type=int, default=PILOT_SEED)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    rng = random.Random(args.seed)

    lemma_strata = sample_lemmas(args.words_dir, rng)
    flat_lemmas: List[str] = []
    for stratum, sl in lemma_strata.items():
        flat_lemmas.extend(sl)
        logger.info("  lemma stratum %s: %d items", stratum, len(sl))
    logger.info("total lemmas sampled: %d", len(flat_lemmas))

    surf_strata = sample_surfaces(
        args.words_dir, rng,
        religious_lemma_slugs=lemma_strata["classical_religious"],
        proper_noun_lemma_slugs=lemma_strata["proper_nouns"],
    )
    flat_surfaces: List[str] = []
    for stratum, sl in surf_strata.items():
        flat_surfaces.extend(sl)
        logger.info("  surface stratum %s: %d items", stratum, len(sl))
    logger.info("total surfaces sampled: %d", len(flat_surfaces))

    payload = {
        "seed": args.seed,
        "lemma_strata": lemma_strata,
        "surface_strata": surf_strata,
        "lemmas": flat_lemmas,
        "surfaces": flat_surfaces,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("wrote %s", args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
