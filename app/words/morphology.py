"""CAMeL Tools wrapper — Arabic morphological analyzer + generator.

This module is the project's gateway to CAMeL Tools (NYU Abu Dhabi). It
provides two operations:

1. :func:`analyze` — given a surface form (any Arabic token), return the
   morphological analyses: lemma, root, POS, clitics, person/number/gender,
   case, gloss, etc. Used to populate lemma + surface page metadata.

2. :func:`generate_paradigm` — given a lemma + POS, return every surface
   form the lemma can produce (full conjugation for verbs, full
   declension for nouns). Used to populate the lemma page's `forms[]`
   list, including forms not present in the corpus.

Both use the CALIMA-MSA-r13 database (the MSA / classical-Arabic
morphology DB shipped with CAMeL Tools, ~40 MB). Production code paths
should NOT initialize the database eagerly — the loader is module-level
to be importable in tests without the DB present.

License notes (informational, not enforced here): CAMeL Tools is MIT;
the calima-msa-r13 database is GPL v2. We use it strictly at generation
time to derive per-word JSON output; the derived JSONs (per-word pages)
are our own work product.

POS taxonomy used by CAMeL Tools differs from our Phase 1 prompt's
VALID_POS_TAGS. Translation table lives in :data:`POS_TRANSLATION_TO_OURS`.
"""
from __future__ import annotations

import functools
import re
from typing import Dict, List, Optional, Tuple

from .normalize import normalize_for_match, slug


# ---------------------------------------------------------------------------
# Lazy loaders — initialize the DB once on first use, cache afterwards
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=1)
def _get_analyzer():
    """Initialize CAMeL Tools analyzer (lazy + cached).

    Loads ~40 MB of calima-msa-r13 morphology database on first call.
    Subsequent calls return the cached instance.
    """
    from camel_tools.morphology.database import MorphologyDB
    from camel_tools.morphology.analyzer import Analyzer
    db = MorphologyDB.builtin_db("calima-msa-r13", flags="a")
    return Analyzer(db)


@functools.lru_cache(maxsize=1)
def _get_generator():
    """Initialize CAMeL Tools generator (lazy + cached).

    Loads ~40 MB on first call. Subsequent calls return the cached
    instance.
    """
    from camel_tools.morphology.database import MorphologyDB
    from camel_tools.morphology.generator import Generator
    db = MorphologyDB.builtin_db("calima-msa-r13", flags="g")
    return Generator(db)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze(surface_form: str) -> List[Dict]:
    """Return morphological analyses for a surface form.

    Multiple analyses are typical for ambiguous surfaces — e.g. ``قال``
    could be a verb (past 3ms "he said") or a noun in another context.
    All are returned; downstream code may pick the most-likely via a
    disambiguator or take all.

    Args:
        surface_form: Arabic token from a chunk's arabic_text.

    Returns:
        List of analysis dicts. Each contains many fields including
        ``diac`` (diacritized form), ``lex`` (lemma identifier),
        ``pos`` (POS tag), ``root``, ``gloss``, ``asp`` (aspect),
        ``per`` (person), ``gen`` (gender), ``num`` (number), etc.
        Empty list if the surface form is unanalyzable.
    """
    if not surface_form:
        return []
    analyzer = _get_analyzer()
    try:
        return analyzer.analyze(surface_form)
    except Exception:
        return []


def get_best_analysis(surface_form: str) -> Optional[Dict]:
    """Return the most-probable analysis for a surface form.

    Disambiguation by descending preference:

    1. **Diacritization match** — if any analysis's ``diac`` equals the
       input surface form, prefer that one. The analyzer often returns
       several lex candidates for an ambiguous surface; the one whose
       ``diac`` exactly reproduces the input is the most-faithful read.
    2. **lex_logprob** — log probability of (surface, lex, pos), populated
       in the calima-msa-r13 database. Higher (closer to 0) = more
       probable. We tried ``pos_freq`` first but it is ``None`` for every
       analysis in this DB build — see the disambiguator note in
       :func:`canonical_diacritized_lemma`.
    3. **pos_lex_logprob** — fallback log probability when lex_logprob
       is also missing.
    4. Insertion order — the analyzer's natural ordering as last resort.
    """
    analyses = analyze(surface_form)
    if not analyses:
        return None
    # 1. exact diacritization match (best signal we have)
    target_diac = slug(surface_form)
    exact_diac = [a for a in analyses if slug(a.get("diac", "")) == target_diac]
    pool = exact_diac if exact_diac else analyses

    def _logprob(a, key):
        v = a.get(key)
        try:
            return float(v)
        except (TypeError, ValueError):
            return float("-inf")

    # 2/3. score by lex_logprob with pos_lex_logprob tiebreaker
    return max(
        pool,
        key=lambda a: (_logprob(a, "lex_logprob"), _logprob(a, "pos_lex_logprob")),
    )


def extract_lemma(surface_form: str) -> Optional[str]:
    """Return the canonical lemma for a surface form.

    Convenience wrapper: lemma is the ``lex`` field of the best
    analysis. ``lex`` in CAMeL Tools is undiacritized — for lemma slug
    derivation, we apply :func:`slug` (NFC).

    Args:
        surface_form: Arabic token.

    Returns:
        Lemma string, NFC-normalized. None if no analysis found.
    """
    analysis = get_best_analysis(surface_form)
    if not analysis:
        return None
    lex = analysis.get("lex")
    return slug(lex) if lex else None


def extract_root(surface_form: str) -> Optional[str]:
    """Return the Arabic root for a surface form.

    CAMeL Tools formats roots with ``.`` as the letter separator, e.g.
    ``ق.و.ل`` for the root q-w-l. We preserve that format.

    Args:
        surface_form: Arabic token.

    Returns:
        Root string in ``ل.و.ق`` format. None if no analysis found.
    """
    analysis = get_best_analysis(surface_form)
    if not analysis:
        return None
    return analysis.get("root")


def generate_paradigm(
    lemma: str, pos: str = "verb"
) -> List[Dict]:
    """Generate all inflected forms for a lemma.

    For verbs, produces ~60 forms across the asp × per × gen × num
    matrix. For nouns, produces forms across case × num × def.

    The lemma argument is matched against CAMeL Tools' internal lex
    field (undiacritized). If the lemma comes from :func:`extract_lemma`
    no conversion is needed — same source.

    Args:
        lemma: Undiacritized lemma string (the ``lex`` value from
            :func:`extract_lemma`).
        pos: Part-of-speech tag in CAMeL Tools' taxonomy. Common values:
            "verb", "noun", "adj", "adv". Default "verb".

    Returns:
        List of analysis dicts (same shape as :func:`analyze` output),
        one per generated form. ``diac`` is the diacritized surface form
        that should be slugified into the surface-page URL.
    """
    if not lemma:
        return []
    gen = _get_generator()
    try:
        return gen.generate(lemma, {"pos": pos})
    except Exception:
        return []


def paradigm_by_role(lemma: str, pos: str = "verb") -> List[Dict]:
    """Return paradigm forms grouped by structural role.

    Wraps :func:`generate_paradigm` and produces a flat list of role
    descriptors. Each entry: ``{role, form, ...metadata}`` where role
    is a canonical key like ``past_3ms``, ``imperative_2fs``,
    ``verbal_noun``, ``active_participle``, etc.

    De-duplicates by (role, diac). When CAMeL Tools returns multiple
    analyses for the same (asp, per, gen, num) combination, we keep
    only the first diacritized form.

    Args:
        lemma: Undiacritized lemma.
        pos: POS tag.

    Returns:
        Sorted list of role-tagged paradigm entries. Empty list if
        the lemma yields no generated forms.
    """
    raw = generate_paradigm(lemma, pos)
    if not raw:
        return []

    by_role: Dict[Tuple[str, str], Dict] = {}
    for entry in raw:
        diac = entry.get("diac", "").strip()
        if not diac:
            continue
        role = _role_key(entry)
        if not role:
            continue
        key = (role, slug(diac))
        if key in by_role:
            continue
        by_role[key] = {
            "role": role,
            "form": slug(diac),
            "diacritized": diac,
            "asp": entry.get("asp"),
            "per": entry.get("per"),
            "gen": entry.get("gen"),
            "num": entry.get("num"),
            "cas": entry.get("cas"),
            "stt": entry.get("stt"),
            "vox": entry.get("vox"),
            "mod": entry.get("mod"),
        }

    return sorted(
        by_role.values(),
        key=lambda e: _role_sort_key(e["role"]),
    )


# ---------------------------------------------------------------------------
# Role key derivation
# ---------------------------------------------------------------------------

# Maps CAMeL aspect codes to readable labels.
_ASP_LABEL = {"p": "past", "i": "present", "c": "imperative"}
_PER_LABEL = {"1": "1", "2": "2", "3": "3"}
_GEN_LABEL = {"m": "m", "f": "f", "c": "c"}
_NUM_LABEL = {"s": "s", "d": "d", "p": "p"}


def _role_key(entry: Dict) -> Optional[str]:
    """Derive a stable role key like 'past_3ms' from an analysis dict.

    Returns None if the role can't be determined (e.g., a derived form
    like a participle that lacks per/gen/num).
    """
    pos = entry.get("pos") or ""
    asp = entry.get("asp") or ""
    per = entry.get("per") or ""
    gen = entry.get("gen") or ""
    num = entry.get("num") or ""

    if asp in _ASP_LABEL and per and gen and num:
        return f"{_ASP_LABEL[asp]}_{per}{gen}{num}"

    # Participles / verbal nouns / nouns
    if pos == "verbal_noun":
        return "verbal_noun"
    if pos in ("adj.act", "verb.act_partic"):
        return "active_participle"
    if pos in ("adj.pass", "verb.pass_partic"):
        return "passive_participle"

    return None


_ROLE_ORDER = {
    "past_1cs": 0, "past_1cp": 1,
    "past_2ms": 2, "past_2fs": 3, "past_2md": 4, "past_2fd": 5, "past_2mp": 6, "past_2fp": 7,
    "past_3ms": 8, "past_3fs": 9, "past_3md": 10, "past_3fd": 11, "past_3mp": 12, "past_3fp": 13,
    "present_1cs": 14, "present_1cp": 15,
    "present_2ms": 16, "present_2fs": 17, "present_2md": 18, "present_2fd": 19, "present_2mp": 20, "present_2fp": 21,
    "present_3ms": 22, "present_3fs": 23, "present_3md": 24, "present_3fd": 25, "present_3mp": 26, "present_3fp": 27,
    "imperative_2ms": 28, "imperative_2fs": 29, "imperative_2md": 30, "imperative_2fd": 31, "imperative_2mp": 32, "imperative_2fp": 33,
    "verbal_noun": 34,
    "active_participle": 35,
    "passive_participle": 36,
}


def _role_sort_key(role: str) -> int:
    """Return a sort index for a role; unknown roles sort last."""
    # Some CAMeL roles use 'c' (common) for gender — map past_1cs etc.
    # Handle direct match first.
    if role in _ROLE_ORDER:
        return _ROLE_ORDER[role]
    # Try with gender 'c' -> 'm' substitution for past_1cs/1cp etc.
    if role.startswith("past_1") or role.startswith("present_1"):
        return _ROLE_ORDER.get(role.replace("1c", "1c"), 99)
    return 99


# ---------------------------------------------------------------------------
# POS translation
# ---------------------------------------------------------------------------

# CAMeL Tools POS values seen in calima-msa-r13:
#   verb, noun, adj, adv, prep, conj, pron, part, intj, abbrev, digit,
#   noun_prop, noun_quant, noun_num, adj_comp, adj_num, fut_part, neg_part,
#   interrog_part, focus_part, prog_part, voc_part, ...
#
# Our project's compact taxonomy (from validate_result VALID_POS_TAGS):
#   N V ADJ ADV PREP CONJ PRON DET PART INTJ REL DEM NEG COND INTERR

POS_TRANSLATION_TO_OURS: Dict[str, str] = {
    "verb": "V",
    "noun": "N",
    "noun_prop": "N",
    "noun_quant": "N",
    "noun_num": "N",
    "adj": "ADJ",
    "adj_comp": "ADJ",
    "adj_num": "ADJ",
    "adv": "ADV",
    "prep": "PREP",
    "conj": "CONJ",
    "sub_conj": "CONJ",
    "pron": "PRON",
    "rel_pron": "REL",
    "dem_pron": "DEM",
    "interrog_pron": "INTERR",
    "interrog_part": "INTERR",
    "part": "PART",
    "neg_part": "NEG",
    "focus_part": "PART",
    "voc_part": "PART",
    "fut_part": "PART",
    "prog_part": "PART",
    "intj": "INTJ",
    "abbrev": "N",
    "digit": "N",
}


def translate_pos(camel_pos: Optional[str]) -> str:
    """Translate a CAMeL POS tag to our project's taxonomy.

    Falls back to ``"N"`` for unknown tags (CAMeL has many granular
    sub-types; collapsing unknowns to noun is the safe default).
    """
    if not camel_pos:
        return "N"
    return POS_TRANSLATION_TO_OURS.get(camel_pos, "N")


# ---------------------------------------------------------------------------
# Coverage measurement helper
# ---------------------------------------------------------------------------

def measure_coverage(surface_forms: List[str]) -> Dict[str, float]:
    """Run analyzer on each surface; report coverage stats.

    Useful as a sanity check after Phase A extracts the corpus surface
    set: how many forms does CAMeL Tools actually recognize? Low
    coverage (say < 80%) indicates we'd need an LLM-driven fallback
    for lemmatization.

    Args:
        surface_forms: List of surface forms to test.

    Returns:
        Dict with keys ``total``, ``analyzed``, ``unanalyzed``,
        ``coverage`` (fraction 0-1), and ``avg_analyses_per_form``.
    """
    total = len(surface_forms)
    analyzed = 0
    total_analyses = 0
    for form in surface_forms:
        anls = analyze(form)
        if anls:
            analyzed += 1
            total_analyses += len(anls)
    return {
        "total": total,
        "analyzed": analyzed,
        "unanalyzed": total - analyzed,
        "coverage": (analyzed / total) if total else 0.0,
        "avg_analyses_per_form": (total_analyses / analyzed) if analyzed else 0.0,
    }
