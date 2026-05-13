"""Page builders for the per-word JSON output.

Produces two kinds of pages:

- **Surface pages** (`/words/surfaces/{surface}.json`) — lightweight,
  one per unique surface form in the corpus. Contains the surface's
  occurrence paths + a pointer to its lemma page (lazy-loaded by UI).

- **Lemma pages** (`/words/lemmas/{lemma}.json`) — heavier, one per
  unique lemma. Contains root, paradigm, cross-references to external
  lexicons (Lane's, QAC, Wiktextract). LLM-synthesized content fields
  (definitions, translations, etymology) are left as ``None`` here and
  filled in by a separate LLM phase in a future session.

This module does NO LLM calls — output is deterministic from CAMeL
Tools + pre-downloaded source indexes. Safe to run unattended.

Lookup keys across sources:
- ``corpus_surfaces``: Arabic NFC keys (from corpus extraction).
- ``qac_lemma_index``: Arabic UTF-8 keys (lemmas from QAC v0.4).
- ``wiktextract_summary``: Arabic NFC keys (Wiktionary headwords).
- ``lanes_orth_index``: **Buckwalter-encoded** keys (Perseus quirk).
  We build an Arabic-keyed reverse map in :func:`build_lanes_arabic_index`
  using CAMeL's bw2ar mapper.
"""
from __future__ import annotations

import functools
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from .morphology import (
    POS_TRANSLATION_TO_OURS,
    analyze,
    extract_lemma,
    extract_root,
    generate_paradigm,
    get_best_analysis,
    paradigm_by_role,
)
from .normalize import normalize_for_match, slug

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lane's (Perseus Buckwalter) → Arabic NFC helpers
# ---------------------------------------------------------------------------

# Perseus uses standard Buckwalter PLUS a small extension set:
# - `^` appears as a marker we strip (purpose unclear; possibly Perseus's
#   own annotation for shadda placement or a tag we can't decode).
# - Digits like ``1`` appear positionally in some entries.
# - We strip these before applying CAMeL's bw2ar mapper.
_PERSEUS_EXTRA_CHARS = re.compile(r"[\^0-9]")


@functools.lru_cache(maxsize=1)
def _get_bw2ar():
    """Lazy CAMeL Tools Buckwalter→Arabic mapper."""
    from camel_tools.utils.charmap import CharMapper
    return CharMapper.builtin_mapper("bw2ar")


# Same Arabic-diacritic set as the scraper's strip_diacritics — kept in
# sync. Used to derive the hawramani lookup key from a lemma's NFC slug.
_ARABIC_DIACRITICS = set("ًٌٍَُِّْٰـ")


def strip_arabic_diacritics(s: Optional[str]) -> str:
    """Remove Arabic diacritic marks while preserving alif/ya/hamza variants."""
    if not s:
        return ""
    return "".join(c for c in s if c not in _ARABIC_DIACRITICS)


def root_to_slug(root: Optional[str]) -> Optional[str]:
    """Convert a CAMeL root (``ق.#.ل``) to a URL-safe slug (``ق-_-ل``).

    Transformations:

    - ``.`` (CAMeL's radical separator) → ``-`` (URL-friendly, avoids
      router-extension-detection edge cases).
    - ``#`` (CAMeL's weak/hollow-radical placeholder) → ``_``. ``_`` is
      URL-safe in path segments AND never appears in CAMeL root
      strings (Arabic letters + ``.`` + ``#`` only), so it can't
      collide with a real root.

    Returns ``None`` for empty/None input. CAMeL's "FOREIGN" sentinel
    root (used for unknown tokens) maps to ``None`` so we don't write
    a root page for it.

    Examples:
        ``ق.#.ل`` (root q-w-l, "say") → ``ق-_-ل``
        ``ك.ت.ب`` (root k-t-b, "write") → ``ك-ت-ب``
    """
    if not root:
        return None
    if root == "FOREIGN":
        return None
    return root.replace(".", "-").replace("#", "_")


def perseus_bw_to_arabic(bw: str) -> str:
    """Convert a Perseus-encoded Buckwalter string to NFC Arabic.

    Strips Perseus-specific extension chars (``^``, digits) then applies
    CAMeL Tools' bw2ar mapper. The result is NFC-normalized via :func:`slug`.

    Best-effort: the output may still contain rare unmapped characters
    where the Perseus encoding is ambiguous; we don't try to recover those.
    """
    if not bw:
        return ""
    stripped = _PERSEUS_EXTRA_CHARS.sub("", bw)
    mapper = _get_bw2ar()
    try:
        ar = mapper.map_string(stripped)
    except Exception:
        return ""
    return slug(ar)


def build_lanes_arabic_index(
    orth_index: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """Reverse-map Lane's orth_index from Buckwalter keys to Arabic NFC.

    Multiple Buckwalter spellings can collapse to the same Arabic form
    after stripping diacritics, so values are accumulated as lists.
    """
    out: Dict[str, List[str]] = {}
    for bw_key, entry_ids in orth_index.items():
        ar_key = perseus_bw_to_arabic(bw_key)
        if not ar_key:
            continue
        out.setdefault(ar_key, []).extend(entry_ids)
    return out


# ---------------------------------------------------------------------------
# Canonical diacritized lemma derivation
# ---------------------------------------------------------------------------

# Citation-form role per POS family. For verbs the citation form is the
# past 3ms ("he said"); for nouns the singular nominative indefinite.
# Adjectives and adverbs follow the noun convention. Other POS just use
# the analyzer's diac field if any.
_CITATION_ROLE_BY_POS_FAMILY = {
    "verb": "past_3ms",
    # Nouns/adjectives — paradigm_by_role doesn't tag singular forms with
    # roles by default; we fall back to picking the first form when this
    # is the case.
}


@functools.lru_cache(maxsize=20000)
def canonical_diacritized_lemma(lex: str, pos: str = "verb") -> str:
    """Return the canonical diacritized form of a lemma (citation form).

    For verbs, this is past 3ms (e.g., ``قَالَ`` for the root ق-و-ل).
    For nouns and adjectives, returns the first generated form, since
    CAMeL's role-tagging doesn't cover noun citation forms cleanly.
    For unknown/unanalyzable lemmas, returns the input ``lex`` as-is.

    LRU-cached so repeated lookups during a batch run are O(1).

    Args:
        lex: Undiacritized lemma (``lex`` field from CAMeL analysis).
        pos: CAMeL POS (e.g., "verb", "noun", "adj").

    Returns:
        NFC-normalized diacritized lemma string. Falls back to ``slug(lex)``
        if the paradigm generator produces nothing.
    """
    if not lex:
        return ""
    base_pos = _strip_pos_dot_suffix(pos) or "verb"

    if base_pos == "verb":
        # Find the past_3ms entry in the role-tagged paradigm.
        for entry in paradigm_by_role(lex, pos=base_pos):
            if entry.get("role") == "past_3ms":
                d = entry.get("diacritized")
                if d:
                    return slug(d)
        # If past_3ms missing (rare), fall back to any tagged form.
        roles = paradigm_by_role(lex, pos=base_pos)
        if roles:
            d = roles[0].get("diacritized")
            if d:
                return slug(d)
    else:
        # Nouns/adj: just take the first generated diac.
        raw = generate_paradigm(lex, pos=base_pos)
        for entry in raw:
            d = entry.get("diac")
            if d:
                return slug(d)

    return slug(lex)


# ---------------------------------------------------------------------------
# WordPageBuilder
# ---------------------------------------------------------------------------

class WordPageBuilder:
    """Builds surface + lemma page dicts using pre-loaded source data.

    Hold this once per build run and call ``build_surface`` / ``build_lemma``
    in a loop. Source-index lookups are O(1) dict access; the heavy work
    is CAMeL Tools morphological analysis on the surface forms.

    Attributes:
        corpus_surfaces: Mapping NFC surface → ``{count, paths[]}`` from
            :func:`corpus_extract.extract_corpus_surface_set`.
        qac_lemma_index: Mapping QAC lemma (UTF-8 Arabic) → ``{lemma, root,
            pos, occurrences[]}``. From ``quranic-arabic-corpus/lemma_index.json``.
        wiktextract_summary: Mapping Arabic NFC → ``{entry_count, pos_tags,
            has_etymology, sense_count}``. From ``wiktextract-arabic/summary_index.json``.
        lanes_arabic_index: Mapping Arabic NFC → list of Lane's entry IDs.
            Built via :func:`build_lanes_arabic_index` from the raw orth_index.
    """

    def __init__(
        self,
        corpus_surfaces: Optional[Dict[str, Dict]] = None,
        qac_lemma_index: Optional[Dict[str, Dict]] = None,
        wiktextract_summary: Optional[Dict[str, Dict]] = None,
        lanes_arabic_index: Optional[Dict[str, List[str]]] = None,
        wiktextract_full: Optional[Dict[str, List[Dict]]] = None,
        lanes_entries: Optional[Dict[str, Dict]] = None,
        hawramani_entries: Optional[Dict[str, Dict]] = None,
    ):
        self.corpus_surfaces = corpus_surfaces or {}
        self.qac_lemma_index = qac_lemma_index or {}
        self.wiktextract_summary = wiktextract_summary or {}
        self.lanes_arabic_index = lanes_arabic_index or {}
        # Optional: the full slim Wiktextract dump (word → [entry, ...]).
        # When provided, build_lemma() populates definition/etymology/ipa
        # from it instead of leaving them as null.
        self.wiktextract_full = wiktextract_full or {}
        # Optional: structured Lane's entries keyed by entry_id (the
        # n-prefixed IDs that appear in cross_references.lanes.entry_ids).
        # When provided, build_lemma() populates lanes_definition.
        self.lanes_entries = lanes_entries or {}
        # Optional: structured hawramani classical-lexicon entries keyed
        # by fetched_slug (the diacritic-stripped Arabic word that we
        # used to fetch the page). When provided, build_lemma() populates
        # classical_definitions with content from 38+ classical lexicons
        # including al-Mufradat (Raghib), Lisan al-Arab, Taj al-Arus,
        # Mufradat (Farahi), Misbah al-Munir, etc.
        self.hawramani_entries = hawramani_entries or {}
        # Normalized-form reverse indexes for fuzzy lookups across the
        # three external sources (each uses a slightly different
        # diacritization convention, so we also look up by the
        # alif/ya-unified, diacritic-stripped key).
        self._qac_normalized = _build_normalized_index(self.qac_lemma_index)
        self._wikt_normalized = _build_normalized_index(self.wiktextract_summary)
        self._wikt_full_normalized = _build_normalized_index(self.wiktextract_full)
        self._lanes_normalized = _build_normalized_list_index(self.lanes_arabic_index)
        # Corpus normalized index — different sources diacritize lemmas
        # differently, and even within the corpus the same word may
        # appear in two slightly different diacritization variants
        # across verses. The normalized fallback unions counts.
        self._corpus_normalized = _build_normalized_corpus_index(
            self.corpus_surfaces
        )
        # Track lemma → matched Wiktextract entries so the build script
        # can write a corpus-filtered slim file post-build.
        self.wikt_matched_lemmas: Dict[str, List[Dict]] = {}

    # ---- surface page -----------------------------------------------------

    def build_surface(self, surface: str) -> Dict:
        """Build a surface-page dict for one diacritized surface form.

        Output shape:
            {
              "surface": "وَقَالَ",
              "slug": "وَقَالَ",  # NFC
              "occurrence_count": int,
              "occurrence_paths": [list of /books/... paths],
              "morphology": {
                "lemma_slug": "قَالَ",
                "root": "ق.#.ل" or null,        # raw CAMeL notation
                "root_slug": "ق-_-ل" or null,    # URL-safe form
                "pos": "V" or null,
                "pos_camel": "verb" or null,  # raw CAMeL pos
                "clitics": {"prc0", "prc1", "prc2", "prc3", "enc0"}  # any present
              } | null,  # null if unanalyzable
              "lemma_link": "/words/lemmas/قَالَ" | null,
              "root_link":  "/words/roots/ق-_-ل" | null,
            }

        ``occurrence_paths`` come from the corpus surface set. If the
        surface isn't in the corpus set we still build a page but with
        zero occurrences.

        Both ``lemma_link`` and ``root_link`` are precomputed so the UI
        can render them directly without duplicating slug-derivation
        logic. The morphology block also carries the bare slug forms
        (``lemma_slug``, ``root_slug``) for callers that need just the
        identifier.
        """
        key = slug(surface)
        corpus_entry = self.corpus_surfaces.get(key, {})
        analysis = get_best_analysis(key)

        morph: Optional[Dict] = None
        lemma_slug: Optional[str] = None
        root_slug_str: Optional[str] = None
        if analysis and _is_useful_analysis(analysis):
            lex = analysis.get("lex")
            pos_camel = analysis.get("pos") or ""
            # Canonical diacritized lemma — same slug the lemma page uses.
            lemma_slug = (
                canonical_diacritized_lemma(lex, pos_camel) if lex else None
            )
            root = analysis.get("root") or None
            root_slug_str = root_to_slug(root) if root else None
            morph = {
                "lemma_slug": lemma_slug,
                "root": root,
                "root_slug": root_slug_str,
                "pos": POS_TRANSLATION_TO_OURS.get(pos_camel),
                "pos_camel": pos_camel or None,
                "clitics": _extract_clitics(analysis),
            }

        return {
            "surface": surface,
            "slug": key,
            "occurrence_count": corpus_entry.get("count", 0),
            "occurrence_paths": corpus_entry.get("paths", []),
            "morphology": morph,
            "lemma_link": f"/words/lemmas/{lemma_slug}" if lemma_slug else None,
            "root_link": f"/words/roots/{root_slug_str}" if root_slug_str else None,
        }

    # ---- lemma page -------------------------------------------------------

    def build_lemma(self, lemma: str, pos_hint: str = "verb") -> Dict:
        """Build a lemma-page dict for one diacritized lemma form.

        Output shape:
            {
              "lemma": "قَالَ",
              "slug": "قَالَ",
              "root": "ل.و.ق" | null,
              "pos": "V" | null,
              "pos_camel": "verb" | null,
              "paradigm": [
                {"role": "past_3ms", "form": "قَالَ", "diacritized": "قَالَ",
                 "in_corpus": bool, "count": int | null}, ...
              ],
              "frequency_in_corpus": int,  # sum across paradigm forms
              "cross_references": {
                "qac": {"found": bool, "lemma_key": ..., "root": ..., "pos": ...,
                        "occurrence_count": int},
                "wiktextract": {"found": bool, "entry_count": int, "pos_tags": [...]},
                "lanes": {"found": bool, "entry_ids": [...]},
              },
              "translations": null,    # filled by LLM phase
              "definition": null,      # filled by LLM phase
              "etymology": null,       # filled by LLM phase
            }

        ``pos_hint`` tells CAMeL Tools which POS to run the generator
        against (defaults to verb; pass "noun" for nouns, etc.). The
        analyzer is run on the lemma itself to detect the actual POS;
        the hint is only used by the paradigm generator.
        """
        key = slug(lemma)
        # Get authoritative POS/root from analyzing the lemma form itself.
        # The "lex" returned will often equal lemma_slug.
        analysis = get_best_analysis(key)
        pos_camel: Optional[str] = analysis.get("pos") if analysis else None
        root = analysis.get("root") if analysis else None
        pos_label = POS_TRANSLATION_TO_OURS.get(pos_camel) if pos_camel else None

        # If analyzer disagrees with hint, prefer analyzer's POS for
        # the paradigm generator (better fidelity).
        gen_pos = _strip_pos_dot_suffix(pos_camel) or pos_hint

        # CAMeL's generator wants the undiacritized "lex" form. The
        # analyzer's ``lex`` is the proper key — fall back to NFC slug
        # if no analysis.
        gen_lemma = (analysis.get("lex") if analysis else None) or key
        paradigm_raw = paradigm_by_role(gen_lemma, pos=gen_pos)

        paradigm: List[Dict] = []
        total_freq = 0
        for entry in paradigm_raw:
            form_key = entry.get("form")
            corpus_hit = self._lookup_corpus_form(form_key) if form_key else None
            count = corpus_hit.get("count") if corpus_hit else None
            in_corpus = corpus_hit is not None
            if in_corpus and count:
                total_freq += count
            paradigm.append({
                "role": entry.get("role"),
                "form": form_key,
                "in_corpus": in_corpus,
                "count": count,
                "asp": entry.get("asp"),
                "per": entry.get("per"),
                "gen": entry.get("gen"),
                "num": entry.get("num"),
            })

        root_slug = root_to_slug(root)

        # Merge Wiktextract-derived content where we have data on disk.
        # Falls back to None when the lemma isn't in Wiktionary's Arabic
        # entries (~24% of our lemmas) — those go to the LLM phase later.
        wikt_entries = self._lookup_wiktextract_full(key, gen_lemma)
        if wikt_entries:
            self.wikt_matched_lemmas[key] = wikt_entries
        definition = _build_definition_from_wiktextract(wikt_entries)
        etymology = _build_etymology_from_wiktextract(wikt_entries)
        ipa = _build_ipa_from_wiktextract(wikt_entries)

        # Merge Lane's Lexicon structured entries where available. These
        # provide classical English-language definitions covering ~67% of
        # our lemmas and complement Wiktextract (which leans modern).
        lanes_entry_ids = self._find_lanes_entry_ids(key, gen_lemma)
        lanes_definition = _build_lanes_definition(
            self.lanes_entries, lanes_entry_ids
        )

        # Merge hawramani's multi-lexicon classical-Arabic-lexicon
        # content (al-Mufradat, Lisan al-Arab, Taj al-Arus, etc.). The
        # lookup key is the diacritic-stripped lemma form, matching
        # the scraper's URL slug.
        classical_definitions = _build_classical_definitions_from_hawramani(
            self.hawramani_entries, key, gen_lemma,
        )

        return {
            "lemma": lemma,
            "slug": key,
            "root": root,
            "root_slug": root_slug,
            "root_link": f"/words/roots/{root_slug}" if root_slug else None,
            "pos": pos_label,
            "pos_camel": pos_camel,
            "paradigm": paradigm,
            "frequency_in_corpus": total_freq,
            "cross_references": {
                "qac": self._lookup_qac(key, gen_lemma),
                "wiktextract": self._lookup_wiktextract(key, gen_lemma),
                "lanes": self._lookup_lanes(key, gen_lemma),
            },
            # Multilingual translations (10 non-English target languages)
            # still need LLM — Wiktionary's Arabic-side entries don't
            # carry foreign-language translations.
            "translations": None,
            "definition": definition,
            "etymology": etymology,
            "ipa": ipa,
            "lanes_definition": lanes_definition,
            "classical_definitions": classical_definitions,
        }

    # ---- root page --------------------------------------------------------

    def build_root(self, root: str, lemmas: List[Dict]) -> Dict:
        """Build a root-page dict.

        Args:
            root: The CAMeL root string (e.g., ``ق.#.ل``).
            lemmas: List of lemma summary dicts. Each must have at least
                ``slug``, ``pos``, ``frequency_in_corpus``. Ordering is
                preserved as supplied — caller decides sort.

        Output shape:
            {
              "root": "ق.#.ل",
              "slug": "ق-#-ل",
              "lemmas": [
                {"slug": "قالَ", "pos": "V", "frequency": 7066},
                {"slug": "أَقالَ", "pos": "V", "frequency": 123},
                ...
              ],
              "lemma_count": 18,
              "total_frequency": 7204,
              "translations": null,
              "definition": null,
              "etymology": null,
            }
        """
        total = sum(l.get("frequency", 0) or 0 for l in lemmas)
        return {
            "root": root,
            "slug": root_to_slug(root),
            "lemmas": lemmas,
            "lemma_count": len(lemmas),
            "total_frequency": total,
            # Future LLM-filled fields — the root carries a shared
            # semantic gloss across all its lemmas (e.g. "speech /
            # saying" for ق-و-ل).
            "translations": None,
            "definition": None,
            "etymology": None,
        }

    # ---- corpus lookup ----------------------------------------------------

    def _lookup_corpus_form(self, form: str) -> Optional[Dict]:
        """Look up a surface form in the corpus, direct or normalized.

        Falls back to the normalized index if the exact diacritized
        form isn't present, which is common: CAMeL Tools' generator
        and the corpus chunks sometimes use different diacritization
        conventions for the same word (e.g. ``قالَ`` vs ``قَالَ``).
        """
        if not form:
            return None
        hit = self.corpus_surfaces.get(form)
        if hit:
            return hit
        n = normalize_for_match(form)
        if not n:
            return None
        return self._corpus_normalized.get(n)

    # ---- cross-reference lookups ------------------------------------------

    def _lookup_qac(self, lemma_diac: str, lemma_lex: str) -> Dict:
        """Look up lemma in the Quranic Arabic Corpus.

        QAC lemma keys are UTF-8 Arabic with idiosyncratic partial
        diacritization (e.g., ``قالَ`` not ``قَالَ``). Try direct match
        first, then a normalized (diacritics-stripped + alif/ya unified)
        fallback.
        """
        entry, matched_key = _lookup_with_fallback(
            self.qac_lemma_index, self._qac_normalized, lemma_diac, lemma_lex
        )
        if entry:
            occurrences = entry.get("occurrences", [])
            return {
                "found": True,
                "lemma_key": matched_key,
                "root": entry.get("root"),
                "pos": entry.get("pos"),
                "occurrence_count": len(occurrences),
            }
        return {"found": False}

    def _lookup_wiktextract(self, lemma_diac: str, lemma_lex: str) -> Dict:
        """Look up lemma in the Wiktextract Arabic summary index."""
        entry, _matched = _lookup_with_fallback(
            self.wiktextract_summary, self._wikt_normalized, lemma_lex, lemma_diac
        )
        if entry:
            return {
                "found": True,
                "entry_count": entry.get("entry_count", 0),
                "pos_tags": entry.get("pos_tags", []),
                "has_etymology": entry.get("has_etymology", False),
                "sense_count": entry.get("sense_count", 0),
            }
        return {"found": False}

    def _lookup_wiktextract_full(
        self, lemma_diac: str, lemma_lex: str
    ) -> List[Dict]:
        """Return the full list of Wiktextract entries for a lemma.

        Same key conventions as :meth:`_lookup_wiktextract` (the summary
        version) — Wiktionary headwords are typically undiacritized, so
        try ``lex`` first, then the diacritized form, then normalized
        fallback.

        Returns an empty list when no entries found OR no full dump was
        provided to the builder.
        """
        if not self.wiktextract_full:
            return []
        for key in (lemma_lex, lemma_diac):
            if not key:
                continue
            entries = self.wiktextract_full.get(key)
            if entries:
                return entries
        for key in (lemma_lex, lemma_diac):
            if not key:
                continue
            n = normalize_for_match(key)
            entries = self._wikt_full_normalized.get(n)
            if entries:
                return entries
        return []

    def _lookup_lanes(self, lemma_diac: str, lemma_lex: str) -> Dict:
        """Look up lemma in the Arabic-keyed Lane's index.

        Returns ``{found, entry_ids, search_url}``. ``search_url`` is a
        generic WordPress-search link on lanelexicon.com that opens a
        results page for the lemma (no stable per-entry deep linking is
        available on any Lane's viewer).
        """
        entry_ids = self._find_lanes_entry_ids(lemma_diac, lemma_lex)
        if entry_ids:
            search_url = _build_lanes_search_url(lemma_diac or lemma_lex)
            return {
                "found": True,
                "entry_ids": entry_ids,
                "search_url": search_url,
            }
        return {"found": False}

    def _find_lanes_entry_ids(
        self, lemma_diac: str, lemma_lex: str
    ) -> List[str]:
        """Resolve entry-id list for a lemma (direct or normalized)."""
        for key in (lemma_diac, lemma_lex):
            if not key:
                continue
            entry_ids = self.lanes_arabic_index.get(key)
            if entry_ids:
                return entry_ids
        for key in (lemma_diac, lemma_lex):
            if not key:
                continue
            n = normalize_for_match(key)
            entry_ids = self._lanes_normalized.get(n)
            if entry_ids:
                return entry_ids
        return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# CAMeL clitic fields. ``prcN`` are proclitics (e.g., wa-, fa-, bi-, al-);
# ``encN`` are enclitics (e.g., -hu, -hum). Empty string == no clitic.
_CLITIC_FIELDS = ("prc0", "prc1", "prc2", "prc3", "enc0", "enc1")


def _extract_clitics(analysis: Dict) -> Dict[str, str]:
    """Extract non-empty clitic markers from a CAMeL analysis dict.

    CAMeL uses ``"0"`` for "no clitic" and ``"na"`` for "not applicable"
    (e.g., on a totally unknown token). Both are filtered out — only
    real proclitic/enclitic markers remain.
    """
    clitics: Dict[str, str] = {}
    for f in _CLITIC_FIELDS:
        v = analysis.get(f)
        if v and v != "0" and v != "na":
            clitics[f] = v
    return clitics


def _is_useful_analysis(analysis: Dict) -> bool:
    """Heuristic: did CAMeL actually recognize this surface form?

    CAMeL Tools doesn't raise on unrecognized input — it returns a
    fallback analysis with ``pos="foreign"`` and ``root="FOREIGN"``
    (or all-``None``). We treat those as no analysis.
    """
    pos = analysis.get("pos")
    root = analysis.get("root")
    if not pos and not root:
        return False
    if pos == "foreign" or root == "FOREIGN":
        return False
    return True


_POS_DOT_SUFFIX_RE = re.compile(r"\.[a-z_]+$")


def _strip_pos_dot_suffix(pos: Optional[str]) -> Optional[str]:
    """Strip CAMeL's POS sub-tags (e.g., ``noun_prop`` → still ``noun_prop``;
    ``verb.act_partic`` → ``verb``). The generator API expects the base POS.
    """
    if not pos:
        return None
    return _POS_DOT_SUFFIX_RE.sub("", pos)


def _build_normalized_index(d: Dict[str, Any]) -> Dict[str, Any]:
    """Build a normalized-key reverse map for a dict.

    Multiple entries can collapse to the same normalized key; the last
    one wins (good enough for fuzzy lookup — these sources don't
    enumerate every diacritization variant).
    """
    out: Dict[str, Any] = {}
    for key, value in d.items():
        n = normalize_for_match(key)
        if n:
            out[n] = value
    return out


def _build_normalized_list_index(
    d: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """Build a normalized-key reverse map; concatenate values on collisions."""
    out: Dict[str, List[str]] = {}
    for key, value in d.items():
        n = normalize_for_match(key)
        if not n:
            continue
        out.setdefault(n, []).extend(value)
    return out


def _build_normalized_corpus_index(
    corpus_surfaces: Dict[str, Dict],
) -> Dict[str, Dict]:
    """Normalized-key corpus index. Unions counts + paths across variants.

    Multiple diacritization variants of the same surface form collapse
    to one normalized key. We union the path lists and sum the counts
    so a paradigm-form lookup against the normalized index returns the
    aggregate (count, paths) across all spelling variants.
    """
    out: Dict[str, Dict] = {}
    for key, value in corpus_surfaces.items():
        n = normalize_for_match(key)
        if not n:
            continue
        bucket = out.setdefault(n, {"count": 0, "paths": []})
        bucket["count"] += value.get("count", 0)
        bucket["paths"].extend(value.get("paths", []))
    # De-dup + sort paths once at the end.
    for bucket in out.values():
        bucket["paths"] = sorted(set(bucket["paths"]))
    return out


# ---------------------------------------------------------------------------
# hawramani classical lexicons → lemma-page content extraction
# ---------------------------------------------------------------------------


def _build_classical_definitions_from_hawramani(
    hawramani_entries: Dict[str, Dict],
    lemma_diac: str,
    lemma_lex: str,
) -> Optional[Dict]:
    """Build the ``classical_definitions`` field from hawramani data.

    Looks up the lemma's diacritic-stripped form in the hawramani index
    (keyed by ``fetched_slug``). For the matching page, picks the
    matching headword block (preferring the headword whose stripped form
    matches the lemma; otherwise the first non-empty block) and emits
    one entry per lexicon.

    Output shape:
        {
          "source": "hawramani",
          "url": "https://arabiclexicon.hawramani.com/{slug}/",
          "headword_ar": "قال",
          "entries": [
            {"lexicon_id": "dictionary_31", "lexicon_en": ..., "lexicon_ar": ...,
             "permalink": ..., "body_html": "..."},
            ...
          ]
        }

    Returns ``None`` when no entries are found.
    """
    if not hawramani_entries:
        return None
    # Try multiple key forms — the scraper uses stripped-diacritics
    # form so check that first, then raw diacritized as fallback.
    keys_to_try = []
    for k in (lemma_diac, lemma_lex):
        if not k:
            continue
        stripped = strip_arabic_diacritics(k)
        if stripped and stripped not in keys_to_try:
            keys_to_try.append(stripped)
        if k not in keys_to_try:
            keys_to_try.append(k)
    page = None
    matched_key = None
    for k in keys_to_try:
        page = hawramani_entries.get(k)
        if page:
            matched_key = k
            break
    if not page:
        return None
    headwords = page.get("headwords") or []
    if not headwords:
        return None

    # Pick the headword block whose Arabic form most closely matches our
    # lemma. Preference order:
    #   1. headword whose stripped form == our lemma_diac stripped form
    #   2. first headword with non-empty entries
    target_stripped = strip_arabic_diacritics(lemma_diac)
    chosen: Optional[Dict] = None
    for hw in headwords:
        hw_stripped = strip_arabic_diacritics(hw.get("headword_ar", ""))
        if hw_stripped == target_stripped and hw.get("entries"):
            chosen = hw
            break
    if chosen is None:
        for hw in headwords:
            if hw.get("entries"):
                chosen = hw
                break
    if chosen is None:
        return None

    return {
        "source": "hawramani",
        "url": page.get("url"),
        "headword_ar": chosen.get("headword_ar"),
        "entries": list(chosen.get("entries") or []),
    }


# ---------------------------------------------------------------------------
# Lane's Lexicon → lemma-page content extraction
# ---------------------------------------------------------------------------

import urllib.parse as _urllib_parse


def _build_lanes_search_url(lemma_or_lex: str) -> Optional[str]:
    """Return the lanelexicon.com WordPress-search URL for a lemma.

    No Lane's viewer offers stable per-entry-id deep linking, so we
    fall back to a search URL on lanelexicon.com that takes the user
    to a results page for the lemma. The lemma is percent-encoded.
    """
    if not lemma_or_lex:
        return None
    return (
        "https://lanelexicon.com/?s="
        + _urllib_parse.quote(lemma_or_lex, safe="")
    )


def _build_lanes_definition(
    lanes_entries: Dict[str, Dict],
    entry_ids: List[str],
) -> Optional[Dict]:
    """Build a lemma page's ``lanes_definition`` field from Lane's entries.

    Given the list of Lane's entry IDs that match a lemma (provided by
    :meth:`WordPageBuilder._find_lanes_entry_ids`), pulls each entry's
    structured body from ``lanes_entries`` and returns a payload the UI
    can render.

    Output shape:
        {
          "source": "lanes",
          "entries": [
            {
              "entry_id": "n42874",
              "headword_ar": "قَالَ",
              "root": "qwl",
              "body": [<segments — italic_en / arabic / text / quote /
                       page_break>],
              "source_refs": ["S", "K", ...]
            },
            ...
          ]
        }

    Returns ``None`` when the lemma has no Lane's entries OR none of its
    entry IDs are present in the supplied index (e.g., when the
    structured index file is missing — same fallback behavior as the
    Wiktextract path).

    Body segments are NOT truncated or capped — full content preserved
    per user direction.
    """
    if not entry_ids or not lanes_entries:
        return None
    entries: List[Dict] = []
    for eid in entry_ids:
        entry = lanes_entries.get(eid)
        if not entry:
            continue
        entries.append({
            "entry_id": eid,
            "headword_ar": entry.get("headword_ar") or None,
            "root": entry.get("root") or None,
            "body": entry.get("body") or [],
            "source_refs": entry.get("source_refs") or [],
        })
    if not entries:
        return None
    return {"source": "lanes", "entries": entries}


# ---------------------------------------------------------------------------
# Wiktextract → lemma-page content extraction
# ---------------------------------------------------------------------------

# Cap on examples kept per sense — Wiktextract often has 5+ examples per
# sense; the surplus blows up file size without much UI value. The full
# unfiltered list remains in the WordSources slim if a reader wants more.
_MAX_EXAMPLES_PER_SENSE = 2


def _build_definition_from_wiktextract(
    entries: List[Dict],
) -> Optional[Dict]:
    """Build the lemma page's ``definition`` field from Wiktextract entries.

    Output shape:
        {
          "source": "wiktextract",
          "senses": [
            {"pos": "verb", "gloss": "to say", "tags": [], "examples": [...]},
            {"pos": "verb", "gloss": "to tell", ...},
            {"pos": "noun", "gloss": "saying", ...},
          ]
        }

    All senses from all entries are concatenated, each tagged with the
    parent entry's POS so the UI can group them. Empty senses are
    dropped. Examples are capped (see :data:`_MAX_EXAMPLES_PER_SENSE`).

    Returns ``None`` when there are no usable senses.
    """
    if not entries:
        return None
    senses: List[Dict] = []
    for entry in entries:
        entry_pos = entry.get("pos") or ""
        for sense in entry.get("senses") or []:
            glosses = sense.get("glosses") or []
            if not glosses:
                continue
            # Wiktextract sometimes stores multiple sub-glosses under one
            # sense (e.g., ["to say", "speak"]) — join with semicolons so
            # they render as one paragraph but the structure is intact.
            gloss_text = "; ".join(g for g in glosses if g)
            if not gloss_text:
                continue
            sense_entry: Dict = {
                "pos": entry_pos or None,
                "gloss": gloss_text,
            }
            tags = sense.get("tags") or []
            if tags:
                sense_entry["tags"] = tags
            examples = []
            for ex in (sense.get("examples") or [])[:_MAX_EXAMPLES_PER_SENSE]:
                if not ex.get("text"):
                    continue
                ex_out = {"text": ex["text"]}
                if ex.get("english"):
                    ex_out["english"] = ex["english"]
                examples.append(ex_out)
            if examples:
                sense_entry["examples"] = examples
            senses.append(sense_entry)
    if not senses:
        return None
    return {"source": "wiktextract", "senses": senses}


def _build_etymology_from_wiktextract(
    entries: List[Dict],
) -> Optional[Dict]:
    """Build the lemma page's ``etymology`` field.

    Wiktextract usually stores etymology once per entry (not per sense).
    When multiple entries exist (different POS), we keep one entry per
    unique etymology_text. Returns ``None`` if no entry has etymology.
    """
    if not entries:
        return None
    seen_text: set = set()
    texts: List[str] = []
    for entry in entries:
        et = entry.get("etymology_text")
        if et and et not in seen_text:
            seen_text.add(et)
            texts.append(et)
    if not texts:
        return None
    return {
        "source": "wiktextract",
        # Join with a separator so the UI shows distinct etymologies
        # explicitly. Most lemmas have one; verbs occasionally pair with
        # a noun entry that shares the etymology.
        "text": "\n\n".join(texts),
    }


def _build_ipa_from_wiktextract(
    entries: List[Dict],
) -> Optional[List[str]]:
    """Return the deduplicated IPA pronunciation list from Wiktextract.

    The Wiktextract slim already extracts an ``ipa`` list per entry
    (during the download step). Here we union them across entries and
    drop duplicates while preserving first-seen order.
    """
    if not entries:
        return None
    seen: set = set()
    out: List[str] = []
    for entry in entries:
        for ipa in entry.get("ipa") or []:
            if ipa not in seen:
                seen.add(ipa)
                out.append(ipa)
    return out or None


def _lookup_with_fallback(
    direct: Dict[str, Any],
    normalized: Dict[str, Any],
    *keys: str,
) -> Tuple[Optional[Any], Optional[str]]:
    """Lookup helper: try each ``keys[i]`` in direct map first, then in
    the normalized map (via :func:`normalize_for_match`).

    Returns ``(matched_value, matched_key)`` or ``(None, None)``.
    """
    for k in keys:
        if not k:
            continue
        v = direct.get(k)
        if v:
            return v, k
    for k in keys:
        if not k:
            continue
        n = normalize_for_match(k)
        v = normalized.get(n)
        if v:
            return v, k
    return None, None
