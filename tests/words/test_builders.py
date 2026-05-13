"""Tests for app.words.builders."""
from __future__ import annotations

import pytest

# CAMeL Tools is heavy; skip the whole module if not installed.
camel_tools = pytest.importorskip("camel_tools")

from app.words.builders import (
    WordPageBuilder,
    _build_classical_definitions_from_hawramani,
    _build_definition_from_wiktextract,
    _build_etymology_from_wiktextract,
    _build_ipa_from_wiktextract,
    _build_lanes_definition,
    _build_lanes_search_url,
    _build_normalized_index,
    _build_normalized_list_index,
    _extract_clitics,
    _lookup_with_fallback,
    _strip_pos_dot_suffix,
    build_lanes_arabic_index,
    canonical_diacritized_lemma,
    perseus_bw_to_arabic,
    root_to_slug,
    strip_arabic_diacritics,
)


# ---------------------------------------------------------------------------
# Perseus → Arabic conversion
# ---------------------------------------------------------------------------

class TestPerseusBwToArabic:
    def test_empty(self):
        assert perseus_bw_to_arabic("") == ""
        assert perseus_bw_to_arabic(None) == ""  # type: ignore[arg-type]

    def test_strips_perseus_extensions(self):
        # `^` and digits are Perseus-specific markers.
        # Result should be readable Arabic.
        result = perseus_bw_to_arabic("kataba")
        assert "ك" in result
        assert "ت" in result
        assert "ب" in result

    def test_handles_perseus_marker(self):
        # `^` should be stripped before bw2ar.
        result = perseus_bw_to_arabic("$uw^obuwbN")
        # Stripping `^` and applying bw2ar: $uwobuwbN -> شُوْبُوبٌ-ish
        assert "ش" in result  # $ -> ش

    def test_handles_digits(self):
        # Digits are stripped before bw2ar.
        result = perseus_bw_to_arabic("b1iSor")
        # Should map to بِصْر roughly
        assert "ب" in result
        assert "ص" in result


class TestBuildLanesArabicIndex:
    def test_basic(self):
        orth = {
            "kataba": ["n1", "n2"],
            "katoba": ["n3"],  # different bw, may collide with kataba after norm
        }
        result = build_lanes_arabic_index(orth)
        assert isinstance(result, dict)
        # Each value should be a list of entry IDs.
        for v in result.values():
            assert isinstance(v, list)
            assert all(isinstance(x, str) for x in v)

    def test_empty(self):
        assert build_lanes_arabic_index({}) == {}


# ---------------------------------------------------------------------------
# Canonical diacritized lemma
# ---------------------------------------------------------------------------

class TestCanonicalDiacritizedLemma:
    def test_verb_returns_past_3ms(self):
        # CAMeL lex for "to say" is قال (undiacritized).
        # Past 3ms is قالَ.
        result = canonical_diacritized_lemma("قال", "verb")
        assert result.startswith("قال") or "قَال" in result

    def test_empty(self):
        assert canonical_diacritized_lemma("", "verb") == ""

    def test_unanalyzable_falls_back_to_lex(self):
        # Made-up lemma that won't produce a paradigm.
        result = canonical_diacritized_lemma("xxxنxxx", "verb")
        # Returns NFC of input.
        assert result == "xxxنxxx"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestExtractClitics:
    def test_no_clitics(self):
        analysis = {"prc0": "0", "prc1": "0", "enc0": "0"}
        assert _extract_clitics(analysis) == {}

    def test_with_clitics(self):
        analysis = {"prc0": "0", "prc2": "wa_sub", "enc0": "0", "prc1": "bi_prep"}
        result = _extract_clitics(analysis)
        assert result == {"prc2": "wa_sub", "prc1": "bi_prep"}

    def test_empty_string_clitic_dropped(self):
        analysis = {"prc0": "", "prc1": "bi_prep"}
        assert _extract_clitics(analysis) == {"prc1": "bi_prep"}


class TestStripPosDotSuffix:
    def test_with_dot(self):
        assert _strip_pos_dot_suffix("verb.act_partic") == "verb"

    def test_no_dot(self):
        assert _strip_pos_dot_suffix("noun") == "noun"

    def test_noun_underscore_preserved(self):
        # noun_prop has underscore, not dot — preserved.
        assert _strip_pos_dot_suffix("noun_prop") == "noun_prop"

    def test_empty(self):
        assert _strip_pos_dot_suffix("") is None
        assert _strip_pos_dot_suffix(None) is None


class TestNormalizedIndexBuilders:
    def test_build_normalized_index(self):
        d = {"قَالَ": {"x": 1}, "بَيْت": {"y": 2}}
        result = _build_normalized_index(d)
        assert len(result) == 2
        # Values preserved.
        assert {"x": 1} in result.values()
        assert {"y": 2} in result.values()

    def test_build_normalized_list_index_concatenates(self):
        d = {"قَالَ": ["n1"], "قال": ["n2", "n3"]}
        result = _build_normalized_list_index(d)
        # Both should collapse to same normalized key.
        assert len(result) == 1
        # Combined value.
        only_value = next(iter(result.values()))
        assert set(only_value) == {"n1", "n2", "n3"}


class TestLookupWithFallback:
    def test_direct_hit(self):
        direct = {"قَالَ": "X"}
        normalized = {}
        v, k = _lookup_with_fallback(direct, normalized, "قَالَ", "قال")
        assert v == "X"
        assert k == "قَالَ"

    def test_normalized_fallback(self):
        # Direct miss, normalized hit on second key.
        direct = {"قالَ": "X"}  # different diacritization
        from app.words.normalize import normalize_for_match
        normalized = {normalize_for_match("قالَ"): "X"}
        v, k = _lookup_with_fallback(direct, normalized, "قَالَ", "قال")
        assert v == "X"

    def test_no_match(self):
        v, k = _lookup_with_fallback({}, {}, "xxx", "yyy")
        assert v is None
        assert k is None

    def test_empty_keys_skipped(self):
        direct = {"كتب": "X"}
        v, k = _lookup_with_fallback(direct, {}, "", "كتب")
        assert v == "X"


# ---------------------------------------------------------------------------
# WordPageBuilder
# ---------------------------------------------------------------------------

@pytest.fixture
def builder_with_data():
    """Pre-populated builder with tiny synthetic source indexes."""
    corpus = {
        "قَالَ": {"count": 5, "paths": ["/books/x:1", "/books/x:2"]},
        "وَقَالَ": {"count": 3, "paths": ["/books/x:3"]},
        "بَيْت": {"count": 2, "paths": ["/books/x:4"]},
    }
    qac = {
        "قَالَ": {"lemma": "قَالَ", "root": "قول", "pos": "V",
                   "occurrences": [{"location": "1:1:1:1", "surface": "قَالَ"}]},
    }
    wikt = {
        "قال": {"entry_count": 1, "pos_tags": ["verb"], "has_etymology": False,
                "sense_count": 3},
    }
    lanes_ar = {"قال": ["n1", "n2"]}
    return WordPageBuilder(corpus, qac, wikt, lanes_ar)


class TestBuildSurface:
    def test_surface_in_corpus(self, builder_with_data):
        page = builder_with_data.build_surface("قَالَ")
        assert page["surface"] == "قَالَ"
        assert page["slug"] == "قَالَ"
        assert page["occurrence_count"] == 5
        assert page["occurrence_paths"] == ["/books/x:1", "/books/x:2"]
        morph = page["morphology"]
        assert morph is not None
        # Lemma should be derived even when corpus surface count exists.
        assert page["lemma_link"] is not None
        # Root precomputed in both slug + link form so the UI doesn't
        # have to duplicate root-to-slug logic.
        assert morph["root_slug"] is not None
        assert "." not in morph["root_slug"]
        assert "#" not in morph["root_slug"]
        assert page["root_link"] == f"/words/roots/{morph['root_slug']}"

    def test_surface_not_in_corpus(self, builder_with_data):
        page = builder_with_data.build_surface("قَالَتْ")
        # Not in our 3-entry corpus.
        assert page["occurrence_count"] == 0
        assert page["occurrence_paths"] == []
        # Should still have morphology from CAMeL.
        assert page["morphology"] is not None

    def test_compound_surface_has_clitic(self, builder_with_data):
        page = builder_with_data.build_surface("وَقَالَ")
        morph = page["morphology"]
        # `wa-` proclitic should be detected.
        assert morph is not None
        assert morph["clitics"]
        # Some clitic field should mention "wa".
        assert any("wa" in str(v) for v in morph["clitics"].values())

    def test_unanalyzable_surface(self, builder_with_data):
        page = builder_with_data.build_surface("xxxzzzqqq")
        assert page["morphology"] is None
        assert page["lemma_link"] is None
        assert page["root_link"] is None


class TestBuildLemma:
    def test_lemma_basic_fields(self, builder_with_data):
        page = builder_with_data.build_lemma("قَالَ")
        assert page["lemma"] == "قَالَ"
        assert page["slug"] == "قَالَ"
        assert page["pos"] == "V"
        assert page["pos_camel"] == "verb"
        # Root should resemble CAMeL's hollow-verb format (q.#.l).
        assert page["root"] is not None
        # Paradigm should have many forms for a verb.
        assert len(page["paradigm"]) > 10
        # Frequency aggregated across in-corpus paradigm forms.
        assert page["frequency_in_corpus"] >= 0
        # LLM-filled fields are None.
        assert page["translations"] is None
        assert page["definition"] is None
        assert page["etymology"] is None

    def test_lemma_paradigm_marks_corpus_hits(self, builder_with_data):
        page = builder_with_data.build_lemma("قَالَ")
        # قَالَ itself should be in corpus → at least one paradigm form
        # marked in_corpus=True.
        has_corpus_hit = any(p.get("in_corpus") for p in page["paradigm"])
        assert has_corpus_hit

    def test_lemma_cross_references(self, builder_with_data):
        page = builder_with_data.build_lemma("قَالَ")
        refs = page["cross_references"]
        # All three sources have data for قَالَ in our fixture.
        assert refs["qac"]["found"] is True
        assert refs["qac"]["root"] == "قول"
        assert refs["wiktextract"]["found"] is True
        assert refs["lanes"]["found"] is True
        assert "n1" in refs["lanes"]["entry_ids"]

    def test_lemma_has_root_link(self, builder_with_data):
        page = builder_with_data.build_lemma("قَالَ")
        # Root link / slug should be set when root is known.
        assert page["root"] is not None
        assert page["root_slug"] is not None
        assert page["root_link"] is not None
        assert page["root_link"].startswith("/words/roots/")
        # Slug should have no `.` or `#` (URL-safe).
        assert "." not in page["root_slug"]
        assert "#" not in page["root_slug"]

    def test_paradigm_has_no_redundant_diacritized(self, builder_with_data):
        page = builder_with_data.build_lemma("قَالَ")
        for entry in page["paradigm"]:
            # We dropped paradigm[].diacritized — only `form` remains.
            assert "diacritized" not in entry


class TestRootToSlug:
    def test_basic_root(self):
        assert root_to_slug("ك.ت.ب") == "ك-ت-ب"

    def test_hollow_root_uses_underscore(self):
        # Weak/hollow radical # → _
        assert root_to_slug("ق.#.ل") == "ق-_-ل"

    def test_no_dots_or_hashes_in_output(self):
        s = root_to_slug("ق.#.ل")
        assert "." not in s
        assert "#" not in s

    def test_foreign_returns_none(self):
        assert root_to_slug("FOREIGN") is None

    def test_empty_returns_none(self):
        assert root_to_slug("") is None
        assert root_to_slug(None) is None


class TestWiktextractExtractors:
    def test_definition_basic(self):
        entries = [{
            "pos": "verb",
            "senses": [
                {"glosses": ["to say"], "examples": [
                    {"text": "قَالَ", "english": "He said"},
                    {"text": "قُلْتُ", "english": "I said"},
                    {"text": "extra", "english": None},  # should be capped
                ]},
                {"glosses": ["to tell"], "tags": ["transitive"]},
            ],
        }]
        result = _build_definition_from_wiktextract(entries)
        assert result["source"] == "wiktextract"
        assert len(result["senses"]) == 2
        first = result["senses"][0]
        assert first["pos"] == "verb"
        assert first["gloss"] == "to say"
        assert len(first["examples"]) == 2  # capped at _MAX_EXAMPLES_PER_SENSE
        assert first["examples"][0]["text"] == "قَالَ"
        assert first["examples"][0]["english"] == "He said"
        # Sense 2 has tags + no examples
        second = result["senses"][1]
        assert second["gloss"] == "to tell"
        assert second["tags"] == ["transitive"]
        assert "examples" not in second

    def test_definition_joins_subglosses(self):
        entries = [{
            "pos": "verb",
            "senses": [{"glosses": ["to advocate", "to propound"]}],
        }]
        result = _build_definition_from_wiktextract(entries)
        assert result["senses"][0]["gloss"] == "to advocate; to propound"

    def test_definition_merges_multiple_entries(self):
        entries = [
            {"pos": "verb", "senses": [{"glosses": ["to say"]}]},
            {"pos": "noun", "senses": [{"glosses": ["saying"]}]},
        ]
        result = _build_definition_from_wiktextract(entries)
        assert len(result["senses"]) == 2
        poss = {s["pos"] for s in result["senses"]}
        assert poss == {"verb", "noun"}

    def test_definition_empty_returns_none(self):
        assert _build_definition_from_wiktextract([]) is None
        # entries with no glosses
        entries = [{"pos": "verb", "senses": [{"glosses": []}]}]
        assert _build_definition_from_wiktextract(entries) is None

    def test_etymology_basic(self):
        entries = [{"etymology_text": "From PIE root *bʰeh₂-."}]
        result = _build_etymology_from_wiktextract(entries)
        assert result["source"] == "wiktextract"
        assert "PIE root" in result["text"]

    def test_etymology_dedupes_across_entries(self):
        entries = [
            {"etymology_text": "From PIE root *bʰeh₂-."},
            {"etymology_text": "From PIE root *bʰeh₂-."},  # dup
            {"etymology_text": "Alternative etymology."},
        ]
        result = _build_etymology_from_wiktextract(entries)
        # Two unique etymologies joined.
        assert result["text"].count("PIE root") == 1
        assert "Alternative" in result["text"]

    def test_etymology_missing_returns_none(self):
        assert _build_etymology_from_wiktextract([]) is None
        assert _build_etymology_from_wiktextract([{"pos": "verb"}]) is None

    def test_ipa_basic(self):
        entries = [
            {"ipa": ["/qaːla/", "/qaːl/"]},
            {"ipa": ["/qaːla/", "/ɡaːl/"]},  # /qaːla/ is dup
        ]
        result = _build_ipa_from_wiktextract(entries)
        assert result == ["/qaːla/", "/qaːl/", "/ɡaːl/"]

    def test_ipa_missing_returns_none(self):
        assert _build_ipa_from_wiktextract([]) is None
        assert _build_ipa_from_wiktextract([{"pos": "verb"}]) is None


class TestLanesExtractors:
    def test_search_url_basic(self):
        url = _build_lanes_search_url("قَالَ")
        assert url is not None
        assert url.startswith("https://lanelexicon.com/?s=")
        # Arabic must be percent-encoded
        assert "قَالَ" not in url

    def test_search_url_empty(self):
        assert _build_lanes_search_url("") is None
        assert _build_lanes_search_url(None) is None

    def test_definition_basic(self):
        lanes_entries = {
            "n1": {
                "headword_ar": "قَالَ",
                "root": "qwl",
                "body": [{"kind": "italic_en", "text": "He said"}],
                "source_refs": ["S", "K"],
            },
        }
        result = _build_lanes_definition(lanes_entries, ["n1"])
        assert result["source"] == "lanes"
        assert len(result["entries"]) == 1
        first = result["entries"][0]
        assert first["entry_id"] == "n1"
        assert first["headword_ar"] == "قَالَ"
        assert first["source_refs"] == ["S", "K"]
        assert len(first["body"]) == 1

    def test_definition_multiple_entries(self):
        lanes_entries = {
            "n1": {"headword_ar": "قَالَ", "body": [], "source_refs": []},
            "n2": {"headword_ar": "قَالَ", "body": [], "source_refs": []},
        }
        result = _build_lanes_definition(lanes_entries, ["n1", "n2"])
        assert len(result["entries"]) == 2
        assert [e["entry_id"] for e in result["entries"]] == ["n1", "n2"]

    def test_definition_missing_entries_returns_none(self):
        # No entry_ids
        assert _build_lanes_definition({"n1": {}}, []) is None
        # entry_ids that don't exist in the index
        assert _build_lanes_definition({"n1": {}}, ["n99"]) is None
        # No index provided
        assert _build_lanes_definition({}, ["n1"]) is None

    def test_lanes_lookup_includes_search_url(self):
        # WordPageBuilder._lookup_lanes should include search_url when found.
        builder = WordPageBuilder(lanes_arabic_index={"قال": ["n1"]})
        result = builder._lookup_lanes("قال", "قال")
        assert result["found"] is True
        assert result["entry_ids"] == ["n1"]
        assert result["search_url"].startswith("https://lanelexicon.com/?s=")

    def test_lanes_lookup_no_search_url_when_not_found(self):
        builder = WordPageBuilder()
        result = builder._lookup_lanes("xxx", "xxx")
        assert result == {"found": False}


class TestStripArabicDiacritics:
    def test_basic(self):
        # قَالَ → قال
        assert strip_arabic_diacritics("قَالَ") == "قال"

    def test_preserves_alif_variants(self):
        # We strip diacritics but NOT alif/ya/hamza unification
        assert strip_arabic_diacritics("أَرَادَ") == "أراد"  # hamza preserved
        assert strip_arabic_diacritics("إِبْرَاهِيم") == "إبراهيم"

    def test_empty(self):
        assert strip_arabic_diacritics("") == ""
        assert strip_arabic_diacritics(None) == ""


class TestClassicalDefinitionsFromHawramani:
    @pytest.fixture
    def hawramani_data(self):
        return {
            "قال": {
                "fetched_slug": "قال",
                "url": "https://arabiclexicon.hawramani.com/قال/",
                "headwords": [
                    {
                        "headword_ar": "قال",
                        "summary": "Entries on قال in 2 dictionaries",
                        "entries": [
                            {"lexicon_id": "dictionary_31",
                             "lexicon_en": "Al-Rāghib, Mufradāt",
                             "lexicon_ar": "المفردات",
                             "permalink": "https://x/#1",
                             "body_html": "<p>قال body</p>"},
                            {"lexicon_id": "dictionary_1",
                             "lexicon_en": "Ibn Manẓūr, Lisān",
                             "lexicon_ar": "لسان العرب",
                             "permalink": "https://x/#2",
                             "body_html": "<p>lisan body</p>"},
                        ],
                    },
                    # second headword with different stripped form — won't match
                    {"headword_ar": "قِيلَ", "summary": "...",
                     "entries": [{"lexicon_id": "dictionary_1",
                                  "lexicon_en": "x", "lexicon_ar": "",
                                  "permalink": "", "body_html": "qīla body"}]},
                ],
            },
        }

    def test_basic_lookup(self, hawramani_data):
        result = _build_classical_definitions_from_hawramani(
            hawramani_data, "قَالَ", "قال",
        )
        assert result is not None
        assert result["source"] == "hawramani"
        assert result["url"] == "https://arabiclexicon.hawramani.com/قال/"
        assert result["headword_ar"] == "قال"  # matched stripped form
        assert len(result["entries"]) == 2
        assert result["entries"][0]["lexicon_id"] == "dictionary_31"

    def test_no_match_returns_none(self, hawramani_data):
        result = _build_classical_definitions_from_hawramani(
            hawramani_data, "xxxnotfound", "xxxnotfound",
        )
        assert result is None

    def test_empty_index_returns_none(self):
        assert _build_classical_definitions_from_hawramani({}, "قَالَ", "قال") is None

    def test_picks_headword_block_matching_stripped_form(self, hawramani_data):
        # When the page has multiple headword blocks, prefer the one
        # whose stripped form matches our lemma's stripped form.
        result = _build_classical_definitions_from_hawramani(
            hawramani_data, "قَالَ", "قال",
        )
        # قَالَ.strip = قال — should match the "قال" block, not "قِيلَ".
        assert result["headword_ar"] == "قال"
        # The "قِيلَ" block has only one entry; "قال" has two.
        assert len(result["entries"]) == 2


class TestBuildLemmaWithHawramaniContent:
    def test_lemma_classical_definitions_populated(self):
        corpus = {"قَالَ": {"count": 1, "paths": []}}
        hawramani = {
            "قال": {
                "fetched_slug": "قال",
                "url": "https://arabiclexicon.hawramani.com/قال/",
                "headwords": [
                    {"headword_ar": "قال", "summary": "x",
                     "entries": [{"lexicon_id": "dictionary_31",
                                  "lexicon_en": "Mufradāt",
                                  "lexicon_ar": "المفردات",
                                  "permalink": "https://x/#1",
                                  "body_html": "<p>body</p>"}]},
                ],
            },
        }
        builder = WordPageBuilder(
            corpus_surfaces=corpus,
            hawramani_entries=hawramani,
        )
        page = builder.build_lemma("قَالَ")
        cd = page["classical_definitions"]
        assert cd is not None
        assert cd["source"] == "hawramani"
        assert len(cd["entries"]) == 1
        assert cd["entries"][0]["lexicon_id"] == "dictionary_31"

    def test_lemma_classical_definitions_null_without_data(self):
        builder = WordPageBuilder(
            corpus_surfaces={"قَالَ": {"count": 1, "paths": []}},
        )
        page = builder.build_lemma("قَالَ")
        assert page["classical_definitions"] is None


class TestBuildLemmaWithLanesContent:
    @pytest.fixture
    def builder_with_lanes(self):
        corpus = {"قَالَ": {"count": 1, "paths": []}}
        return WordPageBuilder(
            corpus_surfaces=corpus,
            lanes_arabic_index={"قال": ["n1", "n2"]},
            lanes_entries={
                "n1": {
                    "headword_ar": "قَالَ",
                    "root": "qwl",
                    "body": [
                        {"kind": "italic_en", "text": "He said; uttered"},
                        {"kind": "text", "text": "(S, K)"},
                    ],
                    "source_refs": ["S", "K"],
                },
                "n2": {
                    "headword_ar": "قَالَ",
                    "root": "qyl",
                    "body": [{"kind": "italic_en", "text": "He napped"}],
                    "source_refs": ["Mgh"],
                },
            },
        )

    def test_lemma_lanes_definition_populated(self, builder_with_lanes):
        page = builder_with_lanes.build_lemma("قَالَ")
        ld = page["lanes_definition"]
        assert ld is not None
        assert ld["source"] == "lanes"
        # Both entries from cross_references should be merged.
        assert len(ld["entries"]) == 2

    def test_lemma_lanes_definition_null_without_index(self):
        # Lane's cross-ref found but no structured index → null definition
        builder = WordPageBuilder(
            corpus_surfaces={"قَالَ": {"count": 1, "paths": []}},
            lanes_arabic_index={"قال": ["n1"]},
            lanes_entries={},
        )
        page = builder.build_lemma("قَالَ")
        assert page["lanes_definition"] is None
        # But the cross-ref still reports found + search URL.
        assert page["cross_references"]["lanes"]["found"] is True
        assert page["cross_references"]["lanes"]["search_url"] is not None


class TestBuildLemmaWithWiktContent:
    @pytest.fixture
    def builder_with_wikt(self):
        corpus = {"قَالَ": {"count": 5, "paths": ["/books/x:1"]}}
        # Full slim keyed on undiacritized lex form.
        wikt_full = {
            "قال": [{
                "pos": "verb",
                "senses": [{"glosses": ["to say"]}],
                "etymology_text": "From Proto-Semitic.",
                "ipa": ["/qaːla/"],
            }],
        }
        return WordPageBuilder(
            corpus_surfaces=corpus,
            wiktextract_full=wikt_full,
        )

    def test_lemma_definition_populated(self, builder_with_wikt):
        page = builder_with_wikt.build_lemma("قَالَ")
        assert page["definition"] is not None
        assert page["definition"]["source"] == "wiktextract"

    def test_lemma_etymology_populated(self, builder_with_wikt):
        page = builder_with_wikt.build_lemma("قَالَ")
        assert page["etymology"] is not None
        assert "Proto-Semitic" in page["etymology"]["text"]

    def test_lemma_ipa_populated(self, builder_with_wikt):
        page = builder_with_wikt.build_lemma("قَالَ")
        assert page["ipa"] == ["/qaːla/"]

    def test_lemma_translations_still_null(self, builder_with_wikt):
        # Wiktextract Arabic-side entries don't carry foreign-language
        # translations; the LLM phase handles those.
        page = builder_with_wikt.build_lemma("قَالَ")
        assert page["translations"] is None

    def test_no_wikt_full_leaves_fields_null(self):
        builder = WordPageBuilder(
            corpus_surfaces={"قَالَ": {"count": 1, "paths": []}},
        )
        page = builder.build_lemma("قَالَ")
        assert page["definition"] is None
        assert page["etymology"] is None
        assert page["ipa"] is None


class TestBuildRoot:
    def test_basic(self, builder_with_data):
        lemmas = [
            {"slug": "قَالَ", "pos": "V", "frequency": 7066},
            {"slug": "أَقَالَ", "pos": "V", "frequency": 123},
            {"slug": "قَوْل", "pos": "N", "frequency": 200},
        ]
        page = builder_with_data.build_root("ق.#.ل", lemmas)
        assert page["root"] == "ق.#.ل"
        assert page["slug"] == "ق-_-ل"
        assert page["lemmas"] == lemmas  # caller-supplied order preserved
        assert page["lemma_count"] == 3
        assert page["total_frequency"] == 7066 + 123 + 200
        # LLM-fillable fields are null
        assert page["translations"] is None
        assert page["definition"] is None
        assert page["etymology"] is None

    def test_empty_lemmas(self, builder_with_data):
        page = builder_with_data.build_root("ك.ت.ب", [])
        assert page["lemma_count"] == 0
        assert page["total_frequency"] == 0
        assert page["lemmas"] == []
