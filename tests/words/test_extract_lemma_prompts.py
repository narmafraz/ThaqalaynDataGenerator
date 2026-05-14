"""Unit tests for scripts/extract_lemma_translation_prompts.py.

Covers POS-aligned gloss picking, Lane's body rendering, classical
definitions summarization, and the per-lemma item builder.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


def _import_extractor():
    here = Path(__file__).resolve().parents[2]
    target = here / "scripts" / "extract_lemma_translation_prompts.py"
    spec = importlib.util.spec_from_file_location("_lemma_extractor", str(target))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def ext():
    return _import_extractor()


# ────────────────── pick_aligned_gloss ──────────────────


def test_pick_aligned_gloss_verb_pos(ext) -> None:
    senses = [
        {"pos": "verb", "gloss": "to say"},
        {"pos": "noun", "gloss": "saying"},
    ]
    assert ext.pick_aligned_gloss("verb", senses) == "to say"


def test_pick_aligned_gloss_preposition_skips_verb_homograph(ext) -> None:
    """إِلَى is both a preposition 'to/toward' and a verb 'to promise'.
    Path C originally got the verb gloss; the POS-aligned picker fixes that.
    """
    senses = [
        {"pos": "verb", "gloss": "to promise"},
        {"pos": "preposition", "gloss": "to, toward"},
    ]
    assert ext.pick_aligned_gloss("prep", senses) == "to, toward"


def test_pick_aligned_gloss_function_word_no_fallback(ext) -> None:
    """For function-word POS, if no aligned sense exists, return empty
    string rather than risk surfacing the wrong gloss."""
    senses = [{"pos": "verb", "gloss": "to come"}]
    assert ext.pick_aligned_gloss("part", senses) == ""


def test_pick_aligned_gloss_content_word_falls_back_to_senses0(ext) -> None:
    """For content-word POS with no aligned sense, falls back to senses[0]
    (better an unaligned gloss than nothing for nouns/verbs)."""
    senses = [{"pos": "name", "gloss": "Tuareg"}]
    out = ext.pick_aligned_gloss("noun", senses)
    assert out == "Tuareg"


def test_pick_aligned_gloss_empty_senses(ext) -> None:
    assert ext.pick_aligned_gloss("verb", []) == ""
    assert ext.pick_aligned_gloss("noun", []) == ""


# ────────────────── render_lanes_body ──────────────────


def test_render_lanes_body_concatenates_text_and_italic(ext) -> None:
    entries = [{"body": [
        {"kind": "text", "text": "He said,"},
        {"kind": "italic_en", "text": "spake, talked."},
        {"kind": "arabic", "text_ar": "قَالَ", "text_bw": "qaAla"},
    ]}]
    out = ext.render_lanes_body(entries)
    assert "He said," in out
    assert "spake, talked." in out
    assert "قَالَ" in out


def test_render_lanes_body_falls_back_to_buckwalter(ext) -> None:
    """Arabic segments use text_ar by default but fall back to text_bw."""
    entries = [{"body": [{"kind": "arabic", "text_bw": "qaAla"}]}]
    out = ext.render_lanes_body(entries)
    assert "qaAla" in out


def test_render_lanes_body_skips_page_breaks(ext) -> None:
    entries = [{"body": [
        {"kind": "text", "text": "before"},
        {"kind": "page_break", "page": 100},
        {"kind": "text", "text": "after"},
    ]}]
    out = ext.render_lanes_body(entries)
    assert "100" not in out
    assert "before" in out and "after" in out


def test_render_lanes_body_separates_multiple_entries(ext) -> None:
    entries = [
        {"body": [{"kind": "text", "text": "first entry"}]},
        {"body": [{"kind": "text", "text": "second entry"}]},
    ]
    out = ext.render_lanes_body(entries)
    assert "first entry" in out
    assert "second entry" in out
    assert "|" in out  # separator between entries


def test_render_lanes_body_empty_returns_empty_string(ext) -> None:
    assert ext.render_lanes_body([]) == ""
    assert ext.render_lanes_body([{"body": []}]) == ""


# ────────────────── render_classical_definitions ──────────────────


def test_render_classical_definitions_strips_html(ext) -> None:
    cd = {"entries": [{
        "lexicon_en": "Al-Mufradat",
        "body_html": "<span>Some <b>Arabic</b> text</span>",
    }]}
    out = ext.render_classical_definitions(cd)
    assert "<" not in out and ">" not in out
    assert "Al-Mufradat" in out
    assert "Arabic" in out


def test_render_classical_definitions_caps_at_max_entries(ext) -> None:
    cd = {"entries": [
        {"lexicon_en": f"Lex{i}", "body_html": f"body_{i}"}
        for i in range(10)
    ]}
    out = ext.render_classical_definitions(cd, max_entries=3)
    assert "body_0" in out and "body_1" in out and "body_2" in out
    assert "body_5" not in out
    assert "body_9" not in out


def test_render_classical_definitions_caps_at_max_chars(ext) -> None:
    cd = {"entries": [
        {"lexicon_en": "Big", "body_html": "x" * 5000},
    ]}
    out = ext.render_classical_definitions(cd, max_chars=1000)
    assert len(out) <= 1000


def test_render_classical_definitions_handles_none(ext) -> None:
    assert ext.render_classical_definitions(None) == ""
    assert ext.render_classical_definitions({}) == ""
    assert ext.render_classical_definitions({"entries": []}) == ""


# ────────────────── humanize_pos ──────────────────


def test_humanize_pos_known_codes(ext) -> None:
    assert ext.humanize_pos("verb") == "Verb"
    assert ext.humanize_pos("noun") == "Noun"
    assert ext.humanize_pos("noun_prop") == "Proper Noun"
    assert ext.humanize_pos("prep") == "Preposition"


def test_humanize_pos_unknown_returns_input(ext) -> None:
    assert ext.humanize_pos("zzz_unknown") == "zzz_unknown"
    assert ext.humanize_pos("") == "unknown"


# ────────────────── extract_lemma_item ──────────────────


def test_extract_lemma_item_full(ext) -> None:
    data = {
        "slug": "قَالَ",
        "lemma": "قَالَ",
        "pos": "V",
        "pos_camel": "verb",
        "definition": {"senses": [{"pos": "verb", "gloss": "to say"}]},
        "lanes_definition": {"entries": [
            {"body": [{"kind": "text", "text": "He said, spake."}]}
        ]},
        "frequency_in_corpus": 8421,
    }
    item = ext.extract_lemma_item(data)
    assert item is not None
    assert item["slug"] == "قَالَ"
    assert item["lemma_ar"] == "قَالَ"
    assert item["pos"] == "V"
    assert item["pos_label"] == "Verb"
    assert item["en_gloss"] == "to say"
    assert "He said, spake." in item["lane_body"]
    assert item["freq"] == 8421
    assert item["classical_summary"] == ""  # none provided


def test_extract_lemma_item_returns_none_without_slug(ext) -> None:
    assert ext.extract_lemma_item({"lemma": "X"}) is None
    assert ext.extract_lemma_item({}) is None


def test_extract_lemma_item_handles_missing_optional_fields(ext) -> None:
    data = {"slug": "x", "pos_camel": "noun"}
    item = ext.extract_lemma_item(data)
    assert item["en_gloss"] == ""
    assert item["lane_body"] == ""
    assert item["classical_summary"] == ""


# ────────────────── walk_lemmas (filesystem test) ──────────────────


def test_walk_lemmas_loads_all_files(ext, tmp_path) -> None:
    (tmp_path / "lemmas").mkdir()
    for i in range(3):
        slug = f"lemma_{i}"
        (tmp_path / "lemmas" / f"{slug}.json").write_text(
            json.dumps({"slug": slug, "lemma": slug, "pos_camel": "noun"}),
            encoding="utf-8",
        )
    items = ext.walk_lemmas(tmp_path)
    assert len(items) == 3
    slugs = {it["slug"] for it in items}
    assert slugs == {"lemma_0", "lemma_1", "lemma_2"}


def test_walk_lemmas_respects_slug_filter(ext, tmp_path) -> None:
    (tmp_path / "lemmas").mkdir()
    for slug in ("a", "b", "c"):
        (tmp_path / "lemmas" / f"{slug}.json").write_text(
            json.dumps({"slug": slug, "pos_camel": "noun"}),
            encoding="utf-8",
        )
    items = ext.walk_lemmas(tmp_path, slug_filter={"a", "c"})
    slugs = {it["slug"] for it in items}
    assert slugs == {"a", "c"}


def test_walk_lemmas_raises_on_missing_dir(ext, tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        ext.walk_lemmas(tmp_path)  # no lemmas/ subdir
