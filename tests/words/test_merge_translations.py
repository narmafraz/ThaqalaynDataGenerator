"""Unit tests for scripts/merge_translations_into_pages.py.

Tests the pure-Python pieces (validation, gloss cleaning, page-merge
logic) on synthetic file trees. End-to-end merge of real Spark output
is exercised by the smoke test in PATH_B_SPARK_LOG.md.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


def _import_merger():
    here = Path(__file__).resolve().parents[2]
    target = here / "scripts" / "merge_translations_into_pages.py"
    spec = importlib.util.spec_from_file_location("_merger", str(target))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def merger():
    return _import_merger()


@pytest.fixture
def valid_response_payload() -> dict:
    return {
        "slug": "قَالَ",
        "kind": "lemma",
        "parsed": {"glosses": {
            "en": "to say", "fa": "گفتن", "ur": "کہنا",
            "tr": "söylemek", "id": "berkata", "bn": "বলা",
            "es": "decir", "fr": "dire", "de": "sagen",
            "ru": "сказать", "zh": "说",
        }},
        "issues": [],
        "meta": {"elapsed": 2.1, "input_tokens": 200, "output_tokens": 150,
                 "model": "qwen36-fast", "backend": "spark"},
    }


# ────────────────── make_attribution ──────────────────


def test_make_attribution_shape(merger):
    a = merger.make_attribution("qwen36-35b-heretic")
    assert a["model"] == "qwen36-35b-heretic"
    assert a["pipeline_version"] == merger.PIPELINE_VERSION
    # ISO 8601 date
    assert len(a["generated_date"]) == 10
    assert a["generated_date"].count("-") == 2


# ────────────────── is_valid_response ──────────────────


def test_is_valid_response_clean(merger, valid_response_payload):
    assert merger.is_valid_response(valid_response_payload) is True


def test_is_valid_response_rejects_missing_parsed(merger):
    assert merger.is_valid_response({"slug": "x", "parsed": None}) is False


def test_is_valid_response_rejects_with_issues(merger, valid_response_payload):
    valid_response_payload["issues"] = ["fa: missing or empty"]
    assert merger.is_valid_response(valid_response_payload) is False


def test_is_valid_response_rejects_empty_glosses(merger):
    payload = {"slug": "x", "parsed": {"glosses": {}}, "issues": []}
    assert merger.is_valid_response(payload) is False


def test_is_valid_response_rejects_no_glosses_field(merger):
    payload = {"slug": "x", "parsed": {}, "issues": []}
    assert merger.is_valid_response(payload) is False


# ────────────────── cleaned_glosses ──────────────────


def test_cleaned_glosses_strips_whitespace(merger):
    parsed = {"glosses": {"en": "  to say  ", "fa": "گفتن\n",
                          "tr": " sanki"}}
    out = merger.cleaned_glosses(parsed)
    assert out["en"] == "to say"
    assert out["fa"] == "گفتن"
    assert out["tr"] == "sanki"


def test_cleaned_glosses_handles_none_input(merger):
    assert merger.cleaned_glosses(None) == {}
    assert merger.cleaned_glosses({}) == {}


def test_cleaned_glosses_skips_non_string_values(merger):
    parsed = {"glosses": {"en": "ok", "fa": None, "ur": 42}}
    out = merger.cleaned_glosses(parsed)
    assert out == {"en": "ok"}


# ────────────────── merge_into_page ──────────────────


def test_merge_into_page_writes_translations(merger, tmp_path):
    page_path = tmp_path / "قَالَ.json"
    page_path.write_text(
        json.dumps({"slug": "قَالَ", "translations": None,
                    "frequency_in_corpus": 100}, ensure_ascii=False),
        encoding="utf-8",
    )
    glosses = {"en": "to say", "fa": "گفتن"}
    attribution = merger.make_attribution("qwen36-fast")
    result = merger.merge_into_page(page_path, glosses, attribution,
                                     overwrite=False)
    assert result == "updated"
    after = json.loads(page_path.read_text(encoding="utf-8"))
    assert after["translations"] == glosses
    assert after["translations_attribution"]["model"] == "qwen36-fast"
    assert after["frequency_in_corpus"] == 100  # other fields preserved


def test_merge_into_page_skips_existing_without_overwrite(merger, tmp_path):
    page_path = tmp_path / "p.json"
    existing_trans = {"en": "old translation"}
    page_path.write_text(
        json.dumps({"slug": "p", "translations": existing_trans},
                   ensure_ascii=False),
        encoding="utf-8",
    )
    result = merger.merge_into_page(
        page_path, {"en": "new"}, merger.make_attribution("m"),
        overwrite=False,
    )
    assert result == "skipped_existing"
    after = json.loads(page_path.read_text(encoding="utf-8"))
    assert after["translations"] == existing_trans  # unchanged


def test_merge_into_page_overwrites_when_flag_set(merger, tmp_path):
    page_path = tmp_path / "p.json"
    page_path.write_text(
        json.dumps({"slug": "p", "translations": {"en": "old"}},
                   ensure_ascii=False),
        encoding="utf-8",
    )
    result = merger.merge_into_page(
        page_path, {"en": "new"}, merger.make_attribution("m"),
        overwrite=True,
    )
    assert result == "updated"
    after = json.loads(page_path.read_text(encoding="utf-8"))
    assert after["translations"]["en"] == "new"


def test_merge_into_page_handles_missing_page(merger, tmp_path):
    page_path = tmp_path / "absent.json"
    result = merger.merge_into_page(
        page_path, {"en": "x"}, merger.make_attribution("m"),
        overwrite=False,
    )
    assert result == "missing_page"


# ────────────────── merge_pass ──────────────────


def test_merge_pass_end_to_end(merger, tmp_path):
    """Build a tiny synthetic file tree and run merge_pass."""
    # Layout:
    #   tmp_path/
    #     words/lemmas/قَالَ.json + رُكُوع.json (the page targets)
    #     sources/translation/lemma_responses/قَالَ.json + رُكُوع.json
    words = tmp_path / "words"
    sources = tmp_path / "sources"
    lemmas = words / "lemmas"
    responses = sources / "translation" / "lemma_responses"
    lemmas.mkdir(parents=True)
    responses.mkdir(parents=True)

    for slug in ("قَالَ", "رُكُوع"):
        (lemmas / f"{slug}.json").write_text(
            json.dumps({"slug": slug, "translations": None},
                       ensure_ascii=False),
            encoding="utf-8",
        )
        (responses / f"{slug}.json").write_text(
            json.dumps({
                "slug": slug, "kind": "lemma",
                "parsed": {"glosses": {"en": "test", "fa": "تست"}},
                "issues": [],
            }, ensure_ascii=False),
            encoding="utf-8",
        )

    tally = merger.merge_pass(
        pass_="lemma",
        word_sources_dir=sources,
        words_dir=words,
        round_subdir=None,
        model_name="qwen36-fast",
        overwrite=False,
    )
    assert tally == {"updated": 2, "skipped_existing": 0,
                     "skipped_invalid": 0, "missing_page": 0}

    # Verify the file content
    final = json.loads((lemmas / "قَالَ.json").read_text(encoding="utf-8"))
    assert final["translations"] == {"en": "test", "fa": "تست"}
    assert "translations_attribution" in final


def test_merge_pass_skips_invalid_responses(merger, tmp_path):
    words = tmp_path / "words"
    sources = tmp_path / "sources"
    (words / "lemmas").mkdir(parents=True)
    (sources / "translation" / "lemma_responses").mkdir(parents=True)

    # Page exists but response has issues — should be skipped
    (words / "lemmas" / "x.json").write_text(
        json.dumps({"slug": "x", "translations": None}),
        encoding="utf-8",
    )
    (sources / "translation" / "lemma_responses" / "x.json").write_text(
        json.dumps({
            "slug": "x", "parsed": {"glosses": {"en": "ok"}},
            "issues": ["fa: missing"],
        }),
        encoding="utf-8",
    )

    tally = merger.merge_pass(
        pass_="lemma", word_sources_dir=sources, words_dir=words,
        round_subdir=None, model_name="m", overwrite=False,
    )
    assert tally["skipped_invalid"] == 1
    assert tally["updated"] == 0

    # Page should still have translations=None
    final = json.loads((words / "lemmas" / "x.json").read_text(encoding="utf-8"))
    assert final["translations"] is None
