"""Unit tests for app.words.spark_translation.

Only covers the pure-Python pieces (schema, prompt builders, validator).
End-to-end Spark calls are exercised by the pilot runner in
`scripts/run_path_b_pilot.py` and the round-by-round logs.
"""
from __future__ import annotations

import asyncio

import pytest

from app.words.spark_translation import (
    LANGUAGES,
    LANG_FULL_NAMES,
    MAX_GLOSS_CHARS,
    MAX_OUTPUT_TOKENS,
    NON_LATIN_LANGS,
    build_lemma_user_message,
    build_schema,
    build_surface_user_message,
    translate_lemma,
    translate_surface,
    validate_translations,
)


# ────────────────────────── language config ──────────────────────────


def test_languages_constant_excludes_arabic() -> None:
    """`ar` is intentionally dropped — the slug IS the canonical Arabic."""
    assert "ar" not in LANGUAGES
    assert len(LANGUAGES) == 11
    # English must be first so it's the natural anchor reference.
    assert LANGUAGES[0] == "en"


def test_lang_full_names_covers_every_language() -> None:
    assert set(LANG_FULL_NAMES.keys()) == set(LANGUAGES)
    for name in LANG_FULL_NAMES.values():
        assert name and isinstance(name, str)


def test_non_latin_langs_subset_of_languages() -> None:
    assert NON_LATIN_LANGS <= set(LANGUAGES)


# ────────────────────────── schema ──────────────────────────


def test_schema_requires_all_11_languages() -> None:
    s = build_schema()
    glosses = s["properties"]["glosses"]
    assert set(glosses["properties"].keys()) == set(LANGUAGES)
    assert set(glosses["required"]) == set(LANGUAGES)
    assert glosses["additionalProperties"] is False
    assert s["additionalProperties"] is False
    # Top-level requires `glosses`
    assert s["required"] == ["glosses"]


def test_schema_each_lang_is_string() -> None:
    s = build_schema()
    for lang, spec in s["properties"]["glosses"]["properties"].items():
        assert spec == {"type": "string"}, f"{lang}: unexpected spec {spec}"


# ────────────────────────── lemma prompt ──────────────────────────


def test_lemma_prompt_includes_required_fields() -> None:
    out = build_lemma_user_message({
        "lemma_ar": "قَالَ",
        "pos": "V",
        "pos_label": "Verb",
        "en_gloss": "to say",
        "lane_body": "He said, spake, talked. The most common verb of speech...",
    })
    assert "قَالَ" in out
    assert "Verb" in out
    assert "to say" in out
    assert "He said" in out
    assert "schema" in out.lower()


def test_lemma_prompt_omits_empty_optional_fields() -> None:
    out = build_lemma_user_message({"lemma_ar": "قَالَ", "pos": "V"})
    assert "قَالَ" in out
    assert "English gloss" not in out
    assert "Lane's Lexicon" not in out


def test_lemma_prompt_truncates_lane_body() -> None:
    huge = "X" * 5000
    out = build_lemma_user_message({"lemma_ar": "قَالَ", "pos": "V", "lane_body": huge})
    # Must contain SOME of the body but not all 5000 chars
    assert "X" * 100 in out
    assert "X" * 5000 not in out


# ────────────────────────── surface prompt ──────────────────────────


def test_surface_prompt_includes_clitic_breakdown_and_lemma_anchor() -> None:
    out = build_surface_user_message({
        "surface_ar": "وَبِالْعَهْدِ",
        "lemma_ar": "عَهْد",
        "pos": "N",
        "pos_label": "Noun",
        "clitic_breakdown": 'proclitics: wa- "and" + bi- "with/by" + al- "the"',
        "lemma_translations": {
            "en": "pact, covenant",
            "fa": "پیمان",
            "ur": "عہد",
            "tr": "söz",
            "id": "perjanjian",
            "bn": "চুক্তি",
            "es": "pacto",
            "fr": "pacte",
            "de": "Bund",
            "ru": "договор",
            "zh": "盟约",
        },
        "en_gloss": "pact, covenant",
        "lane_body": "A compact, covenant, league...",
    })
    assert "وَبِالْعَهْدِ" in out
    assert "عَهْد" in out
    assert "proclitics:" in out
    # Anchor must include some lemma translation
    assert "پیمان" in out or "covenant" in out
    # Stem POS appears
    assert "Noun" in out


def test_surface_prompt_works_without_clitics() -> None:
    out = build_surface_user_message({
        "surface_ar": "قَالَ",
        "lemma_ar": "قَالَ",
        "pos": "V",
        "clitic_breakdown": "",
        "lemma_translations": {"en": "to say"},
    })
    assert "Clitic decomposition:" not in out
    assert "قَالَ" in out


def test_surface_prompt_includes_corpus_contexts_when_present() -> None:
    out = build_surface_user_message({
        "surface_ar": "وَقَالَ",
        "lemma_ar": "قَالَ",
        "pos": "V",
        "clitic_breakdown": 'proclitics: wa- "and"',
        "lemma_translations": {"en": "to say"},
        "corpus_contexts": [
            {"path": "/books/al-kafi:1:1:1", "window": "ثم نظر إليه وَقَالَ إن من ربكم"},
            {"path": "/books/al-kafi:1:1:2", "window": "فأقبل عليه وَقَالَ لنا أبو عبد الله"},
        ],
    })
    assert "Corpus usage examples" in out
    assert "إن من ربكم" in out


def test_surface_prompt_caps_corpus_contexts_at_3() -> None:
    contexts = [{"window": f"window-{i}"} for i in range(10)]
    out = build_surface_user_message({
        "surface_ar": "قَالَ",
        "pos": "V",
        "lemma_translations": {"en": "to say"},
        "corpus_contexts": contexts,
    })
    # Only first 3 windows should appear
    for i in range(3):
        assert f"window-{i}" in out
    for i in range(3, 10):
        assert f"window-{i}" not in out


# ────────────────────────── validator ──────────────────────────


@pytest.fixture
def valid_payload() -> dict:
    return {"glosses": {
        "en": "to say",
        "fa": "گفتن",
        "ur": "کہنا",
        "tr": "söylemek",
        "id": "berkata",
        "bn": "বলা",
        "es": "decir",
        "fr": "dire",
        "de": "sagen",
        "ru": "сказать",
        "zh": "说",
    }}


def test_validate_clean_payload(valid_payload) -> None:
    assert validate_translations(valid_payload) == []


def test_validate_flags_missing_lang(valid_payload) -> None:
    del valid_payload["glosses"]["fa"]
    issues = validate_translations(valid_payload)
    assert any("fa" in i and "missing" in i for i in issues)


def test_validate_flags_empty_string(valid_payload) -> None:
    valid_payload["glosses"]["tr"] = ""
    issues = validate_translations(valid_payload)
    assert any("tr" in i and "empty" in i for i in issues)


def test_validate_flags_too_long_gloss(valid_payload) -> None:
    valid_payload["glosses"]["en"] = "x" * (MAX_GLOSS_CHARS + 1)
    issues = validate_translations(valid_payload)
    assert any("en" in i and "exceeds" in i for i in issues)


def test_validate_flags_latin_letters_in_non_latin_lang(valid_payload) -> None:
    valid_payload["glosses"]["fa"] = "goftan"  # romanized, should be گفتن
    issues = validate_translations(valid_payload)
    assert any("fa" in i and "Latin" in i for i in issues)


def test_validate_allows_digits_in_non_latin_lang(valid_payload) -> None:
    # e.g. "100 ریال" — digits are not Latin letters
    valid_payload["glosses"]["fa"] = "گفتن (1)"
    assert validate_translations(valid_payload) == []


def test_validate_rejects_non_dict() -> None:
    assert validate_translations(None) != []  # type: ignore[arg-type]
    assert validate_translations({}) != []
    assert validate_translations({"glosses": "not a dict"}) != []


# ────────────────────────── retry + concurrency wiring ──────────────────────────


def test_max_output_tokens_is_within_qwen_safe_band() -> None:
    """The doc claims ~300 to avoid the }-loop pathology. Lock that constant."""
    assert MAX_OUTPUT_TOKENS == 300


def test_translate_lemma_with_mocked_backend(monkeypatch) -> None:
    """Smoke-test the call wrapper with a fake `call_openai`."""
    async def fake_call_openai(system, user, **kw):
        return {
            "result": '{"glosses": {"en":"to say","fa":"گفتن","ur":"کہنا","tr":"söylemek","id":"berkata","bn":"বলা","es":"decir","fr":"dire","de":"sagen","ru":"сказать","zh":"说"}}',
            "elapsed": 1.2, "input_tokens": 200, "output_tokens": 220,
            "model": kw.get("model", "qwen36-fast"), "backend": "spark",
        }

    monkeypatch.setattr(
        "app.pipeline_cli.openai_backend.call_openai", fake_call_openai
    )

    result = asyncio.run(translate_lemma({"lemma_ar": "قَالَ", "pos": "V"}))
    assert result["kind"] == "lemma"
    assert result["parsed"] is not None
    assert result["issues"] == []
    assert result["parsed"]["glosses"]["en"] == "to say"
    assert result["meta"]["backend"] == "spark"


def test_translate_lemma_records_parse_failure(monkeypatch) -> None:
    """Garbage JSON → issues populated, parsed None, raw preserved."""
    async def fake_call_openai(system, user, **kw):
        return {
            "result": "{not even close to JSON",
            "elapsed": 0.5, "input_tokens": 10, "output_tokens": 5,
            "model": "qwen36-fast", "backend": "spark",
        }

    monkeypatch.setattr(
        "app.pipeline_cli.openai_backend.call_openai", fake_call_openai
    )

    result = asyncio.run(translate_lemma({"lemma_ar": "قَالَ", "pos": "V"}))
    assert result["parsed"] is None
    assert any("parse_error" in i for i in result["issues"])
    assert result["raw"] == "{not even close to JSON"


def test_translate_surface_with_mocked_backend(monkeypatch) -> None:
    async def fake_call_openai(system, user, **kw):
        return {
            "result": '{"glosses": {"en":"and by the covenant","fa":"و با پیمان","ur":"اور عہد کے ذریعے","tr":"ve sözleşme ile","id":"dan dengan perjanjian","bn":"এবং চুক্তির মাধ্যমে","es":"y por el pacto","fr":"et par le pacte","de":"und durch den Bund","ru":"и по договору","zh":"并以盟约"}}',
            "elapsed": 1.5, "input_tokens": 300, "output_tokens": 250,
            "model": "qwen36-fast", "backend": "spark",
        }

    monkeypatch.setattr(
        "app.pipeline_cli.openai_backend.call_openai", fake_call_openai
    )

    result = asyncio.run(translate_surface({
        "surface_ar": "وَبِالْعَهْدِ",
        "lemma_ar": "عَهْد",
        "pos": "N",
        "clitic_breakdown": 'proclitics: wa- "and" + bi- "with/by" + al- "the"',
        "lemma_translations": {"en": "pact, covenant"},
    }))
    assert result["kind"] == "surface"
    assert result["parsed"]["glosses"]["en"] == "and by the covenant"
    assert result["issues"] == []
