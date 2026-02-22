"""Tests for the AI translation pipeline."""

import json
import os
import pytest
from unittest.mock import patch, MagicMock

from app.ai_translation import (
    SUPPORTED_LANGUAGES,
    LanguageConfig,
    TranslationRequest,
    TranslationResult,
    estimate_batch_cost,
    extract_verses_from_chapter,
    generate_batch_requests,
    generate_sample_translations,
    ingest_translations,
    make_translator_id,
    make_translator_metadata,
    parse_batch_results,
    walk_book_chapters,
    write_batch_file,
)


# ===================================================================
# LanguageConfig tests
# ===================================================================

class TestLanguageConfig:
    def test_all_tier1_languages_defined(self):
        tier1 = ["ur", "tr", "fa", "id", "bn"]
        for lang in tier1:
            assert lang in SUPPORTED_LANGUAGES, f"Missing Tier 1 language: {lang}"

    def test_all_tier2_languages_defined(self):
        tier2 = ["es", "fr", "de", "ru", "zh"]
        for lang in tier2:
            assert lang in SUPPORTED_LANGUAGES, f"Missing Tier 2 language: {lang}"

    def test_language_config_has_required_fields(self):
        for code, config in SUPPORTED_LANGUAGES.items():
            assert config.code == code
            assert config.name
            assert config.native_name


# ===================================================================
# TranslationRequest tests
# ===================================================================

class TestTranslationRequest:
    def test_to_batch_request_format(self):
        req = TranslationRequest(
            custom_id="al-kafi:1:1:1:1__ur",
            verse_path="/books/al-kafi:1:1:1:1",
            target_lang="ur",
            arabic_text="بسم الله الرحمن الرحيم",
            english_text="In the name of God",
            context="Book: al-kafi, Chapter: Chapter 1",
        )
        batch = req.to_batch_request()

        assert batch["custom_id"] == "al-kafi:1:1:1:1__ur"
        assert "params" in batch
        assert batch["params"]["model"] == "claude-haiku-4-5-20251001"
        assert batch["params"]["max_tokens"] == 2048
        assert "system" in batch["params"]
        assert "Urdu" in batch["params"]["system"]
        assert len(batch["params"]["messages"]) == 1
        assert batch["params"]["messages"][0]["role"] == "user"
        assert "بسم الله" in batch["params"]["messages"][0]["content"]
        assert "In the name of God" in batch["params"]["messages"][0]["content"]

    def test_to_batch_request_without_english(self):
        req = TranslationRequest(
            custom_id="test__tr",
            verse_path="/books/test:1",
            target_lang="tr",
            arabic_text="بسم الله",
            english_text="",
        )
        batch = req.to_batch_request()
        content = batch["params"]["messages"][0]["content"]
        assert "English" not in content

    def test_to_batch_request_unsupported_lang(self):
        req = TranslationRequest(
            custom_id="test__xx",
            verse_path="/books/test:1",
            target_lang="xx",
            arabic_text="test",
            english_text="",
        )
        with pytest.raises(ValueError, match="Unsupported language"):
            req.to_batch_request()


# ===================================================================
# TranslationResult tests
# ===================================================================

class TestTranslationResult:
    def test_parse_custom_id(self):
        path, lang = TranslationResult.parse_custom_id("al-kafi:1:1:1:1__ur")
        assert path == "/books/al-kafi:1:1:1:1"
        assert lang == "ur"

    def test_parse_custom_id_simple(self):
        path, lang = TranslationResult.parse_custom_id("quran:1:1__zh")
        assert path == "/books/quran:1:1"
        assert lang == "zh"

    def test_parse_custom_id_invalid(self):
        with pytest.raises(ValueError, match="Invalid custom_id"):
            TranslationResult.parse_custom_id("no-separator")


# ===================================================================
# Translator metadata tests
# ===================================================================

class TestTranslatorMetadata:
    def test_make_translator_id(self):
        assert make_translator_id("ur") == "ur.ai"
        assert make_translator_id("zh") == "zh.ai"

    def test_make_translator_metadata(self):
        meta = make_translator_metadata("ur")
        assert meta["id"] == "ur.ai"
        assert meta["lang"] == "ur"
        assert meta["ai_generated"] is True
        assert "AI" in meta["disclaimer"]
        assert "Urdu" in meta["name"]

    def test_make_translator_metadata_unsupported(self):
        with pytest.raises(ValueError, match="Unsupported language"):
            make_translator_metadata("xx")


# ===================================================================
# Verse extraction tests
# ===================================================================

class TestVerseExtraction:
    @pytest.fixture
    def chapter_json(self, tmp_path, monkeypatch):
        """Create a test chapter JSON file."""
        dest_dir = tmp_path / "data"
        dest_dir.mkdir()
        monkeypatch.setenv("DESTINATION_DIR", str(dest_dir) + "/")

        books_dir = dest_dir / "books" / "test"
        books_dir.mkdir(parents=True)

        chapter_data = {
            "index": "test:1",
            "kind": "verse_list",
            "data": {
                "path": "/books/test:1",
                "titles": {"en": "Test Chapter"},
                "verse_translations": ["en.test"],
                "verses": [
                    {
                        "path": "/books/test:1:1",
                        "part_type": "Hadith",
                        "text": ["بسم الله الرحمن الرحيم"],
                        "translations": {
                            "en.test": ["In the name of God"]
                        },
                    },
                    {
                        "path": "/books/test:1:2",
                        "part_type": "Hadith",
                        "text": ["الحمد لله رب العالمين"],
                        "translations": {
                            "en.test": ["Praise be to God"]
                        },
                    },
                    {
                        "part_type": "Heading",
                        "text": ["Section header"],
                    },
                ],
            },
        }

        filepath = books_dir / "1.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(chapter_data, f, ensure_ascii=False)

        return dest_dir

    def test_extract_verses(self, chapter_json):
        verses = extract_verses_from_chapter("/books/test:1")
        assert len(verses) == 2
        assert verses[0]["path"] == "/books/test:1:1"
        assert "بسم الله" in verses[0]["arabic_text"]
        assert "In the name of God" in verses[0]["english_text"]
        assert verses[0]["chapter_title"] == "Test Chapter"

    def test_extract_skips_headings(self, chapter_json):
        verses = extract_verses_from_chapter("/books/test:1")
        paths = [v["path"] for v in verses]
        assert len(paths) == 2  # heading excluded

    def test_extract_missing_chapter(self, chapter_json):
        verses = extract_verses_from_chapter("/books/nonexistent:1")
        assert verses == []


# ===================================================================
# Book walking tests
# ===================================================================

class TestWalkBookChapters:
    @pytest.fixture
    def book_hierarchy(self, tmp_path, monkeypatch):
        """Create a test book with nested chapters."""
        dest_dir = tmp_path / "data"
        dest_dir.mkdir()
        monkeypatch.setenv("DESTINATION_DIR", str(dest_dir) + "/")

        books_dir = dest_dir / "books" / "test"
        books_dir.mkdir(parents=True)
        ch1_dir = books_dir / "1"
        ch1_dir.mkdir()

        # Book root with chapters (no verses)
        book_data = {
            "data": {
                "path": "/books/test",
                "chapters": [
                    {"path": "/books/test:1"},
                ],
            },
        }
        with open(books_dir.parent / "test.json", "w", encoding="utf-8") as f:
            json.dump(book_data, f)

        # Chapter with verses (leaf)
        chapter_data = {
            "data": {
                "path": "/books/test:1",
                "verses": [
                    {"path": "/books/test:1:1", "part_type": "Hadith", "text": ["test"]},
                ],
            },
        }
        with open(ch1_dir.with_suffix(".json"), "w", encoding="utf-8") as f:
            json.dump(chapter_data, f)

        return dest_dir

    def test_walk_finds_leaf_chapters(self, book_hierarchy):
        paths = walk_book_chapters("/books/test")
        assert "/books/test:1" in paths

    def test_walk_missing_book(self, book_hierarchy):
        paths = walk_book_chapters("/books/nonexistent")
        assert paths == []


# ===================================================================
# Batch file generation tests
# ===================================================================

class TestBatchFileGeneration:
    def test_write_batch_file(self, tmp_path):
        requests = [
            TranslationRequest(
                custom_id="test:1:1__ur",
                verse_path="/books/test:1:1",
                target_lang="ur",
                arabic_text="بسم الله",
                english_text="In the name of God",
            ),
            TranslationRequest(
                custom_id="test:1:2__ur",
                verse_path="/books/test:1:2",
                target_lang="ur",
                arabic_text="الحمد لله",
                english_text="Praise be to God",
            ),
        ]

        output = str(tmp_path / "batch.jsonl")
        write_batch_file(requests, output)

        with open(output, "r", encoding="utf-8") as f:
            lines = f.readlines()

        assert len(lines) == 2
        first = json.loads(lines[0])
        assert first["custom_id"] == "test:1:1__ur"
        assert "params" in first


# ===================================================================
# Batch results parsing tests
# ===================================================================

class TestBatchResultsParsing:
    def test_parse_success(self, tmp_path):
        results_data = [
            {
                "custom_id": "test:1:1__ur",
                "result": {
                    "type": "succeeded",
                    "message": {
                        "content": [{"text": "اللہ کے نام سے"}]
                    }
                }
            },
        ]
        results_path = str(tmp_path / "results.jsonl")
        with open(results_path, "w", encoding="utf-8") as f:
            for item in results_data:
                json.dump(item, f, ensure_ascii=False)
                f.write("\n")

        results = parse_batch_results(results_path)
        assert len(results) == 1
        assert results[0].success is True
        assert results[0].translated_text == "اللہ کے نام سے"
        assert results[0].verse_path == "/books/test:1:1"
        assert results[0].target_lang == "ur"

    def test_parse_error(self, tmp_path):
        results_data = [
            {
                "custom_id": "test:1:1__ur",
                "result": {
                    "type": "errored",
                    "error": {"message": "Rate limit exceeded"}
                }
            },
        ]
        results_path = str(tmp_path / "results.jsonl")
        with open(results_path, "w", encoding="utf-8") as f:
            for item in results_data:
                json.dump(item, f, ensure_ascii=False)
                f.write("\n")

        results = parse_batch_results(results_path)
        assert len(results) == 1
        assert results[0].success is False
        assert "Rate limit" in results[0].error


# ===================================================================
# Translation ingestion tests
# ===================================================================

class TestIngestion:
    @pytest.fixture
    def chapter_for_ingestion(self, tmp_path, monkeypatch):
        """Set up a chapter file that can be modified by ingestion."""
        dest_dir = tmp_path / "data"
        dest_dir.mkdir()
        monkeypatch.setenv("DESTINATION_DIR", str(dest_dir) + "/")

        books_dir = dest_dir / "books" / "test"
        books_dir.mkdir(parents=True)

        chapter_data = {
            "index": "test:1",
            "kind": "verse_list",
            "data": {
                "path": "/books/test:1",
                "titles": {"en": "Test"},
                "verse_translations": ["en.test"],
                "verses": [
                    {
                        "path": "/books/test:1:1",
                        "part_type": "Hadith",
                        "text": ["بسم الله"],
                        "translations": {"en.test": ["In the name of God"]},
                    },
                ],
            },
        }
        filepath = books_dir / "1.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(chapter_data, f, ensure_ascii=False)

        return dest_dir

    def test_ingest_adds_translation(self, chapter_for_ingestion):
        results = [
            TranslationResult(
                custom_id="test:1:1__ur",
                verse_path="/books/test:1:1",
                target_lang="ur",
                translated_text="اللہ کے نام سے",
            ),
        ]

        counters = ingest_translations(results)
        assert counters["ingested"] == 1
        assert counters["errors"] == 0

        # Verify the file was updated
        from app.lib_db import load_json
        data = load_json("/books/test:1")
        verse = data["data"]["verses"][0]
        assert "ur.ai" in verse["translations"]
        assert verse["translations"]["ur.ai"] == ["اللہ کے نام سے"]
        assert "ur.ai" in data["data"]["verse_translations"]

    def test_ingest_dry_run(self, chapter_for_ingestion):
        results = [
            TranslationResult(
                custom_id="test:1:1__ur",
                verse_path="/books/test:1:1",
                target_lang="ur",
                translated_text="test",
            ),
        ]

        counters = ingest_translations(results, dry_run=True)
        assert counters["ingested"] == 1

        # Verify the file was NOT modified
        from app.lib_db import load_json
        data = load_json("/books/test:1")
        verse = data["data"]["verses"][0]
        assert "ur.ai" not in verse.get("translations", {})

    def test_ingest_skips_failed_results(self, chapter_for_ingestion):
        results = [
            TranslationResult(
                custom_id="test:1:1__ur",
                verse_path="/books/test:1:1",
                target_lang="ur",
                translated_text="",
                success=False,
                error="API error",
            ),
        ]

        counters = ingest_translations(results)
        assert counters["errors"] == 1
        assert counters["ingested"] == 0


# ===================================================================
# Cost estimation tests
# ===================================================================

class TestCostEstimation:
    def test_estimate_returns_all_fields(self):
        requests = [
            TranslationRequest(
                custom_id="test__ur",
                verse_path="/books/test:1",
                target_lang="ur",
                arabic_text="test",
                english_text="test",
            ),
        ] * 100

        cost = estimate_batch_cost(requests)
        assert cost["num_requests"] == 100
        assert "estimated_input_tokens" in cost
        assert "estimated_output_tokens" in cost
        assert "estimated_total_cost_usd" in cost
        assert cost["estimated_total_cost_usd"] >= 0

    def test_estimate_zero_requests(self):
        cost = estimate_batch_cost([])
        assert cost["num_requests"] == 0
        assert cost["estimated_total_cost_usd"] == 0

    def test_estimate_scales_linearly(self):
        req = TranslationRequest(
            custom_id="test__ur",
            verse_path="/books/test:1",
            target_lang="ur",
            arabic_text="test",
            english_text="test",
        )
        cost_1k = estimate_batch_cost([req] * 1000)
        cost_10k = estimate_batch_cost([req] * 10000)
        # Cost should scale ~10x (use large numbers to avoid rounding to 0)
        assert cost_10k["estimated_total_cost_usd"] > 0
        assert cost_10k["estimated_input_tokens"] == cost_1k["estimated_input_tokens"] * 10
        assert cost_10k["estimated_output_tokens"] == cost_1k["estimated_output_tokens"] * 10


# ===================================================================
# Sample translation generation tests
# ===================================================================

class TestSampleTranslations:
    @pytest.fixture
    def sample_book(self, tmp_path, monkeypatch):
        """Set up minimal book data for sample generation."""
        dest_dir = tmp_path / "data"
        dest_dir.mkdir()
        monkeypatch.setenv("DESTINATION_DIR", str(dest_dir) + "/")

        books_dir = dest_dir / "books" / "test"
        books_dir.mkdir(parents=True)

        # Book root
        book_data = {
            "data": {
                "path": "/books/test",
                "chapters": [{"path": "/books/test:1"}],
            },
        }
        with open(books_dir.parent / "test.json", "w", encoding="utf-8") as f:
            json.dump(book_data, f)

        # Chapter with 3 verses
        chapter_data = {
            "data": {
                "path": "/books/test:1",
                "titles": {"en": "Chapter 1"},
                "verses": [
                    {
                        "path": f"/books/test:1:{i}",
                        "part_type": "Hadith",
                        "text": [f"Arabic text {i}"],
                        "translations": {"en.test": [f"English text {i}"]},
                    }
                    for i in range(1, 4)
                ],
            },
        }
        ch_dir = books_dir / "1.json"
        with open(ch_dir, "w", encoding="utf-8") as f:
            json.dump(chapter_data, f, ensure_ascii=False)

        return dest_dir

    def test_generates_correct_count(self, sample_book):
        results = generate_sample_translations("test", ["ur"], count=2)
        assert len(results) == 2

    def test_generates_for_multiple_langs(self, sample_book):
        results = generate_sample_translations("test", ["ur", "tr"], count=2)
        assert len(results) == 4  # 2 verses * 2 langs
        langs = set(r.target_lang for r in results)
        assert langs == {"ur", "tr"}

    def test_sample_has_content(self, sample_book):
        results = generate_sample_translations("test", ["ur"], count=1)
        assert len(results) == 1
        assert results[0].translated_text
        assert results[0].verse_path.startswith("/books/")
