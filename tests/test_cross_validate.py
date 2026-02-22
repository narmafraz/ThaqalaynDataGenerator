"""Tests for cross_validate.py cross-validation pipeline."""
import json
import os

import pytest

from app.arabic_normalization import ComparisonTier
from app.cross_validate import (
    _extract_arabic_from_thaqalayn_net,
    _get_arabic_texts_from_generated,
    cross_validate_all_kafi,
    validate_chapter,
)


class TestExtractArabicFromThaqalaynNet:
    """Tests for parsing thaqalayn.net chapter HTML."""

    def test_extracts_single_hadith(self, tmp_path):
        html = (
            '<body></body>'
            '<hr>'
            '<p dir="rtl">محمد بن يحيى عن أحمد بن محمد</p>'
            '<p>Muhammad ibn Yahya narrated from Ahmad</p>'
        )
        f = tmp_path / "test.html"
        f.write_text(html, encoding="utf-8")
        result = _extract_arabic_from_thaqalayn_net(str(f))
        assert len(result) == 1
        assert "محمد بن يحيى" in result[0]

    def test_extracts_multiple_hadiths(self, tmp_path):
        html = (
            '<body></body>'
            '<hr>'
            '<p dir="rtl">حديث أول</p>'
            '<p>First hadith</p>'
            '<hr>'
            '<p dir="rtl">حديث ثاني</p>'
            '<p>Second hadith</p>'
        )
        f = tmp_path / "test.html"
        f.write_text(html, encoding="utf-8")
        result = _extract_arabic_from_thaqalayn_net(str(f))
        assert len(result) == 2
        assert "حديث أول" in result[0]
        assert "حديث ثاني" in result[1]

    def test_joins_multi_paragraph_arabic(self, tmp_path):
        html = (
            '<body></body>'
            '<hr>'
            '<p dir="rtl">سطر أول</p>'
            '<p dir="rtl">سطر ثاني</p>'
            '<p>English translation</p>'
        )
        f = tmp_path / "test.html"
        f.write_text(html, encoding="utf-8")
        result = _extract_arabic_from_thaqalayn_net(str(f))
        assert len(result) == 1
        assert "سطر أول" in result[0]
        assert "سطر ثاني" in result[0]

    def test_skips_body_sections(self, tmp_path):
        html = (
            '<body><p dir="rtl">not a hadith</p></body>'
            '<hr>'
            '<p dir="rtl">actual hadith</p>'
            '<p>translation</p>'
        )
        f = tmp_path / "test.html"
        f.write_text(html, encoding="utf-8")
        result = _extract_arabic_from_thaqalayn_net(str(f))
        assert len(result) == 1
        assert "actual hadith" in result[0]

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.html"
        f.write_text("", encoding="utf-8")
        result = _extract_arabic_from_thaqalayn_net(str(f))
        assert result == []

    def test_no_rtl_paragraphs(self, tmp_path):
        html = '<hr><p>Only English text</p>'
        f = tmp_path / "test.html"
        f.write_text(html, encoding="utf-8")
        result = _extract_arabic_from_thaqalayn_net(str(f))
        assert result == []


class TestGetArabicTextsFromGenerated:
    """Tests for extracting Arabic from generated chapter data."""

    def test_extracts_verse_texts(self):
        data = {
            "verses": [
                {"text": ["حديث أول"], "part_type": "Hadith"},
                {"text": ["حديث ثاني"], "part_type": "Hadith"},
            ]
        }
        result = _get_arabic_texts_from_generated(data)
        assert len(result) == 2
        assert result[0] == "حديث أول"

    def test_includes_narrator_chain(self):
        data = {
            "verses": [
                {
                    "text": ["قال الإمام"],
                    "part_type": "Hadith",
                    "narrator_chain": {"text": "محمد بن يحيى عن أحمد"},
                },
            ]
        }
        result = _get_arabic_texts_from_generated(data)
        assert len(result) == 1
        assert "محمد بن يحيى" in result[0]
        assert "قال الإمام" in result[0]

    def test_skips_heading_verses(self):
        data = {
            "verses": [
                {"text": ["عنوان"], "part_type": "Heading"},
                {"text": ["حديث"], "part_type": "Hadith"},
            ]
        }
        result = _get_arabic_texts_from_generated(data)
        assert len(result) == 1
        assert result[0] == "حديث"

    def test_empty_verses(self):
        result = _get_arabic_texts_from_generated({"verses": []})
        assert result == []

    def test_missing_text_and_chain(self):
        data = {"verses": [{"part_type": "Hadith"}]}
        result = _get_arabic_texts_from_generated(data)
        assert result == []

    def test_joins_multi_text(self):
        data = {
            "verses": [
                {"text": ["line1", "line2"], "part_type": "Hadith"},
            ]
        }
        result = _get_arabic_texts_from_generated(data)
        assert result[0] == "line1 line2"


class TestValidateChapter:
    """Tests for validate_chapter() with mocked data."""

    def test_returns_none_when_no_generated_data(self, monkeypatch):
        monkeypatch.setattr(
            "app.cross_validate._load_generated_chapter", lambda p: None
        )
        result = validate_chapter(1, 1, 1)
        assert result is None

    def test_returns_none_when_no_thaqalayn_file(self, monkeypatch):
        monkeypatch.setattr(
            "app.cross_validate._load_generated_chapter",
            lambda p: {"verses": [{"text": ["test"], "part_type": "Hadith"}]},
        )
        monkeypatch.setattr(
            "app.cross_validate._find_thaqalayn_net_file", lambda v, b, c: None
        )
        result = validate_chapter(1, 1, 1)
        assert result is None

    def test_compares_matching_texts(self, monkeypatch, tmp_path):
        # Mock generated chapter
        monkeypatch.setattr(
            "app.cross_validate._load_generated_chapter",
            lambda p: {
                "verses": [
                    {"text": ["محمد بن يحيى"], "part_type": "Hadith"},
                ]
            },
        )

        # Create a real thaqalayn.net file
        html = (
            '<body></body>'
            '<hr>'
            '<p dir="rtl">محمد بن يحيى</p>'
            '<p>Translation</p>'
        )
        html_file = tmp_path / "test.html"
        html_file.write_text(html, encoding="utf-8")
        monkeypatch.setattr(
            "app.cross_validate._find_thaqalayn_net_file",
            lambda v, b, c: str(html_file),
        )

        report = validate_chapter(1, 1, 1)
        assert report is not None
        assert report.total == 1
        assert report.exact_count == 1

    def test_detects_substantive_differences(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            "app.cross_validate._load_generated_chapter",
            lambda p: {
                "verses": [
                    {"text": ["كتاب العقل والجهل"], "part_type": "Hadith"},
                ]
            },
        )

        html = (
            '<body></body>'
            '<hr>'
            '<p dir="rtl">كتاب الفقه والأحكام</p>'
            '<p>Translation</p>'
        )
        html_file = tmp_path / "test.html"
        html_file.write_text(html, encoding="utf-8")
        monkeypatch.setattr(
            "app.cross_validate._find_thaqalayn_net_file",
            lambda v, b, c: str(html_file),
        )

        report = validate_chapter(1, 1, 1)
        assert report is not None
        assert report.substantive_count == 1


class TestCrossValidateAllKafi:
    """Tests for full cross-validation pipeline."""

    def test_writes_summary_file(self, tmp_path, monkeypatch):
        """Verify summary.json is created even with no chapters."""
        dest_dir = str(tmp_path / "data") + "/"
        monkeypatch.setenv("DESTINATION_DIR", dest_dir)
        os.makedirs(dest_dir, exist_ok=True)

        # Point to empty chapter dir
        monkeypatch.setattr(
            "app.cross_validate.THAQALAYN_NET_CHAPTER_DIR",
            str(tmp_path / "empty_chapters"),
        )
        os.makedirs(tmp_path / "empty_chapters", exist_ok=True)

        totals = cross_validate_all_kafi()
        assert totals["chapters_compared"] == 0

        summary_path = os.path.join(
            dest_dir, "validation", "cross-validation", "summary.json"
        )
        assert os.path.exists(summary_path)

        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
        assert summary["kind"] == "validation_summary"
        assert summary["book"] == "al-kafi"
