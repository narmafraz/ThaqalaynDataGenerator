"""Tests for the WikiShia MediaWiki API scraper.

These tests use mock data and do not make real network requests.
"""

import json
import pytest
from unittest.mock import patch, MagicMock

from app.wikishia.scraper import (
    BiographyData,
    WikiShiaScraper,
    _extract_birth_date,
    _extract_death_date,
    _extract_era,
    _extract_reliability,
    _extract_summary,
    _extract_teachers,
    _extract_students,
    _extract_infobox_field,
    _strip_templates,
)


# === Sample wikitext for testing ===

SAMPLE_WIKITEXT_KULAYNI = """{{Infobox scholar
| Name = Muhammad ibn Ya'qub al-Kulayni
| Birth = ~250 AH/864 CE
| Death = 329 AH/941 CE
| Era = Early Islamic period
| Reliability = Thiqah
| Teachers = Ali ibn Ibrahim al-Qummi, Muhammad ibn Yahya al-Attar
| Students = Ahmad ibn Ahmad al-Kufi, Ibn Abi Rafi'
}}

'''Muhammad ibn Ya'qub ibn Ishaq al-Kulayni al-Razi''' (Arabic: محمد بن يعقوب بن إسحاق الكليني الرازي) (d. [[329 AH]]/[[941 CE]]) was one of the most prominent Shia hadith scholars and the compiler of ''[[al-Kafi]]'', the most authoritative Shia hadith collection.

== Life ==
Al-Kulayni was born in the village of Kulayn near [[Ray, Iran|Ray]]. He traveled to various cities to collect hadith, eventually settling in [[Baghdad]].

== Works ==
His magnum opus, ''al-Kafi'', took approximately twenty years to compile and contains 16,199 hadiths organized into three sections.

== See also ==
* [[Al-Kafi]]

[[Category:Shia hadith scholars]]
[[Category:329 AH deaths]]
"""

SAMPLE_WIKITEXT_MINIMAL = """'''Test Person''' was a narrator of hadith.

He was known for being trustworthy and reliable in his narrations.
"""

SAMPLE_WIKITEXT_EMPTY_INFOBOX = """{{Infobox scholar
| Name = Unknown Scholar
| Birth =
| Death = Unknown
}}

A lesser-known scholar.
"""


class TestBiographyData:
    """Test BiographyData serialization."""

    def test_to_dict_full(self):
        bio = BiographyData()
        bio.title = "Test"
        bio.birth_date = "250 AH"
        bio.death_date = "329 AH"
        bio.era = "Early Islamic"
        bio.reliability = "Thiqah"
        bio.teachers = ["Teacher 1", "Teacher 2"]
        bio.students = ["Student 1"]
        bio.biography_summary = "A great scholar."
        bio.wikishia_url = "https://en.wikishia.net/view/Test"

        d = bio.to_dict()
        assert d["birth_date"] == "250 AH"
        assert d["death_date"] == "329 AH"
        assert d["era"] == "Early Islamic"
        assert d["reliability"] == "Thiqah"
        assert len(d["teachers"]) == 2
        assert len(d["students"]) == 1
        assert d["biography_summary"] == "A great scholar."
        assert d["biography_source"] == "WikiShia"

    def test_to_dict_minimal(self):
        bio = BiographyData()
        bio.title = "Test"

        d = bio.to_dict()
        assert d["biography_source"] == "WikiShia"
        assert "birth_date" not in d
        assert "teachers" not in d

    def test_to_dict_excludes_none_and_empty(self):
        bio = BiographyData()
        bio.title = "Test"
        bio.birth_date = None
        bio.teachers = []

        d = bio.to_dict()
        assert "birth_date" not in d
        assert "teachers" not in d


class TestStripTemplates:
    """Test wikitext template stripping."""

    def test_simple_template(self):
        text = "{{template}}content"
        assert _strip_templates(text) == "content"

    def test_nested_templates(self):
        text = "{{outer {{inner}} end}}after"
        assert _strip_templates(text) == "after"

    def test_no_templates(self):
        text = "plain text"
        assert _strip_templates(text) == "plain text"

    def test_multiple_templates(self):
        text = "before{{a}}middle{{b}}after"
        result = _strip_templates(text)
        assert result == "beforemiddleafter"


class TestExtractInfoboxField:
    """Test infobox field extraction from wikitext."""

    def test_simple_field(self):
        wikitext = "| Birth = 250 AH\n| Death = 329 AH\n}}"
        result = _extract_infobox_field(wikitext, "Birth")
        assert result == "250 AH"

    def test_field_with_wiki_links(self):
        wikitext = "| Birth = [[250 AH|250 AH]]\n| Death = 329\n}}"
        result = _extract_infobox_field(wikitext, "Birth")
        assert result == "250 AH"

    def test_missing_field(self):
        wikitext = "| Name = Test\n}}"
        result = _extract_infobox_field(wikitext, "Birth")
        assert result is None

    def test_empty_field(self):
        wikitext = "| Birth = \n| Death = 329\n}}"
        result = _extract_infobox_field(wikitext, "Birth")
        assert result is None

    def test_unknown_value(self):
        wikitext = "| Birth = Unknown\n}}"
        result = _extract_infobox_field(wikitext, "Birth")
        assert result is None


class TestExtractBirthDate:
    """Test birth date extraction."""

    def test_birth_field(self):
        result = _extract_birth_date(SAMPLE_WIKITEXT_KULAYNI)
        assert result is not None
        assert "250" in result

    def test_no_birth(self):
        result = _extract_birth_date(SAMPLE_WIKITEXT_MINIMAL)
        assert result is None


class TestExtractDeathDate:
    """Test death date extraction."""

    def test_death_field(self):
        result = _extract_death_date(SAMPLE_WIKITEXT_KULAYNI)
        assert result is not None
        assert "329" in result

    def test_unknown_death(self):
        result = _extract_death_date(SAMPLE_WIKITEXT_EMPTY_INFOBOX)
        assert result is None  # "Unknown" is filtered out


class TestExtractEra:
    """Test era extraction."""

    def test_era_field(self):
        result = _extract_era(SAMPLE_WIKITEXT_KULAYNI)
        assert result is not None
        assert "Islamic" in result

    def test_no_era(self):
        result = _extract_era(SAMPLE_WIKITEXT_MINIMAL)
        assert result is None


class TestExtractReliability:
    """Test reliability extraction."""

    def test_reliability_from_infobox(self):
        result = _extract_reliability(SAMPLE_WIKITEXT_KULAYNI)
        assert result is not None

    def test_reliability_from_text(self):
        """Detect reliability from body text mentions."""
        wikitext = "He was considered thiqah by most rijal scholars.\n\n"
        result = _extract_reliability(wikitext)
        assert result is not None
        assert "Trustworthy" in result

    def test_no_reliability(self):
        wikitext = "A person about whom little is known.\n\n"
        result = _extract_reliability(wikitext)
        assert result is None


class TestExtractTeachers:
    """Test teacher extraction."""

    def test_teachers_from_infobox(self):
        result = _extract_teachers(SAMPLE_WIKITEXT_KULAYNI)
        assert len(result) >= 2
        assert any("Ali" in t for t in result)

    def test_no_teachers(self):
        result = _extract_teachers(SAMPLE_WIKITEXT_MINIMAL)
        assert result == []


class TestExtractStudents:
    """Test student extraction."""

    def test_students_from_infobox(self):
        result = _extract_students(SAMPLE_WIKITEXT_KULAYNI)
        assert len(result) >= 1

    def test_no_students(self):
        result = _extract_students(SAMPLE_WIKITEXT_MINIMAL)
        assert result == []


class TestExtractSummary:
    """Test biography summary extraction."""

    def test_summary_from_article(self):
        result = _extract_summary(SAMPLE_WIKITEXT_KULAYNI)
        assert result is not None
        assert len(result) > 50
        assert "Kulayni" in result or "hadith" in result

    def test_summary_from_minimal(self):
        result = _extract_summary(SAMPLE_WIKITEXT_MINIMAL)
        assert result is not None
        # First paragraph is too short (<50 chars), so second paragraph is used
        assert "trustworthy" in result or "narrator" in result

    def test_summary_capped_at_1000_chars(self):
        long_text = "A" * 2000 + "\n\n"
        result = _extract_summary(long_text)
        if result:
            assert len(result) <= 1000


class TestWikiShiaScraper:
    """Test WikiShiaScraper methods with mocked API responses."""

    @patch("app.wikishia.scraper._make_api_request")
    def test_search_narrator(self, mock_request):
        """Test search_narrator returns results from API."""
        mock_request.return_value = {
            "query": {
                "search": [
                    {"title": "Muhammad ibn Ya'qub al-Kulayni", "pageid": 1, "snippet": "..."},
                    {"title": "Al-Kulayni", "pageid": 2, "snippet": "..."},
                ]
            }
        }

        scraper = WikiShiaScraper(delay=0)
        results = scraper.search_narrator("Kulayni")
        assert len(results) == 2
        assert results[0]["title"] == "Muhammad ibn Ya'qub al-Kulayni"

    @patch("app.wikishia.scraper._make_api_request")
    def test_search_narrator_no_results(self, mock_request):
        mock_request.return_value = {"query": {"search": []}}

        scraper = WikiShiaScraper(delay=0)
        results = scraper.search_narrator("nonexistent")
        assert results == []

    @patch("app.wikishia.scraper._make_api_request")
    def test_search_narrator_api_error(self, mock_request):
        mock_request.return_value = None

        scraper = WikiShiaScraper(delay=0)
        results = scraper.search_narrator("test")
        assert results == []

    @patch("app.wikishia.scraper._make_api_request")
    def test_get_page_wikitext(self, mock_request):
        mock_request.return_value = {
            "parse": {
                "wikitext": {"*": SAMPLE_WIKITEXT_KULAYNI}
            }
        }

        scraper = WikiShiaScraper(delay=0)
        text = scraper.get_page_wikitext("Muhammad ibn Ya'qub al-Kulayni")
        assert text is not None
        assert "al-Kafi" in text

    @patch("app.wikishia.scraper._make_api_request")
    def test_get_page_wikitext_not_found(self, mock_request):
        mock_request.return_value = None

        scraper = WikiShiaScraper(delay=0)
        text = scraper.get_page_wikitext("Nonexistent Page")
        assert text is None

    @patch("app.wikishia.scraper._make_api_request")
    def test_get_page_categories(self, mock_request):
        mock_request.return_value = {
            "query": {
                "pages": {
                    "1": {
                        "categories": [
                            {"title": "Category:Shia hadith scholars"},
                            {"title": "Category:329 AH deaths"},
                        ]
                    }
                }
            }
        }

        scraper = WikiShiaScraper(delay=0)
        cats = scraper.get_page_categories("Test")
        assert "Shia hadith scholars" in cats
        assert "329 AH deaths" in cats

    @patch("app.wikishia.scraper._make_api_request")
    def test_get_biography(self, mock_request):
        """Test full biography extraction from mocked wikitext."""
        mock_request.return_value = {
            "parse": {
                "wikitext": {"*": SAMPLE_WIKITEXT_KULAYNI}
            }
        }

        scraper = WikiShiaScraper(delay=0)
        bio = scraper.get_biography("Muhammad ibn Ya'qub al-Kulayni")

        assert bio is not None
        assert bio.title == "Muhammad ibn Ya'qub al-Kulayni"
        assert bio.birth_date is not None
        assert bio.death_date is not None
        assert bio.biography_summary is not None
        assert bio.biography_source == "WikiShia"
        assert "wikishia.net" in bio.wikishia_url

    @patch("app.wikishia.scraper._make_api_request")
    def test_get_biography_not_found(self, mock_request):
        mock_request.return_value = None

        scraper = WikiShiaScraper(delay=0)
        bio = scraper.get_biography("Nonexistent")
        assert bio is None
