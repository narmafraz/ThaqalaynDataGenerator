"""Tests for kafi_sarwar.py parser utility functions.

Tests pure functions: we_dont_care, sitepath_from_filepath,
add_chapter_content (with minimal HTML fixtures).
"""
import os
import pytest

from app.kafi_sarwar import (
    MIN_ARABIC_SIMILARITY,
    SARWAR_TRANSLATION_ID,
    arabic_similarity,
    extract_hadith_number,
    V8_HADITH_CUMSUM,
    sitepath_from_filepath,
    we_dont_care,
)
from app.lib_model import ProcessingReport
from app.models import Chapter, PartType, Verse


class TestSarwarWeDontCare:
    """Tests for kafi_sarwar.we_dont_care()."""

    def test_body_tag_returns_true(self):
        assert we_dont_care("<body>") is True

    def test_body_close_tag_returns_true(self):
        assert we_dont_care("</body>") is True

    def test_body_in_content(self):
        assert we_dont_care("<html><body><p>text</p>") is True

    def test_normal_html_returns_false(self):
        assert we_dont_care("<p>Some hadith text</p>") is False

    def test_empty_string_returns_false(self):
        assert we_dont_care("") is False


class TestSitepathFromFilepath:
    """Tests for sitepath_from_filepath()."""

    def test_converts_forward_slash_path(self):
        result = sitepath_from_filepath("/data/thaqalayn_net/chapter/1/2/3/4.html")
        assert result == "1/2/3/4"

    def test_converts_backslash_path(self):
        result = sitepath_from_filepath(
            "C:\\data\\thaqalayn_net\\chapter\\1\\2\\3\\4.html"
        )
        assert result == "1/2/3/4"

    def test_strips_html_extension(self):
        result = sitepath_from_filepath("/some/path/chapter/1/2/3.html")
        assert not result.endswith(".html")

    def test_single_level_path(self):
        result = sitepath_from_filepath("/data/chapter/1.html")
        assert result == "1"


class TestSarwarConstants:
    """Tests for kafi_sarwar.py constants."""

    def test_sarwar_translation_id(self):
        assert SARWAR_TRANSLATION_ID == "en.sarwar"

    def test_v8_hadith_cumsum_length(self):
        # Volume 8 has 52 chapters
        assert len(V8_HADITH_CUMSUM) == 52

    def test_v8_hadith_cumsum_is_monotonically_increasing(self):
        for i in range(1, len(V8_HADITH_CUMSUM)):
            assert V8_HADITH_CUMSUM[i] >= V8_HADITH_CUMSUM[i - 1], (
                f"V8_HADITH_CUMSUM is not monotonically increasing at index {i}: "
                f"{V8_HADITH_CUMSUM[i-1]} -> {V8_HADITH_CUMSUM[i]}"
            )

    def test_v8_hadith_cumsum_starts_at_1(self):
        assert V8_HADITH_CUMSUM[0] == 1

    def test_v8_hadith_cumsum_ends_at_597(self):
        assert V8_HADITH_CUMSUM[-1] == 597


class TestAddChapterContent:
    """Tests for kafi_sarwar.add_chapter_content() with minimal fixtures."""

    def test_skips_zero_file(self, tmp_path):
        """Files ending in /0.html should be skipped."""
        from app.kafi_sarwar import add_chapter_content

        chapter = Chapter()
        chapter.path = "/books/al-kafi:1:1:1"
        chapter.titles = {"en": "Test", "ar": "اختبار"}
        chapter.verses = []
        chapter.verse_translations = ["en.hubeali"]

        zero_file = tmp_path / "0.html"
        zero_file.write_text("<p>content</p>", encoding="utf-8")

        report = ProcessingReport()
        add_chapter_content(chapter, str(zero_file), report=report)
        assert len(chapter.verses) == 0
        assert len(report.sequence_errors) > 0

    def test_adds_sarwar_translation_id(self, tmp_path):
        """Should add sarwar translation ID to verse_translations."""
        from app.kafi_sarwar import add_chapter_content

        # The parser splits on <hr>, then for each hadith section:
        # - finds all <p> tags
        # - reads RTL paragraphs (Arabic) while is_rtl_tag
        # - reads next paragraph (English translation)
        # is_rtl_tag checks for dir="rtl" attribute
        # First segment must contain <body> so we_dont_care skips it
        hadith_html = (
            '<body></body>'
            '<hr>'
            '<p dir="rtl">محمد بن يحيى</p>'
            '<p>Muhammad ibn Yahya narrated...</p>'
            '<p>&nbsp;</p><p>&nbsp;</p><p>&nbsp;</p>'
        )

        filepath = str(tmp_path / "chapter" / "1" / "2" / "3" / "1.html")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(hadith_html)

        # Create a chapter with matching verses
        chapter = Chapter()
        chapter.path = "/books/al-kafi:1:1:1"
        chapter.titles = {"en": "Test Chapter", "ar": "باب"}
        chapter.crumbs = []
        chapter.verse_translations = ["en.hubeali"]

        verse = Verse()
        verse.part_type = PartType.Hadith
        verse.text = ["محمد بن يحيى"]
        verse.translations = {"en.hubeali": ["English hubeali"]}
        chapter.verses = [verse]

        report = ProcessingReport()
        add_chapter_content(chapter, filepath, report=report)
        assert SARWAR_TRANSLATION_ID in chapter.verse_translations

    def test_doesnt_duplicate_sarwar_translation_id(self, tmp_path):
        """Should not add sarwar translation ID if already present."""
        from app.kafi_sarwar import add_chapter_content

        hadith_html = (
            '<body></body>'
            '<hr>'
            '<p dir="rtl">محمد بن يحيى</p>'
            '<p>Muhammad ibn Yahya narrated...</p>'
            '<p>&nbsp;</p><p>&nbsp;</p><p>&nbsp;</p>'
        )

        filepath = str(tmp_path / "chapter" / "1" / "2" / "3" / "1.html")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(hadith_html)

        chapter = Chapter()
        chapter.path = "/books/al-kafi:1:1:1"
        chapter.titles = {"en": "Test"}
        chapter.crumbs = []
        chapter.verse_translations = ["en.hubeali", SARWAR_TRANSLATION_ID]

        verse = Verse()
        verse.part_type = PartType.Hadith
        verse.text = ["محمد بن يحيى"]
        verse.translations = {"en.hubeali": ["English"]}
        chapter.verses = [verse]

        report = ProcessingReport()
        add_chapter_content(chapter, filepath, report=report)
        sarwar_count = chapter.verse_translations.count(SARWAR_TRANSLATION_ID)
        assert sarwar_count == 1


class TestHadithNumberPrefix:
    def test_extracts_common_forms(self):
        assert extract_hadith_number("5. Ali has narrated") == 5
        assert extract_hadith_number(" 12. text") == 12
        assert extract_hadith_number("3- text") == 3
        assert extract_hadith_number("7: text") == 7

    def test_none_for_unnumbered(self):
        assert extract_hadith_number("The Holy Prophet was born on") is None
        assert extract_hadith_number("") is None
        assert extract_hadith_number(None) is None


class TestArabicSimilarity:
    def test_same_hadith_across_orthography(self):
        # HubeAli diacritized vs thaqalayn.net plain: same hadith, high overlap.
        a = ["مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ عَنِ الْحَجَّالِ عَنْ حَمَّادٍ قَالَ سَمِعْتُ أَبَا عَبْدِ اللَّهِ"]
        b = ["محمد بن يحيى عن احمد بن محمد عن الحجال عن حماد قال سمعت ابا عبد الله"]
        assert arabic_similarity(a, b) >= 0.8

    def test_different_hadith_low(self):
        a = ["ولد النبي صلى الله عليه وآله في الثاني عشر من شهر ربيع الاول في عام الفيل يوم الجمعة"]
        b = ["قلت لابي عبد الله عليه السلام كان رسول الله يختم القرآن في شهر رمضان مرة واحدة او اكثر"]
        assert 0 <= arabic_similarity(a, b) < MIN_ARABIC_SIMILARITY

    def test_not_comparable_when_one_side_empty(self):
        assert arabic_similarity([], ["نص عربي هنا"]) == -1.0
        assert arabic_similarity(["نص عربي هنا"], None) == -1.0


def _make_chapter(arabic_texts):
    from app.models import Chapter, PartType, Verse
    chapter = Chapter()
    chapter.path = "/books/al-kafi:1:4:111"
    chapter.titles = {"en": "Test", "ar": "باب"}
    chapter.crumbs = []
    chapter.verse_translations = ["en.hubeali"]
    chapter.verses = []
    for t in arabic_texts:
        v = Verse()
        v.part_type = PartType.Hadith
        v.text = [t]
        v.translations = {}
        chapter.verses.append(v)
    return chapter


def _write_sections(tmp_path, sections):
    html = "<body></body>"
    for ar, en in sections:
        html += f'<hr><p dir="rtl">{ar}</p><p>{en}</p><p>&nbsp;</p><p>&nbsp;</p><p>&nbsp;</p>'
    filepath = str(tmp_path / "chapter" / "1" / "4" / "111.html")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)
    return filepath


class TestPreambleOffsetFix:
    """The N8 bug: an unnumbered preamble section used to shift every
    following hadith's translation by one."""

    AR1 = "محمد بن يحيى عن احمد بن محمد عن ابن فضال عن عبد الله بن محمد قال ولد النبي في مكة"
    AR2 = "محمد بن يحيى عن احمد بن محمد عن الحجال عن حماد قال سمعت ابا عبد الله يقول في المدينة"

    def test_preamble_skipped_and_numbers_align(self, tmp_path):
        from app.kafi_sarwar import add_chapter_content
        from app.lib_model import ProcessingReport

        chapter = _make_chapter([self.AR1, self.AR2])
        filepath = _write_sections(tmp_path, [
            ("ولد النبي صلى الله عليه وآله في عام الفيل", "The Holy Prophet was born in the year of the Elephant"),
            (self.AR1, "1. Muhammad ibn Yahya from ibn Faddal: born in Mecca"),
            (self.AR2, "2. Muhammad ibn Yahya from al-Hajjal from Hammad: in Medina"),
        ])
        report = ProcessingReport()
        add_chapter_content(chapter, filepath, report=report)
        assert chapter.verses[0].translations[SARWAR_TRANSLATION_ID][0].startswith("1.")
        assert chapter.verses[1].translations[SARWAR_TRANSLATION_ID][0].startswith("2.")
        # preamble logged, not attached
        assert any("preamble" in e for e in report.sequence_errors)

    def test_mismatched_content_not_attached(self, tmp_path):
        from app.kafi_sarwar import add_chapter_content
        from app.lib_model import ProcessingReport

        # Section numbered 1 whose Arabic is UNRELATED text (no shared isnad)
        # - e.g. a preamble mislabeled with a number, or wrong-chapter drift.
        # (Adjacent hadith sharing a chain stay above the floor by design; the
        # number prefix, not similarity, is what fixes adjacency.)
        chapter = _make_chapter([self.AR1])
        filepath = _write_sections(tmp_path, [
            ("ولد النبي صلى الله عليه وآله في الثاني عشر من شهر ربيع الاول في عام الفيل يوم الجمعة مع الزوال",
             "1. Something translated"),
        ])
        report = ProcessingReport()
        add_chapter_content(chapter, filepath, report=report)
        assert SARWAR_TRANSLATION_ID not in chapter.verses[0].translations
        assert any("NOT attached" in e for e in report.sequence_errors)

    def test_positional_fallback_when_numbering_not_from_one(self, tmp_path):
        from app.kafi_sarwar import add_chapter_content
        from app.lib_model import ProcessingReport

        # Volume-8 style: prefixes are volume-global (start > 1) -> positional.
        chapter = _make_chapter([self.AR1])
        filepath = _write_sections(tmp_path, [
            (self.AR1, "313. Volume-global numbered hadith text"),
        ])
        report = ProcessingReport()
        add_chapter_content(chapter, filepath, report=report)
        assert chapter.verses[0].translations[SARWAR_TRANSLATION_ID][0].startswith("313.")


    def test_unnumbered_first_hadith_attaches_numbering_starts_at_two(self, tmp_path):
        """1:4:117 class: hadith 1 is an unnumbered poetry report, so the
        chapter's numbering starts at "2.". The unnumbered section must attach
        to verse 1 (similarity-gated), and "2." to verse 2 - the old fallback
        shifted everything by one."""
        from app.kafi_sarwar import add_chapter_content
        from app.lib_model import ProcessingReport

        chapter = _make_chapter([self.AR1, self.AR2])
        filepath = _write_sections(tmp_path, [
            ("ولد النبي في عام الفيل",
             "A preamble note about the birth year"),  # matches nothing -> skipped
            (self.AR1, "Unnumbered first hadith: poetry couplet translated"),
            (self.AR2, "2. Muhammad ibn Yahya from al-Hajjal from Hammad: in Medina"),
        ])
        report = ProcessingReport()
        add_chapter_content(chapter, filepath, report=report)
        assert chapter.verses[0].translations[SARWAR_TRANSLATION_ID][0].startswith("Unnumbered first")
        assert chapter.verses[1].translations[SARWAR_TRANSLATION_ID][0].startswith("2.")
        assert any("preamble" in e for e in report.sequence_errors)
