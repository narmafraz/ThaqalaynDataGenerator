"""Tests for the ghbook.ir HTML parser (Tahdhib al-Ahkam and al-Istibsar)."""

import pytest
from bs4 import BeautifulSoup

from app.ghbook_parser import (
    FOOTNOTE_SEPARATOR,
    count_babs,
    count_hadiths,
    extract_elements,
    get_heading_level,
    is_footnote_separator,
    is_metadata_line,
    is_non_content,
    is_page_number,
    parse_istibsar,
    parse_tahdhib,
    split_hadith_text_istibsar,
    split_hadith_text_tahdhib,
    _extract_volume_number,
    _is_bab_heading,
    _is_intro_heading,
)
from app.models import Language, PartType


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _make_tahdhib_html(body_content: str) -> str:
    """Wrap body content in a minimal Tahdhib-style HTML document."""
    return f"""<HTML dir=rtl><HEAD></HEAD>
<BODY>
<DIV><DIV>
<H1 class=content_h1>تهذيب الاحكام</H1>
{body_content}
</DIV></DIV>
</BODY></HTML>"""


def _make_istibsar_html(body_content: str) -> str:
    """Wrap body content in a minimal Istibsar-style HTML document."""
    return f"""<HTML dir=rtl><HEAD></HEAD>
<BODY>
<DIV><DIV>
<H1 class=content_h1>الإستبصار</H1>
{body_content}
</DIV></DIV>
</BODY></HTML>"""


def _make_bab_with_hadiths(bab_title, hadiths, heading_tag="H4"):
    """Build HTML for a bab with numbered hadiths (Tahdhib format)."""
    lines = [f'<{heading_tag} class=content_{heading_tag.lower()}>{bab_title}</{heading_tag}>']
    for num, text in hadiths:
        lines.append(
            f'<P class=content_paragraph><SPAN class=content_text>'
            f'({num}) {num} - {text}</SPAN></P>'
        )
    return "\n".join(lines)


def _make_istibsar_bab_with_hadiths(bab_title, hadiths):
    """Build HTML for a bab with numbered hadiths (Istibsar format)."""
    lines = [f'<H6 class=content_h6>{bab_title}</H6>']
    for num, text in hadiths:
        lines.append(
            f'<P class=content_paragraph><SPAN class=content_text>'
            f'{num}- {text}</SPAN></P>'
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tests: heading level detection
# ---------------------------------------------------------------------------

class TestGetHeadingLevel:
    def test_h1(self):
        tag = BeautifulSoup('<H1 class=content_h1>title</H1>', 'html.parser').find('h1')
        assert get_heading_level(tag) == 1

    def test_h6(self):
        tag = BeautifulSoup('<H6 class=content_h6>bab</H6>', 'html.parser').find('h6')
        assert get_heading_level(tag) == 6

    def test_no_content_class(self):
        tag = BeautifulSoup('<H2>plain heading</H2>', 'html.parser').find('h2')
        assert get_heading_level(tag) is None

    def test_non_heading_tag(self):
        tag = BeautifulSoup('<P class=content_h1>not a heading</P>', 'html.parser').find('p')
        assert get_heading_level(tag) is None

    def test_h10(self):
        tag = BeautifulSoup('<H3 class=content_h10>deep</H3>', 'html.parser').find('h3')
        assert get_heading_level(tag) == 10


# ---------------------------------------------------------------------------
# Tests: text classification
# ---------------------------------------------------------------------------

class TestTextClassification:
    def test_footnote_separator(self):
        assert is_footnote_separator("********") is True
        assert is_footnote_separator("  ********  ") is True
        assert is_footnote_separator("normal text") is False

    def test_page_number_tahdhib(self):
        assert is_page_number("ص: 5") is True
        assert is_page_number("ص: 123") is True
        assert is_page_number("some text") is False

    def test_page_number_istibsar(self):
        assert is_page_number("[ صفحه 7]") is True
        assert is_page_number("[صفحه 123]") is True

    def test_metadata_line(self):
        assert is_metadata_line("-روایت-1-4-روایت-430-593") is True
        assert is_metadata_line("normal text") is False

    def test_is_non_content(self):
        assert is_non_content("") is True
        assert is_non_content("   ") is True
        assert is_non_content("********") is True
        assert is_non_content("ص: 10") is True
        assert is_non_content("-روایت-1-4") is True
        assert is_non_content("actual hadith text") is False


# ---------------------------------------------------------------------------
# Tests: hadith number extraction
# ---------------------------------------------------------------------------

class TestSplitHadithTextTahdhib:
    def test_standard_format(self):
        result = split_hadith_text_tahdhib("(1) 1 - مَا أَخْبَرَنِي بِهِ")
        assert result == (1, "مَا أَخْبَرَنِي بِهِ")

    def test_large_number(self):
        result = split_hadith_text_tahdhib("(1234) 1234 - وَ بِهَذَا")
        assert result == (1234, "وَ بِهَذَا")

    def test_without_parenthesized_number(self):
        # Some hadiths omit the parenthesized number: "11-11- text"
        result = split_hadith_text_tahdhib("11-11- وَ بِهَذَا")
        # This won't match our primary regex, which expects (N) N -
        # The "11-" will match as num=11 with the optional parens group
        assert result is None or result[0] == 11

    def test_with_dash_variant(self):
        result = split_hadith_text_tahdhib("(6) 6 - فَأَمَّا اَلْخَبَرُ")
        assert result == (6, "فَأَمَّا اَلْخَبَرُ")

    def test_non_hadith_text(self):
        result = split_hadith_text_tahdhib("وَ هَذَا اَلْحَدِيثُ قَدْ مَضَى")
        assert result is None

    def test_footnote_ref_no_match(self):
        # "(1) في ب..." has no second number, so it doesn't match the hadith pattern
        result = split_hadith_text_tahdhib("(1) في ب و المطبوعة (اجسادهم).")
        assert result is None


class TestSplitHadithTextIstibsar:
    def test_standard_format(self):
        result = split_hadith_text_istibsar("1- أخَبرَنَيِ الشّيخُ")
        assert result == (1, "أخَبرَنَيِ الشّيخُ")

    def test_large_number(self):
        result = split_hadith_text_istibsar("456- وَ بِهَذَا الإِسنَادِ")
        assert result == (456, "وَ بِهَذَا الإِسنَادِ")

    def test_non_hadith(self):
        result = split_hadith_text_istibsar("فَلَيسَ ينُاَفيِ مَا قَدّمنَاهُ")
        assert result is None

    def test_empty_remaining(self):
        # If there's only a number with no text after, reject it
        result = split_hadith_text_istibsar("5- ")
        assert result is None


# ---------------------------------------------------------------------------
# Tests: heading classification
# ---------------------------------------------------------------------------

class TestIsIntroHeading:
    def test_ishara(self):
        assert _is_intro_heading("اشارة") is True

    def test_muqaddima(self):
        assert _is_intro_heading("مقدّمة الناشر") is True

    def test_fehrest(self):
        assert _is_intro_heading("فهرس الكتاب") is True

    def test_bab_heading(self):
        assert _is_intro_heading("بَابُ اَلْأَحْدَاثِ") is False


class TestIsBabHeading:
    def test_bab_with_diacritics(self):
        assert _is_bab_heading("بَابُ اَلْأَحْدَاثِ اَلْمُوجِبَةِ لِلطَّهَارَةِ") is True

    def test_bab_without_diacritics(self):
        assert _is_bab_heading("باب المياه") is True

    def test_numbered_heading(self):
        assert _is_bab_heading("1 - بَابُ اَلْأَحْدَاثِ") is True

    def test_abwab_heading(self):
        assert _is_bab_heading("أَبْوَابُ اَلزِّيَادَاتِ") is True

    def test_non_bab(self):
        assert _is_bab_heading("كِتَابُ اَلطَّهَارَةِ") is False

    def test_muqaddima(self):
        assert _is_bab_heading("مقدّمة الكتاب للمحقق") is False


class TestExtractVolumeNumber:
    def test_volume_with_kitab(self):
        assert _extract_volume_number("المجلد 1-كِتَابُ اَلطَّهَارَةِ") == 1

    def test_volume_plain(self):
        assert _extract_volume_number("المجلد 10") == 10

    def test_no_number(self):
        assert _extract_volume_number("اشارة") is None


# ---------------------------------------------------------------------------
# Tests: element extraction
# ---------------------------------------------------------------------------

class TestExtractElements:
    def test_headings_and_paragraphs(self):
        html = """<HTML><BODY>
        <H2 class=content_h2>Volume 1</H2>
        <P class=content_paragraph><SPAN class=content_text>paragraph text</SPAN></P>
        </BODY></HTML>"""
        soup = BeautifulSoup(html, "html.parser")
        elements = extract_elements(soup)
        assert len(elements) == 2
        assert elements[0] == ("heading", "Volume 1", 2)
        assert elements[1] == ("paragraph", "paragraph text", 0)

    def test_ignores_non_content_spans(self):
        html = """<HTML><BODY>
        <P><SPAN>not content_text class</SPAN></P>
        <P class=content_paragraph><SPAN class=content_text>real content</SPAN></P>
        </BODY></HTML>"""
        soup = BeautifulSoup(html, "html.parser")
        elements = extract_elements(soup)
        assert len(elements) == 1
        assert elements[0][1] == "real content"

    def test_no_body(self):
        soup = BeautifulSoup("<HTML><HEAD></HEAD></HTML>", "html.parser")
        assert extract_elements(soup) == []


# ---------------------------------------------------------------------------
# Tests: Tahdhib parser
# ---------------------------------------------------------------------------

class TestParseTahdhib:
    def test_minimal_book(self):
        """Parse a minimal Tahdhib with 1 volume, 1 bab, 2 hadiths."""
        bab = _make_bab_with_hadiths(
            "1 - بَابُ اَلْأَحْدَاثِ",
            [(1, "مَا أَخْبَرَنِي"), (2, "وَ بِهَذَا اَلْإِسْنَادِ")],
        )
        html = _make_tahdhib_html(f"""
        <DIV><H2 class=content_h2>المجلد 1-كِتَابُ اَلطَّهَارَةِ</H2>
        <DIV>{bab}</DIV>
        </DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_tahdhib(soup)

        assert book.part_type == PartType.Book
        assert len(book.chapters) == 1  # 1 volume
        vol = book.chapters[0]
        assert vol.part_type == PartType.Volume
        assert len(vol.chapters) == 1  # 1 bab
        bab_ch = vol.chapters[0]
        assert bab_ch.part_type == PartType.Chapter
        assert len(bab_ch.verses) == 2
        assert bab_ch.verses[0].part_type == PartType.Hadith
        assert "مَا أَخْبَرَنِي" in bab_ch.verses[0].text[0]

    def test_multiple_babs(self):
        """Parse with multiple babs in one volume."""
        bab1 = _make_bab_with_hadiths(
            "1 - بَابُ الأول",
            [(1, "حديث أول"), (2, "حديث ثان")],
        )
        bab2 = _make_bab_with_hadiths(
            "2 - بَابُ الثاني",
            [(3, "حديث ثالث")],
        )
        html = _make_tahdhib_html(f"""
        <DIV><H2 class=content_h2>المجلد 1-كِتَابُ اَلطَّهَارَةِ</H2>
        <DIV>{bab1}</DIV>
        <DIV>{bab2}</DIV>
        </DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_tahdhib(soup)

        vol = book.chapters[0]
        assert len(vol.chapters) == 2
        assert len(vol.chapters[0].verses) == 2
        assert len(vol.chapters[1].verses) == 1

    def test_multiple_volumes(self):
        """Parse with multiple volumes."""
        bab1 = _make_bab_with_hadiths(
            "1 - بَابُ الأول", [(1, "حديث")],
        )
        bab2 = _make_bab_with_hadiths(
            "1 - بَابُ الثاني", [(1, "حديث آخر")],
        )
        html = _make_tahdhib_html(f"""
        <DIV><H2 class=content_h2>المجلد 1-كِتَابُ اَلطَّهَارَةِ</H2>
        <DIV>{bab1}</DIV></DIV>
        <DIV><H2 class=content_h2>المجلد 2-كِتَابُ اَلصَّلاَةِ</H2>
        <DIV>{bab2}</DIV></DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_tahdhib(soup)

        assert len(book.chapters) == 2
        assert "Volume 1" in book.chapters[0].titles[Language.EN.value]
        assert "Volume 2" in book.chapters[1].titles[Language.EN.value]

    def test_skips_intro_sections(self):
        """Intro sections (اشارة, مقدمة) should not create babs."""
        html = _make_tahdhib_html("""
        <DIV><H2 class=content_h2>المجلد 1-كِتَابُ اَلطَّهَارَةِ</H2>
        <DIV><H3 class=content_h3>مقدّمة الناشر</H3>
        <P class=content_paragraph><SPAN class=content_text>intro text</SPAN></P>
        </DIV>
        <DIV><H4 class=content_h4>1 - بَابُ اَلْأَحْدَاثِ</H4>
        <P class=content_paragraph><SPAN class=content_text>(1) 1 - حديث</SPAN></P>
        </DIV>
        </DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_tahdhib(soup)

        vol = book.chapters[0]
        assert len(vol.chapters) == 1  # Only the bab, not the intro
        assert "بَابُ" in vol.chapters[0].titles[Language.AR.value]

    def test_skips_footnotes(self):
        """Footnotes (between ******** and page number) should be excluded."""
        html = _make_tahdhib_html("""
        <DIV><H2 class=content_h2>المجلد 1-كتاب</H2>
        <DIV><H4 class=content_h4>1 - بَابُ الأول</H4>
        <P class=content_paragraph><SPAN class=content_text>(1) 1 - حديث أصلي</SPAN></P>
        <P class=content_paragraph><SPAN class=content_text>********</SPAN></P>
        <P class=content_paragraph><SPAN class=content_text>(1) هذا تعليق</SPAN></P>
        <P class=content_paragraph><SPAN class=content_text>ص: 5</SPAN></P>
        <P class=content_paragraph><SPAN class=content_text>(2) 2 - حديث ثاني</SPAN></P>
        </DIV></DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_tahdhib(soup)

        bab = book.chapters[0].chapters[0]
        assert len(bab.verses) == 2
        # Footnote text should not appear in hadith content
        for verse in bab.verses:
            assert "تعليق" not in verse.text[0]

    def test_hadith_continuation(self):
        """Multiple paragraphs for one hadith should be merged."""
        html = _make_tahdhib_html("""
        <DIV><H2 class=content_h2>المجلد 1-كتاب</H2>
        <DIV><H4 class=content_h4>1 - بَابُ الأول</H4>
        <P class=content_paragraph><SPAN class=content_text>(1) 1 - بداية الحديث</SPAN></P>
        <P class=content_paragraph><SPAN class=content_text>تتمة الحديث</SPAN></P>
        <P class=content_paragraph><SPAN class=content_text>(2) 2 - حديث ثاني</SPAN></P>
        </DIV></DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_tahdhib(soup)

        bab = book.chapters[0].chapters[0]
        assert len(bab.verses) == 2
        assert "بداية الحديث" in bab.verses[0].text[0]
        assert "تتمة الحديث" in bab.verses[0].text[0]

    def test_count_helpers(self):
        bab = _make_bab_with_hadiths(
            "1 - بَابُ", [(1, "حديث"), (2, "حديث"), (3, "حديث")],
        )
        html = _make_tahdhib_html(f"""
        <DIV><H2 class=content_h2>المجلد 1-كتاب</H2>
        <DIV>{bab}</DIV></DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_tahdhib(soup)
        assert count_hadiths(book) == 3
        assert count_babs(book) == 1

    def test_empty_book(self):
        html = _make_tahdhib_html("")
        soup = BeautifulSoup(html, "html.parser")
        book = parse_tahdhib(soup)
        assert book.chapters == []
        assert count_hadiths(book) == 0


# ---------------------------------------------------------------------------
# Tests: Istibsar parser
# ---------------------------------------------------------------------------

class TestParseIstibsar:
    def test_minimal_book(self):
        """Parse a minimal Istibsar with 1 volume, 1 bab, 2 hadiths."""
        bab = _make_istibsar_bab_with_hadiths(
            "1- بَابُ مِقدَارِ المَاءِ",
            [(1, "أخَبرَنَيِ الشّيخُ"), (2, "وَ بِهَذَا الإِسنَادِ")],
        )
        html = _make_istibsar_html(f"""
        <DIV><H2 class=content_h2>المجلد 1</H2>
        <DIV><H3 class=content_h3>الجزء الأول</H3>
        <DIV><H4 class=content_h4>كِتَابُ الطّهَارَةِ</H4>
        <DIV><H5 class=content_h5>أَبوَابُ المِيَاهِ</H5>
        <DIV>{bab}</DIV>
        </DIV></DIV></DIV></DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_istibsar(soup)

        assert book.part_type == PartType.Book
        assert len(book.chapters) == 1  # 1 volume
        vol = book.chapters[0]
        assert vol.part_type == PartType.Volume
        assert len(vol.chapters) == 1  # 1 bab
        bab_ch = vol.chapters[0]
        assert len(bab_ch.verses) == 2
        assert "أخَبرَنَيِ" in bab_ch.verses[0].text[0]

    def test_multiple_babs(self):
        """Multiple babs in Istibsar."""
        bab1 = _make_istibsar_bab_with_hadiths(
            "1- بَابُ الأول", [(1, "حديث أول")],
        )
        bab2 = _make_istibsar_bab_with_hadiths(
            "2- بَابُ الثاني", [(2, "حديث ثان")],
        )
        html = _make_istibsar_html(f"""
        <DIV><H2 class=content_h2>المجلد 1</H2>
        <DIV><H3 class=content_h3>الجزء الأول</H3>
        <DIV><H4 class=content_h4>كِتَابُ الطّهَارَةِ</H4>
        <DIV><H5 class=content_h5>أبواب المياه</H5>
        <DIV>{bab1}</DIV>
        <DIV>{bab2}</DIV>
        </DIV></DIV></DIV></DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_istibsar(soup)

        vol = book.chapters[0]
        assert len(vol.chapters) == 2

    def test_multiple_volumes(self):
        bab1 = _make_istibsar_bab_with_hadiths(
            "1- بَابُ", [(1, "حديث")],
        )
        bab2 = _make_istibsar_bab_with_hadiths(
            "1- بَابُ", [(1, "حديث")],
        )
        html = _make_istibsar_html(f"""
        <DIV><H2 class=content_h2>المجلد 1</H2>
        <DIV><H3 class=content_h3>الجزء الأول</H3>
        <DIV><H4 class=content_h4>كتاب</H4>
        <DIV><H5 class=content_h5>أبواب</H5>
        <DIV>{bab1}</DIV>
        </DIV></DIV></DIV></DIV>
        <DIV><H2 class=content_h2>المجلد 2</H2>
        <DIV><H3 class=content_h3>الجزء الثاني</H3>
        <DIV><H4 class=content_h4>كتاب</H4>
        <DIV><H5 class=content_h5>أبواب</H5>
        <DIV>{bab2}</DIV>
        </DIV></DIV></DIV></DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_istibsar(soup)

        assert len(book.chapters) == 2

    def test_skips_rivayat_markers(self):
        """Istibsar -روایت- metadata lines should be excluded."""
        html = _make_istibsar_html("""
        <DIV><H2 class=content_h2>المجلد 1</H2>
        <DIV><H3 class=content_h3>الجزء</H3>
        <DIV><H4 class=content_h4>كتاب</H4>
        <DIV><H5 class=content_h5>أبواب</H5>
        <DIV><H6 class=content_h6>1- بَابُ</H6>
        <P class=content_paragraph><SPAN class=content_text>1- حديث أصلي</SPAN></P>
        <P class=content_paragraph><SPAN class=content_text>-روایت-1-4-روایت-430-593</SPAN></P>
        <P class=content_paragraph><SPAN class=content_text>2- حديث ثاني</SPAN></P>
        </DIV></DIV></DIV></DIV></DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_istibsar(soup)

        bab = book.chapters[0].chapters[0]
        assert len(bab.verses) == 2
        for verse in bab.verses:
            assert "روایت" not in verse.text[0]

    def test_skips_page_markers(self):
        """Istibsar [ صفحه N] markers should be excluded."""
        html = _make_istibsar_html("""
        <DIV><H2 class=content_h2>المجلد 1</H2>
        <DIV><H3 class=content_h3>الجزء</H3>
        <DIV><H4 class=content_h4>كتاب</H4>
        <DIV><H5 class=content_h5>أبواب</H5>
        <DIV><H6 class=content_h6>1- بَابُ</H6>
        <P class=content_paragraph><SPAN class=content_text>1- حديث</SPAN></P>
        <P class=content_paragraph><SPAN class=content_text>[ صفحه 7]</SPAN></P>
        <P class=content_paragraph><SPAN class=content_text>تتمة</SPAN></P>
        </DIV></DIV></DIV></DIV></DIV>""")

        soup = BeautifulSoup(html, "html.parser")
        book = parse_istibsar(soup)

        bab = book.chapters[0].chapters[0]
        assert len(bab.verses) == 1
        assert "صفحه" not in bab.verses[0].text[0]
        assert "تتمة" in bab.verses[0].text[0]


# ---------------------------------------------------------------------------
# Tests: integration with real HTML files (skipped if not present)
# ---------------------------------------------------------------------------

class TestRealFiles:
    @pytest.fixture
    def tahdhib_soup(self):
        """Load real Tahdhib HTML if available."""
        from app import config
        import os
        filepath = config.get_raw_path("ghbook_ir", "tahdhib-al-ahkam", "book.htm")
        if not os.path.exists(filepath):
            pytest.skip("Tahdhib HTML not available")
        with open(filepath, "r", encoding="utf-8") as f:
            return BeautifulSoup(f.read(), "html.parser")

    @pytest.fixture
    def istibsar_soup(self):
        """Load real Istibsar HTML if available."""
        from app import config
        import os
        filepath = config.get_raw_path("ghbook_ir", "al-istibsar", "book.htm")
        if not os.path.exists(filepath):
            pytest.skip("Istibsar HTML not available")
        with open(filepath, "r", encoding="utf-8") as f:
            return BeautifulSoup(f.read(), "html.parser")

    def test_tahdhib_has_10_volumes(self, tahdhib_soup):
        book = parse_tahdhib(tahdhib_soup)
        assert len(book.chapters) == 10

    def test_tahdhib_volume_titles(self, tahdhib_soup):
        book = parse_tahdhib(tahdhib_soup)
        for i, vol in enumerate(book.chapters):
            assert vol.part_type == PartType.Volume
            en_title = vol.titles.get(Language.EN.value, "")
            assert f"Volume {i + 1}" in en_title

    def test_tahdhib_has_hadiths(self, tahdhib_soup):
        book = parse_tahdhib(tahdhib_soup)
        total = count_hadiths(book)
        # Expected ~13,590 hadiths — allow generous range for parser variations
        assert total > 10000, f"Expected >10000 hadiths, got {total}"
        assert total < 20000, f"Expected <20000 hadiths, got {total}"

    def test_tahdhib_has_babs(self, tahdhib_soup):
        book = parse_tahdhib(tahdhib_soup)
        total = count_babs(book)
        assert total > 100, f"Expected >100 babs, got {total}"

    def test_tahdhib_all_volumes_have_content(self, tahdhib_soup):
        book = parse_tahdhib(tahdhib_soup)
        for i, vol in enumerate(book.chapters):
            assert len(vol.chapters) > 0, f"Volume {i+1} has no babs"
            vol_hadiths = sum(len(b.verses or []) for b in vol.chapters)
            assert vol_hadiths > 0, f"Volume {i+1} has no hadiths"

    def test_tahdhib_first_hadith_content(self, tahdhib_soup):
        book = parse_tahdhib(tahdhib_soup)
        first_vol = book.chapters[0]
        # Find the first bab that actually has hadiths
        first_bab = None
        for bab in first_vol.chapters:
            if bab.verses:
                first_bab = bab
                break
        assert first_bab is not None, "No bab with hadiths found in Volume 1"
        first_hadith = first_bab.verses[0]
        assert first_hadith.text is not None
        assert len(first_hadith.text) == 1
        assert len(first_hadith.text[0]) > 20

    def test_istibsar_has_4_volumes(self, istibsar_soup):
        book = parse_istibsar(istibsar_soup)
        assert len(book.chapters) == 4

    def test_istibsar_has_hadiths(self, istibsar_soup):
        book = parse_istibsar(istibsar_soup)
        total = count_hadiths(book)
        # Expected ~5,511 hadiths
        assert total > 3000, f"Expected >3000 hadiths, got {total}"
        assert total < 10000, f"Expected <10000 hadiths, got {total}"

    def test_istibsar_has_babs(self, istibsar_soup):
        book = parse_istibsar(istibsar_soup)
        total = count_babs(book)
        assert total > 50, f"Expected >50 babs, got {total}"

    def test_istibsar_all_volumes_have_content(self, istibsar_soup):
        book = parse_istibsar(istibsar_soup)
        for i, vol in enumerate(book.chapters):
            assert len(vol.chapters) > 0, f"Volume {i+1} has no babs"

    def test_istibsar_hadith_text_is_arabic(self, istibsar_soup):
        """Verify parsed hadith text contains Arabic characters."""
        book = parse_istibsar(istibsar_soup)
        arabic_re = re.compile(r'[\u0600-\u06FF]')
        first_vol = book.chapters[0]
        first_bab = first_vol.chapters[0]
        for verse in first_bab.verses[:5]:
            assert arabic_re.search(verse.text[0]), \
                f"Hadith text doesn't contain Arabic: {verse.text[0][:50]}"


import re  # needed at module level for test_istibsar_hadith_text_is_arabic
