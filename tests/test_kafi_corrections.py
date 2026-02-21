"""Tests for kafi_corrections.py -- the corrections layer for source HTML fixes."""
from app.kafi_corrections import file_correction, CORRECTIONS


class TestFileCorrection:
    """Test that file_correction applies the right fixes."""

    def test_no_correction_for_unknown_file(self):
        """Files not in CORRECTIONS dict should be returned unchanged."""
        content = "Some HTML content with Chapater"
        result = file_correction("/path/to/unknown.xhtml", content)
        assert result == content

    def test_chapter_typo_chapater(self):
        """c072.xhtml has 'Chapater 4' -> 'Chapter 4'"""
        content = "Some text with Chapater 4 in it"
        result = file_correction("/some/path/c072.xhtml", content)
        assert "Chapter 4" in result
        assert "Chapater 4" not in result

    def test_chapter_typo_chater(self):
        """c134.xhtml has 'Chater' -> 'Chapter'"""
        content = "Title is Chater 5 here"
        result = file_correction("/vol2/c134.xhtml", content)
        assert "Chapter" in result
        assert "Chater" not in result

    def test_chapter_typo_cahpter(self):
        """c223.xhtml has 'Cahpter' -> 'Chapter'"""
        content = "Cahpter 10 text"
        result = file_correction("/any/c223.xhtml", content)
        assert "Chapter" in result
        assert "Cahpter" not in result

    def test_chapter_typo_chapaater(self):
        """c281.xhtml has 'Chapaater' -> 'Chapter'"""
        content = "Chapaater 1 stuff"
        result = file_correction("/v4/c281.xhtml", content)
        assert "Chapter" in result
        assert "Chapaater" not in result

    def test_chapter_typo_chhapter(self):
        """c336.xhtml has 'Chhapter 140' -> 'Chapter 140'"""
        content = "Chhapter 140 text"
        result = file_correction("/v6/c336.xhtml", content)
        assert "Chapter 140" in result
        assert "Chhapter" not in result

    def test_footnote_removal(self):
        """c005.xhtml removes a footnote reference (including trailing space)."""
        content = 'text <a id="_ftnref13"/><sup>[13]</sup> more text'
        result = file_correction("/c005.xhtml", content)
        assert '<a id="_ftnref13"/>' not in result
        assert "text more text" in result

    def test_hadith_number_correction(self):
        """c020.xhtml fixes '46214-' -> '14462-'"""
        content = "46214- hadith text"
        result = file_correction("/v8/c020.xhtml", content)
        assert "14462-" in result
        assert "46214-" not in result

    def test_span_to_style_correction_c107(self):
        """c107.xhtml merges span bold/underline into parent p style."""
        before = '<p style="text-align: justify" dir="rtl">&#1576;&#1575;<span style="font-weight: bold; text-decoration: underline">'
        after_expected = '<p style="text-align: justify; font-weight: bold; text-decoration: underline" dir="rtl">&#1576;&#1575;'
        result = file_correction("/c107.xhtml", before)
        assert after_expected in result

    def test_multiple_corrections_applied(self):
        """Files with multiple corrections get all of them applied."""
        # c107.xhtml has 2 corrections: span->style and closing </span></p>
        content = (
            '<p style="text-align: justify" dir="rtl">&#1576;&#1575;'
            '<span style="font-weight: bold; text-decoration: underline">'
            'text</span></p>'
        )
        result = file_correction("/c107.xhtml", content)
        assert "<span" not in result
        assert "</span>" not in result

    def test_correction_only_uses_filename(self):
        """file_correction uses os.path.basename, so only the filename matters."""
        content = "Chapater 4 text"
        result1 = file_correction("/long/path/to/c072.xhtml", content)
        result2 = file_correction("c072.xhtml", content)
        assert result1 == result2

    def test_corrections_dict_has_entries(self):
        """Sanity check that CORRECTIONS dict is non-empty."""
        assert len(CORRECTIONS) > 10

    def test_all_corrections_have_before_after(self):
        """Every correction entry must have 'before' and 'after' keys."""
        for filename, corrections in CORRECTIONS.items():
            for i, correction in enumerate(corrections):
                assert "before" in correction, \
                    f"{filename} correction {i} missing 'before'"
                assert "after" in correction, \
                    f"{filename} correction {i} missing 'after'"
