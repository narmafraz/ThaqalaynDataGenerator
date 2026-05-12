"""Tests for app.words.hawramani."""
from __future__ import annotations

import pytest

from app.words.hawramani import (
    LEXICON_LEGEND,
    parse_hawramani_page,
    sanitize_html,
)


# ---------------------------------------------------------------------------
# HTML sanitizer
# ---------------------------------------------------------------------------

class TestSanitizeHtml:
    def test_keeps_allowed_tags(self):
        s = "<p>hello <b>bold</b> and <i>italic</i></p>"
        result = sanitize_html(s)
        assert "<p>" in result and "</p>" in result
        assert "<b>bold</b>" in result
        assert "<i>italic</i>" in result

    def test_strips_script(self):
        s = '<p>text</p><script>alert("xss")</script><p>more</p>'
        result = sanitize_html(s)
        assert "<script" not in result
        assert "alert" not in result
        assert "<p>text</p>" in result
        assert "<p>more</p>" in result

    def test_strips_style(self):
        s = '<style>body{color:red}</style><p>hi</p>'
        result = sanitize_html(s)
        assert "<style" not in result
        assert "body{color:red}" not in result

    def test_strips_iframe(self):
        s = '<p>before</p><iframe src="x"></iframe><p>after</p>'
        result = sanitize_html(s)
        assert "<iframe" not in result

    def test_strips_event_handlers(self):
        s = '<a href="x" onclick="hack()">link</a>'
        result = sanitize_html(s)
        assert "onclick" not in result
        assert 'href="x"' in result
        assert ">link</a>" in result

    def test_strips_data_attributes(self):
        s = '<span data-tracking-id="xyz" lang="ar">word</span>'
        result = sanitize_html(s)
        assert "data-tracking" not in result
        assert 'lang="ar"' in result

    def test_strips_javascript_urls(self):
        s = '<a href="javascript:alert(1)">click</a>'
        result = sanitize_html(s)
        assert "javascript:" not in result

    def test_strips_disallowed_tags_keeps_content(self):
        # font isn't in our allowlist — should be dropped but content kept
        s = '<p>before<font color="red">styled</font>after</p>'
        result = sanitize_html(s)
        assert "<font" not in result
        assert "styled" in result

    def test_self_closing_br(self):
        s = "line1<br>line2"
        result = sanitize_html(s)
        assert "<br>" in result

    def test_preserves_class_on_span(self):
        # hawramani marks Arabic spans with class="ar"
        s = '<span class="ar">عربي</span>'
        result = sanitize_html(s)
        assert 'class="ar"' in result

    def test_escapes_unsafe_chars_in_text(self):
        s = "<p>1 < 2 & 3 > 0</p>"
        result = sanitize_html(s)
        # text-content < > & should remain as HTML entities
        assert "&amp;" in result
        # NOT a literal < or > in text content
        assert "1 < 2 & 3 > 0</p>" not in result

    def test_empty(self):
        assert sanitize_html("") == ""

    def test_whitespace_collapsed(self):
        s = "<p>   lots   of    spaces   </p>"
        result = sanitize_html(s)
        # Repeated whitespace collapsed to single space
        assert "    " not in result


# ---------------------------------------------------------------------------
# Page parser
# ---------------------------------------------------------------------------

@pytest.fixture
def minimal_hawramani_page():
    """Synthetic 'hawramani page' with one headword + two lexicons."""
    return """
    <html><body><main><article>
    <div class="dictionary-entry-container">
      <div class="dictionary-entry-title-wrapper">
        <div class="dictionary-entry-title-container">
          <h1 class="dictionary-entry-title"><span><dfn>قال</dfn></span></h1>
        </div>
        <div class="description-of-entry">Entries on قال in 2 Arabic dictionaries</div>
      </div>
      <div class="dictionary-entry-content">
        <div id="abc" class="definition-container dictionary_31">
          <div class="entry-meta">
            <div class="credits">
              <a href="https://x/">Al-Rāghib, al-Mufradāt
                <span class="ar">المفردات للراغب</span>
              </a>
            </div>
            <div class="sectionperma">https://arabiclexicon.hawramani.com/?p=1#abc</div>
          </div>
          <div class="definition">قال: <i>he said</i><br>From root ق-و-ل.</div>
        </div>
        <div id="def" class="definition-container dictionary_1">
          <div class="entry-meta">
            <div class="credits">
              <a href="https://y/">Ibn Manẓūr, Lisān al-ʿArab
                <span class="ar">لسان العرب</span>
              </a>
            </div>
            <div class="sectionperma">https://arabiclexicon.hawramani.com/?p=1#def</div>
          </div>
          <div class="definition"><p>Long entry text…</p></div>
        </div>
      </div>
    </div>
    </article></main></body></html>
    """


class TestParseHawramaniPage:
    def test_basic_structure(self, minimal_hawramani_page):
        result = parse_hawramani_page(minimal_hawramani_page, "قال")
        assert result["fetched_slug"] == "قال"
        assert result["url"].startswith("https://arabiclexicon.hawramani.com/")
        assert len(result["headwords"]) == 1
        hw = result["headwords"][0]
        assert hw["headword_ar"] == "قال"
        assert "2 Arabic dictionaries" in hw["summary"]
        assert len(hw["entries"]) == 2

    def test_lexicon_metadata(self, minimal_hawramani_page):
        result = parse_hawramani_page(minimal_hawramani_page, "قال")
        entries = result["headwords"][0]["entries"]
        # First entry: Mufradat
        e1 = entries[0]
        assert e1["lexicon_id"] == "dictionary_31"
        assert "Mufradāt" in e1["lexicon_en"]
        assert "المفردات" in e1["lexicon_ar"]
        assert "?p=1#abc" in e1["permalink"]
        # Body preserved with sanitization
        assert "<i>he said</i>" in e1["body_html"]
        assert "<br>" in e1["body_html"]
        # Second entry: Lisan
        e2 = entries[1]
        assert e2["lexicon_id"] == "dictionary_1"
        assert "Lisān" in e2["lexicon_en"]

    def test_empty_page_returns_empty_dict(self):
        result = parse_hawramani_page(
            "<html><body><h1>Not Found</h1></body></html>", "xxx",
        )
        assert result == {}

    def test_multiple_headwords(self):
        html = """
        <main>
        <div class="dictionary-entry-container">
          <h1 class="dictionary-entry-title"><span><dfn>قال</dfn></span></h1>
          <div class="description-of-entry">Entries on قال...</div>
          <div class="definition-container dictionary_1">
            <div class="credits"><a href="x">Lex 1</a></div>
            <div class="sectionperma">perma1</div>
            <div class="definition">body1</div>
          </div>
        </div>
        <div class="dictionary-entry-container">
          <h1 class="dictionary-entry-title"><span><dfn>قَال</dfn></span></h1>
          <div class="description-of-entry">Entries on قَال...</div>
          <div class="definition-container dictionary_2">
            <div class="credits"><a href="x">Lex 2</a></div>
            <div class="sectionperma">perma2</div>
            <div class="definition">body2</div>
          </div>
        </div>
        </main>
        """
        result = parse_hawramani_page(html, "قال")
        assert len(result["headwords"]) == 2
        assert result["headwords"][0]["headword_ar"] == "قال"
        assert result["headwords"][1]["headword_ar"] == "قَال"


# ---------------------------------------------------------------------------
# Legend
# ---------------------------------------------------------------------------

class TestLexiconLegend:
    def test_known_lexicons_present(self):
        # Spot-check that key lexicons are in the legend.
        assert "dictionary_1" in LEXICON_LEGEND  # Lisan
        assert "dictionary_31" in LEXICON_LEGEND  # Mufradat (Raghib)
        assert "dictionary_25" in LEXICON_LEGEND  # Taj al-Arus
        assert "dictionary_49" in LEXICON_LEGEND  # Lane's

    def test_each_entry_has_en_and_ar(self):
        for lid, names in LEXICON_LEGEND.items():
            assert "en" in names, lid
            assert "ar" in names, lid
            assert names["en"], lid  # non-empty English name
