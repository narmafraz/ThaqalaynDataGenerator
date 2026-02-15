from bs4 import BeautifulSoup, NavigableString
from app.lib_bs4 import is_rtl_tag, is_tag, get_contents


class TestHTMLParsingUtils:
    """Test BeautifulSoup utility functions"""

    def test_is_rtl_tag_true(self):
        """Test RTL tag detection"""
        tag = BeautifulSoup('<div dir="rtl">Text</div>', 'html.parser').div
        assert is_rtl_tag(tag) is True

    def test_is_rtl_tag_false(self):
        """Test non-RTL tag"""
        tag = BeautifulSoup('<div>Text</div>', 'html.parser').div
        assert is_rtl_tag(tag) is False

    def test_is_rtl_tag_ltr(self):
        """Test LTR tag returns false"""
        tag = BeautifulSoup('<div dir="ltr">Text</div>', 'html.parser').div
        assert is_rtl_tag(tag) is False

    def test_is_tag_with_tag(self):
        """Test Tag type detection"""
        tag = BeautifulSoup('<div>Text</div>', 'html.parser').div
        assert is_tag(tag) is True

    def test_is_tag_with_string(self):
        """Test string is not a tag"""
        text = NavigableString("text")
        assert is_tag(text) is False

    def test_get_contents_text_only(self):
        """Test content extraction with text only"""
        tag = BeautifulSoup('<div>Simple text</div>', 'html.parser').div
        assert get_contents(tag) == "Simple text"

    def test_get_contents_multiple_elements(self):
        """Test content extraction joins multiple elements"""
        tag = BeautifulSoup('<div>Text <b>bold</b> more</div>', 'html.parser').div
        result = get_contents(tag)
        assert "Text" in result
        assert "bold" in result
        assert "more" in result

    def test_get_contents_nested_tags(self):
        """Test content extraction with nested tags"""
        tag = BeautifulSoup('<div><span>Hello</span></div>', 'html.parser').div
        result = get_contents(tag)
        assert "<span>Hello</span>" in result
