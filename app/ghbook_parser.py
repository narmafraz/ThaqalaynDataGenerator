"""Parser for ghbook.ir HTML files (Tahdhib al-Ahkam and al-Istibsar).

Parses single large HTML files downloaded from ghbook.ir into Chapter/Verse
hierarchy for the Thaqalayn data pipeline.

Both books use CSS classes content_h1..content_hN for headings and
content_paragraph/content_text for body text. The hierarchy differs:

Tahdhib al-Ahkam:
  h1 = Book title
  h2 = Volume + Kitab (e.g. "المجلد 1-كتاب الطهارة")
  h3 = Kitab or section
  h4 = Bab (chapter with hadiths)
  h5 = Sub-bab (in "abwab al-ziyadat" supplementary sections)
  Hadith pattern: "(N) N - text..." or "N-N- text..."

al-Istibsar:
  h1 = Book title
  h2 = Volume (e.g. "المجلد 1")
  h3 = Part (e.g. "الجزء الأول")
  h4 = Kitab
  h5 = Section (abwab)
  h6 = Bab (chapter with hadiths)
  Hadith pattern: "N- text..."
"""

import logging
import os
import re
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup, Tag

from app import config
from app.models import Chapter, Language, PartType, Verse

logger = logging.getLogger(__name__)

# Regex patterns for hadith numbering
# Tahdhib: "(1) 1 - text" or "1-1- text" or "(1) 1 text" at start of content_text
TAHDHIB_HADITH_RE = re.compile(
    r'^\s*(?:\((\d+)\)\s*)?(\d+)\s*[-\u2013]\s*'
)

# Istibsar: "1- text" at start of content_text
ISTIBSAR_HADITH_RE = re.compile(
    r'^\s*(\d+)\s*-\s*'
)

# Footnote separator
FOOTNOTE_SEPARATOR = "********"

# Page number patterns
PAGE_NUM_RE_TAHDHIB = re.compile(r'^ص:\s*\d+$')
PAGE_NUM_RE_ISTIBSAR = re.compile(r'^\[\s*صفحه\s+\d+\s*\]$')

# Metadata line (Istibsar has -روایت- markers)
RIVAYAT_RE = re.compile(r'^-روایت-')


def load_html(book_key: str) -> BeautifulSoup:
    """Load and parse a ghbook.ir HTML file."""
    filepath = config.get_raw_path("ghbook_ir", book_key, "book.htm")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"ghbook.ir HTML not found: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        return BeautifulSoup(f.read(), "html.parser")


def get_heading_level(tag: Tag) -> Optional[int]:
    """Extract heading level from a tag with class content_hN."""
    if tag.name and tag.name.upper().startswith("H"):
        classes = tag.get("class", [])
        if isinstance(classes, str):
            classes = [classes]
        for cls in classes:
            m = re.match(r'content_h(\d+)', cls)
            if m:
                return int(m.group(1))
    return None


def get_text(tag: Tag) -> str:
    """Extract text content from a tag."""
    return tag.get_text(strip=True)


def is_footnote_separator(text: str) -> bool:
    """Check if text is a footnote separator."""
    return text.strip().startswith(FOOTNOTE_SEPARATOR)


def is_page_number(text: str) -> bool:
    """Check if text is a page number marker."""
    return bool(PAGE_NUM_RE_TAHDHIB.match(text.strip()) or
                PAGE_NUM_RE_ISTIBSAR.match(text.strip()))


def is_metadata_line(text: str) -> bool:
    """Check if text is a metadata/rivayat marker."""
    return bool(RIVAYAT_RE.match(text.strip()))


def is_non_content(text: str) -> bool:
    """Check if a paragraph is non-content (footnote, page number, metadata)."""
    stripped = text.strip()
    if not stripped:
        return True
    if is_footnote_separator(stripped):
        return True
    if is_page_number(stripped):
        return True
    if is_metadata_line(stripped):
        return True
    return False


def extract_elements(soup: BeautifulSoup) -> List[Tuple[str, str, int]]:
    """Extract a flat list of (type, text, heading_level) from the HTML.

    Returns tuples of:
      ("heading", title_text, level)  - for heading tags
      ("paragraph", text, 0)          - for content paragraphs
    """
    elements = []
    body = soup.find("body")
    if not body:
        return elements

    for tag in body.descendants:
        if not isinstance(tag, Tag):
            continue

        level = get_heading_level(tag)
        if level is not None:
            text = get_text(tag)
            if text:
                elements.append(("heading", text, level))
            continue

        classes = tag.get("class", [])
        if isinstance(classes, str):
            classes = [classes]
        if "content_text" in classes and tag.name == "span":
            # Only take direct content_text spans inside content_paragraph
            parent = tag.parent
            if parent and "content_paragraph" in (parent.get("class") or []):
                text = get_text(tag)
                if text:
                    elements.append(("paragraph", text, 0))

    return elements


def split_hadith_text_tahdhib(text: str) -> Optional[Tuple[int, str]]:
    """Try to extract hadith number and text from a Tahdhib paragraph.

    Returns (hadith_number, remaining_text) or None.
    """
    m = TAHDHIB_HADITH_RE.match(text)
    if m:
        # Use the second group (the actual sequential number)
        num = int(m.group(2))
        remaining = text[m.end():].strip()
        return (num, remaining)
    return None


def split_hadith_text_istibsar(text: str) -> Optional[Tuple[int, str]]:
    """Try to extract hadith number and text from an Istibsar paragraph.

    Returns (hadith_number, remaining_text) or None.
    """
    m = ISTIBSAR_HADITH_RE.match(text)
    if m:
        num = int(m.group(1))
        remaining = text[m.end():].strip()
        # Reject if remaining text is empty or looks like footnote reference
        if remaining:
            return (num, remaining)
    return None


def _is_intro_heading(text: str) -> bool:
    """Check if a heading is an intro/non-content section."""
    intro_markers = [
        "اشارة", "مقدّمة", "مقدمة", "فهرس", "مقدّمه",
    ]
    stripped = text.strip()
    for marker in intro_markers:
        if marker in stripped:
            return True
    return False


def parse_tahdhib(soup: BeautifulSoup) -> Chapter:
    """Parse Tahdhib al-Ahkam HTML into a Chapter hierarchy.

    Hierarchy: Book -> Volume -> Bab -> Hadith
    """
    elements = extract_elements(soup)

    book = Chapter()
    book.part_type = PartType.Book
    book.titles = {
        Language.EN.value: "Tahdhib al-Ahkam",
        Language.AR.value: "تهذيب الأحكام",
    }
    book.chapters = []

    current_volume = None
    current_bab = None
    current_hadith_num = 0
    in_footnotes = False
    collecting_hadith_text = []
    bab_heading_level = None  # Track what heading level babs appear at

    for elem_type, text, level in elements:
        if elem_type == "heading":
            # Flush any pending hadith text
            if collecting_hadith_text and current_bab:
                _flush_hadith(current_bab, current_hadith_num,
                              collecting_hadith_text)
                collecting_hadith_text = []

            in_footnotes = False

            if level == 1:
                continue  # Book title, skip

            if level == 2:
                # Volume heading
                if _is_intro_heading(text):
                    continue
                vol_num = _extract_volume_number(text)
                current_volume = Chapter()
                current_volume.part_type = PartType.Volume
                current_volume.titles = {
                    Language.AR.value: text.strip(),
                    Language.EN.value: f"Volume {vol_num}" if vol_num else text.strip(),
                }
                current_volume.chapters = []
                book.chapters.append(current_volume)
                current_bab = None
                bab_heading_level = None
                continue

            # Bab-level headings (h3, h4, or h5 depending on context)
            if current_volume is None:
                continue  # Skip intro content before first volume

            if _is_intro_heading(text):
                current_bab = None
                continue

            # Detect bab: heading that contains "باب" or is numbered "N - ..."
            is_bab = _is_bab_heading(text)

            if is_bab:
                bab_heading_level = level
                current_bab = Chapter()
                current_bab.part_type = PartType.Chapter
                current_bab.titles = {Language.AR.value: text.strip()}
                current_bab.verses = []
                current_bab.verse_start_index = 0
                current_volume.chapters.append(current_bab)
                current_hadith_num = 0
                continue

            # Other headings within a volume but not a bab — treat as a
            # section grouping heading (e.g. "أبواب الزيادات")
            # We skip these and let their child babs be attached to the volume

        elif elem_type == "paragraph":
            if current_bab is None:
                continue  # Skip content before first bab

            if is_footnote_separator(text):
                in_footnotes = True
                continue

            if in_footnotes:
                if is_page_number(text):
                    in_footnotes = False
                continue

            if is_non_content(text):
                continue

            # Try to parse as hadith start
            parsed = split_hadith_text_tahdhib(text)
            if parsed:
                # Flush previous hadith
                if collecting_hadith_text:
                    _flush_hadith(current_bab, current_hadith_num,
                                  collecting_hadith_text)

                current_hadith_num, remaining = parsed
                collecting_hadith_text = [remaining] if remaining else []
            else:
                # Continuation of current hadith or commentary
                if current_hadith_num > 0:
                    collecting_hadith_text.append(text)
                # else: commentary before first hadith in bab, skip

    # Flush final hadith
    if collecting_hadith_text and current_bab:
        _flush_hadith(current_bab, current_hadith_num,
                      collecting_hadith_text)

    return book


def parse_istibsar(soup: BeautifulSoup) -> Chapter:
    """Parse al-Istibsar HTML into a Chapter hierarchy.

    Hierarchy: Book -> Volume -> Bab -> Hadith
    """
    elements = extract_elements(soup)

    book = Chapter()
    book.part_type = PartType.Book
    book.titles = {
        Language.EN.value: "al-Istibsar",
        Language.AR.value: "الاستبصار",
    }
    book.chapters = []

    current_volume = None
    current_bab = None
    current_hadith_num = 0
    collecting_hadith_text = []

    for elem_type, text, level in elements:
        if elem_type == "heading":
            # Flush any pending hadith text
            if collecting_hadith_text and current_bab:
                _flush_hadith(current_bab, current_hadith_num,
                              collecting_hadith_text)
                collecting_hadith_text = []

            if level == 1:
                continue  # Book title

            if level == 2:
                # Volume heading
                if _is_intro_heading(text):
                    continue
                vol_num = _extract_volume_number(text)
                current_volume = Chapter()
                current_volume.part_type = PartType.Volume
                current_volume.titles = {
                    Language.AR.value: text.strip(),
                    Language.EN.value: f"Volume {vol_num}" if vol_num else text.strip(),
                }
                current_volume.chapters = []
                book.chapters.append(current_volume)
                current_bab = None
                continue

            if current_volume is None:
                continue

            if _is_intro_heading(text):
                current_bab = None
                continue

            # In Istibsar, babs are at h6 (numbered "N- باب...")
            # h3=Part, h4=Kitab, h5=Section — all skipped as grouping
            if level == 6:
                current_bab = Chapter()
                current_bab.part_type = PartType.Chapter
                current_bab.titles = {Language.AR.value: text.strip()}
                current_bab.verses = []
                current_bab.verse_start_index = 0
                current_volume.chapters.append(current_bab)
                current_hadith_num = 0
                continue

        elif elem_type == "paragraph":
            if current_bab is None:
                continue

            if is_non_content(text):
                continue

            # Try to parse as hadith start
            parsed = split_hadith_text_istibsar(text)
            if parsed:
                num, remaining = parsed
                # Validate sequential numbering (allow some gaps for commentary)
                if num >= current_hadith_num:
                    # Flush previous hadith
                    if collecting_hadith_text:
                        _flush_hadith(current_bab, current_hadith_num,
                                      collecting_hadith_text)

                    current_hadith_num = num
                    collecting_hadith_text = [remaining] if remaining else []
                else:
                    # Number went backwards — likely a footnote reference
                    if current_hadith_num > 0:
                        collecting_hadith_text.append(text)
            else:
                if current_hadith_num > 0:
                    collecting_hadith_text.append(text)

    # Flush final hadith
    if collecting_hadith_text and current_bab:
        _flush_hadith(current_bab, current_hadith_num,
                      collecting_hadith_text)

    return book


def _flush_hadith(bab: Chapter, hadith_num: int, text_parts: List[str]) -> None:
    """Create a Verse from accumulated hadith text and append to the bab."""
    combined = " ".join(text_parts).strip()
    if not combined:
        return

    verse = Verse()
    verse.part_type = PartType.Hadith
    verse.text = [combined]
    bab.verses.append(verse)


def _extract_volume_number(text: str) -> Optional[int]:
    """Extract volume number from heading text like 'المجلد 1-...' or 'المجلد 1'."""
    m = re.search(r'(\d+)', text)
    if m:
        return int(m.group(1))
    return None


def _is_bab_heading(text: str) -> bool:
    """Check if a heading text represents a bab (chapter with hadiths).

    Bab headings typically contain 'باب' or start with a number like '1 - باب'.
    """
    stripped = text.strip()
    if 'بَابُ' in stripped or 'بَابِ' in stripped or 'باب' in stripped:
        return True
    # Numbered chapter heading: "1 - ..." or "أبواب ..."
    if re.match(r'^\d+\s*[-\u2013]', stripped):
        return True
    if 'أَبْوَابُ' in stripped or 'أبواب' in stripped:
        return True
    return False


def count_hadiths(book: Chapter) -> int:
    """Count total hadiths across all volumes and babs."""
    total = 0
    for vol in (book.chapters or []):
        for bab in (vol.chapters or []):
            total += len(bab.verses or [])
    return total


def count_babs(book: Chapter) -> int:
    """Count total babs across all volumes."""
    total = 0
    for vol in (book.chapters or []):
        total += len(vol.chapters or [])
    return total
