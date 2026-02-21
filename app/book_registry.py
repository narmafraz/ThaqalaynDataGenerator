"""Declarative book registry for Thaqalayn data generation.

Adding a new book requires:
1. Add a BookConfig entry to BOOK_REGISTRY
2. Write a parser function that returns a Chapter tree
3. (Optional) Add post-processing steps

The registry drives both init_books() (book list generation) and
main_add.py (pipeline orchestration).
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from app.models.enums import Language


@dataclass
class BookConfig:
    """Configuration for a single book in the Thaqalayn library."""

    # Identity
    slug: str                      # e.g. "al-kafi", "quran", "nahj-al-balagha"
    index: int                     # Unique book number (1 = Quran, 2 = Al-Kafi, ...)
    path: str                      # e.g. "/books/al-kafi"

    # Display
    titles: Dict[str, str] = field(default_factory=dict)        # {lang: title}
    descriptions: Dict[str, str] = field(default_factory=dict)  # {lang: description}

    # Metadata
    author: Optional[Dict[str, str]] = None       # {lang: author_name}
    translator: Optional[Dict[str, str]] = None   # {lang: translator_name}
    source_url: Optional[str] = None               # URL to original source

    # Default translation selections
    default_verse_translation_ids: Optional[Dict[str, str]] = None  # {lang: translation_id}


# ---------------------------------------------------------------------------
# Registry of all books
# ---------------------------------------------------------------------------

BOOK_REGISTRY: List[BookConfig] = [
    BookConfig(
        slug="quran",
        index=1,
        path="/books/quran",
        titles={
            Language.EN.value: "The Holy Quran",
            Language.AR.value: "\u0627\u0644\u0642\u0631\u0622\u0646 \u0627\u0644\u0643\u0631\u064a\u0645",
        },
        descriptions={
            Language.EN.value: "The Holy Quran with multiple translations",
        },
        source_url="https://tanzil.net/",
        default_verse_translation_ids={
            "en": "en.qarai",
            "fa": "fa.makarem",
        },
    ),
    BookConfig(
        slug="al-kafi",
        index=2,
        path="/books/al-kafi",
        titles={
            Language.EN.value: "Al-Kafi",
            Language.AR.value: "\u0627\u0644\u0643\u0627\u0641\u064a",
        },
        descriptions={
            Language.EN.value: "The most reliable Shia hadith collection compiled by Shaykh al-Kulayni",
        },
        author={
            Language.EN.value: "Shaykh al-Kulayni",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0643\u0644\u064a\u0646\u064a",
        },
        source_url="https://thaqalayn.net/",
        default_verse_translation_ids={
            "en": "en.hubeali",
        },
    ),
]


def get_book_config(slug: str) -> Optional[BookConfig]:
    """Look up a book config by slug."""
    for book in BOOK_REGISTRY:
        if book.slug == slug:
            return book
    return None


def get_next_book_index() -> int:
    """Return the next available book index for new registrations."""
    if not BOOK_REGISTRY:
        return 1
    return max(b.index for b in BOOK_REGISTRY) + 1
