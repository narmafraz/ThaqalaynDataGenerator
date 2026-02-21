"""Declarative book registry for Thaqalayn data generation.

Adding a new book requires:
1. Add a BookConfig entry to BOOK_REGISTRY
2. Write a parser function that returns a Chapter tree
3. (Optional) Add post-processing steps

The registry drives both init_books() (book list generation) and
main_add.py (pipeline orchestration).
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

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
    # -----------------------------------------------------------------------
    # Existing books (Quran + Al-Kafi) — parsed by dedicated parsers
    # -----------------------------------------------------------------------
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

    # -----------------------------------------------------------------------
    # ThaqalaynAPI books — parsed by thaqalayn_api.py transformer
    # -----------------------------------------------------------------------

    # The Four Books (completing the set)
    BookConfig(
        slug="man-la-yahduruhu-al-faqih",
        index=3,
        path="/books/man-la-yahduruhu-al-faqih",
        titles={
            Language.EN.value: "Man La Yahduruhu al-Faqih",
            Language.AR.value: "\u0645\u0646 \u0644\u0627 \u064a\u062d\u0636\u0631\u0647 \u0627\u0644\u0641\u0642\u064a\u0647",
        },
        author={
            Language.EN.value: "Shaykh al-Saduq",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0635\u062f\u0648\u0642",
        },
        source_url="https://thaqalayn.net/",
    ),

    # Primary Hadith Collections
    BookConfig(
        slug="nahj-al-balagha",
        index=4,
        path="/books/nahj-al-balagha",
        titles={
            Language.EN.value: "Nahj al-Balagha",
            Language.AR.value: "\u0646\u0647\u062c \u0627\u0644\u0628\u0644\u0627\u063a\u0629",
        },
        author={
            Language.EN.value: "Sharif al-Radi",
            Language.AR.value: "\u0627\u0644\u0634\u0631\u064a\u0641 \u0627\u0644\u0631\u0636\u064a",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="al-amali-mufid",
        index=5,
        path="/books/al-amali-mufid",
        titles={
            Language.EN.value: "Al-Amali (al-Mufid)",
            Language.AR.value: "\u0627\u0644\u0623\u0645\u0627\u0644\u064a (\u0627\u0644\u0645\u0641\u064a\u062f)",
        },
        author={
            Language.EN.value: "Shaykh al-Mufid",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0645\u0641\u064a\u062f",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="al-amali-saduq",
        index=6,
        path="/books/al-amali-saduq",
        titles={
            Language.EN.value: "Al-Amali (al-Saduq)",
            Language.AR.value: "\u0627\u0644\u0623\u0645\u0627\u0644\u064a (\u0627\u0644\u0635\u062f\u0648\u0642)",
        },
        author={
            Language.EN.value: "Shaykh al-Saduq",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0635\u062f\u0648\u0642",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="kamil-al-ziyarat",
        index=7,
        path="/books/kamil-al-ziyarat",
        titles={
            Language.EN.value: "Kamil al-Ziyarat",
            Language.AR.value: "\u0643\u0627\u0645\u0644 \u0627\u0644\u0632\u064a\u0627\u0631\u0627\u062a",
        },
        author={
            Language.EN.value: "Ibn Qulawayh al-Qummi",
            Language.AR.value: "\u0627\u0628\u0646 \u0642\u0648\u0644\u0648\u064a\u0647 \u0627\u0644\u0642\u0645\u064a",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="kitab-al-ghayba-numani",
        index=8,
        path="/books/kitab-al-ghayba-numani",
        titles={
            Language.EN.value: "Kitab al-Ghayba (al-Nu'mani)",
            Language.AR.value: "\u0643\u062a\u0627\u0628 \u0627\u0644\u063a\u064a\u0628\u0629 (\u0627\u0644\u0646\u0639\u0645\u0627\u0646\u064a)",
        },
        author={
            Language.EN.value: "Muhammad ibn Ibrahim al-Nu'mani",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="kitab-al-ghayba-tusi",
        index=9,
        path="/books/kitab-al-ghayba-tusi",
        titles={
            Language.EN.value: "Kitab al-Ghayba (al-Tusi)",
            Language.AR.value: "\u0643\u062a\u0627\u0628 \u0627\u0644\u063a\u064a\u0628\u0629 (\u0627\u0644\u0637\u0648\u0633\u064a)",
        },
        author={
            Language.EN.value: "Shaykh al-Tusi",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0637\u0648\u0633\u064a",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="kitab-al-mumin",
        index=10,
        path="/books/kitab-al-mumin",
        titles={
            Language.EN.value: "Kitab al-Mu'min",
            Language.AR.value: "\u0643\u062a\u0627\u0628 \u0627\u0644\u0645\u0624\u0645\u0646",
        },
        author={
            Language.EN.value: "Husayn ibn Sa'id al-Ahwazi",
        },
        source_url="https://thaqalayn.net/",
    ),

    # Additional collections
    BookConfig(
        slug="al-tawhid",
        index=11,
        path="/books/al-tawhid",
        titles={
            Language.EN.value: "Al-Tawhid",
            Language.AR.value: "\u0627\u0644\u062a\u0648\u062d\u064a\u062f",
        },
        author={
            Language.EN.value: "Shaykh al-Saduq",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0635\u062f\u0648\u0642",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="uyun-akhbar-al-rida",
        index=12,
        path="/books/uyun-akhbar-al-rida",
        titles={
            Language.EN.value: "Uyun Akhbar al-Rida",
            Language.AR.value: "\u0639\u064a\u0648\u0646 \u0623\u062e\u0628\u0627\u0631 \u0627\u0644\u0631\u0636\u0627",
        },
        author={
            Language.EN.value: "Shaykh al-Saduq",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0635\u062f\u0648\u0642",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="al-khisal",
        index=13,
        path="/books/al-khisal",
        titles={
            Language.EN.value: "Al-Khisal",
            Language.AR.value: "\u0627\u0644\u062e\u0635\u0627\u0644",
        },
        author={
            Language.EN.value: "Shaykh al-Saduq",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0635\u062f\u0648\u0642",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="maani-al-akhbar",
        index=14,
        path="/books/maani-al-akhbar",
        titles={
            Language.EN.value: "Ma'ani al-Akhbar",
            Language.AR.value: "\u0645\u0639\u0627\u0646\u064a \u0627\u0644\u0623\u062e\u0628\u0627\u0631",
        },
        author={
            Language.EN.value: "Shaykh al-Saduq",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0635\u062f\u0648\u0642",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="kamal-al-din",
        index=15,
        path="/books/kamal-al-din",
        titles={
            Language.EN.value: "Kamal al-Din wa Tamam al-Ni'ma",
            Language.AR.value: "\u0643\u0645\u0627\u0644 \u0627\u0644\u062f\u064a\u0646 \u0648\u062a\u0645\u0627\u0645 \u0627\u0644\u0646\u0639\u0645\u0629",
        },
        author={
            Language.EN.value: "Shaykh al-Saduq",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0635\u062f\u0648\u0642",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="thawab-al-amal",
        index=16,
        path="/books/thawab-al-amal",
        titles={
            Language.EN.value: "Thawab al-A'mal wa 'Iqab al-A'mal",
            Language.AR.value: "\u062b\u0648\u0627\u0628 \u0627\u0644\u0623\u0639\u0645\u0627\u0644 \u0648\u0639\u0642\u0627\u0628 \u0627\u0644\u0623\u0639\u0645\u0627\u0644",
        },
        author={
            Language.EN.value: "Shaykh al-Saduq",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0635\u062f\u0648\u0642",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="kitab-al-zuhd",
        index=17,
        path="/books/kitab-al-zuhd",
        titles={
            Language.EN.value: "Kitab al-Zuhd",
            Language.AR.value: "\u0643\u062a\u0627\u0628 \u0627\u0644\u0632\u0647\u062f",
        },
        author={
            Language.EN.value: "Husayn ibn Sa'id al-Ahwazi",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="risalat-al-huquq",
        index=18,
        path="/books/risalat-al-huquq",
        titles={
            Language.EN.value: "Risalat al-Huquq",
            Language.AR.value: "\u0631\u0633\u0627\u0644\u0629 \u0627\u0644\u062d\u0642\u0648\u0642",
        },
        author={
            Language.EN.value: "Imam Zayn al-Abidin",
            Language.AR.value: "\u0627\u0644\u0625\u0645\u0627\u0645 \u0632\u064a\u0646 \u0627\u0644\u0639\u0627\u0628\u062f\u064a\u0646",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="fadail-al-shia",
        index=19,
        path="/books/fadail-al-shia",
        titles={
            Language.EN.value: "Fada'il al-Shi'a",
            Language.AR.value: "\u0641\u0636\u0627\u0626\u0644 \u0627\u0644\u0634\u064a\u0639\u0629",
        },
        author={
            Language.EN.value: "Shaykh al-Saduq",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0635\u062f\u0648\u0642",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="sifat-al-shia",
        index=20,
        path="/books/sifat-al-shia",
        titles={
            Language.EN.value: "Sifat al-Shi'a",
            Language.AR.value: "\u0635\u0641\u0627\u062a \u0627\u0644\u0634\u064a\u0639\u0629",
        },
        author={
            Language.EN.value: "Shaykh al-Saduq",
            Language.AR.value: "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0635\u062f\u0648\u0642",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="kitab-al-duafa",
        index=21,
        path="/books/kitab-al-duafa",
        titles={
            Language.EN.value: "Kitab al-Du'afa'",
            Language.AR.value: "\u0643\u062a\u0627\u0628 \u0627\u0644\u0636\u0639\u0641\u0627\u0621",
        },
        author={
            Language.EN.value: "Ahmad ibn al-Husayn al-Ghada'iri",
        },
        source_url="https://thaqalayn.net/",
    ),
    BookConfig(
        slug="mujam-al-ahadith-al-mutabara",
        index=22,
        path="/books/mujam-al-ahadith-al-mutabara",
        titles={
            Language.EN.value: "Mu'jam al-Ahadith al-Mu'tabara",
            Language.AR.value: "\u0645\u0639\u062c\u0645 \u0627\u0644\u0623\u062d\u0627\u062f\u064a\u062b \u0627\u0644\u0645\u0639\u062a\u0628\u0631\u0629",
        },
        author={
            Language.EN.value: "Muhammad Asif al-Muhsini",
        },
        source_url="https://thaqalayn.net/",
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
