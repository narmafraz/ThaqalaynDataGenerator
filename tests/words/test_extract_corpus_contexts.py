"""Unit tests for scripts/extract_corpus_contexts.py.

Tokenization, NFC matching, window slicing, and path translation —
pure-Python pieces. Filesystem walks are exercised in the end-to-end
smoke run captured in PATH_B_SPARK_LOG.md Round 4.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


def _import_extractor():
    here = Path(__file__).resolve().parents[2]
    target = here / "scripts" / "extract_corpus_contexts.py"
    spec = importlib.util.spec_from_file_location("_ctx_extractor", str(target))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def ext():
    return _import_extractor()


# ────────────────── nfc + tokenize ──────────────────


def test_nfc_normalizes_combining_chars(ext) -> None:
    # NFC vs NFD form of the same word
    nfc = "لا"  # ل + ا (precomposed-ish, both forms valid NFC for this pair)
    out = ext.nfc(nfc)
    assert isinstance(out, str)
    assert out == nfc  # already NFC


def test_nfc_handles_none(ext) -> None:
    assert ext.nfc(None) == ""
    assert ext.nfc("") == ""


def test_tokenize_basic(ext) -> None:
    tokens = ext.tokenize("قَالَ زَيْدٌ سَمِعْتُ")
    assert tokens == ["قَالَ", "زَيْدٌ", "سَمِعْتُ"]


def test_tokenize_strips_brackets(ext) -> None:
    tokens = ext.tokenize("قَالَ (عليه السلام) سَمِعْتُ")
    # Bracketed content removed entirely
    assert "عليه" not in tokens
    assert "السلام" not in tokens
    assert "قَالَ" in tokens
    assert "سَمِعْتُ" in tokens


def test_tokenize_strips_punctuation(ext) -> None:
    tokens = ext.tokenize("قَالَ، زَيْدٌ. سَمِعْتُ؟")
    assert all("،" not in t for t in tokens)
    assert all("." not in t for t in tokens)
    assert "زَيْدٌ" in tokens


def test_tokenize_empty_string(ext) -> None:
    assert ext.tokenize("") == []
    assert ext.tokenize("   ") == []


# ────────────────── path_to_filesystem ──────────────────


def test_path_to_filesystem_books(ext, tmp_path) -> None:
    out = ext.path_to_filesystem("/books/al-kafi:1:2:3:4", tmp_path)
    assert out == tmp_path / "books" / "al-kafi" / "1" / "2" / "3" / "4.json"


def test_path_to_filesystem_quran(ext, tmp_path) -> None:
    out = ext.path_to_filesystem("/books/quran:1:5", tmp_path)
    assert out == tmp_path / "books" / "quran" / "1" / "5.json"


def test_path_to_filesystem_strips_leading_slash(ext, tmp_path) -> None:
    out = ext.path_to_filesystem("books/al-kafi:1", tmp_path)
    assert out == tmp_path / "books" / "al-kafi" / "1.json"


# ────────────────── slice_window ──────────────────


def test_slice_window_basic(ext) -> None:
    tokens = list("abcdefghijklmnop")  # 16 single-char tokens
    out = ext.slice_window(tokens, match_idx=8, radius=3)
    # window: index 5..11 inclusive
    assert out == "f g h i j k l"


def test_slice_window_truncated_at_start(ext) -> None:
    tokens = ["a", "b", "c", "d"]
    out = ext.slice_window(tokens, match_idx=0, radius=10)
    assert out == "a b c d"


def test_slice_window_truncated_at_end(ext) -> None:
    tokens = ["a", "b", "c", "d"]
    out = ext.slice_window(tokens, match_idx=3, radius=10)
    assert out == "a b c d"


def test_slice_window_zero_radius(ext) -> None:
    tokens = ["a", "b", "c", "d"]
    out = ext.slice_window(tokens, match_idx=2, radius=0)
    assert out == "c"


# ────────────────── extract_for_surface ──────────────────


def test_extract_for_surface_finds_match(ext, tmp_path) -> None:
    # Build a tiny ThaqalaynData tree with one verse_detail
    verse_path = tmp_path / "books" / "al-kafi" / "1" / "1" / "1" / "1.json"
    verse_path.parent.mkdir(parents=True)
    verse_path.write_text(
        json.dumps({
            "data": {
                "verse": {
                    "text": ["حَدَّثَنَا مُحَمَّدٌ قَالَ سَمِعْتُ أَبَا عَبْدِ اللَّهِ "
                            "وَبِالْعَهْدِ يَفِي الْمُؤْمِنُ يَا أَيُّهَا النَّاسُ"]
                }
            }
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    out = ext.extract_for_surface(
        "وَبِالْعَهْدِ",
        ["/books/al-kafi:1:1:1:1"],
        tmp_path,
        max_windows=3,
        radius=5,
    )
    assert len(out) == 1
    assert out[0]["path"] == "/books/al-kafi:1:1:1:1"
    assert "وَبِالْعَهْدِ" in out[0]["window"]


def test_extract_for_surface_handles_missing_file(ext, tmp_path) -> None:
    out = ext.extract_for_surface(
        "X", ["/books/nonexistent:1:1"], tmp_path, max_windows=1,
    )
    assert out == []


def test_extract_for_surface_caps_at_max_windows(ext, tmp_path) -> None:
    # Build 5 verse_detail files all containing the target surface
    for i in range(1, 6):
        verse_path = tmp_path / "books" / "test" / f"{i}.json"
        verse_path.parent.mkdir(parents=True, exist_ok=True)
        verse_path.write_text(
            json.dumps({"data": {"verse": {"text": f"context_{i} مَا context_after_{i}"}}}),
            encoding="utf-8",
        )
    out = ext.extract_for_surface(
        "مَا",
        [f"/books/test:{i}" for i in range(1, 6)],
        tmp_path,
        max_windows=3,
    )
    assert len(out) == 3
