"""Tests for the verse-counts manifest builder."""

from __future__ import annotations

import json
from pathlib import Path

from app.verse_counts import _chapter_count, build, write_manifest


def _shell(index: str, refs: list[dict]) -> dict:
    return {
        "index": index,
        "kind": "verse_list",
        "data": {"verse_refs": refs},
    }


def _ref(part_type: str, local_index: int = 1, path: str = "") -> dict:
    out = {"local_index": local_index, "part_type": part_type}
    if path:
        out["path"] = path
    return out


def _write(p: Path, obj: dict) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f)


def test_chapter_count_counts_hadith_and_verse_only():
    chapter = _shell("foo:1:1", [
        _ref("Heading"),
        _ref("Hadith", path="/books/foo:1:1:1"),
        _ref("Hadith", path="/books/foo:1:1:2"),
        _ref("Verse", path="/books/foo:1:1:3"),
        _ref("Heading"),
    ])
    assert _chapter_count(chapter) == 3


def test_chapter_count_supports_legacy_inline_verses():
    chapter = {
        "index": "legacy:1",
        "kind": "verse_list",
        "data": {"verses": [_ref("Hadith"), _ref("Hadith"), _ref("Heading")]},
    }
    assert _chapter_count(chapter) == 2


def test_chapter_count_returns_zero_for_non_verse_list_kinds():
    assert _chapter_count({"kind": "chapter_list", "data": {}}) == 0
    assert _chapter_count({"kind": "verse_detail", "data": {}}) == 0
    assert _chapter_count({}) == 0


def test_build_aggregates_across_books_and_chapters(tmp_path: Path):
    # Book 1 with two chapters
    _write(tmp_path / "books" / "alpha" / "1" / "1.json",
           _shell("alpha:1:1", [_ref("Hadith"), _ref("Hadith"), _ref("Heading")]))
    _write(tmp_path / "books" / "alpha" / "1" / "2.json",
           _shell("alpha:1:2", [_ref("Hadith")]))

    # Book 2 — Quran-style 2-level
    _write(tmp_path / "books" / "beta" / "1.json",
           _shell("beta:1", [_ref("Verse"), _ref("Verse"), _ref("Verse")]))

    # complete/ dir must be skipped
    _write(tmp_path / "books" / "complete" / "alpha.json",
           _shell("alpha", [_ref("Hadith")] * 50))

    # Non-verse_list (chapter list) must be skipped
    _write(tmp_path / "books" / "alpha" / "1.json",
           {"index": "alpha:1", "kind": "chapter_list", "data": {}})

    out = build(tmp_path)
    assert set(out.keys()) == {"alpha", "beta"}
    assert out["alpha"]["total"] == 3
    assert out["alpha"]["by_chapter"] == {"alpha:1:1": 2, "alpha:1:2": 1}
    assert out["beta"]["total"] == 3
    assert out["beta"]["by_chapter"] == {"beta:1": 3}


def test_build_skips_books_with_zero_countable_verses(tmp_path: Path):
    _write(tmp_path / "books" / "empty-book" / "1.json",
           _shell("empty-book:1", [_ref("Heading"), _ref("Heading")]))
    out = build(tmp_path)
    assert "empty-book" not in out


def test_build_tolerates_malformed_json(tmp_path: Path):
    _write(tmp_path / "books" / "ok" / "1.json",
           _shell("ok:1", [_ref("Hadith")]))
    bad = tmp_path / "books" / "ok" / "broken.json"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text("{ not valid json", encoding="utf-8")

    out = build(tmp_path)
    assert out["ok"]["total"] == 1


def test_build_ignores_chapter_with_missing_index(tmp_path: Path):
    chapter = _shell("ok:1", [_ref("Hadith")])
    chapter.pop("index")
    _write(tmp_path / "books" / "ok" / "1.json", chapter)
    out = build(tmp_path)
    assert out == {}


def test_write_manifest_creates_index_dir_and_returns_path(tmp_path: Path):
    _write(tmp_path / "books" / "ok" / "1.json",
           _shell("ok:1", [_ref("Hadith"), _ref("Verse")]))

    out_path = write_manifest(tmp_path)

    assert out_path == tmp_path / "index" / "verse-counts.json"
    assert out_path.is_file()
    with open(out_path, encoding="utf-8") as f:
        data = json.load(f)
    assert data["ok"]["total"] == 2


def test_write_manifest_raises_when_books_dir_missing(tmp_path: Path):
    import pytest
    with pytest.raises(FileNotFoundError):
        write_manifest(tmp_path)


def test_write_manifest_emits_compact_utf8_arabic(tmp_path: Path):
    """Arabic in chapter indexes (rare but possible) survives round-trip and is not escaped."""
    chapter = _shell("ok:كتاب", [_ref("Hadith")])
    _write(tmp_path / "books" / "ok" / "1.json", chapter)
    out_path = write_manifest(tmp_path)
    raw = out_path.read_text(encoding="utf-8")
    assert "كتاب" in raw  # not \uXXXX-escaped
    assert "\n" not in raw  # compact
