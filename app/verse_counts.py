"""Build the verse-counts manifest consumed by the Angular reading-progress tracker.

Output file: `<DESTINATION_DIR>/index/verse-counts.json`

Schema:
    {
      "<book-slug>": {
        "total": <int>,                                  # countable verses in book
        "by_chapter": {"<chapter-index>": <int>, ...},   # per-chapter counts
      },
      ...
    }

Each `<chapter-index>` is the JSON `index` field of a `verse_list` shell file
(e.g. "al-kafi:1:1:1", "quran:2"). The frontend rolls these up to
volume / surah granularity by prefix-matching keys.

Only entries with `part_type` of `Hadith` or `Verse` are counted; `Heading`
entries (~43 across the corpus) are skipped because they are not navigable
units a reader would mark as read.
"""

from __future__ import annotations

import json
from pathlib import Path


COUNTED_PART_TYPES = {"Hadith", "Verse"}
SKIP_DIRS = {"complete"}  # books/complete/ holds aggregated full-text dumps


def _chapter_count(chapter_json: dict) -> int:
    if chapter_json.get("kind") != "verse_list":
        return 0
    data = chapter_json.get("data") or {}
    refs = data.get("verse_refs")
    if refs is None:
        # legacy inline format (no shell)
        refs = data.get("verses") or []
    return sum(1 for r in refs if r.get("part_type") in COUNTED_PART_TYPES)


def build(data_root: Path) -> dict:
    """Walk `<data_root>/books/*/` and return the manifest dict."""
    books_dir = data_root / "books"
    out: dict[str, dict] = {}

    for book_dir in sorted(books_dir.iterdir()):
        if not book_dir.is_dir() or book_dir.name in SKIP_DIRS:
            continue

        slug = book_dir.name
        by_chapter: dict[str, int] = {}
        total = 0

        for json_path in book_dir.rglob("*.json"):
            try:
                with open(json_path, encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue

            if data.get("kind") != "verse_list":
                continue

            n = _chapter_count(data)
            if n == 0:
                continue
            idx = data.get("index")
            if not idx:
                continue
            by_chapter[idx] = n
            total += n

        if total > 0:
            out[slug] = {"total": total, "by_chapter": by_chapter}

    return out


def write_manifest(data_root: Path, out_path: Path | None = None) -> Path:
    """Build the manifest and write it to `out_path`. Returns the path written."""
    data_root = Path(data_root).resolve()
    if not (data_root / "books").is_dir():
        raise FileNotFoundError(f"ThaqalaynData/books not found under {data_root}")

    out_path = out_path or (data_root / "index" / "verse-counts.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    manifest = build(data_root)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, separators=(",", ":"))

    return out_path
