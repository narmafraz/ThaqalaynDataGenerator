"""
Data validation tests for generated ThaqalaynData JSON output.

These tests validate the actual JSON files in ../ThaqalaynData/ to ensure
schema correctness, UTF-8 integrity, navigation consistency, cross-references,
narrator chain validity, and index file well-formedness.

All tests are skipped if DESTINATION_DIR is not set or the data directory
does not contain generated files.
"""
import json
import os
import re

import pytest

from app.lib_db import get_destination_dir

# ---------------------------------------------------------------------------
# Skip guard – every test in this module requires generated data
# ---------------------------------------------------------------------------
DATA_DIR = get_destination_dir()
_has_data = bool(DATA_DIR) and os.path.isfile(
    os.path.join(DATA_DIR, "books", "books.json")
)

pytestmark = pytest.mark.skipif(
    not _has_data,
    reason="Requires generated ThaqalaynData (set DESTINATION_DIR)",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_json(rel_path: str):
    """Load a JSON file relative to DATA_DIR."""
    with open(os.path.join(DATA_DIR, rel_path), "r", encoding="utf-8") as f:
        return json.load(f)


def _path_to_file(path: str) -> str:
    """Convert a canonical path like /books/quran:1 to a filesystem path."""
    sanitised = path.replace(":", "/")
    if sanitised.startswith("/"):
        sanitised = sanitised[1:]
    return os.path.join(DATA_DIR, sanitised + ".json")


def _file_exists(path: str) -> bool:
    """Check whether a canonical path resolves to an existing JSON file."""
    return os.path.isfile(_path_to_file(path))


VALID_KINDS = {"chapter_list", "verse_list", "person_content", "person_list"}
VALID_PART_TYPES = {"Book", "Volume", "Chapter", "Hadith", "Verse", "Heading"}


# =========================================================================
# 1. Schema validation – JSON wrapper
# =========================================================================

class TestSchemaWrapper:
    """Every JSON file must have {index, kind, data} with valid kind."""

    def test_books_json_wrapper(self):
        data = _load_json("books/books.json")
        assert "index" in data
        assert "kind" in data
        assert "data" in data
        assert data["kind"] in VALID_KINDS

    def test_quran_metadata_wrapper(self):
        data = _load_json("books/quran.json")
        assert data["kind"] == "chapter_list"
        assert "chapters" in data["data"]

    def test_alkafi_metadata_wrapper(self):
        data = _load_json("books/al-kafi.json")
        assert data["kind"] == "chapter_list"
        assert "chapters" in data["data"]

    def test_quran_chapter_wrapper(self):
        """Spot-check quran chapter file for correct wrapper."""
        data = _load_json("books/quran/1.json")
        assert data["kind"] in VALID_KINDS
        assert "index" in data
        assert "data" in data
        assert "verses" in data["data"] or "chapters" in data["data"]

    def test_alkafi_chapter_wrapper(self):
        """Spot-check al-kafi chapter file."""
        data = _load_json("books/al-kafi/1/1/1.json")
        assert data["kind"] in VALID_KINDS
        assert "index" in data
        assert "data" in data

    @pytest.mark.parametrize("sura", [1, 2, 36, 67, 114])
    def test_quran_sura_files_have_valid_wrapper(self, sura):
        path = f"books/quran/{sura}.json"
        if not os.path.isfile(os.path.join(DATA_DIR, path)):
            pytest.skip(f"Sura {sura} file not found")
        data = _load_json(path)
        assert data["kind"] in VALID_KINDS
        assert "data" in data

    def test_narrator_file_wrapper(self):
        data = _load_json("people/narrators/1.json")
        assert data["kind"] == "person_content"
        assert "index" in data
        assert "data" in data


# =========================================================================
# 2. UTF-8 integrity – Arabic text must not be escaped
# =========================================================================

class TestUtf8Integrity:
    """Arabic text must be stored as actual Unicode, not \\uXXXX escapes."""

    def test_quran_verse_has_arabic_text(self):
        data = _load_json("books/quran/1.json")
        verse = data["data"]["verses"][0]
        text = verse["text"][0]
        # Must contain Arabic characters directly (not escaped)
        assert re.search(r"[\u0600-\u06FF]", text), \
            f"Expected Arabic characters in verse text, got: {text!r}"

    def test_alkafi_hadith_has_arabic_text(self):
        data = _load_json("books/al-kafi/1/1/1.json")
        verse = data["data"]["verses"][0]
        text = verse["text"][0]
        assert re.search(r"[\u0600-\u06FF]", text), \
            f"Expected Arabic characters in hadith text, got: {text!r}"

    def test_alkafi_narrator_chain_has_arabic(self):
        data = _load_json("books/al-kafi/1/1/1.json")
        verse = data["data"]["verses"][0]
        chain = verse.get("narrator_chain", {})
        chain_text = chain.get("text", "")
        assert re.search(r"[\u0600-\u06FF]", chain_text), \
            "Narrator chain text should contain Arabic characters"

    def test_quran_raw_file_no_unicode_escapes(self):
        """Read raw file bytes and verify no \\uXXXX for Arabic code points."""
        path = os.path.join(DATA_DIR, "books", "quran", "1.json")
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
        # Should not have \\u06XX style escapes for Arabic
        arabic_escapes = re.findall(r"\\u06[0-9a-fA-F]{2}", raw)
        assert len(arabic_escapes) == 0, \
            f"Found escaped Arabic characters: {arabic_escapes[:5]}"

    def test_narrator_title_has_arabic(self):
        data = _load_json("people/narrators/1.json")
        titles = data["data"]["titles"]
        assert "ar" in titles
        assert re.search(r"[\u0600-\u06FF]", titles["ar"]), \
            "Narrator Arabic title should contain Arabic characters"


# =========================================================================
# 3. Navigation links – nav prev/next/up point to real files
# =========================================================================

class TestNavigationLinks:
    """chapter.nav prev/next/up must point to files that exist."""

    def _check_nav(self, data_obj, context=""):
        nav = data_obj.get("nav")
        if not nav:
            return
        errors = []
        for direction in ("prev", "next", "up"):
            target = nav.get(direction)
            if target and not _file_exists(target):
                errors.append(f"{context} nav.{direction}={target} file missing")
        assert not errors, "\n".join(errors)

    def test_quran_sura1_nav(self):
        data = _load_json("books/quran/1.json")["data"]
        self._check_nav(data, "quran:1")
        # Sura 1 should have no prev, should have next
        assert "prev" not in data.get("nav", {}) or data["nav"]["prev"] is None
        assert data["nav"]["next"] == "/books/quran:2"

    def test_quran_sura114_nav(self):
        data = _load_json("books/quran/114.json")["data"]
        self._check_nav(data, "quran:114")
        # Last sura should have prev, no next
        assert data["nav"]["prev"] == "/books/quran:113"
        assert "next" not in data.get("nav", {}) or data["nav"].get("next") is None

    def test_quran_middle_sura_nav(self):
        data = _load_json("books/quran/36.json")["data"]
        self._check_nav(data, "quran:36")
        assert data["nav"]["prev"] == "/books/quran:35"
        assert data["nav"]["next"] == "/books/quran:37"

    def test_alkafi_chapter_nav_up(self):
        data = _load_json("books/al-kafi/1/1/1.json")["data"]
        self._check_nav(data, "al-kafi:1:1:1")
        assert data["nav"]["up"] == "/books/al-kafi:1:1"

    @pytest.mark.parametrize("sura", range(1, 115))
    def test_all_quran_suras_nav_valid(self, sura):
        path = f"books/quran/{sura}.json"
        if not os.path.isfile(os.path.join(DATA_DIR, path)):
            pytest.skip(f"Sura {sura} not found")
        data = _load_json(path)["data"]
        self._check_nav(data, f"quran:{sura}")


# =========================================================================
# 4. Cross-references – relations fields reference valid paths
# =========================================================================

class TestCrossReferences:
    """Quran verse 'Mentioned In' and hadith 'Mentions' must point to real files."""

    def _collect_relations(self, data_obj):
        """Collect all relation target paths from a verse."""
        relations = data_obj.get("relations")
        if not relations:
            return []
        targets = []
        for relation_type, paths in relations.items():
            if isinstance(paths, list):
                targets.extend(paths)
            elif isinstance(paths, set):
                targets.extend(paths)
        return targets

    def test_quran_sura1_verse1_relations_valid(self):
        data = _load_json("books/quran/1.json")["data"]
        verse = data["verses"][0]
        targets = self._collect_relations(verse)
        for target in targets:
            # Relations point to hadith paths which are deeply nested
            # They may point to /books/al-kafi:V:B:C:H which maps to
            # books/al-kafi/V/B/C.json (the verse is within the chapter file)
            # So we check the chapter file exists (strip last segment)
            parts = target.rsplit(":", 1)
            chapter_path = parts[0] if len(parts) > 1 else target
            assert _file_exists(chapter_path), \
                f"Relation target chapter {chapter_path} (from {target}) not found"

    def test_alkafi_hadith_mentions_valid(self):
        """Check that 'Mentions' relations in al-kafi hadiths point to real Quran files."""
        data = _load_json("books/al-kafi/1/1/1.json")["data"]
        for verse in data.get("verses", []):
            relations = verse.get("relations")
            if not relations:
                continue
            mentions = relations.get("Mentions", [])
            for target in mentions:
                # Mentions point to /books/quran:S:V — chapter file is /books/quran:S
                parts = target.rsplit(":", 1)
                chapter_path = parts[0] if len(parts) > 1 else target
                assert _file_exists(chapter_path), \
                    f"Hadith at {verse.get('path')} mentions {target} but {chapter_path} not found"

    def test_sample_quran_suras_relations_valid(self):
        """Spot-check several suras for valid relation targets."""
        for sura in [1, 2, 3, 4, 5, 36, 112]:
            path = f"books/quran/{sura}.json"
            if not os.path.isfile(os.path.join(DATA_DIR, path)):
                continue
            data = _load_json(path)["data"]
            for verse in data.get("verses", []):
                targets = self._collect_relations(verse)
                for target in targets:
                    parts = target.rsplit(":", 1)
                    chapter_path = parts[0] if len(parts) > 1 else target
                    assert _file_exists(chapter_path), \
                        f"quran:{sura} verse relation to {target}: chapter file {chapter_path} missing"


# =========================================================================
# 5. Narrator chain refs – narrator IDs correspond to files
# =========================================================================

class TestNarratorChains:
    """Narrator parts in hadith chains must point to existing narrator files."""

    def test_alkafi_1_1_1_narrator_refs_valid(self):
        data = _load_json("books/al-kafi/1/1/1.json")["data"]
        errors = []
        for verse in data.get("verses", []):
            chain = verse.get("narrator_chain")
            if not chain:
                continue
            for part in chain.get("parts", []):
                if part["kind"] == "narrator":
                    narrator_path = part["path"]
                    # Path is like /people/narrators/1 -> people/narrators/1.json
                    file_path = _path_to_file(narrator_path)
                    if not os.path.isfile(file_path):
                        errors.append(
                            f"Verse {verse.get('path')}: narrator {narrator_path} file missing"
                        )
        assert not errors, "\n".join(errors)

    def test_narrator_chain_parts_have_valid_kinds(self):
        data = _load_json("books/al-kafi/1/1/1.json")["data"]
        for verse in data.get("verses", []):
            chain = verse.get("narrator_chain")
            if not chain:
                continue
            for part in chain.get("parts", []):
                assert part["kind"] in ("narrator", "plain"), \
                    f"Invalid chain part kind: {part['kind']} in {verse.get('path')}"

    def test_narrator_chain_has_text(self):
        data = _load_json("books/al-kafi/1/1/1.json")["data"]
        for verse in data.get("verses", []):
            chain = verse.get("narrator_chain")
            if not chain:
                continue
            assert "text" in chain, \
                f"Narrator chain in {verse.get('path')} missing 'text' field"
            assert len(chain["text"]) > 0, \
                f"Narrator chain in {verse.get('path')} has empty text"

    def test_narrator_file_has_required_fields(self):
        data = _load_json("people/narrators/1.json")
        assert "index" in data
        assert "kind" in data
        assert data["kind"] == "person_content"
        narrator = data["data"]
        assert "titles" in narrator
        assert "verse_paths" in narrator
        assert "path" in narrator
        assert isinstance(narrator["verse_paths"], list)

    def test_narrator_verse_paths_reference_real_chapters(self):
        """Verify narrator verse_paths point to chapters that exist."""
        data = _load_json("people/narrators/1.json")["data"]
        for vpath in data.get("verse_paths", []):
            # Verse path like /books/al-kafi:1:1:1:1 -> chapter is /books/al-kafi:1:1:1
            parts = vpath.rsplit(":", 1)
            chapter_path = parts[0] if len(parts) > 1 else vpath
            assert _file_exists(chapter_path), \
                f"Narrator 1 verse_path {vpath}: chapter {chapter_path} not found"

    def test_narrator_subchains_ids_consistent(self):
        """Verify subchain keys match their narrator_ids arrays."""
        data = _load_json("people/narrators/1.json")["data"]
        for key, chain_data in data.get("subchains", {}).items():
            expected_ids = [int(x) for x in key.split("-")]
            assert chain_data["narrator_ids"] == expected_ids, \
                f"Subchain key '{key}' does not match narrator_ids {chain_data['narrator_ids']}"

    def test_sample_narrator_files_exist(self):
        """Check that a sample of narrator files exist and are well-formed."""
        for nid in [1, 2, 5, 10, 50, 100]:
            path = f"people/narrators/{nid}.json"
            fpath = os.path.join(DATA_DIR, path)
            if not os.path.isfile(fpath):
                continue
            data = _load_json(path)
            assert data["kind"] == "person_content"
            assert "data" in data
            assert "titles" in data["data"]


# =========================================================================
# 6. Index files – well-formed
# =========================================================================

class TestIndexFiles:
    """Index files must be well-formed JSON with expected structure."""

    def test_translations_json_structure(self):
        data = _load_json("index/translations.json")
        assert isinstance(data, dict)
        assert len(data) > 0
        for tid, tinfo in data.items():
            assert "id" in tinfo
            assert "lang" in tinfo
            assert "name" in tinfo
            assert tinfo["id"] == tid

    def test_translations_cover_expected_languages(self):
        data = _load_json("index/translations.json")
        langs = {t["lang"] for t in data.values()}
        assert "en" in langs, "No English translations found"
        assert "fa" in langs, "No Farsi translations found"

    def test_books_en_json_structure(self):
        data = _load_json("index/books.en.json")
        assert isinstance(data, dict)
        assert len(data) > 0
        # Should contain top-level book paths
        assert "/books/al-kafi" in data
        assert "/books/al-kafi:1" in data

    def test_books_ar_json_structure(self):
        data = _load_json("index/books.ar.json")
        assert isinstance(data, dict)
        assert len(data) > 0

    def test_books_en_entries_have_title(self):
        data = _load_json("index/books.en.json")
        errors = []
        for path, entry in data.items():
            if "title" not in entry:
                errors.append(f"{path} missing 'title'")
        assert not errors, "\n".join(errors[:20])

    def test_books_en_entries_reference_valid_paths(self):
        """Each path in the books index should correspond to a real file."""
        data = _load_json("index/books.en.json")
        missing = []
        for path in data:
            if not _file_exists(path):
                missing.append(path)
        assert not missing, \
            f"{len(missing)} index paths have no file: {missing[:10]}"

    def test_quran_verse_translations_reference_known_ids(self):
        """Verse translations in quran chapters should reference known translation IDs."""
        translations = _load_json("index/translations.json")
        data = _load_json("books/quran/1.json")["data"]
        unknown = []
        for tid in data.get("verse_translations", []):
            if tid not in translations:
                unknown.append(tid)
        assert not unknown, f"Unknown translation IDs: {unknown}"


# =========================================================================
# 7. Complete files
# =========================================================================

class TestCompleteFiles:
    """Complete aggregated files must exist and have valid structure."""

    def test_complete_quran_exists(self):
        path = os.path.join(DATA_DIR, "books", "complete", "quran.json")
        assert os.path.isfile(path), "books/complete/quran.json not found"

    def test_complete_alkafi_exists(self):
        path = os.path.join(DATA_DIR, "books", "complete", "al-kafi.json")
        assert os.path.isfile(path), "books/complete/al-kafi.json not found"

    def test_complete_quran_has_chapters(self):
        data = _load_json("books/complete/quran.json")
        # Complete files may store data directly (no wrapper) or with wrapper
        content = data.get("data", data)
        chapters = content.get("chapters", [])
        assert len(chapters) == 114, f"Expected 114 suras, got {len(chapters)}"

    def test_complete_alkafi_has_volumes(self):
        data = _load_json("books/complete/al-kafi.json")
        content = data.get("data", data)
        chapters = content.get("chapters", [])
        assert len(chapters) == 8, f"Expected 8 volumes, got {len(chapters)}"


# =========================================================================
# 8. Snapshot tests – key chapters
# =========================================================================

class TestSnapshotKeyChapters:
    """Verify key structural properties of important chapters."""

    def test_quran_fatiha_has_7_verses(self):
        data = _load_json("books/quran/1.json")["data"]
        assert data["verse_count"] == 7
        assert len(data["verses"]) == 7

    def test_quran_fatiha_verse_indexes_sequential(self):
        data = _load_json("books/quran/1.json")["data"]
        for i, verse in enumerate(data["verses"], 1):
            assert verse["local_index"] == i, \
                f"Verse {i} has local_index {verse['local_index']}"

    def test_quran_fatiha_verse_paths_correct(self):
        data = _load_json("books/quran/1.json")["data"]
        for verse in data["verses"]:
            expected = f"/books/quran:1:{verse['local_index']}"
            assert verse["path"] == expected, \
                f"Expected {expected}, got {verse['path']}"

    def test_quran_fatiha_has_multiple_translations(self):
        data = _load_json("books/quran/1.json")["data"]
        verse = data["verses"][0]
        assert "translations" in verse
        assert len(verse["translations"]) > 10, \
            "Al-Fatiha verse 1 should have many translations"

    def test_quran_fatiha_titles_multilingual(self):
        data = _load_json("books/quran/1.json")["data"]
        assert "ar" in data["titles"]
        assert "en" in data["titles"]
        assert data["titles"]["en"] == "The Opening"

    def test_alkafi_1_1_1_has_hadiths(self):
        data = _load_json("books/al-kafi/1/1/1.json")["data"]
        assert data["verse_count"] > 0
        assert len(data["verses"]) > 0

    def test_alkafi_1_1_1_first_hadith_structure(self):
        data = _load_json("books/al-kafi/1/1/1.json")["data"]
        hadith = data["verses"][0]
        assert hadith["part_type"] in VALID_PART_TYPES
        assert hadith["index"] == 1
        assert hadith["local_index"] == 1
        assert "text" in hadith
        assert isinstance(hadith["text"], list)
        assert "translations" in hadith
        assert "narrator_chain" in hadith

    def test_alkafi_1_1_1_path_correct(self):
        data = _load_json("books/al-kafi/1/1/1.json")["data"]
        assert data["path"] == "/books/al-kafi:1:1:1"

    def test_alkafi_metadata_8_volumes(self):
        data = _load_json("books/al-kafi.json")["data"]
        assert len(data["chapters"]) == 8

    def test_quran_metadata_114_suras(self):
        data = _load_json("books/quran.json")["data"]
        assert len(data["chapters"]) == 114

    def test_quran_all_114_sura_files_exist(self):
        missing = []
        for sura in range(1, 115):
            path = os.path.join(DATA_DIR, "books", "quran", f"{sura}.json")
            if not os.path.isfile(path):
                missing.append(sura)
        assert not missing, f"Missing sura files: {missing}"

    def test_books_json_lists_two_books(self):
        data = _load_json("books/books.json")["data"]
        assert len(data["chapters"]) == 2
        paths = {ch["path"] for ch in data["chapters"]}
        assert "/books/quran" in paths
        assert "/books/al-kafi" in paths


# =========================================================================
# 9. Verse indexing consistency
# =========================================================================

class TestVerseIndexing:
    """Global and local indexes must be consistent."""

    def test_quran_fatiha_global_indexes_sequential(self):
        data = _load_json("books/quran/1.json")["data"]
        indexes = [v["index"] for v in data["verses"]]
        for i in range(1, len(indexes)):
            assert indexes[i] == indexes[i - 1] + 1, \
                f"Non-sequential global indexes: {indexes[i-1]} -> {indexes[i]}"

    def test_alkafi_1_1_1_local_indexes_sequential(self):
        data = _load_json("books/al-kafi/1/1/1.json")["data"]
        hadith_verses = [
            v for v in data["verses"]
            if v.get("part_type") != "Heading"
        ]
        local_indexes = [v["local_index"] for v in hadith_verses]
        for i in range(1, len(local_indexes)):
            assert local_indexes[i] == local_indexes[i - 1] + 1, \
                f"Non-sequential local indexes in al-kafi:1:1:1: " \
                f"{local_indexes[i-1]} -> {local_indexes[i]}"

    def test_quran_verse_part_types(self):
        data = _load_json("books/quran/1.json")["data"]
        for verse in data["verses"]:
            assert verse["part_type"] == "Verse", \
                f"Quran verse should have part_type 'Verse', got '{verse['part_type']}'"

    def test_alkafi_hadith_part_types(self):
        data = _load_json("books/al-kafi/1/1/1.json")["data"]
        for verse in data["verses"]:
            assert verse["part_type"] in ("Hadith", "Heading"), \
                f"Al-Kafi verse should be Hadith or Heading, got '{verse['part_type']}'"
