"""
Data validation tests for generated ThaqalaynData JSON output.

These tests validate the actual JSON files in ../ThaqalaynData/ to ensure
schema correctness, UTF-8 integrity, navigation consistency, cross-references,
narrator chain validity, and index file well-formedness.

All chapter files use SHELL FORMAT (verse_refs, not inline verses).
Verse content lives in individual verse_detail files.

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


VALID_KINDS = {"chapter_list", "verse_list", "verse_detail", "person_content", "person_list"}
VALID_PART_TYPES = {"Book", "Volume", "Chapter", "Hadith", "Verse", "Heading", "Section"}


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

    def test_quran_chapter_is_shell_format(self):
        """Quran chapter files use shell format with verse_refs."""
        data = _load_json("books/quran/1.json")
        assert data["kind"] == "verse_list"
        assert "index" in data
        assert "data" in data
        assert "verse_refs" in data["data"], "Shell format requires verse_refs"

    def test_alkafi_chapter_is_shell_format(self):
        """Al-Kafi chapter files use shell format with verse_refs."""
        data = _load_json("books/al-kafi/1/1/1.json")
        assert data["kind"] == "verse_list"
        assert "index" in data
        assert "data" in data
        assert "verse_refs" in data["data"], "Shell format requires verse_refs"

    @pytest.mark.parametrize("sura", [1, 2, 36, 67, 114])
    def test_quran_sura_files_have_valid_wrapper(self, sura):
        path = f"books/quran/{sura}.json"
        if not os.path.isfile(os.path.join(DATA_DIR, path)):
            pytest.skip(f"Sura {sura} file not found")
        data = _load_json(path)
        assert data["kind"] in VALID_KINDS
        assert "data" in data

    def test_verse_detail_wrapper(self):
        """Individual verse_detail files have correct wrapper."""
        data = _load_json("books/quran/1/1.json")
        assert data["kind"] == "verse_detail"
        assert "verse" in data["data"]

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

    def test_quran_verse_detail_has_arabic_text(self):
        """Quran verse_detail file has Arabic text."""
        data = _load_json("books/quran/1/1.json")
        verse = data["data"]["verse"]
        text = verse["text"][0]
        assert re.search(r"[\u0600-\u06FF]", text), \
            f"Expected Arabic characters in verse text, got: {text!r}"

    def test_alkafi_verse_detail_has_arabic_text(self):
        """Al-Kafi verse_detail file has Arabic text."""
        data = _load_json("books/al-kafi/1/1/1/1.json")
        verse = data["data"]["verse"]
        text = verse["text"][0]
        assert re.search(r"[\u0600-\u06FF]", text), \
            f"Expected Arabic characters in hadith text, got: {text!r}"

    def test_alkafi_verse_detail_narrator_chain_has_arabic(self):
        """Al-Kafi verse_detail narrator chain parts have Arabic text."""
        data = _load_json("books/al-kafi/1/1/1/1.json")
        verse = data["data"]["verse"]
        chain = verse.get("narrator_chain", {})
        parts = chain.get("parts", [])
        all_text = " ".join(p.get("text", "") for p in parts)
        assert re.search(r"[\u0600-\u06FF]", all_text), \
            "Narrator chain parts should contain Arabic characters"

    def test_quran_raw_file_no_unicode_escapes(self):
        """Read raw file bytes and verify no \\uXXXX for Arabic code points."""
        path = os.path.join(DATA_DIR, "books", "quran", "1", "1.json")
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
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
        assert "prev" not in data.get("nav", {}) or data["nav"]["prev"] is None
        assert data["nav"]["next"] == "/books/quran:2"

    def test_quran_sura114_nav(self):
        data = _load_json("books/quran/114.json")["data"]
        self._check_nav(data, "quran:114")
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
# 4. Cross-references – relations in verse_detail files
# =========================================================================

class TestCrossReferences:
    """Verse_detail relations must point to real files."""

    def _collect_relations(self, verse_obj):
        """Collect all relation target paths from a verse."""
        relations = verse_obj.get("relations")
        if not relations:
            return []
        targets = []
        for relation_type, paths in relations.items():
            if isinstance(paths, list):
                targets.extend(paths)
            elif isinstance(paths, set):
                targets.extend(paths)
        return targets

    def test_quran_verse_detail_relations_valid(self):
        """Quran verse_detail relations point to real chapter files."""
        data = _load_json("books/quran/1/1.json")
        verse = data["data"]["verse"]
        targets = self._collect_relations(verse)
        for target in targets:
            parts = target.rsplit(":", 1)
            chapter_path = parts[0] if len(parts) > 1 else target
            assert _file_exists(chapter_path), \
                f"Relation target chapter {chapter_path} (from {target}) not found"

    def test_alkafi_verse_detail_mentions_valid(self):
        """Al-Kafi verse_detail 'Mentions' relations point to real Quran files."""
        # Find a verse_detail with Mentions
        for h in range(1, 20):
            path = f"books/al-kafi/1/1/1/{h}.json"
            fpath = os.path.join(DATA_DIR, path)
            if not os.path.isfile(fpath):
                continue
            data = _load_json(path)
            verse = data["data"]["verse"]
            mentions = verse.get("relations", {}).get("Mentions", [])
            for target in mentions:
                parts = target.rsplit(":", 1)
                chapter_path = parts[0] if len(parts) > 1 else target
                assert _file_exists(chapter_path), \
                    f"Hadith {path} mentions {target} but {chapter_path} not found"

    def test_verse_detail_shared_chain_relations_valid(self):
        """Shared Chain relations point to real verse_detail files."""
        for h in range(1, 20):
            path = f"books/al-kafi/1/1/1/{h}.json"
            fpath = os.path.join(DATA_DIR, path)
            if not os.path.isfile(fpath):
                continue
            data = _load_json(path)
            verse = data["data"]["verse"]
            shared = verse.get("relations", {}).get("Shared Chain", [])
            for target in shared:
                assert _file_exists(target), \
                    f"Shared Chain target {target} from {path} not found"

    def test_verse_detail_quotes_quran_relations_valid(self):
        """Quotes Quran relations point to real Quran verse_detail files."""
        for h in range(1, 20):
            path = f"books/al-kafi/1/1/1/{h}.json"
            fpath = os.path.join(DATA_DIR, path)
            if not os.path.isfile(fpath):
                continue
            data = _load_json(path)
            verse = data["data"]["verse"]
            quotes = verse.get("relations", {}).get("Quotes Quran", [])
            for target in quotes:
                parts = target.rsplit(":", 1)
                chapter_path = parts[0] if len(parts) > 1 else target
                assert _file_exists(chapter_path), \
                    f"Quotes Quran target {target} from {path} not found"


# =========================================================================
# 5. Narrator chain refs – narrator IDs correspond to files
# =========================================================================

class TestNarratorChains:
    """Narrator parts in verse_detail chains must point to existing narrator files."""

    def test_alkafi_verse_detail_narrator_refs_valid(self):
        """Narrator chain parts in verse_detail files reference real narrator files."""
        errors = []
        for h in range(1, 20):
            path = f"books/al-kafi/1/1/1/{h}.json"
            fpath = os.path.join(DATA_DIR, path)
            if not os.path.isfile(fpath):
                continue
            data = _load_json(path)
            verse = data["data"]["verse"]
            chain = verse.get("narrator_chain")
            if not chain:
                continue
            for part in chain.get("parts", []):
                if part["kind"] == "narrator":
                    narrator_path = part["path"]
                    file_path = _path_to_file(narrator_path)
                    if not os.path.isfile(file_path):
                        errors.append(
                            f"Verse {verse.get('path')}: narrator {narrator_path} file missing"
                        )
        assert not errors, "\n".join(errors)

    def test_narrator_chain_parts_have_valid_kinds(self):
        data = _load_json("books/al-kafi/1/1/1/1.json")
        verse = data["data"]["verse"]
        chain = verse.get("narrator_chain")
        if not chain:
            return
        for part in chain.get("parts", []):
            assert part["kind"] in ("narrator", "plain"), \
                f"Invalid chain part kind: {part['kind']}"

    def test_narrator_chain_has_parts(self):
        """Narrator chains have parts (text field removed in Phase 2)."""
        data = _load_json("books/al-kafi/1/1/1/1.json")
        verse = data["data"]["verse"]
        chain = verse.get("narrator_chain")
        if not chain:
            return
        assert "parts" in chain, "Narrator chain missing 'parts' field"
        assert len(chain["parts"]) > 0, "Narrator chain has empty parts"

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
        """Each path in the books index should correspond to a real file.

        Known gap: tahdhib-al-ahkam has some chapter entries in the index
        from ThaqalaynAPI but the chapter-level files may not exist since
        the book was parsed at a different granularity from ghbook.ir.
        """
        data = _load_json("index/books.en.json")
        missing = []
        for path in data:
            if not _file_exists(path):
                missing.append(path)
        # Allow a small number of known gaps
        assert len(missing) < 100, \
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

    def test_related_chapters_index_structure(self):
        """related_chapters.json has valid structure."""
        path = os.path.join(DATA_DIR, "index", "related_chapters.json")
        if not os.path.isfile(path):
            pytest.skip("related_chapters.json not found")
        data = _load_json("index/related_chapters.json")
        assert isinstance(data, dict)
        assert len(data) > 0
        # Check a sample entry
        for chapter_path, related in list(data.items())[:5]:
            assert isinstance(related, list)
            for entry in related:
                assert "path" in entry
                assert "title" in entry
                assert "book" in entry
                assert "score" in entry


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
        content = data.get("data", data)
        chapters = content.get("chapters", [])
        assert len(chapters) == 114, f"Expected 114 suras, got {len(chapters)}"

    def test_complete_alkafi_has_volumes(self):
        data = _load_json("books/complete/al-kafi.json")
        content = data.get("data", data)
        chapters = content.get("chapters", [])
        assert len(chapters) == 8, f"Expected 8 volumes, got {len(chapters)}"


# =========================================================================
# 8. Snapshot tests – shell format chapter files + verse_detail files
# =========================================================================

class TestSnapshotKeyChapters:
    """Verify key structural properties of important chapters and verses."""

    def test_quran_fatiha_has_7_verse_refs(self):
        data = _load_json("books/quran/1.json")["data"]
        assert data["verse_count"] == 7
        refs = data["verse_refs"]
        verse_refs = [r for r in refs if r["part_type"] == "Verse"]
        assert len(verse_refs) == 7

    def test_quran_fatiha_verse_refs_sequential(self):
        data = _load_json("books/quran/1.json")["data"]
        refs = [r for r in data["verse_refs"] if r["part_type"] == "Verse"]
        for i, ref in enumerate(refs, 1):
            assert ref["local_index"] == i, \
                f"Verse ref {i} has local_index {ref['local_index']}"

    def test_quran_fatiha_verse_refs_have_paths(self):
        data = _load_json("books/quran/1.json")["data"]
        for ref in data["verse_refs"]:
            if ref["part_type"] == "Verse":
                assert "path" in ref, f"Verse ref missing path: {ref}"
                expected = f"/books/quran:1:{ref['local_index']}"
                assert ref["path"] == expected

    def test_quran_fatiha_verse_detail_has_translations(self):
        """First Quran verse_detail has many translations."""
        data = _load_json("books/quran/1/1.json")
        verse = data["data"]["verse"]
        assert "translations" in verse
        assert len(verse["translations"]) > 10, \
            "Al-Fatiha verse 1 should have many translations"

    def test_quran_fatiha_titles_multilingual(self):
        data = _load_json("books/quran/1.json")["data"]
        assert "ar" in data["titles"]
        assert "en" in data["titles"]
        assert data["titles"]["en"] == "The Opening"

    def test_alkafi_1_1_1_has_verse_refs(self):
        data = _load_json("books/al-kafi/1/1/1.json")["data"]
        assert data["verse_count"] > 0
        assert len(data["verse_refs"]) > 0

    def test_alkafi_verse_detail_structure(self):
        """Al-Kafi first hadith verse_detail has expected fields."""
        data = _load_json("books/al-kafi/1/1/1/1.json")
        assert data["kind"] == "verse_detail"
        verse = data["data"]["verse"]
        assert verse["part_type"] in VALID_PART_TYPES
        assert "text" in verse
        assert isinstance(verse["text"], list)
        assert "translations" in verse
        assert "narrator_chain" in verse

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

    def test_books_json_lists_books(self):
        data = _load_json("books/books.json")["data"]
        assert len(data["chapters"]) >= 2
        paths = {ch["path"] for ch in data["chapters"]}
        assert "/books/quran" in paths
        assert "/books/al-kafi" in paths


# =========================================================================
# 9. Verse indexing consistency (via verse_detail files)
# =========================================================================

class TestVerseIndexing:
    """Verse_detail files have correct indexing and part types."""

    def test_quran_verse_detail_part_type(self):
        data = _load_json("books/quran/1/1.json")
        verse = data["data"]["verse"]
        assert verse["part_type"] == "Verse"

    def test_alkafi_verse_detail_part_type(self):
        data = _load_json("books/al-kafi/1/1/1/1.json")
        verse = data["data"]["verse"]
        assert verse["part_type"] in ("Hadith", "Heading")

    def test_quran_fatiha_verse_details_sequential(self):
        """All 7 verse_detail files for Fatiha have sequential local_index."""
        for i in range(1, 8):
            data = _load_json(f"books/quran/1/{i}.json")
            verse = data["data"]["verse"]
            assert verse["local_index"] == i, \
                f"Quran 1:{i} has local_index {verse['local_index']}"

    def test_alkafi_1_1_1_verse_details_sequential(self):
        """Al-Kafi 1:1:1 verse_detail files have sequential local_index for Hadith types."""
        indexes = []
        for h in range(1, 30):
            path = f"books/al-kafi/1/1/1/{h}.json"
            fpath = os.path.join(DATA_DIR, path)
            if not os.path.isfile(fpath):
                break
            data = _load_json(path)
            verse = data["data"]["verse"]
            if verse["part_type"] == "Hadith":
                indexes.append(verse["local_index"])
        assert len(indexes) > 0, "No hadith verse_detail files found"
        for i in range(1, len(indexes)):
            assert indexes[i] == indexes[i - 1] + 1, \
                f"Non-sequential local indexes: {indexes[i-1]} -> {indexes[i]}"


# =========================================================================
# 10. Data completeness – counts and coverage
# =========================================================================

class TestDataCompleteness:
    """Verify known data counts for regression detection."""

    def test_quran_total_verse_count(self):
        data = _load_json("books/quran.json")["data"]
        assert data["verse_count"] == 6236, \
            f"Expected Quran to have 6236 verses, got {data['verse_count']}"

    def test_quran_verse_sum_across_suras(self):
        """Sum of all sura verse counts should equal 6236."""
        data = _load_json("books/quran.json")["data"]
        total = sum(ch["verse_count"] for ch in data["chapters"])
        assert total == 6236, f"Sum of sura verse_counts is {total}, expected 6236"

    def test_alkafi_total_verse_count(self):
        data = _load_json("books/al-kafi.json")["data"]
        assert data["verse_count"] == 15385, \
            f"Expected Al-Kafi to have 15385 verses/headings, got {data['verse_count']}"

    def test_alkafi_has_8_volumes(self):
        data = _load_json("books/al-kafi.json")["data"]
        assert len(data["chapters"]) == 8

    def test_narrator_count(self):
        """Narrator count should match the canonical registry output."""
        data = _load_json("people/narrators/index.json")
        count = len(data["data"])
        assert count == 4415, \
            f"Expected 4415 narrators, got {count}"

    def test_narrator_index_coverage(self):
        """Every narrator ID in the index has a corresponding JSON file."""
        data = _load_json("people/narrators/index.json")
        missing = []
        for nid in data["data"]:
            fpath = os.path.join(DATA_DIR, "people", "narrators", f"{nid}.json")
            if not os.path.isfile(fpath):
                missing.append(nid)
        assert not missing, \
            f"{len(missing)} narrators in index have no file: {missing[:20]}"

    def test_narrator_files_all_in_index(self):
        """Every narrator JSON file (except index.json and featured.json) should be in the index."""
        data = _load_json("people/narrators/index.json")
        index_ids = set(data["data"].keys())
        narrators_dir = os.path.join(DATA_DIR, "people", "narrators")
        orphans = []
        for fname in os.listdir(narrators_dir):
            if not fname.endswith(".json"):
                continue
            nid = fname.replace(".json", "")
            # Skip non-numeric files (index.json, featured.json)
            if not nid.isdigit():
                continue
            if nid not in index_ids:
                orphans.append(nid)
        assert not orphans, \
            f"{len(orphans)} narrator files not in index: {orphans[:20]}"

    def test_narrator_1_snapshot(self):
        """Snapshot key properties of narrator 1."""
        data = _load_json("people/narrators/1.json")["data"]
        assert "ar" in data["titles"]
        assert len(data["verse_paths"]) > 0
        assert len(data["subchains"]) > 0
        assert data["path"] == "/people/narrators/1"

    def test_books_json_snapshot(self):
        """Snapshot: books.json lists Quran and Al-Kafi."""
        data = _load_json("books/books.json")["data"]
        titles = {ch["titles"]["en"] for ch in data["chapters"]}
        assert "The Holy Quran" in titles
        assert "Al-Kafi" in titles

    @pytest.mark.parametrize("sura", range(1, 115))
    def test_every_quran_sura_has_verse_refs(self, sura):
        """Every Quran sura shell file has verse_refs."""
        path = f"books/quran/{sura}.json"
        if not os.path.isfile(os.path.join(DATA_DIR, path)):
            pytest.skip(f"Sura {sura} not found")
        data = _load_json(path)["data"]
        refs = data.get("verse_refs", [])
        assert len(refs) > 0, \
            f"Sura {sura} has no verse_refs"
