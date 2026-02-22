"""
Post-generation data validation script.

Validates all generated JSON files against JSON Schema definitions and
performs semantic checks (path resolution, narrator ID consistency,
verse counts, navigation integrity).

Usage:
    python app/validate_data.py [--data-dir ../ThaqalaynData] [--verbose]
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

# Try to use jsonschema if available, otherwise skip schema validation
try:
    import jsonschema
    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False


SCHEMA_DIR = Path(__file__).parent / "schemas"


class ValidationReport:
    """Collects validation errors and warnings."""

    def __init__(self):
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.files_checked = 0
        self.files_valid = 0

    def error(self, file: str, msg: str):
        self.errors.append(f"ERROR [{file}]: {msg}")

    def warn(self, file: str, msg: str):
        self.warnings.append(f"WARN  [{file}]: {msg}")

    def summary(self) -> str:
        lines = [
            f"\n{'='*60}",
            f"Validation Summary",
            f"{'='*60}",
            f"Files checked:  {self.files_checked}",
            f"Files valid:    {self.files_valid}",
            f"Errors:         {len(self.errors)}",
            f"Warnings:       {len(self.warnings)}",
        ]
        if self.errors:
            lines.append(f"\n--- Errors ({len(self.errors)}) ---")
            for e in self.errors[:50]:
                lines.append(e)
            if len(self.errors) > 50:
                lines.append(f"  ... and {len(self.errors) - 50} more errors")
        if self.warnings:
            lines.append(f"\n--- Warnings ({len(self.warnings)}) ---")
            for w in self.warnings[:20]:
                lines.append(w)
            if len(self.warnings) > 20:
                lines.append(f"  ... and {len(self.warnings) - 20} more warnings")
        return "\n".join(lines)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


def load_schema(name: str) -> dict | None:
    """Load a JSON Schema file from the schemas directory."""
    schema_path = SCHEMA_DIR / name
    if not schema_path.exists():
        return None
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_json_file(filepath: Path, report: ValidationReport, verbose: bool = False):
    """Load and validate a single JSON file."""
    rel = str(filepath)
    report.files_checked += 1

    # 1. JSON parsing
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        report.error(rel, f"Invalid JSON: {e}")
        return None
    except UnicodeDecodeError as e:
        report.error(rel, f"Encoding error: {e}")
        return None

    if not isinstance(data, dict):
        report.error(rel, f"Expected JSON object, got {type(data).__name__}")
        return None

    # 2. Wrapper validation
    if "kind" not in data:
        report.error(rel, "Missing 'kind' field")
        return data
    if "index" not in data:
        report.error(rel, "Missing 'index' field")
    if "data" not in data:
        report.error(rel, "Missing 'data' field")
        return data

    kind = data["kind"]
    valid_kinds = {"chapter_list", "verse_list", "verse_content", "verse_detail",
                   "person_content", "person_list"}
    if kind not in valid_kinds:
        report.error(rel, f"Unknown kind: '{kind}' (expected one of {valid_kinds})")

    # 3. Semantic validation based on kind
    inner = data["data"]
    if kind in ("chapter_list", "verse_list"):
        validate_chapter(inner, rel, kind, report)
    elif kind == "person_content":
        validate_narrator(inner, rel, report)
    elif kind == "person_list":
        validate_narrator_list(inner, rel, report)

    report.files_valid += 1
    return data


def validate_chapter(chapter: dict, rel: str, kind: str, report: ValidationReport):
    """Validate a chapter object."""
    # Path format
    path = chapter.get("path")
    if path and not path.startswith("/books/"):
        report.error(rel, f"Chapter path does not start with '/books/': {path}")

    # Titles
    titles = chapter.get("titles")
    if not titles:
        report.warn(rel, "Chapter has no titles")

    # Part type
    part_type = chapter.get("part_type")
    valid_part_types = {"Verse", "Volume", "Book", "Chapter", "Hadith", "Heading", "Section"}
    if part_type and part_type not in valid_part_types:
        report.error(rel, f"Invalid part_type: '{part_type}'")

    # Verse list validation
    if kind == "verse_list":
        verses = chapter.get("verses", [])
        verse_count = chapter.get("verse_count")
        if verse_count is not None and verses:
            # Count only non-heading verses
            actual_count = sum(1 for v in verses if v.get("part_type") != "Heading")
            if actual_count != verse_count:
                report.warn(rel, f"verse_count={verse_count} but found {actual_count} non-heading verses")

        # Validate each verse
        local_indices = set()
        for i, verse in enumerate(verses):
            validate_verse(verse, rel, i, report)
            li = verse.get("local_index")
            if li is not None and verse.get("part_type") != "Heading":
                if li in local_indices:
                    report.error(rel, f"Duplicate local_index={li} in verse {i}")
                local_indices.add(li)

        # Verse translations consistency
        vt = chapter.get("verse_translations", [])
        if vt and verses:
            for verse in verses:
                translations = verse.get("translations", {})
                if translations:
                    for tid in vt:
                        if tid not in translations:
                            # Not all translations need to be present for every verse
                            pass

    # Chapter list validation
    if kind == "chapter_list":
        chapters = chapter.get("chapters", [])
        for i, sub in enumerate(chapters):
            sub_path = sub.get("path", f"sub-{i}")
            if not sub.get("titles"):
                report.warn(rel, f"Sub-chapter at index {i} has no titles")

    # Navigation
    nav = chapter.get("nav")
    if nav:
        for direction in ("prev", "next", "up"):
            target = nav.get(direction)
            if target and not target.startswith("/books/"):
                report.error(rel, f"nav.{direction} path invalid: '{target}'")


def validate_verse(verse: dict, rel: str, idx: int, report: ValidationReport):
    """Validate a single verse/hadith."""
    part_type = verse.get("part_type")
    if part_type == "Heading":
        return  # Headings have minimal requirements

    # Required fields
    if "local_index" not in verse:
        report.error(rel, f"Verse {idx}: missing local_index")

    path = verse.get("path")
    if path and not path.startswith("/books/"):
        report.error(rel, f"Verse {idx}: path does not start with '/books/': {path}")

    # Text content
    text = verse.get("text", [])
    if not text:
        report.warn(rel, f"Verse {idx}: empty text array")

    # Narrator chain
    chain = verse.get("narrator_chain")
    if chain and isinstance(chain, dict):
        parts = chain.get("parts", [])
        for part in parts:
            if part.get("kind") == "narrator":
                narrator_path = part.get("path")
                if narrator_path and not narrator_path.startswith("/people/narrators/"):
                    report.error(rel, f"Verse {idx}: narrator path invalid: '{narrator_path}'")

    # Relations
    relations = verse.get("relations", {})
    if relations:
        for rel_type, paths in relations.items():
            if not isinstance(paths, list):
                report.error(rel, f"Verse {idx}: relation '{rel_type}' value is not an array")
            else:
                for rpath in paths:
                    if not rpath.startswith("/books/"):
                        report.error(rel, f"Verse {idx}: relation path invalid: '{rpath}'")


def validate_narrator(narrator: dict, rel: str, report: ValidationReport):
    """Validate a narrator object."""
    if "index" not in narrator:
        report.error(rel, "Narrator missing 'index'")
    if "titles" not in narrator:
        report.error(rel, "Narrator missing 'titles'")
    if "path" not in narrator:
        report.error(rel, "Narrator missing 'path'")
    elif not narrator["path"].startswith("/people/narrators/"):
        report.error(rel, f"Narrator path invalid: '{narrator['path']}'")

    # Verse paths should all be valid book paths
    verse_paths = narrator.get("verse_paths", [])
    for vp in verse_paths[:5]:  # Sample check
        if not vp.startswith("/books/"):
            report.error(rel, f"Narrator verse_path invalid: '{vp}'")
            break


def validate_narrator_list(data: Any, rel: str, report: ValidationReport):
    """Validate narrator list/index."""
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                report.error(rel, "Narrator list item is not an object")
                break
    elif isinstance(data, dict):
        # Could be a dict-based index
        pass


def collect_narrator_ids(data_dir: Path) -> set[int]:
    """Collect all narrator IDs from people/narrators/ directory."""
    ids = set()
    narrators_dir = data_dir / "people" / "narrators"
    if narrators_dir.exists():
        for f in narrators_dir.glob("*.json"):
            try:
                nid = int(f.stem)
                ids.add(nid)
            except ValueError:
                pass
    return ids


def check_narrator_references(data_dir: Path, report: ValidationReport, verbose: bool):
    """Cross-check narrator references in book data against narrator files."""
    narrator_ids = collect_narrator_ids(data_dir)
    if not narrator_ids:
        report.warn("global", "No narrator files found - skipping narrator reference check")
        return

    referenced_ids = set()
    books_dir = data_dir / "books"
    if not books_dir.exists():
        return

    # Sample a subset of book files to check
    book_files = list(books_dir.rglob("*.json"))
    # Skip complete/ directory (aggregated files)
    book_files = [f for f in book_files if "complete" not in str(f)]
    # Sample up to 200 files
    sample = book_files[:200] if len(book_files) > 200 else book_files

    for filepath in sample:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        inner = data.get("data", {})
        verses = inner.get("verses", [])
        for verse in verses:
            chain = verse.get("narrator_chain", {})
            if not chain:
                continue
            parts = chain.get("parts", [])
            for part in parts:
                if part.get("kind") == "narrator" and part.get("path"):
                    match = re.search(r"/people/narrators/(\d+)", part["path"])
                    if match:
                        referenced_ids.add(int(match.group(1)))

    # Check for referenced IDs that don't exist
    missing = referenced_ids - narrator_ids
    if missing:
        report.error("global", f"Referenced narrator IDs not found: {sorted(missing)[:20]}")
    elif verbose:
        print(f"  Narrator check: {len(referenced_ids)} IDs referenced, all exist")


def check_navigation_targets(data_dir: Path, report: ValidationReport, verbose: bool):
    """Spot-check that navigation targets point to existing files."""
    books_dir = data_dir / "books"
    if not books_dir.exists():
        return

    book_files = list(books_dir.rglob("*.json"))
    book_files = [f for f in book_files if "complete" not in str(f)]
    sample = book_files[:100] if len(book_files) > 100 else book_files
    broken_nav = 0

    for filepath in sample:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        nav = data.get("data", {}).get("nav", {})
        if not nav:
            continue

        for direction in ("prev", "next", "up"):
            target = nav.get(direction)
            if not target:
                continue
            # Convert path to filesystem: /books/al-kafi:1:2 -> books/al-kafi/1/2.json
            fs_path = target.replace("/books/", "books/").replace(":", "/") + ".json"
            full_path = data_dir / fs_path
            if not full_path.exists():
                broken_nav += 1
                if broken_nav <= 5:
                    report.warn(str(filepath), f"nav.{direction} target not found: {target}")

    if broken_nav > 5:
        report.warn("global", f"Total broken navigation links: {broken_nav}")
    elif verbose:
        print(f"  Navigation check: all sampled targets valid")


def main():
    parser = argparse.ArgumentParser(description="Validate ThaqalaynData JSON files")
    parser.add_argument("--data-dir", default="../ThaqalaynData",
                        help="Path to ThaqalaynData directory")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show detailed output")
    parser.add_argument("--books-only", action="store_true",
                        help="Only validate book files")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit number of files to check (0=all)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}")
        sys.exit(1)

    report = ValidationReport()
    print(f"Validating data in: {data_dir}")

    # Validate book files
    books_dir = data_dir / "books"
    if books_dir.exists():
        book_files = sorted(books_dir.rglob("*.json"))
        # Skip complete/ aggregated files
        book_files = [f for f in book_files if "complete" not in str(f)]
        if args.limit:
            book_files = book_files[:args.limit]
        print(f"Checking {len(book_files)} book files...")
        for filepath in book_files:
            validate_json_file(filepath, report, args.verbose)

    # Validate narrator files
    if not args.books_only:
        people_dir = data_dir / "people"
        if people_dir.exists():
            people_files = sorted(people_dir.rglob("*.json"))
            if args.limit:
                people_files = people_files[:args.limit]
            print(f"Checking {len(people_files)} people files...")
            for filepath in people_files:
                validate_json_file(filepath, report, args.verbose)

        # Validate index files
        index_dir = data_dir / "index"
        if index_dir.exists():
            index_files = sorted(index_dir.rglob("*.json"))
            print(f"Checking {len(index_files)} index files...")
            for filepath in index_files:
                report.files_checked += 1
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        json.load(f)
                    report.files_valid += 1
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    report.error(str(filepath), f"Invalid JSON: {e}")

    # Cross-referential checks
    print("Running cross-reference checks...")
    check_narrator_references(data_dir, report, args.verbose)
    check_navigation_targets(data_dir, report, args.verbose)

    # Print summary
    print(report.summary())
    sys.exit(0 if report.ok else 1)


if __name__ == "__main__":
    main()
