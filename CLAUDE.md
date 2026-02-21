# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ThaqalaynDataGenerator parses Islamic scripture data from various sources (HTML, XML) and generates structured JSON files for the Thaqalayn mobile app. The primary sources are:
- **Quran**: Parsed from XML files
- **Al-Kafi**: Shia hadith collection parsed from HTML files (multiple translations)

The generated JSON files are written to a sibling directory `../ThaqalaynData/` (configured via `DESTINATION_DIR` environment variable).

## Environment Setup

```bash
# Install uv (if not already installed)
# macOS/Linux:
curl -LsSf https://astral.sh/uv/install.sh | sh
# Windows:
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Install dependencies
uv sync

# Install with dev dependencies (includes pytest)
uv sync --all-extras
```

## Running the Data Generator

```bash
# Using PowerShell script (sets up environment automatically)
./add_data.ps1

# Or manually with uv
export PYTHONPATH="$PWD:$PWD/app"
export DESTINATION_DIR="../ThaqalaynData/"
uv run python app/main_add.py
```

The main generation pipeline (`app/main_add.py`) runs these steps in order:
1. `init_books()` - Initialize book metadata
2. `init_quran()` - Parse and generate Quran data
3. `init_kafi()` - Parse Al-Kafi hadith collection
4. `add_kafi_sarwar()` - Add Sarwar translation to Al-Kafi
5. `link_quran_kafi()` - Create links between Quran verses and hadith references
6. `kafi_narrators()` - Extract and process narrator chains from hadiths

## Testing

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_kafi_narrators.py

# Run with coverage report
uv run pytest --cov=app --cov-report=html
```

## Code Architecture

### Data Models (`app/models/`)
- **Chapter**: Hierarchical structure that can contain chapters (nested) or verses (leaf nodes)
- **Verse**: Individual verse/hadith with multilingual text, translations, and metadata
- **Translation**: Translation metadata (language, translator name)
- **Crumb**: Breadcrumb navigation for hierarchical content
- **Navigation**: Prev/next/up navigation links between chapters
- **PartType** enum: Distinguishes Books, Volumes, Chapters, Hadiths, Verses

### Core Libraries
- **lib_db.py**: File I/O operations
  - `insert_chapter()`: Recursively writes chapter hierarchy to JSON files
  - `write_file()`: Writes JSON with proper encoding and directory creation
  - `load_chapter()`, `load_json()`: Read generated data
  - Path transformation: Converts paths like `/books/quran:1:1` to `books/quran/1/1.json`

- **lib_model.py**: Indexing and navigation
  - `set_index()`: Recursively assigns hierarchical indexes to chapters/verses
  - Generates breadcrumb trails and prev/next navigation
  - Validates chapter numbering against titles

- **lib_bs4.py**: BeautifulSoup utilities for HTML parsing

### Source Parsers
- **quran.py**: Parses Quran from XML files (quran-data.xml format)
  - Extracts sura/aya structure, sajda positions, translations

- **kafi.py**: Primary Al-Kafi parser (HubeAli translation)
  - Parses hierarchical structure: Volumes → Books → Parts → Chapters → Hadiths
  - Uses `kafi_corrections.py` for manual fixes to source HTML errors

- **kafi_sarwar.py**: Adds Sarwar translation to existing Al-Kafi structure

- **kafi_narrators.py**: Extracts narrator chains (isnad) from hadith text
  - Uses regex patterns to identify Arabic narrator patterns
  - Builds graph data for narrator relationships

- **link_quran_kafi.py**: Creates bidirectional references between Quran verses cited in hadiths

### Queries Directory (`app/queries/`)
Ad-hoc scripts for analyzing generated data:
- `kitab_hujjat_narrators.py`: Generates narrator graph visualization (outputs HTML)
- `kitab_hujjat_verses.py`: Analyzes Quran verse references in specific book
- `dump_verse.py`, `find_text.py`: Utility queries

Run queries directly: `python app/queries/kitab_hujjat_narrators.py`

## Key Design Patterns

1. **Hierarchical Chapter Structure**: Everything is a `Chapter` object. Leaf chapters contain `verses`, intermediate chapters contain `chapters`. This uniform structure simplifies recursive processing.

2. **Path-Based Indexing**: Each chapter/verse has a unique path like `/books/quran:1:5` (Book → Sura 1 → Aya 5). Paths are converted to filesystem paths for JSON output.

3. **Dual Indexing**:
   - `index`: Global verse/chapter number within the entire book
   - `local_index`: Position within immediate parent chapter

4. **Separation of Parsing and Output**: Parsers build in-memory object trees, then `lib_db.insert_chapter()` recursively writes to files. This allows re-generating output without re-parsing.

5. **Corrections Layer**: `kafi_corrections.py` contains manual fixes for source HTML errors, keeping parser logic clean.

6. **ProcessingReport**: Error accumulation uses a `ProcessingReport` class (in `lib_model.py`) that is passed through the pipeline. It replaces the old module-level globals (`SEQUENCE_ERRORS`, `NARRATIONS_WITHOUT_NARRATORS`). All report parameters are optional with `None` default, falling back to a global default report for backward compatibility. Tests should create isolated `ProcessingReport()` instances to avoid state leaks between tests.

7. **Narrator Subchain Optimization**: `getCombinations()` in `kafi_narrators.py` generates only full chains + consecutive pairs (not all contiguous subsequences). A chain of N narrators produces N entries (1 full chain + N-1 pairs) instead of N*(N+1)/2 - N. When the chain has exactly 2 narrators, the full chain equals the only pair, so a dedup check avoids double-counting.

## Environment Variables

- `DESTINATION_DIR`: Output directory for generated JSON files (default: `../ThaqalaynData/`)
- `PYTHONPATH`: Must include project root for imports to work

## Common Issues

- **Import errors**: Ensure `PYTHONPATH` includes both project root and `app/` directory
- **Missing output directory**: The generator creates directories automatically, but `DESTINATION_DIR` parent must exist
- **Encoding issues**: All JSON files use UTF-8 encoding with `ensure_ascii=False` to preserve Arabic text
- **Sequence errors**: Parser validates chapter numbering; errors are logged to the `ProcessingReport.sequence_errors` list (and legacy `SEQUENCE_ERRORS` global) but don't halt execution
- **logger.warn is deprecated**: Use `logger.warning()` instead of `logger.warn()` -- the latter triggers `DeprecationWarning` on Python 3.12+
- **Windows shell rules**: When running tests from the root `scripture/` directory, use `cd ThaqalaynDataGenerator && DESTINATION_DIR=... PYTHONPATH=... .venv/Scripts/python.exe -m pytest` pattern. Do not chain `cd` with `&&` when following TEAM.md rules, but this is sometimes needed when the working directory resets between bash calls
