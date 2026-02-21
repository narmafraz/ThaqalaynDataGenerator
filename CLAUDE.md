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

### Scrapers Directory (`app/scrapers/`)
Scripts to fetch raw hadith data from external sources into `app/raw/`:
- **`scrape_thaqalayn_api.py`**: Scrapes ThaqalaynAPI REST endpoint (`https://www.thaqalayn-api.net/api/v2/`). Fetches hadiths one-by-one with 0.5s delay. Supports `--list` to show available slugs, or pass specific slugs as args. Skips books that already have data on disk.
- **`scrape_hubeali_sulaym.py`**: Scrapes Book of Sulaym ibn Qays from `hubeali.com`. Uses BeautifulSoup to parse HTML. Arabic text extraction currently broken due to encoding issues (see Common Issues).

Both scrapers use `urllib.request` (not `requests`) because the `requests` library is not installed in the venv.

```bash
# Run scrapers (from ThaqalaynDataGenerator root):
source .venv/Scripts/activate
python app/scrapers/scrape_thaqalayn_api.py           # All books
python app/scrapers/scrape_thaqalayn_api.py --list     # List available slugs
python app/scrapers/scrape_thaqalayn_api.py Nahj-al-Balagha-Radi  # Specific book
python app/scrapers/scrape_hubeali_sulaym.py           # Book of Sulaym
```

### Raw Data Inventory (`app/raw/`)

The `raw/` directory is **gitignored** — only scraper scripts in `app/scrapers/` are tracked. Raw data must be re-scraped or restored separately.

| Source Directory | Contents | Hadiths |
|-----------------|----------|---------|
| `thaqalayn_api/` | 25 book folders from ThaqalaynAPI, each with `hadiths.json` | 18,945 |
| `hubeali_com/` | Al-Kafi Vols 1-8, Basair al-Darajaat (HTML), Book of Sulaym (JSON) | ~80 (Sulaym) |
| `thaqalayn_net/` | 2020 site mirror — 23 books as HTML pages | N/A (HTML) |
| `alhassanain_org/` | Usul al-Kafi Vols 1-3 (HTML) | N/A (HTML) |
| `tanzil_net/` | Quran text + 27 translations (XML) | N/A (XML) |
| `corrections/` | Manual JSON fixes for parser edge cases | N/A |

See `raw/thaqalayn_api/README.md` for the full ThaqalaynAPI JSON schema documentation.

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
- **SHELL RULES (CRITICAL — violations trigger approval prompts or shutdown)**:
  - Every command must be a **separate Bash tool call**. No chaining with `&&`.
  - **FORBIDDEN**: `&&` after `cd`, full absolute paths with `cd`, `$(pwd)` (use `$PWD`), `.venv/Scripts/python.exe` (source venv then use `python`), `2>&1`, `2>/dev/null`, `| tail`, `| head`, `sleep N && ...`, `if [ ... ]; then ... fi` one-liners.
  - **ALLOWED patterns** (each as its own Bash call):
    - `pwd`
    - `cd ThaqalaynDataGenerator` (relative, no chaining)
    - `source .venv/Scripts/activate`
    - `DESTINATION_DIR="../ThaqalaynData/" PYTHONPATH="$PWD:$PWD/app" python -m pytest --no-cov -q`
    - `ls app/raw/thaqalayn_api/nahj-al-balagha/hadiths.json` (relative paths for file checks)
  - To run tests: first `cd ThaqalaynDataGenerator` in one call, then `source .venv/Scripts/activate` in another, then the test command in a third.
- **`uv` not in bash PATH on Windows**: The `uv` command may not be available in Git Bash even when installed. Activate the venv first (`source .venv/Scripts/activate`) and then use `python` directly.
- **`requests` not installed**: The venv does not include the `requests` library. Scrapers use `urllib.request` (stdlib) instead. If you need HTTP in new scripts, use `urllib.request.Request` with a `User-Agent` header.
- **Arabic text on Windows console**: Printing Arabic text to Windows console causes `UnicodeEncodeError: 'charmap' codec can't encode character`. Fix with `sys.stdout.reconfigure(encoding='utf-8')` at the top of scripts that print Arabic.
- **hubeali.com Arabic encoding**: The Book of Sulaym page on hubeali.com has encoding issues. Using `raw.decode("utf-8", errors="replace")` prevents crashes but corrupts Arabic characters, causing the scraper to extract 0 Arabic paragraphs. The raw HTML is saved at `raw/hubeali_com/book-of-sulaym/page.html` for future re-parsing with a different approach.

## Data Sources and Gaps

### ThaqalaynAPI (`https://www.thaqalayn-api.net/`)
The primary source for structured hadith data. Provides REST JSON for 33 books from thaqalayn.net. API endpoint: `GET /api/v2/{book-slug}/{hadith-id}`. Book list: `GET /api/v2/allbooks`. Each hadith includes Arabic text, English translation, narrator chain separation (`thaqalaynSanad`/`thaqalaynMatn`), grading fields (`majlisiGrading`, `mohseniGrading`, `behbudiGrading`), and thaqalayn.net URLs. Rate limit: use >= 0.5s delay between requests.

### Tahdhib al-Ahkam & al-Istibsar (Remaining Two of the Four Books)
**BREAKTHROUGH**: ghbook.ir (Qaimiyyah Digital Library) has BOTH books as structured HTML text (Arabic):
- **Tahdhib al-Ahkam** (~13,590 hadiths, 10 vols by al-Tusi): book_id=378, 4,119 pages
  - HTML download: `download.ghbook.ir/downloads.php?id=378&file=378-a-13900129-tahzebalahkam-koli.htm`
  - EPUB download: `download.ghbook.ir/downloads.php?id=378&file=378-ar-tahzebalahkam-koli.epub`
  - Text is fully diacritized, hadith numbers clearly marked (e.g. "(145) 84"), includes footnotes with cross-references
  - English translation (Vols 1-3 only): PDF at almuntazar.ca (Bab ul Qaim Publications)
- **al-Istibsar** (~5,511 hadiths, 4 vols by al-Tusi): book_id=2628
  - HTML download: `download.ghbook.ir/downloads.php?id=2628&file=2628-a-13900308-alestebsar-koli.htm`
  - EPUB download: `download.ghbook.ir/downloads.php?id=2628&file=2628-ar-alestebsar-koli.epub`
  - NO English translation available online anywhere
- **Recommended approach**: Download HTML/EPUB from ghbook.ir, parse with BeautifulSoup, identify hadith boundaries by numbered patterns, extract narrator chains from Arabic text
- **License**: Free distribution explicitly stated ("نشر رایگان مطالب و نرم افزارهای سایت مجاز می باشد")
- **Previous assessment was wrong** -- English-only searches found only PDFs; Arabic/Farsi searches revealed this HTML source

### Other Missing Books
3. **Tuhaf al-Uqul** — available on al-islam.org but English only (no Arabic text).
4. **Al-Ihtijaj** — available on al-shia.org/downloadshiabooks.com (not yet scraped).
5. **Daim al-Islam** — rare, may not have English translation online.
6. **Khasais Al-Aemmah** — rare.
7. **Al-Saqib Fi al-Manaqib** — rare.
