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
Scripts to fetch raw hadith data from external sources into `ThaqalaynDataSources/scraped/`:
- **`scrape_thaqalayn_api.py`**: Scrapes ThaqalaynAPI REST endpoint (`https://www.thaqalayn-api.net/api/v2/`). Fetches hadiths one-by-one with 0.5s delay. Supports `--list` to show available slugs, or pass specific slugs as args. Skips books that already have data on disk.
- **`scrape_hubeali_sulaym.py`**: Scrapes Book of Sulaym ibn Qays from `hubeali.com`. Uses BeautifulSoup to parse HTML. Arabic text extraction currently broken due to encoding issues (see Common Issues).
- **`download_rafed_word.py`**: Downloads Word (.doc) files from rafed.net API for all Four Books. Single HTTP GET per volume via `books.rafed.net/api/download/{id}/doc`. Supports `--tahdhib`, `--istibsar`, `--kafi`, `--faqih`, `--list`. Skips files already on disk.
- **`scrape_rafed_text.py`**: Scrapes page-by-page Arabic text from rafed.net using Playwright (SPA requires JS rendering). Extracts TOC (chapter/page structure) and page text for Tahdhib al-Ahkam and al-Istibsar. Supports `--toc-only`, `--tahdhib`, `--istibsar`, `--vol N`. Requires `playwright` package.
- **`download_ghbook_html.py`**: Downloads HTML files from ghbook.ir (Qaimiyyah Digital Library) for Tahdhib al-Ahkam (book_id=378) and al-Istibsar (book_id=2628). Each book is a single large HTML file. Supports `--tahdhib`, `--istibsar`, `--list`. Skips files already on disk.
- **`scrape_eshia_notes.md`**: Assessment of lib.eshia.ir -- found to be image-based scans (not text), not viable for automated scraping. See file for details and updated cross-validation matrix.

All download scrapers use `urllib.request` (not `requests`). The `scrape_rafed_text.py` scraper requires Playwright for SPA rendering.

```bash
# Run scrapers (from ThaqalaynDataGenerator root):
source .venv/Scripts/activate
python app/scrapers/scrape_thaqalayn_api.py           # All books
python app/scrapers/scrape_thaqalayn_api.py --list     # List available slugs
python app/scrapers/scrape_thaqalayn_api.py Nahj-al-Balagha-Radi  # Specific book
python app/scrapers/scrape_hubeali_sulaym.py           # Book of Sulaym
python app/scrapers/download_rafed_word.py             # All Four Books Word files
python app/scrapers/download_rafed_word.py --tahdhib   # Tahdhib only
python app/scrapers/download_rafed_word.py --kafi      # Al-Kafi only
python app/scrapers/download_ghbook_html.py            # Tahdhib + Istibsar HTML files
python app/scrapers/download_ghbook_html.py --list     # Show download URLs
python app/scrapers/scrape_rafed_text.py --toc-only    # Extract TOCs only (fast)
python app/scrapers/scrape_rafed_text.py --tahdhib     # Scrape Tahdhib page text
```

### Raw Data Inventory (`ThaqalaynDataSources/scraped/`)

Raw source data has been moved to the **ThaqalaynDataSources** sibling repo. The generator reads from it via `SOURCE_DATA_DIR` env var (defaults to `../ThaqalaynDataSources/`). Scrapers also write to ThaqalaynDataSources.

| Source Directory | Contents | Hadiths |
|-----------------|----------|---------|
| `thaqalayn_api/` | 25 book folders from ThaqalaynAPI, each with `hadiths.json` | 18,945 |
| `hubeali_com/` | Al-Kafi Vols 1-8, Basair al-Darajaat (HTML), Book of Sulaym (JSON) | ~80 (Sulaym) |
| `thaqalayn_net/` | 2020 site mirror — 23 books as HTML pages | N/A (HTML) |
| `alhassanain_org/` | Usul al-Kafi Vols 1-3 (HTML) | N/A (HTML) |
| `tanzil_net/` | Quran text + 27 translations (XML) | N/A (XML) |
| `corrections/` | Manual JSON fixes for parser edge cases | N/A |

See `scraped/thaqalayn_api/README.md` for the full ThaqalaynAPI JSON schema documentation.

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

- `SOURCE_DATA_DIR`: Source data directory containing scraped/, ai-pipeline-data/, ai-content/ (default: `../ThaqalaynDataSources/`)
- `DESTINATION_DIR`: Output directory for generated JSON files (default: `../ThaqalaynData/`)
- `PYTHONPATH`: Must include project root for imports to work

## AI Content Pipeline (Claude Code Agents)

AI content is generated by **Claude Code agents directly** — NOT via the Anthropic API (no API key needed). Claude Code itself runs Opus 4.6, so the output quality is identical to what the Batch API produces.

### Three-Pass Workflow

The pipeline uses a multi-pass approach for quality:

1. **Generate (Pass 1)** — `ai-generate` agent (Opus): Extracts verse data, builds prompt, generates all 10 fields, validates schema. For long hadiths (>200 words), uses chunked processing.
2. **Review (Pass 2)** — `ai-review` agent (Sonnet): Runs `review_result()` automated quality checks + expert judgment. Outputs pass/needs_fix/needs_regeneration.
3. **Fix (Pass 3)** — `ai-fix` agent (Opus): For "needs_fix" results, uses `build_fix_prompt()` to correct only the flagged fields. Re-validates after fixing.

The `ai-orchestrate` agent coordinates all three passes across multiple verses with parallelization.

### Agent Files

Agent definitions live in `.claude/agents/` (relative to the `scripture/` root):

| Agent | Model | Role |
|-------|-------|------|
| `ai-generate.md` | Opus | Pass 1: content generation (single-pass or chunked) |
| `ai-review.md` | Sonnet | Pass 2: quality review (read-only) |
| `ai-fix.md` | Opus | Pass 3: targeted field correction |
| `ai-orchestrate.md` | Opus | Orchestrator: coordinates all passes |

### Long Hadith Chunked Processing

Hadiths with >200 Arabic words use chunked processing (threshold: `CHUNKED_PROCESSING_THRESHOLD`):

1. **Structure pass**: Generates all fields EXCEPT `word_analysis` and chunk translations. Defines chunk boundaries with types and word ranges. Includes verse-level translations in all 11 languages.
2. **Detail passes** (per chunk, parallelizable): Generates `word_analysis` entries and chunk translations for each chunk.
3. **Assembly**: `assemble_chunked_result()` concatenates word_analysis, inserts chunk translations, fixes word ranges, and validates.

### Structure Pass Caching (`ai_pipeline_cache.py`)

Intermediate results are cached to avoid redoing expensive work when only translations or word analysis change:

```
ThaqalaynDataSources/ai-content/samples/cache/{verse_id}/
  meta.json       — hashes, versions, timestamps for staleness detection
  structure.json  — structure pass output (chunks, translations, narrator analysis)
  chunk_N.json    — detail pass output for chunk N
```

Three staleness layers:
- **Layer 1** (Arabic text hash): If Arabic text changes, everything is invalidated
- **Layer 2** (structure schema version): If structural fields change, structure + chunks are invalidated
- **Layer 3** (pipeline version / glossary hash / language keys): Only chunk details are invalidated

Key functions: `save_structure_cache()`, `save_chunk_cache()`, `check_cache_staleness()`, `get_cached_or_plan()`, `invalidate_cache()`, `invalidate_chunks()`

### Quality Review Checks (`review_result()`)

Seven automated checks beyond schema validation:

| # | Check | Category | Catches |
|---|-------|----------|---------|
| 1 | Translation length ratio | `length_ratio` | Summaries-as-translations |
| 2 | Arabic echo-back | `arabic_echo` | Untranslated word translations |
| 3 | European diacritics | `missing_diacritics` | ASCII-only Turkish/French/German/Spanish |
| 4 | Quran self-reference | `empty_related_quran` | Quran verses missing self-reference |
| 5 | Chunk coherence | `chunk_translation_mismatch` | Chunk/verse length divergence >30% |
| 6 | Missing isnad chunk | `missing_isnad_chunk` | has_chain=True without isnad chunk |
| 7 | Back-reference detection | `back_reference_no_chain` | Back-ref patterns with has_chain=False |

### Wrapper format

```json
{
  "verse_path": "/books/al-kafi:1:1:1:1",
  "ai_attribution": {
    "model": "claude-opus-4-6-20260205",
    "generated_date": "2026-02-26",
    "pipeline_version": "1.0.0",
    "generation_method": "claude_code_direct"
  },
  "result": { /* validated pipeline result */ }
}
```

### Running generation

Ask Claude Code: "Generate AI samples for the remaining verses in sample_verses.json". The orchestrator will:
- Read `sample_verses.json` for the 20 verse paths
- Skip paths that already have response files in `ai-content/samples/responses/`
- Spawn parallel agents (up to 5 at a time) to generate the remaining samples
- Review each generated response for quality
- Fix any flagged issues automatically
- After all agents complete, run `python -m app.ai_pipeline validate` to verify all 20 pass

### Validation

```bash
cd ThaqalaynDataGenerator
source .venv/Scripts/activate
PYTHONPATH="$PWD:$PWD/app" SOURCE_DATA_DIR="../ThaqalaynDataSources/" python -m app.ai_pipeline validate
```

## API-Only Code (Not Used)

The following functions/modules require an Anthropic API key and are NOT used in the current Claude Code agent workflow:

- `ai_pipeline.write_request_jsonl()` — writes Batch API JSONL requests
- `ai_pipeline.estimate_cost()` — estimates Batch API costs
- `ai_translation.py` — entire module is Batch API translation pipeline

These are preserved for potential future use with the Anthropic Batch API for full-corpus processing.

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
- **hubeali.com Arabic encoding**: The Book of Sulaym page on hubeali.com has encoding issues. Using `raw.decode("utf-8", errors="replace")` prevents crashes but corrupts Arabic characters, causing the scraper to extract 0 Arabic paragraphs. The raw HTML is saved at `scraped/hubeali_com/book-of-sulaym/page.html` for future re-parsing with a different approach.

## Data Sources and Gaps

### ThaqalaynAPI (`https://www.thaqalayn-api.net/`)
The primary source for structured hadith data. Provides REST JSON for 33 books from thaqalayn.net. API endpoint: `GET /api/v2/{book-slug}/{hadith-id}`. Book list: `GET /api/v2/allbooks`. Each hadith includes Arabic text, English translation, narrator chain separation (`thaqalaynSanad`/`thaqalaynMatn`), grading fields (`majlisiGrading`, `mohseniGrading`, `behbudiGrading`), and thaqalayn.net URLs. Rate limit: use >= 0.5s delay between requests.

### Tahdhib al-Ahkam & al-Istibsar (Remaining Two of the Four Books)
Multiple Arabic text sources confirmed. Cross-validation: gather from 2+ independent sources.

**Source 1 - ghbook.ir (Qaimiyyah Digital Library)** -- HTML/EPUB download:
- **Tahdhib al-Ahkam**: book_id=378, 10 vols, 4,119 pages. HTML: `download.ghbook.ir/downloads.php?id=378&file=378-a-13900129-tahzebalahkam-koli.htm`
- **al-Istibsar**: book_id=2628, 4 vols. HTML: `download.ghbook.ir/downloads.php?id=2628&file=2628-a-13900308-alestebsar-koli.htm`
- Free distribution license. Fully diacritized text. Hadith numbers clearly marked.

**Source 2 - rafed.net (Maktabat Rafed)** -- Word download API (easiest to automate):
- **Tahdhib**: Vols 1-10 (view IDs: 722, 731, 734, 735, 736, 737, 741, 745, 747, 752). Word: `books.rafed.net/api/download/{id}/doc`
- **al-Istibsar**: Vols 1-4 (view IDs: 1266, 1307, 1320, 1321). Word: `books.rafed.net/api/download/{id}/doc`
- Also has Al-Kafi (view/372+) and Man La Yahduruhu al-Faqih (view/1414+) -- ALL Four Books
- Page-by-page text at `/view/{id}/page/{n}`. Clean HTML paragraphs. No login required.

**Source 3 - almuntazar.ca** -- PDF backup:
- Tahdhib: Arabic (9 vols) + English Vols 1-3 (Bab ul Qaim Publications)
- al-Istibsar: Arabic (4 vols), NO English

**Recommended**: Download Word from rafed.net API (easiest), download HTML from ghbook.ir, cross-validate. For English: babulqaim.com/almuntazar.ca PDFs (Tahdhib Vols 1-3 only).

### Other Missing Books
3. **Tuhaf al-Uqul** — available on al-islam.org but English only (no Arabic text).
4. **Al-Ihtijaj** — available on al-shia.org/downloadshiabooks.com (not yet scraped).
5. **Daim al-Islam** — rare, may not have English translation online.
6. **Khasais Al-Aemmah** — rare.
7. **Al-Saqib Fi al-Manaqib** — rare.
