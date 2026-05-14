$Env:PYTHONPATH = "$PSScriptRoot;$PSScriptRoot/app"

# Rebuild the ThaqalaynWords output (surfaces/, lemmas/, roots/, index/)
# from the sources in ../ThaqalaynWordSources. Runs the two stages of
# the words pipeline that aren't part of add_data.ps1's hadith pipeline:
#
#   1. build_word_pages.py   — walks corpus_surface_set + CAMeL Tools +
#                              QAC + Wiktextract + Lane's + hawramani
#                              and writes the per-surface/per-lemma/per-root
#                              JSONs. ~30 minutes on the full corpus.
#   2. build_word_indexes.py — walks the output and writes the three
#                              index/*.json files. ~10 seconds.
#
# Reads/writes via the sibling-repo defaults baked into both scripts
# (../ThaqalaynWordSources for inputs, ../ThaqalaynWords for outputs),
# so no env vars beyond PYTHONPATH are needed.
#
# Run this:
#   - after pulling a builders.py change (e.g. new fields, slug logic)
#   - after the corpus grows (new books land in ThaqalaynDataSources +
#     get re-extracted into corpus_surface_set.json)
#   - never as part of a routine hadith-only regen — add_data.ps1
#     intentionally skips it because the word build is slow and word
#     content changes are rare.

Write-Host ""
Write-Host "=== ThaqalaynWords regen ===" -ForegroundColor Cyan
$start = Get-Date

# Clean stale per-page JSONs before rebuilding. build_word_pages.py writes
# but never deletes, so if a previous build wrote files under slug names
# the current build no longer produces, those stragglers would persist and
# accumulate across runs. Index files (index/*.json) are rewritten fully
# each run so they don't need explicit cleanup.
$WordsRoot = Resolve-Path "$PSScriptRoot/../ThaqalaynWords"
foreach ($dir in @("lemmas", "roots", "surfaces")) {
    $p = Join-Path $WordsRoot $dir
    if (Test-Path $p) {
        Write-Host "Cleaning $p ..." -ForegroundColor DarkGray
        Remove-Item -Recurse -Force "$p\*"
    }
}

Write-Host ""
Write-Host "[1/2] Building surfaces + lemmas + roots ..." -ForegroundColor Yellow
uv run python .\scripts\build_word_pages.py --full
if ($LASTEXITCODE -ne 0) {
    Write-Host "build_word_pages.py failed (exit $LASTEXITCODE)" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "[2/2] Building index files ..." -ForegroundColor Yellow
uv run python .\scripts\build_word_indexes.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "build_word_indexes.py failed (exit $LASTEXITCODE)" -ForegroundColor Red
    exit $LASTEXITCODE
}

$elapsed = (Get-Date) - $start
Write-Host ""
Write-Host ("=== Done in {0:mm}m{0:ss}s ===" -f $elapsed) -ForegroundColor Green
Write-Host "Next: cd ../ThaqalaynWords; git status to review, commit, push." -ForegroundColor Cyan
