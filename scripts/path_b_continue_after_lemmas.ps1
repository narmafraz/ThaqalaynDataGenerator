# Path B continuation script - runs once the lemma full pass has
# finished. Designed to be launched in the background so the rest of
# the Path B pipeline (contexts + surface prompts + surface pass +
# merger + index rebuild) chains automatically while the owner is AFK.
#
# Wait condition: ../ThaqalaynWordSources/translation/lemma_responses/
# must contain >= 13000 .json files (the lemma pass target is 13086).
# When that count is reached, the next stages fire in sequence.
#
# All stages are resumable, so re-running this script is safe.
#
# Usage:
#   pwsh -File scripts/path_b_continue_after_lemmas.ps1
#
$ErrorActionPreference = "Continue"
$Env:PYTHONPATH = "$PSScriptRoot\..;$PSScriptRoot\..\app"

$lemmasDir = Resolve-Path "$PSScriptRoot\..\..\ThaqalaynWordSources\translation\lemma_responses"
$contextsFile = "$PSScriptRoot\..\..\ThaqalaynWordSources\translation\surface_contexts.json"
$target = 13000  # leave headroom - pass target is 13086 but some may fail validation
$pollSec = 60

Write-Host "=== Path B continuation script ===" -ForegroundColor Cyan
Write-Host ("Waiting for lemma responses to reach " + $target + "...") -ForegroundColor DarkGray

# Stage 1: wait for lemma pass
while ($true) {
    $count = (Get-ChildItem $lemmasDir -Filter *.json | Measure-Object).Count
    if ($count -ge $target) {
        Write-Host ("Lemma pass complete (" + $count + " files); proceeding") -ForegroundColor Green
        break
    }
    Write-Host ("  [" + (Get-Date -Format HH:mm:ss) + "] lemma_responses: " + $count + " / " + $target) -ForegroundColor DarkGray
    Start-Sleep -Seconds $pollSec
}

# Stage 2: wait for contexts file (may have already finished)
if (-not (Test-Path $contextsFile)) {
    Write-Host "surface_contexts.json missing - extracting now ..." -ForegroundColor Yellow
    Set-Location "$PSScriptRoot\.."
    & .venv\Scripts\python.exe -u scripts\extract_corpus_contexts.py
    if ($LASTEXITCODE -ne 0) {
        Write-Host "extract_corpus_contexts.py failed" -ForegroundColor Red
        exit 1
    }
} else {
    # Was it created BEFORE the lemma pass started? If yes the file is the
    # 76-item pilot output, not the 102K full-corpus output. Re-extract.
    $ctxMtime = (Get-Item $contextsFile).LastWriteTime
    $earliest = (Get-Date).AddHours(-2)
    if ($ctxMtime -lt $earliest) {
        Write-Host "surface_contexts.json is stale; re-extracting ..." -ForegroundColor Yellow
        Set-Location "$PSScriptRoot\.."
        & .venv\Scripts\python.exe -u scripts\extract_corpus_contexts.py
        if ($LASTEXITCODE -ne 0) {
            Write-Host "extract_corpus_contexts.py failed" -ForegroundColor Red
            exit 1
        }
    } else {
        $sizeBytes = (Get-Item $contextsFile).Length
        $sizeMB = [Math]::Round($sizeBytes / 1048576, 1)
        $msg = 'surface_contexts.json present (' + $sizeMB + ' MB)'
        Write-Host $msg -ForegroundColor Green
    }
}

# Stage 3: extract surface prompts for full corpus (with contexts)
Write-Host "Extracting surface prompts for full corpus ..." -ForegroundColor Yellow
Set-Location "$PSScriptRoot\.."
& .venv\Scripts\python.exe scripts\extract_surface_translation_prompts.py `
    --corpus-contexts ..\ThaqalaynWordSources\translation\surface_contexts.json
if ($LASTEXITCODE -ne 0) {
    Write-Host "extract_surface_translation_prompts.py failed" -ForegroundColor Red
    exit 1
}

# Stage 4: Spark surface pass (~6-9 h at 12 workers)
Write-Host "Starting Spark surface pass (~6-9 h with 12 workers) ..." -ForegroundColor Yellow
& .venv\Scripts\python.exe -u scripts\run_path_b_translations.py --pass surface --workers 12
if ($LASTEXITCODE -ne 0) {
    Write-Host "run_path_b_translations.py --pass surface failed" -ForegroundColor Red
    exit 1
}

# Stage 5: merge translations into pages
Write-Host "Merging translations into lemma + surface page JSONs ..." -ForegroundColor Yellow
& .venv\Scripts\python.exe scripts\merge_translations_into_pages.py --pass both
if ($LASTEXITCODE -ne 0) {
    Write-Host "merge_translations_into_pages.py failed" -ForegroundColor Red
    exit 1
}

# Stage 6: rebuild indexes (so index/lemmas.json gets the 11-lang glosses)
Write-Host "Rebuilding word indexes ..." -ForegroundColor Yellow
& .venv\Scripts\python.exe scripts\build_word_indexes.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "build_word_indexes.py failed" -ForegroundColor Red
    exit 1
}

Write-Host "=== Path B continuation complete ===" -ForegroundColor Green
Write-Host "Next: review ThaqalaynWords + ThaqalaynWordSources git status,"
Write-Host "      commit, then revert Path C (commits d0ce4a9 + 34ff19c) in Thaqalayn"
