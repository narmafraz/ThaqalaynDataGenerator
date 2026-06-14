# Run every regen pipeline in dependency order. One place to rebuild everything.
#
#   1. add_data.ps1     -> ThaqalaynData    (hadith pipeline: parse + AI merge)
#   2. regen_words.ps1  -> ThaqalaynWords   (per-word dictionary)
#   3. regen_search.ps1 -> ThaqalaynSearch  (Pagefind bundle; depends on the
#                                            merged ThaqalaynData, so it runs last)
#
# Slow: data is minutes, the word build ~30 min (or ~12 h with -IncludeWordTranslations),
# the search build scales with corpus size. Each sub-script is independently
# runnable and resumable; this is just the convenience "do it all" entry point.
param(
    # Forward to regen_words.ps1 -IncludeTranslations (adds the ~12 h Spark pass).
    [switch]$IncludeWordTranslations
)

$ErrorActionPreference = "Stop"
Write-Host ""
Write-Host "=== regen_all: data -> words -> search ===" -ForegroundColor Cyan
$start = Get-Date

& "$PSScriptRoot/add_data.ps1"

if ($IncludeWordTranslations) {
    & "$PSScriptRoot/regen_words.ps1" -IncludeTranslations
} else {
    & "$PSScriptRoot/regen_words.ps1"
}

& "$PSScriptRoot/regen_search.ps1"

Write-Host ("=== regen_all done in {0:n1} min ===" -f ((Get-Date) - $start).TotalMinutes) -ForegroundColor Green
