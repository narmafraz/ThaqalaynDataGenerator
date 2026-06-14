# Rebuild the ThaqalaynSearch Pagefind bundle (per-language) from ../ThaqalaynData.
#
# Run this AFTER add_data.ps1 has regenerated ThaqalaynData, when search needs
# refreshing. Deliberately kept OUT of add_data.ps1 — the bundle is large and
# slow to build (one fragment file per verse per language), and changes less
# often than the routine hadith add. Same convention as regen_words.ps1.
#
# The build is self-contained Node (../ThaqalaynSearch/build.mjs); it reads
# verse_detail files from ../ThaqalaynData and writes the bundle into the
# ThaqalaynSearch repo. Deploy that bundle to thaqalaynsearch.netlify.app
# (see ThaqalaynSearch/README.md).
#
# Optional args are passed through to build.mjs (e.g. book slugs to limit a
# test build):
#   ./regen_search.ps1                 # all books, all languages
#   ./regen_search.ps1 al-amali-mufid  # one book (testing)

Write-Host ""
Write-Host "=== ThaqalaynSearch regen ===" -ForegroundColor Cyan
$start = Get-Date

$searchDir = Join-Path $PSScriptRoot "../ThaqalaynSearch"
Push-Location $searchDir
try {
    if (-not (Test-Path "node_modules")) {
        Write-Host "Installing npm deps..." -ForegroundColor Yellow
        npm install
    }
    node build.mjs @args
}
finally {
    Pop-Location
}

Write-Host ("Done in {0:n1} min" -f ((Get-Date) - $start).TotalMinutes) -ForegroundColor Green
