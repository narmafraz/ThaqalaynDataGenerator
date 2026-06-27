# Rebuild (and optionally deploy) the ThaqalaynSearch Pagefind bundles.
#
# Build reads verse_detail files from ../ThaqalaynData and writes per-language
# Pagefind bundles into ../ThaqalaynSearch/dist/ (one self-contained bundle per
# language + manifest.json + qref.json). Kept out of add_data.ps1 — the build is
# slow (one fragment file per verse per language) — same convention as
# regen_words.ps1.
#
# Deploy (-Deploy) ships the bundles as MULTIPLE Netlify sites, because a single
# site can't take ~650K files (see SEARCH_OVERHAUL_PLAN.md):
#   - manifest.json + qref.json -> thaqalaynsearch.netlify.app   (meta site)
#   - dist/<lang>               -> thaqalaynsearch-<lang>.netlify.app
# Requires `netlify login` to have been run once. Each per-language deploy is
# ~53K files; deploying all 12 is long, so -Langs limits it for testing.
#
# Usage:
#   ./regen_search.ps1                          # build all books, all languages
#   ./regen_search.ps1 al-amali-mufid           # build one book (testing)
#   ./regen_search.ps1 -Deploy                  # build + deploy meta + all langs
#   ./regen_search.ps1 -Deploy -Langs en        # build + deploy meta + just en
param(
    [switch]$Deploy,
    [string[]]$Langs,                                       # limit deploy to these langs (default: all built)
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$BuildArgs                                    # book slugs forwarded to build.mjs
)

$ErrorActionPreference = "Stop"
Write-Host ""
Write-Host "=== ThaqalaynSearch regen ===" -ForegroundColor Cyan
$start = Get-Date

Push-Location (Join-Path $PSScriptRoot "../ThaqalaynSearch")
try {
    if (-not (Test-Path "node_modules")) {
        Write-Host "Installing npm deps..." -ForegroundColor Yellow
        npm install
    }

    node build.mjs @BuildArgs
    if ($LASTEXITCODE -ne 0) { throw "build.mjs failed (exit $LASTEXITCODE)" }

    if ($Deploy) {
        $manifest = Get-Content "dist/_meta/manifest.json" -Raw | ConvertFrom-Json
        $built = @($manifest.languages.code)
        if ($Langs) { $built = $built | Where-Object { $Langs -contains $_ } }

        # Meta site: manifest + qref (small; deploys in seconds). build.mjs already
        # wrote them into dist/_meta.
        Write-Host "Deploying meta -> thaqalaynsearch" -ForegroundColor Yellow
        netlify deploy --prod --no-build --dir="dist/_meta" --site thaqalaynsearch
        if ($LASTEXITCODE -ne 0) { throw "meta deploy failed" }

        # Per-language bundles (each ~53K files -> minutes each).
        foreach ($l in $built) {
            Write-Host "Deploying $l -> thaqalaynsearch-$l" -ForegroundColor Yellow
            netlify deploy --prod --no-build --dir="dist/$l" --site "thaqalaynsearch-$l"
            if ($LASTEXITCODE -ne 0) { throw "deploy failed for $l" }
        }
    }
}
finally {
    Pop-Location
}

Write-Host ("Done in {0:n1} min" -f ((Get-Date) - $start).TotalMinutes) -ForegroundColor Green
