# Rebuild (and optionally deploy) the ThaqalaynSearch Pagefind bundles.
#
# Build reads verse_detail files from ../ThaqalaynData and writes per-language
# Pagefind bundles into ../ThaqalaynSearch/dist/ (one self-contained bundle per
# language + manifest.json + qref.json). Kept out of add_data.ps1 — the build is
# slow (one fragment file per verse per language) — same convention as
# regen_words.ps1.
#
# Deploy runs BY DEFAULT (-NoDeploy to skip). It ships the bundles as MULTIPLE
# Netlify sites, because a single site can't take ~650K files (see
# SEARCH_OVERHAUL_PLAN.md):
#   - manifest.json + qref.json -> thaqalaynsearch.netlify.app   (meta site)
#   - dist/<lang>               -> thaqalaynsearch-<lang>.netlify.app
# Requires `netlify login` to have been run once. Each per-language deploy is
# ~53K files; deploying all 12 is long, so -Langs limits it for testing.
#
# Usage:
#   ./regen_and_deploy_search.ps1                          # build + deploy meta + all langs (default)
#   ./regen_and_deploy_search.ps1 -NoDeploy                # build only, no deploy
#   ./regen_and_deploy_search.ps1 -Langs en                # build + deploy meta + just en
#   ./regen_and_deploy_search.ps1 al-amali-mufid -NoDeploy # build one book only, no deploy
param(
    [switch]$NoDeploy,                                      # build only; skip the Netlify deploy
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

    $buildArgs = @($BuildArgs)
    if ($Langs) { $buildArgs += @("--langs", ($Langs -join ",")) }
    node build.mjs @buildArgs
    if ($LASTEXITCODE -ne 0) { throw "build.mjs failed (exit $LASTEXITCODE)" }

    if (-not $NoDeploy) {
        # Resilient, resumable multi-site deploy (deploy.mjs): deploys meta + each
        # built language site, skips sites already done for this data_version, and
        # retries transient failures. Re-run regen_and_deploy_search.ps1 to resume after an
        # interruption.
        $deployArgs = @()
        if ($Langs) { $deployArgs += @("--langs", ($Langs -join ",")) }
        node deploy.mjs @deployArgs
        if ($LASTEXITCODE -ne 0) { throw "deploy failed (re-run regen_and_deploy_search.ps1 to resume)" }
    }
}
finally {
    Pop-Location
}

Write-Host ("Done in {0:n1} min" -f ((Get-Date) - $start).TotalMinutes) -ForegroundColor Green
