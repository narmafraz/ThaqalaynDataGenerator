param(
    # When set, also runs the Path B Spark translation pipeline:
    # extract lemma prompts -> Spark lemma pass (~1 h, $0) -> extract
    # corpus contexts -> extract surface prompts (anchored to lemma
    # responses + corpus contexts) -> Spark surface pass (~9-11 h, $0)
    # -> merge translations back into lemma/surface JSONs.
    # Off by default — full rebuild without Spark takes ~30 min, with
    # Spark takes ~12 h. Resumable: existing per-slug response files
    # are skipped, so a partial run can be picked up by re-running.
    # See Thaqalayn/docs/WORDS_PROJECT_PLAN.md "Path B" + PATH_B_SPARK_LOG.md.
    [switch]$IncludeTranslations
)

$Env:PYTHONPATH = "$PSScriptRoot;$PSScriptRoot/app"

# Rebuild the ThaqalaynWords output (surfaces/, lemmas/, roots/, index/)
# from the sources in ../ThaqalaynWordSources.
#
#   1. build_word_pages.py   — walks corpus_surface_set + CAMeL Tools +
#                              QAC + Wiktextract + Lane's + hawramani
#                              and writes the per-surface/per-lemma/per-root
#                              JSONs. ~30 minutes on the full corpus.
#   (optional, -IncludeTranslations)
#   2a. extract_lemma_translation_prompts.py   — emits JSONL prompts
#   2b. run_path_b_translations.py --pass lemma --include-classical
#                                                — Spark lemma pass (~1 h)
#   2c. extract_corpus_contexts.py — pre-extracts ±10-word windows
#   2d. extract_surface_translation_prompts.py — uses lemma + contexts
#   2e. run_path_b_translations.py --pass surface — Spark surface pass (~9-11 h)
#   2f. merge_translations_into_pages.py         — folds translations into
#                                                  ThaqalaynWords/{lemmas,surfaces}/
#
#   3. build_word_indexes.py — walks the output and writes the three
#                              index/*.json files. ~10 seconds.
#                              When -IncludeTranslations was set, the
#                              lemmas index gets the 11-lang `glosses`
#                              map (instead of the single Path C `gloss`).
#
# Reads/writes via the sibling-repo defaults baked into both scripts
# (../ThaqalaynWordSources for inputs, ../ThaqalaynWords for outputs),
# so no env vars beyond PYTHONPATH are needed.
#
# Run this:
#   - after pulling a builders.py change (e.g. new fields, slug logic)
#   - after the corpus grows (new books land in ThaqalaynDataSources +
#     get re-extracted into corpus_surface_set.json)
#   - with -IncludeTranslations only when you want to (re)run the full
#     Spark translation pipeline. That adds ~12 h to wall time and is
#     what populates the per-language word glosses; skip it otherwise.
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

$totalStages = if ($IncludeTranslations) { 9 } else { 3 }

Write-Host ""
Write-Host ("[1/" + $totalStages + "] Building surfaces + lemmas + roots ...") -ForegroundColor Yellow
uv run python .\scripts\build_word_pages.py --full
if ($LASTEXITCODE -ne 0) {
    Write-Host ("build_word_pages.py failed (exit " + $LASTEXITCODE + ")") -ForegroundColor Red
    exit $LASTEXITCODE
}

if ($IncludeTranslations) {
    # ─── Path B: Spark translation pipeline ───
    # Output flows into ../ThaqalaynWordSources/translation/ (JSONL
    # prompts + per-slug raw responses, committed as sacred per the
    # established "never strip data in sources" rule). The merge step
    # folds the translations field back into the per-page JSONs in
    # ../ThaqalaynWords/{lemmas,surfaces}/ so the API serves them.
    #
    # Each stage is resumable — Spark hiccups, machine sleep, or
    # ctrl-C can be picked up by re-running this script with
    # -IncludeTranslations (existing per-slug response files skipped).

    Write-Host ""
    Write-Host ("[2/" + $totalStages + "] Extracting lemma translation prompts ...") -ForegroundColor Yellow
    uv run python .\scripts\extract_lemma_translation_prompts.py
    if ($LASTEXITCODE -ne 0) {
        Write-Host ("extract_lemma_translation_prompts.py failed (exit " + $LASTEXITCODE + ")") -ForegroundColor Red
        exit $LASTEXITCODE
    }

    Write-Host ""
    Write-Host ("[3/" + $totalStages + "] Spark lemma translation pass (~1 h) ...") -ForegroundColor Yellow
    uv run python -u .\scripts\run_path_b_translations.py --pass lemma --workers 8 --include-classical
    if ($LASTEXITCODE -ne 0) {
        Write-Host ("run_path_b_translations.py --pass lemma failed (exit " + $LASTEXITCODE + ")") -ForegroundColor Red
        exit $LASTEXITCODE
    }

    Write-Host ""
    Write-Host ("[4/" + $totalStages + "] Extracting ±10-word corpus context windows ...") -ForegroundColor Yellow
    uv run python .\scripts\extract_corpus_contexts.py
    if ($LASTEXITCODE -ne 0) {
        Write-Host ("extract_corpus_contexts.py failed (exit " + $LASTEXITCODE + ")") -ForegroundColor Red
        exit $LASTEXITCODE
    }

    Write-Host ""
    Write-Host ("[5/" + $totalStages + "] Extracting surface translation prompts ...") -ForegroundColor Yellow
    uv run python .\scripts\extract_surface_translation_prompts.py `
        --corpus-contexts ..\ThaqalaynWordSources\translation\surface_contexts.json
    if ($LASTEXITCODE -ne 0) {
        Write-Host ("extract_surface_translation_prompts.py failed (exit " + $LASTEXITCODE + ")") -ForegroundColor Red
        exit $LASTEXITCODE
    }

    Write-Host ""
    Write-Host ("[6/" + $totalStages + "] Spark surface translation pass (~9-11 h) ...") -ForegroundColor Yellow
    uv run python -u .\scripts\run_path_b_translations.py --pass surface --workers 8
    if ($LASTEXITCODE -ne 0) {
        Write-Host ("run_path_b_translations.py --pass surface failed (exit " + $LASTEXITCODE + ")") -ForegroundColor Red
        exit $LASTEXITCODE
    }

}

# Always merge existing Path B response files into the freshly-built
# lemma/surface JSONs. The merger is idempotent (skips empty/issued
# responses, no-op when no response files exist) so it's safe even
# when the Spark passes didn't run this invocation. WITHOUT this step
# the basic regen would silently wipe out all Path B translations
# (build_word_pages.py emits translations=null for every page).
$mergeStage = if ($IncludeTranslations) { 8 } else { 2 }
Write-Host ""
Write-Host ("[" + $mergeStage + "/" + $totalStages + "] Merging Path B translations into lemma + surface pages ...") -ForegroundColor Yellow
uv run python .\scripts\merge_translations_into_pages.py --pass both
if ($LASTEXITCODE -ne 0) {
    Write-Host ("merge_translations_into_pages.py failed (exit " + $LASTEXITCODE + ")") -ForegroundColor Red
    exit $LASTEXITCODE
}

$indexStage = $totalStages
Write-Host ""
Write-Host ("[" + $indexStage + "/" + $totalStages + "] Building index files ...") -ForegroundColor Yellow
uv run python .\scripts\build_word_indexes.py
if ($LASTEXITCODE -ne 0) {
    Write-Host ("build_word_indexes.py failed (exit " + $LASTEXITCODE + ")") -ForegroundColor Red
    exit $LASTEXITCODE
}

$elapsed = (Get-Date) - $start
Write-Host ""
Write-Host ("=== Done in {0:mm}m{0:ss}s ===" -f $elapsed) -ForegroundColor Green
Write-Host "Next: cd ../ThaqalaynWords; git status to review, commit, push." -ForegroundColor Cyan
