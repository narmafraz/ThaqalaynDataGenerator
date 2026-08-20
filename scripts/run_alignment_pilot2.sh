#!/bin/bash
# Bigger align-scraped pilot (item 9 validation round 2) — ~1,300 queued verses
# across 4 books with different translation styles. --skip-merge: artifacts
# only; Data merge happens once, after the quarantine analysis.
#   al-amali-mufid  : full book (387) incl. retry of the 3 number-prefix quarantines
#   al-kafi         : first 400 incl. retry of 1:2:1:5's two quarantined ids
#   tahdhib-al-ahkam: first 400 (different source/translation style)
#   quran           : first 600 (mostly single-chunk ayahs -> tests eligibility skips)
set -u
cd "$(dirname "$0")/.."
export AI_CONTENT_SUBDIR=corpus
export SOURCE_DATA_DIR="../ThaqalaynDataSources/"
export PYTHONPATH="$PWD:$PWD/app"
PY=.venv/Scripts/python.exe

echo "=== pilot2: al-amali-mufid (full, retry quarantined) ==="
$PY -m app.pipeline_cli.pipeline align-scraped --book al-amali-mufid \
    --langs en --workers 8 --attempt-quarantined --skip-merge

echo "=== pilot2: al-kafi (400, retry quarantined) ==="
$PY -m app.pipeline_cli.pipeline align-scraped --book al-kafi \
    --langs en --workers 8 --attempt-quarantined --skip-merge --max-verses 400

echo "=== pilot2: tahdhib-al-ahkam (400) ==="
$PY -m app.pipeline_cli.pipeline align-scraped --book tahdhib-al-ahkam \
    --langs en --workers 8 --skip-merge --max-verses 400

echo "=== pilot2: quran (600) ==="
$PY -m app.pipeline_cli.pipeline align-scraped --book quran \
    --langs en --workers 8 --skip-merge --max-verses 600

echo "=== pilot2 complete ==="
