# Pipeline v3 — 5-Batch Evaluation Results (2026-03-07)

## Overview

Ran 5 consecutive batches of 20 verses each through the pipeline v3 orchestrator, analyzing results after each batch and implementing improvements before the next. All verses were Al-Kafi hadiths with ≤199 Arabic words (single-pass, not chunked).

**Configuration:** `--workers 10 --max-words 199 --max-verses 20 --fix-model sonnet`

## Batch-by-Batch Results

| Metric | Batch 1 | Batch 2 | Batch 3 | Batch 4 | Batch 5 | Total |
|--------|---------|---------|---------|---------|---------|-------|
| Queued | 20 | 20 | 20 | 20 | 20 | 100 |
| Completed | 17 | 18 | 12 | 14 | 12 | 73 |
| Stalled/Killed | 0 | 0 | 4 | 1 | 3 | 8 |
| Pass (direct) | 7 | 4 | 4 | 4 | 3 | 22 |
| Fixed by fix pass | 0 | 0 | 1 | 3 | 2 | 6 |
| **Effective pass** | **7 (41%)** | **4 (22%)** | **5 (42%)** | **7 (50%)** | **5 (42%)** | **28 (38%)** |
| Needs fix (unfixed) | 7 | 8 | 2 | 1 | 1 | 19 |
| Errors | 4 | 6 | 5 | 7 | 8 | 30 |
| Cost | $29.63 | $52.41 | $15.96 | $36.86 | $16.71 | ~$151 |
| Session time | 47m | 95m | ~80m | 90m | ~80m | ~392m |

**Note:** Many verses were retried across batches (the pipeline picks up previously errored/unfixed verses). The 100 queued slots do not represent 100 unique verses — chronic failures consumed multiple slots.

## Word Count vs Success Rate

Analysis of all 48 verses with stats files:

| Word Bucket | Count | Pass% | Error% | Avg Cost | Avg Gen Time |
|-------------|-------|-------|--------|----------|-------------|
| 1-20 | 7 | **86%** | 14% | $0.96 | 491s |
| 21-40 | 20 | **85%** | 10% | $1.28 | 648s |
| 41-60 | 7 | **86%** | 14% | $1.67 | 889s |
| 61-100 | 9 | **44%** | **56%** | $2.10 | 1110s |
| 101-150 | 4 | **50%** | **50%** | $1.73 | 1380s |

### Key Finding: Sharp Cliff at ~60 Words

- **Below 60 words:** 85-86% pass rate, 10-14% error rate. Reliable.
- **Above 60 words:** Pass rate drops to 44-50%, error rate jumps to 50-56%.

The primary failure mode for longer verses is **truncated or continuation responses** — the model produces partial JSON that starts mid-output or outputs narrative text like "Continuing from entry 41..." instead of the complete result. The `--max-turns 1` flag prevents multi-turn completion, so these are unrecoverable.

### Cost Scales Linearly with Words

Average cost per verse increases roughly linearly: ~$0.96 for ≤20 words up to ~$2.10 for 61-100 words. Generation time also scales: ~8 min for short, ~18-23 min for medium.

## Chronic Failure Verses

Five verses never succeeded across all batches despite 3-7 retry attempts each:

| Verse ID | Words | Starts | Errors | Pattern |
|----------|-------|--------|--------|---------|
| `al-kafi_1_2_22_11` | **8** | 4 | 4 | Always errors (tiny verse, unclear why) |
| `al-kafi_1_2_19_14` | 62 | 7 | 6 | Produces continuation fragments |
| `al-kafi_1_3_12_6` | 96 | 4 | 2 | Stalls (>30 min) or continuation |
| `al-kafi_1_3_10_12` | 123 | 4 | 3 | Stalls or truncated output |
| `al-kafi_1_2_19_13` | 147 | 7 | 2 | Stalls indefinitely, timed out once |

Chronic failures are **not purely word-count driven** — the 8-word verse fails every time. There may be something in the Arabic text content itself (unusual characters, encoding, or structure) that triggers failure modes.

## Fix Pass Performance

The fix pass went from completely broken to working across the evaluation:

| Metric | Batch 1-2 | Batch 3 | Batch 4-5 |
|--------|-----------|---------|-----------|
| Fix attempts | 15 | 3 | 5 |
| Successful fixes | 0 | 1 | 5 |
| **Fix success rate** | **0%** | **33%** | **100% (B4: 75%)** |
| Still failing (false positive) | N/A | 2 | 2 |

Fix failures in batches 1-2 were caused by a JSON parsing bug — the fix model outputs narrative analysis **before** the JSON code fence, but `strip_code_fences()` only handled text starting with `` ``` `` or `{`.

"Still failing" fixes are cases where the fix model correctly analyzes the warnings, determines they are false positives (e.g., German text legitimately lacking ä/ö/ü), and outputs no corrections — so the verse remains `needs_fix`.

## Pipeline Changes Implemented

### Change 1: Enhanced malformed response detection (Post-Batch 1)
**File:** `app/pipeline_cli/pipeline.py` (lines ~430-451)
**Problem:** Model sometimes produces narrative text wrapped in `` ```json `` fences. The malformed check only tested if raw response starts with `{` or `` ` ``, so fenced non-JSON passed.
**Fix:** Added a second check that strips fences and verifies content starts with `{`. Retries on failure.
**Impact:** Caught 2 malformed responses in batch 1 that would have been errors.

### Change 2: Quran ref range support (Post-Batch 1)
**File:** `app/ai_pipeline.py` (lines ~1002-1020)
**Problem:** Validator only accepted `surah:ayah` format (e.g., `96:1`), but model outputs ranges like `96:1-5`.
**Fix:** Parse range format, validate both start and end ayah against `QURAN_SURAH_AYAH_COUNTS`.
**Impact:** Eliminated false validation errors for Quran references with ranges.

### Change 3: Fix model CLI default corrected (Post-Batch 1)
**File:** `app/pipeline_cli/pipeline.py` (line ~813)
**Problem:** The dataclass default was changed to `"sonnet"` but the argparse CLI default at line 813 was still `"haiku"`. Batch 1 ran with haiku fix model.
**Fix:** Changed CLI argparse default to `"sonnet"`.
**Impact:** Fix model now correctly uses Sonnet by default.

### Change 4: Fix pass partial merge — `_deep_merge()` (Post-Batch 1)
**File:** `app/pipeline_cli/verse_processor.py`
**Problem:** The fix prompt instructs "Output a JSON object containing ONLY the corrected fields" but `apply_fix()` expected a complete result with all fields. Every partial fix failed validation.
**Fix:** Added `_deep_merge(base, patch)` helper and `original_result` parameter to `apply_fix()`. When fix response lacks `content_type` (indicating partial), merge corrections into original.
**Impact:** Prerequisite for fix pass to work at all.

### Change 5: Fix raw response archiving (Post-Batch 1)
**File:** `app/pipeline_cli/pipeline.py`
**Problem:** No way to debug fix pass failures — raw fix responses were discarded.
**Fix:** Archive fix raw responses as `{verse_id}.fix.raw.txt` in `raw_responses/` directory.
**Impact:** Enabled diagnosis of the `strip_code_fences` issue in batch 2.

### Change 6: Race condition fix for work_dir (Post-Batch 1)
**File:** `app/pipeline_cli/pipeline.py` (line ~471)
**Problem:** When restarting after abort, old process cleanup can race with new process creation, causing `FileNotFoundError`.
**Fix:** Added defensive `os.makedirs(work_dir, exist_ok=True)` before writing `raw_response.txt`.
**Impact:** Eliminated crash on pipeline restart.

### Change 7: `strip_code_fences()` handles leading narrative text (Post-Batch 2) — HIGHEST IMPACT
**File:** `app/pipeline_cli/verse_processor.py` (lines ~386-400)
**Problem:** Fix model responses start with narrative analysis (e.g., "## Analysis of Flagged Issues") before the JSON code fence. `strip_code_fences()` only handled text starting with `` ``` `` or `{`, so it returned the narrative + JSON as one blob, which failed `json.loads()`.
**Fix:** Added pre-check: if text doesn't start with `` ``` `` or `{`, find the first `` ``` `` fence and strip everything before it.
**Impact:** Fix pass went from 0% success to 75% success. This was the single most impactful change.

### Change 8: German `missing_diacritics` downgraded to "low" severity (Post-Batch 3)
**File:** `app/ai_pipeline_review.py` (line ~329)
**Problem:** German text about Islamic topics often legitimately lacks ä/ö/ü/ß (e.g., "Muhammad sagte..." has no umlauts). The `missing_diacritics` check flagged these as "medium" severity, triggering unnecessary fix passes. Fix model consistently confirmed these were false positives.
**Fix:** Changed severity to `"low"` when `lang == "de"`.
**Impact:** Fewer unnecessary fix passes, reducing cost and improving effective pass rate.

### Change 9: 30-minute timeout for `claude -p` calls (Post-Batch 3)
**File:** `app/pipeline_cli/pipeline.py` (lines ~284-300)
**Problem:** 3-4 verses chronically stall indefinitely — `claude -p` subprocess never returns. Pipeline hangs waiting.
**Fix:** Wrapped `proc.communicate()` in `asyncio.wait_for(timeout=1800)`. On timeout, kills process and retries once.
**Impact:** Pipeline completes instead of hanging forever. Stalled verses get error status and don't block other work.

## Remaining Issues & Recommendations

### 1. High error rate for 60+ word hadiths
The `--max-turns 1` constraint means truncated responses can't be recovered. Options:
- Allow `--max-turns 2` for verses >60 words (higher cost but better completion)
- Pre-split prompt for long verses to reduce output size
- Increase `--max-words` threshold to exclude problematic lengths

### 2. Chronic failure verses
Some verses fail every attempt regardless of word count. Root cause investigation needed — check the Arabic source text for unusual encoding, characters, or structure.

### 3. False positive review warnings
Some `needs_fix` verses can't be fixed because the warnings are legitimate false positives (e.g., Bengali character counting, chunk coherence with multi-byte scripts). Consider:
- Auto-accept fix model's "no change needed" as a pass
- Refine `chunk_translation_mismatch` for multi-byte scripts (Bengali, Chinese)

### 4. Retry waste
Chronic failures consume retry slots across batches. Consider a quarantine mechanism that stops retrying after N failures for the same verse.
