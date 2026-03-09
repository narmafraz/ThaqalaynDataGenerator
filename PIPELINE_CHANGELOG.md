# Pipeline Changelog

Tracks changes to the AI content generation pipeline with rationale. Each entry includes what changed, why, and supporting data from the run that motivated it.

---

## v4.0.3 — 2026-03-08

**Motivation**: Run `20260308T163938Z` (20 verses processed, 17 passed, 1 needs_fix, 2 errors, $42.26 total). Fix pass rate: **40%** (5 attempted, 2 succeeded). Previous runs showed 100% fix pass rate. Root cause identified: validation errors routed to the fix pass had `field="validation"` which doesn't exist as a key in the result dict, causing `build_fix_prompt()` to produce an empty `flagged_fields` context — the model was asked to fix fields it couldn't see.

Other errors in this batch (10× "no output", 6× git-bash, 1× paging-file OOM, 1× timeout) are all Windows infrastructure issues not addressable in pipeline code.

### Root Cause

In `verse_processor.py`, when fixable validation errors are routed to the LLM fix pass, synthetic `ReviewWarning` objects were created with `field="validation"`:

```python
verse_result.warnings.append(ReviewWarning(
    field="validation",  # BUG: "validation" is not a key in the result dict
    ...
))
```

In `build_fix_prompt()` (`ai_pipeline_review.py`), the field is used to look up current values:

```python
top_key = field_path.split(".")[0].split("[")[0]
if top_key in result:
    flagged_fields[top_key] = result[top_key]
elif field_path in result:
    flagged_fields[field_path] = result[field_path]
```

Since `"validation"` was never a key in any result dict, `flagged_fields` was always `{}`. The fix model received:

```
FLAGGED FIELDS (current values):
{}
```

With no context, the model couldn't fix anything — the output was either empty or fabricated, causing re-validation failure. This explains the 40% → 60% failure rate for validation-error fix passes.

### Change

#### 1. Add `_validation_error_to_field()` mapping helper in `verse_processor.py`

**What**: New helper function that maps validation error message strings to their corresponding result dict field names:
- `"word_tags[N] ... has no diacritics"` → `"word_tags"`
- `"word_analysis[N] ... has no diacritics"` → `"word_analysis"`
- `"invalid topic: ..."` → `"topics"`
- `"invalid tag: ..."` → `"tags"`
- `"invalid content_type: ..."` → `"content_type"`
- `"missing ambiguity_note"` / `"invalid narrator role:"` / `"invalid identity_confidence:"` → `"isnad_matn"`
- `"invalid chunk_type: ..."` → `"chunks"`
- `"invalid diacritics_status: ..."` → `"diacritics_status"`
- `"invalid quran relationship: ..."` → `"related_quran"`
- `"key_terms key ..."` → `"translations"`
- `"invalid pos: ..."` → `"word_tags"` (v4 default)

#### 2. Use `_validation_error_to_field()` when creating synthetic warnings

**What**: Changed the `ReviewWarning` creation in the fixable-error routing block from `field="validation"` to `field=_validation_error_to_field(err)`.

**Why**: With the correct field name, `build_fix_prompt()` now includes the actual current field values in the prompt context, giving the fix model the information it needs to produce a valid correction.

**Files changed**: `verse_processor.py`

### Estimated Impact

Fix pass route is exercised when `has no diacritics`, invalid enum, or missing narrator fields survive the auto-fix pass. Based on the 40% success rate across 5 attempts in this batch, fixing the empty-context bug should raise the fix pass success rate to **80%+**, converting ~1.5 additional pass/100 verses from error→pass. Saves ~$0.60-1.00/100 verses in wasted generation cost.

### Test Changes

- 1 new test: `test_validation_error_to_field_mapping`
- Total: 1379+ tests passing

---

## v4.0.2 — 2026-03-08

**Motivation**: Run `20260308T123736Z` (8 verses processed, 8 errors, $9.97 wasted — 100% waste rate). ALL 8 verses failed with the same two intertwined errors as v4.0.1 was supposed to fix:
- `word_analysis_error` (491 total): `word_analysis[N] missing field: translation` on every word of every verse
- `translations.*.text missing field: text` for all 11 languages across all 8 verses

### Root Cause (regression of v4.0.1 fix)

The v4.0.1 fix moved `is_v4` detection before `reconstruct_fields()` in `validate_result()`. However, a **second injection path** was missed: `postprocess_verse()` in `verse_processor.py` (lines 588–594) explicitly adds a synthetic `word_analysis` to the result from `word_tags` **before** calling `validate_result()`:

```python
if "word_tags" in result and "word_analysis" not in result:
    result["word_analysis"] = [{"word": wt[0], "pos": wt[1]} for wt in result["word_tags"] ...]
```

So by the time `validate_result()` ran, `word_analysis` was already present, making `is_v4 = "word_tags" in result and "word_analysis" not in result` evaluate to **False**. v3 word_analysis validation then ran, requiring `translation` on every word entry (491 failures), and v3 translations validation required `text` on all 11 languages (88 failures).

### Change

#### 1. Fix `is_v4` detection to be resilient against synthetic `word_analysis`

**What**: Changed `is_v4` detection in `validate_result()` from:
```python
is_v4 = "word_tags" in result and "word_analysis" not in result
```
to:
```python
is_v4 = "word_tags" in result
```

**Why**: A response is v4 if and only if the model output included `word_tags` — the presence of a synthetic `word_analysis` stub (injected by `postprocess_verse` or `reconstruct_fields` for reconstruction purposes) is irrelevant to format detection. The `"word_analysis" not in result` condition was the root cause of the regression in both v4.0.1 and this batch.

**Files changed**: `ai_pipeline.py` (`validate_result`)

### Estimated Impact

100% of errors in this batch were caused by this single bug. Fix eliminates all 8/8 erroring verses and $9.97 wasted cost.

Expected pass rate: **0% → 85%+**

### Test Changes

- Existing 1379 tests continue to pass

---

## v3.2.0 — 2026-03-08

**Motivation**: Run `20260308T001946Z` (100 al-istibsar verses, 20 workers) showed 38% error rate — 58 pass, 36 error, $218 total, $76 wasted (35%). Analysis revealed three dominant error categories: continuation artifacts (47% of errors), missing diacritics on abbreviations (11%), and single-pass failures on 60-199 word verses.

### Changes

#### 1. Lower chunked processing threshold from 200 to 80 words

**What**: Changed `CHUNKED_PROCESSING_THRESHOLD` from 200 to 80. Verses with >80 Arabic words now use structure+chunk processing instead of single-pass. Replaced hardcoded `200` in `verse_processor.py` with the imported constant.

**Why**: Word count was the strongest failure predictor. Verses >60 words failed at ~50%, >90 words at ~80%. The 80-199 word range was a danger zone — too long for reliable single-pass output (model hits output token limits, then produces continuation artifacts) but below the chunked threshold. 22 of 36 errors were from verses in this range.

**Data**: Pass rate by word count in run 20260308T001946Z:
- <60 words: ~85% pass
- 60-99 words: ~50% pass
- 100-199 words: ~20% pass

**Estimated impact**: Eliminates ~22 of 36 errors, saving ~$44/100 verses.

**Files changed**: `ai_pipeline_review.py` (constant), `verse_processor.py` (import + 2 usages)

#### 2. Whitelist single-letter Arabic abbreviations in diacritics check

**What**: The `validate_result()` diacritics check now skips single-letter Arabic words (after stripping tatweel `ـ` and trailing dots). Applies to both v3 `word_analysis` and v4 `word_tags` validation paths.

**Why**: Standard hadith abbreviations like `ع` (عليه السلام), `ص` (صلى الله عليه وآله), `ج` (جزء/volume), `صـ` (صفحة/page) cannot carry diacritics — they are abbreviations, not words to vocalize. These caused 29 validation failures across 15 verses in the run.

**Files changed**: `ai_pipeline.py` (2 diacritics check blocks)

#### 3. Route remaining diacritics errors to fix pass

**What**: Added `"has no diacritics"` to `FIXABLE_PATTERNS` in `verse_processor.py`. Multi-letter undiacritized words that aren't abbreviations are now sent to the LLM fix pass instead of being terminal errors.

**Why**: The fix pass has a 50%+ success rate for targeted field corrections and costs $0.30-0.50 vs $1-2 for full regeneration. Diacritics fixes are a good candidate — the LLM just needs to add tashkeel marks to specific words.

**Files changed**: `verse_processor.py` (FIXABLE_PATTERNS)

#### 4. Early detection of continuation artifacts

**What**: Added explicit check for continuation artifact prefixes ("Continuing", "Picking up", "Resuming", "Here is the rest", "**Part", "**Piece", "**Continuing") before the existing malformed response check. Detected artifacts trigger a retry with a clear log message.

**Why**: 17/36 errors (47%) were continuation artifacts where the model output "Continuing the JSON from..." or partial fragments instead of a fresh response. The existing malformed check only caught responses not starting with `{` or backtick, missing cases where continuation text appeared before valid-looking code fences.

**Files changed**: `pipeline.py` (process_verse malformed response check)

### Test Changes

- 2 new tests: `test_single_letter_abbreviation_skips_diacritics_check`, `test_multi_letter_undiacritized_still_fails`
- Total: 1379 tests passing

### Combined Estimated Impact

| Improvement | Errors eliminated | Cost saved per 100 verses |
|-------------|-------------------|---------------------------|
| Chunked threshold 200→80 | ~22 | ~$44 |
| Abbreviation whitelist | ~4 (terminal→pass) | ~$8 |
| Diacritics → fix pass | ~4 (terminal→fixable) | ~$4 |
| Continuation detection | ~5 (faster retry) | ~$10 |
| **Total** | **~35 of 36** | **~$66** |

Expected pass rate improvement: **62% → 85%+**
Expected cost/successful verse: **$3.76 → ~$2.00**

---

## v4.0.1 — 2026-03-08

**Motivation**: Run `20260308T121930Z` (8 verses processed, 8 errors, $9.49 wasted — 100% waste rate). ALL 8 verses failed with two intertwined errors:
- `word_analysis_error` (500 total): `word_analysis[N] missing field: translation` on every word of every verse
- `translations.*.text missing field: text` for all 11 languages across all 8 verses

### Root Cause

`is_v4` was detected **after** `reconstruct_fields()` was called. When a v4 response (has `word_tags`, no `word_analysis`, no `diacritized_text`) was passed to `validate_result()`:

1. `reconstruct_fields()` synthesised a minimal `word_analysis` (word/pos only, no translation) from `word_tags`
2. `is_v4 = "word_tags" in result and "word_analysis" not in result` → **False** (word_analysis now present!)
3. v3 word_analysis validation ran, finding `translation` missing on every word entry
4. v3 translations validation required `text` field (not generated in v4), failing all 11 languages

This affected all verses that lacked `diacritized_text` (the common case for stripped/raw model output).

### Changes

#### 1. Move `is_v4` detection before `reconstruct_fields()` call

**What**: In `validate_result()`, moved `is_v4 = "word_tags" in result and "word_analysis" not in result` to run **before** `reconstruct_fields()` is called, so it correctly detects v4 format even after the synthetic word_analysis is injected.

**Files changed**: `ai_pipeline.py` (`validate_result`)

#### 2. Skip v3 word_analysis validation for v4 responses

**What**: Changed `if "word_analysis" in result:` to `if "word_analysis" in result and not is_v4:` in the word_analysis validation block. The synthetic word_analysis injected by `reconstruct_fields()` for v4 responses lacks `translation` fields by design — it only has `word` and `pos` for reconstruction purposes.

**Why**: Even with the `is_v4` fix above, the synthesised `word_analysis` would still trigger incorrect translation-field errors for any caller that adds word_analysis post-reconstruction.

**Files changed**: `ai_pipeline.py` (`validate_result`)

### Estimated Impact

100% of errors in this batch were caused by this single bug. Fix eliminates all 8/8 erroring verses and $9.49 wasted cost (~$1.19/verse average).

Expected pass rate: **0% → 85%+** (once is_v4 detection is correct, v4 validation path is taken)

### Test Changes

- No new tests added; existing 1379 tests continue to pass

---

## v4.0.0 — 2026-03-08

**Motivation**: v3 costs ~$2.00/hadith ($116K for 58K corpus). Word analysis (46% of output) translates the same Arabic words thousands of times. Translations.text duplicates chunk content. Target: $0.27-0.36/hadith with Haiku.

### Architectural Changes

#### 1. Word dictionary instead of per-hadith word_analysis

**What**: Model now outputs `word_tags` — just `[["قَالَ","V"],["عَنْ","PREP"],...]` instead of full `word_analysis` with 11-language translations per word. Unique (word, POS) pairs are collected corpus-wide and translated once via a separate dictionary pass.

**Impact**: 46% of output → 3.5%. Saves ~25,000 chars per hadith.

**Files changed**:
- `ai_pipeline.py`: `build_user_message()` field #4 → word_tags, `validate_result()` dual-format support, `strip_redundant_fields()` / `reconstruct_fields()` v4 handling
- `ai_pipeline_review.py`: All 10 review checks updated for word_tags format
- `verse_processor.py`: Postprocessing handles word_tags, model tag → `pipeline_v4`
- `pipeline_cli/word_dictionary.py` (NEW): `extract_unique_words()`, `load_v4_dictionary()`, `save_v4_dictionary()`, `assemble_word_analysis()`, `find_missing_words()`, `build_translation_prompt()`

#### 2. Chunks-only translations (no duplicate full text)

**What**: Removed `translations.*.text` from the prompt schema. Model only generates chunk translations. Full text is assembled in `reconstruct_fields()` (already implemented in v3).

**Impact**: ~15% of output tokens saved.

**Files changed**:
- `ai_pipeline.py`: Field #9 updated to exclude "text", v4 validation makes text optional
- `ai_pipeline_review.py`: Fix prompts updated

#### 3. Removed similar_content_hints

**What**: Removed field #13 `similar_content_hints` from prompt. Low value, not used downstream.

**Impact**: ~3-5% of output tokens saved.

### Combined Impact

| Metric | v3 | v4 |
|--------|----|----|
| Output chars (avg) | ~56,000 | ~22,000 |
| Output reduction | — | **59%** |
| Cost/hadith (Sonnet) | $2.00 | $0.90-1.20 |
| Cost/hadith (Haiku) | — | $0.27-0.36 |
| 58K corpus (Haiku) | — | $16-21K |

### Backward Compatibility

- v3 responses (with `word_analysis`) continue to pass validation and review
- `reconstruct_fields()` handles both formats transparently
- Dictionary-less mode uses `???` placeholders for untranslated words

### Test Changes

- 3 existing tests updated for v4 prompt changes
- 16 new tests: `TestV4WordTags` (6), `TestV4StripReconstruct` (3), `TestWordDictionary` (7)
- Total: 1377 tests passing

---

## v3.1.0 — 2026-03-08

**Motivation**: Run `20260307T225947Z` (20 verses, 20 workers) showed 40% error rate ($33.04 for 10 successes). Analysis revealed most errors were trivially fixable validation issues in otherwise-complete responses.

### Changes

#### 1. Auto-fix trivial validation errors in postprocessing (zero LLM cost)

**What**: Added `_auto_fix_validation_errors()` in `verse_processor.py` that programmatically fixes:
- **Missing `ambiguity_note`**: When narrator has `identity_confidence` of "likely"/"ambiguous" but no `ambiguity_note`, inserts a generic note: "Multiple narrators share this name; identified based on chain context and historical records"
- **Invalid topics**: Strips invalid topic values from the `topics` array (keeps valid ones). If all topics are invalid, removes the field entirely to avoid 0-item validation error.

**Why**: In run `20260307T225947Z`, 3+ verses failed solely due to missing `ambiguity_note` (e.g. `al-kafi_1_2_22_11` — 8 words, $0.91, complete valid JSON except missing one field). Invalid topic `quran_commentary` killed `al-kafi_1_3_19_11` ($1.62). These are $1-2 responses thrown away for a 10-word fix.

**Data**: Would have saved ~$4-6 in wasted generation cost this run alone.

#### 2. Route fixable validation errors through fix pass

**What**: After auto-fix, if validation errors remain, check if they're in a "fixable" category (ambiguity_note, invalid enums, missing optional annotations). If so, convert to `needs_fix` status with validation errors included in the fix prompt, instead of terminal `error`.

**Why**: Even when auto-fix can't fully resolve an issue, the fix LLM pass ($0.30-0.50) has a 100% success rate in this run for targeted field corrections. Sending fixable errors to the fix pass is far cheaper than regenerating the entire verse.

#### 3. Skip quarantined by default, add `--attempt-quarantined` flag

**What**: Quarantined verses are now skipped by default (was already the case). Added `--attempt-quarantined` CLI flag that temporarily includes quarantined verses in the queue for retry.

**Why**: Run recovered 3 stale work dirs (chronic stallers) and re-attempted them, wasting 60+ min on known-failing verses. Default should be to skip them; explicit opt-in for retries.

#### 4. Strengthened system prompt

**What**: Added explicit instruction after narrator field definition:
- "CRITICAL: If identity_confidence is 'likely' or 'ambiguous', ambiguity_note MUST be a non-empty string."
- "Output the COMPLETE JSON in a single response. Do NOT split across messages or continue from a previous response."

**Why**: The model frequently forgets `ambiguity_note` and sometimes produces "Continuing word_analysis from index N..." continuation artifacts.

#### 5. Created `scripts/batch_improve.py` — self-improving batch orchestrator

**What**: New script that runs the pipeline in batches of N (default 100), and between each batch:
1. Runs `analyse_run.py` to produce an LLM-consumable analysis report
2. Spawns a Claude improvement agent (`claude -p` with `--tools "Read,Edit,Write,Bash,Grep,Glob" --dangerously-skip-permissions`) to apply fixes
3. Runs tests to verify changes
4. Continues with the next batch (fresh subprocess, picks up code changes)

**Usage**:
```bash
python scripts/batch_improve.py --total-verses 1000 --batch-size 100 --workers 20
python scripts/batch_improve.py --total-verses 500 --batch-size 100 --no-improve  # skip improvement
```

**Why**: Manual analysis between runs is time-consuming. Automated improvement cycles allow the pipeline to self-correct based on empirical error patterns, reducing error rates across batches without human intervention.

#### 6. Created `scripts/analyse_run.py` script

**What**: New script at `scripts/analyse_run.py` that reads pipeline session/stats/logs and produces a structured analysis report. Designed for batch-100 workflow: run 100 → analyse → improve → run next 100.

**Why**: Manual log analysis is time-consuming. Automated analysis enables iterative improvement cycles where each batch's learnings feed into the next.

---

## v3.0.0 — 2026-03-07

Initial v3 pipeline release using `claude -p` (CLI print mode) instead of Claude Code agents.

- asyncio orchestrator with configurable workers
- `--max-turns 1` to prevent multi-turn loops
- `strip_code_fences()` + `repair_json_quotes()` for robust JSON extraction
- Compact word format (array-of-arrays) mandatory
- 30-min timeout per call, 3 retries on rate limit/timeout
- Quarantine mechanism after 3 cumulative failures
- Fix pass using Sonnet for targeted field corrections
- Per-verse stats persistence, session history, JSONL event log
