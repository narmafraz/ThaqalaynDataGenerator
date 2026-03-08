# Pipeline Changelog

Tracks changes to the AI content generation pipeline with rationale. Each entry includes what changed, why, and supporting data from the run that motivated it.

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
