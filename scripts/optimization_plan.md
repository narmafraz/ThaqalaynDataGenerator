# Pipeline v3 Optimization Plan

## Overview

Five work items to optimize the claude -p pipeline before production corpus runs.

---

## Item 0: Command-Line Budget — RESOLVED

**Status**: Investigated and resolved. No blocker.

**Findings** (verified on Windows with actual CLI):

| Component | Chars |
|-----------|-------|
| System prompt (no few-shot + compact instructions) | 15,312 |
| CLI flags overhead | ~150 |
| Total with `--system-prompt` | ~15,520 |
| **Remaining for `--json-schema`** | **~17,230** |
| Estimated JSON Schema for 13-field output | ~3,000-5,000 |
| **Margin after both** | **~12,000+** |

Both `--system-prompt` and `--json-schema` fit comfortably within the 32,767 char Windows `CreateProcessW` limit.

**Confirmed working**:
- `--json-schema` + `--system-prompt` + `--output-format json` all work together
- With `--json-schema`, output goes to `structured_output` field (already-parsed dict), NOT `result` (which is empty string `""`)
- `--fallback-model haiku` works (auto-falls back on overload)
- `--max-budget-usd` works but is checked between turns, not within a turn (first turn can exceed budget)
- `--effort low|medium|high` works

**Flags that DON'T exist** (agent research was wrong):
- ~~`--system-prompt-file`~~ — Does NOT exist. Use `$(cat file)` shell substitution instead.
- ~~`--max-turns`~~ — Does NOT exist.
- ~~`--append-system-prompt-file`~~ — Does NOT exist.
- `CLAUDE_CODE_MAX_OUTPUT_TOKENS` — Has NO effect in `--print` mode (confirmed). The 32K `maxOutputTokens` shown in `modelUsage` is informational, not enforced (Phase 0 confirmed model produced 47K tokens).

**File I/O approach** (using `--tools "Read,Write"`):
- Works but costs 2+ turns and ~200-500 extra output tokens per file operation
- Phase 0 showed @file references are 10-20x more expensive than inline
- **Verdict**: NOT recommended for system prompt or output. Stdin/stdout is far cheaper.
- **Only use case**: If command-line limit becomes a real constraint in the future, `$(cat file)` shell substitution is the zero-cost workaround (tested, works)

**Useful flags for pipeline**:
- `--fallback-model haiku` — auto-fallback on Sonnet overload (add to gen calls)
- `--max-budget-usd N` — safety guardrail per call (set to ~$5 to catch runaway generation)
- `--effort low` — could be used for fix pass to reduce cost (needs quality testing)

---

## Item 1: `--json-schema` Structured Output — IMPLEMENTED THEN REVERTED

**Status**: Schema built and tested end-to-end, but **reverted** due to cost analysis (2026-03-07).

**What was built**: `app/pipeline_cli/output_schema.py` — `build_output_schema()` returns full JSON Schema for 13-field output. Validated on real Claude calls. File kept for reference/future direct API use.

**Why reverted**: Head-to-head cost comparison on same verse (al-kafi:1:2:16:13, 17 words):

| Metric | With `--json-schema` | Without | Ratio |
|--------|---------------------|---------|-------|
| Cost | $1.37 | $0.88 | 1.55x |
| Turns | 3 | 1 | 3x |
| Input tokens | 110,921 | 9,976 | 11.1x |
| Output tokens | 36,935 | 34,181 | 1.08x |
| Time | 8.6 min | 7.7 min | 1.12x |

**Root cause**: `claude -p` runs an agent loop. `--json-schema` causes 3 internal turns (schema validation retries), and each turn re-sends the full accumulated context. Output is nearly identical but input multiplies 11x.

**Key insight from Anthropic docs**: Structured outputs at the *API level* add only 2-3% overhead (constrained decoding, grammar cached 24h). The overhead is entirely from the `claude -p` agent wrapper, not from structured outputs themselves. Direct API calls with `output_format` would be cheap.

**Decision**: Drop `--json-schema` from pipeline, add `--max-turns 1` instead. Raw string output with `strip_code_fences()` + `repair_json_quotes()` handles parsing reliably. `output_schema.py` preserved for future direct Anthropic API integration.

**Cost projection at corpus scale (40,578 verses)**:

| Method | Est. $/verse | Corpus total |
|--------|-------------|-------------|
| `claude -p` + `--json-schema` | ~$2.75 | ~$112K |
| `claude -p` + `--max-turns 1` (current) | ~$0.88 | ~$36K |
| Direct Anthropic API | ~$0.15-0.20 | ~$6-8K |
| Anthropic Batch API (50% off) | ~$0.08-0.10 | ~$3-4K |

**Multilingual token note**: 11-language output uses ~1.5 tokens/char average (Arabic/CJK/Cyrillic use 2-3 tok/char). A 23K-char response = ~34K tokens. This is normal encoding overhead, not inflation.

---

## Item 2: Compact Chunk Translations — DROPPED

**Dropped per architect review.** Negative ROI: ~$90 total savings, high silent-bug risk (language swap), added model cognitive load.

---

## Item 3: Trim System Prompt

**Goal**: Reduce input tokens and command-line size.

**Current system prompt**: 15,312 chars.

**Proposed cuts**:
- **Remove empty examples section**: "EXAMPLES: Below are 0 examples..." → remove entirely. Saves ~50 tokens.
- **Trim glossary**: Keep only ambiguous terms. Saves ~100 tokens.
- **KEEP word dictionary** (29 entries, ~300 tokens) — has curated `notes` with contextual guidance.
- **Trim key phrases sample**: 30 → 15 entries. Saves ~150 tokens.

**Implementation**:
1. Modify `build_system_prompt()` to skip "EXAMPLES:" header when examples list is empty
2. Trim glossary to ambiguous terms only
3. Reduce key phrases sample to 15
4. Measure char/token count before and after

**Estimated savings**: ~300-400 input tokens per call. Dollar savings minimal (~$47 across corpus) due to server-side prompt caching. Main benefit: frees ~1,000 chars of command-line budget.

---

## Item 4: Reduce Output Fields Selectively

**Recommended cut**: Remove `similar_content_hints` (field 13).
- Unverified LLM suggestions for nonexistent similarity pipeline
- Saves ~50-100 output tokens/verse (~$30-60 total)
- Can be regenerated in a dedicated pass later

**Larger opportunity noted**: Reducing language count from 11 to 5 priority languages could save $3-5K. Product decision — deferred.

---

## Item 5: Key Terms Prompt Fix

**Goal**: Fix model using English transliteration keys instead of Arabic in `key_terms`.

**Implementation**:
1. Change schema example from `{"arabic_term": "explanation"}` to `{"تَقْوَى": "God-consciousness, piety", "عِلْم": "knowledge, sacred learning"}`
2. Add instruction: "CRITICAL: key_terms keys MUST be Arabic words with full diacritics from the text."
3. Test on 5+ verses

**Impact**: Eliminates ~30 of 38 content errors from E2E test. Zero cost impact.

---

## Item 6: Pipeline Status / Monitoring Tool

**Goal**: Single-command status report before/during production runs.

**Design**: `app/pipeline_cli/pipeline_status.py` — prints progress, quality, cost, quarantined, stale dirs.

**Implementation**:
1. Default: file counting + session log aggregation (fast)
2. `--audit`: re-validate all responses matching current `PIPELINE_VERSION` (slow)
3. CLI: `python -m app.pipeline_cli.pipeline_status [--audit]`

---

## Implementation Order

1. **Item 5** — key terms prompt fix (simplest, highest quality impact, zero risk)
2. **Item 1** — `--json-schema` structured output (biggest reliability win, command-line budget confirmed OK)
3. **Item 3** — trim system prompt (modest savings, low risk)
4. **Item 6** — status tool (needed before production batches)
5. **Item 4** — reduce output fields (product decision, confirm before implementing)

Items 5, 3, and 6 are independent and can be parallelized.
Item 4 needs user confirmation.

---

## Pipeline Flags Summary (for `call_claude()`)

Current:
```python
cmd = [CLAUDE_EXE, "-p", "--model", model, "--output-format", "json",
       "--no-session-persistence", "--setting-sources", "",
       "--system-prompt", system_prompt]
```

Recommended additions:
```python
cmd = [CLAUDE_EXE, "-p", "--model", model, "--output-format", "json",
       "--no-session-persistence", "--setting-sources", "",
       "--system-prompt", system_prompt,
       "--json-schema", json.dumps(schema),        # Item 1: structured output
       "--fallback-model", "haiku",                 # auto-fallback on overload
       "--max-budget-usd", "5.00"]                  # safety guardrail per call
```

For fix pass (no schema, Haiku):
```python
cmd = [CLAUDE_EXE, "-p", "--model", "haiku", "--output-format", "json",
       "--no-session-persistence", "--setting-sources", "",
       "--system-prompt", fix_system_prompt,
       "--max-budget-usd", "2.00"]
```
