# Phase 0: Concurrency Test Results

Date: 2026-03-06

## Key Findings

### 1. Concurrency WORKS

Parallel `claude -p` calls execute concurrently, not sequentially:

| Test | Wall time | Effective per-call |
|------|-----------|-------------------|
| Sequential (2 calls) | 9.24s | 4.6s |
| Parallel 3 | 6.82s | 2.3s |
| Parallel 5 | 11.53s | 2.3s |

5 parallel calls take ~12s total vs ~23s sequential. True concurrency is confirmed.

### 2. CRITICAL: Claude CLI Base Overhead is ~24,500 tokens

Every `claude -p` call loads Claude Code's own system prompt (~24,500 tokens), regardless of
what we send. This is unavoidable overhead.

With `--setting-sources ""`: ~23,650 tokens total context (base only)
With project settings (CLAUDE.md): ~30,770 tokens total context (base + project)

The difference: **~7,100 tokens of CLAUDE.md project context** that we can eliminate with
`--setting-sources ""`. (Earlier run showed higher overhead at 24,579/38,654; the base prompt
size varies slightly between Claude Code versions.)

### 3. Cost Difference

| Mode | Context tokens | Cost per call |
|------|---------------|--------------|
| --setting-sources "" | 24,579 | $0.003-0.005 |
| With project settings | 38,654 | $0.017 |

**Using --setting-sources "" is 3-5x cheaper per call** because it avoids creating
cache entries for CLAUDE.md content.

### 4. Token Overhead Impact on v3 Plan

The revised per-verse token estimate:

| Component | Tokens |
|-----------|--------|
| Claude CLI base system prompt | ~24,500 (unavoidable, but cached) |
| Our system prompt (no few-shot) | ~3,667 |
| User message | ~2,250 |
| AI output | ~15,000 |
| **Total per call** | **~45,400** |

This is higher than the original v3 estimate of ~24,000 because we didn't account for
Claude CLI's base overhead. HOWEVER:

1. The base prompt is CACHED (cache_read, not cache_creation) — it's cheap
2. In v2, agents ALSO load this base prompt (they're Claude Code agents too)
3. The real comparison is input_tokens + output_tokens for billing:
   - v2: ~55,000 total tokens (includes agent overhead, bash round-trips, etc.)
   - v3: ~45,400 total tokens (base prompt cached + our prompt + output)
   - Savings still significant: ~18% raw tokens, but MUCH more because:
     - No orchestrator agent (was ~5K tokens/verse for queue management)
     - No 8 bash round-trips per verse (was ~8K tokens)
     - No Claude reasoning between steps (was ~3K tokens)

### 5. --json-schema Works

```json
{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}
```

Returned structured_output with the schema enforced. This could replace the array-of-arrays
format — we could enforce the exact output JSON schema.

### 6. --no-session-persistence Works

Prevents cluttering session history. Essential for pipeline use.

### 7. --tools "" Doesn't Work with Positional Args

Must use stdin piping when combining with --tools "". But --tools "" may not be needed
if the system prompt simply instructs the model not to use tools.

### 8. --agents Flag

The --agent flag selects a pre-defined agent from .claude/agents/*.md.
The --agents flag defines inline agents as JSON.
Both could potentially be used BUT they load the agent definition file (and its model/tools
config), which adds overhead. For our pipeline, plain --system-prompt + stdin is simpler.

### 9. Agent vs Plain --system-prompt Comparison

Five approaches tested with the same translation prompt (Haiku model):

| Approach | Context tokens | Cost | Time |
|----------|---------------|------|------|
| `--system-prompt` + `--setting-sources ""` | 23,652 | $0.0034 | 7.8s |
| `--system-prompt` (with project settings) | 30,773 | $0.0039 | 11.3s |
| `--append-system-prompt` + `--setting-sources ""` | 27,165 | $0.0036 | 6.4s |
| Inline `--agents` + `--setting-sources ""` | 23,668 | $0.0032 | 5.5s |
| stdin piping + `--setting-sources ""` | 23,652 | $0.0035 | 7.9s |

**Key findings:**

1. **`--system-prompt` and inline `--agents` are equivalent** (~23,650 context tokens). The 16-token
   difference is negligible (agent metadata). No reason to use `--agents` over `--system-prompt`.

2. **`--append-system-prompt` adds ~3,500 tokens** because it keeps Claude Code's default system
   prompt AND appends ours, rather than replacing. Avoid this.

3. **Project settings add ~7,100 tokens** (30,773 vs 23,652). Always use `--setting-sources ""`.

4. **stdin piping is identical** to positional argument for token count. Either works.

5. **All approaches cache the base prompt** (cache_read ~23,642, input=10). After first call,
   the base overhead is cheap (cache read pricing).

**Conclusion**: Use `--system-prompt` + `--setting-sources ""` with positional argument or stdin.
The `--agents` flag offers no advantage and adds complexity. Plain `--system-prompt` is simplest.

### 10. Windows Command-Line Length Limit

The `--system-prompt` flag passes the entire prompt as a command-line argument. On Windows,
the maximum command-line length is ~32,768 characters. The system prompt WITH few-shot examples
is ~30,000+ characters, which exceeds this limit and causes `[WinError 206] The filename or
extension is too long`.

This means **removing few-shot examples is mandatory** for `claude -p` on Windows, not just
a token optimization. The no-few-shot prompt (~15,000 chars) fits comfortably.

Alternative: pipe the system prompt via a temp file, but `--system-prompt` only accepts inline
text, not file paths. We could embed it in a wrapper prompt that reads from stdin, but this
adds complexity for no benefit since we want to remove few-shot anyway.

### 11. Haiku vs Sonnet Generation Quality (Phase 1 Test)

Tested 4 diverse hadiths (14-34 words) from 4 Al-Kafi volumes using --system-prompt (no few-shot):

| Model | Pass Rate | Cost (4 hadiths) | Avg Time | Output Tokens |
|-------|-----------|-------------------|----------|---------------|
| Sonnet | 4/4 PASS (0 warnings) | $4.88 | 625s | 166,452 |
| Haiku | 4/4 PASS (0 warnings) | $0.79 | 273s | 147,077 |

**Haiku is 6.2x cheaper, 2.3x faster, and matches Sonnet quality** on all 10 automated
review checks (zero high/medium warnings for both). Both models produce valid schema,
correct translations, proper diacritics, and accurate narrator extraction.

NOTE: Output token counts are high because each hadith generates 11-language translations,
word-by-word analysis, narrator extraction, tagging, and summaries — even for a 14-word hadith.

**UPDATE (Phase 1 follow-up)**: Haiku FAILS on medium/long hadiths (99-281 words):
- 99w: 16 validation errors (compound POS tags like CONJ+N)
- 114w: Malformed JSON (missing comma at char 26740)
- 142w: Empty response (output appears truncated)
- 281w: Extra data in JSON (model added commentary after JSON)

**Revised recommendation**: Use **Sonnet** for generation. Haiku is only reliable for
short hadiths (<50 words). Since ~45% of corpus is medium/long, a mixed approach adds
complexity for limited benefit. Sonnet is reliable across all lengths.

### 12. @file References Are Expensive

Testing `@file` references (for loading system prompt from file) showed:
- Haiku via @file: $0.18/call, 250s (vs $0.01 via --system-prompt for same simple prompt)
- Sonnet via @file: $0.81/call, 450s

The @file approach triggers Claude Code's Read tool, adding multi-turn overhead. For production,
use --system-prompt directly (10-20x cheaper per call).

### 13. E2E Pipeline Test (Phase 1 Completion)

Full pipeline tested: `prepare_verse()` → `claude -p --model sonnet` → `postprocess_verse()`.

**Test hadith**: al-kafi:1:1:1:3 (32 words)

| Metric | Value |
|--------|-------|
| Elapsed | 728s (~12 min) |
| Cost | $1.26 |
| Output tokens | 47,689 |
| Result chars | 30,671 |
| Turns | 1 (single) |
| Schema errors | 0 (after compact expansion) |
| Content errors | 38 (key_terms format + ambiguity_note) |

**Key discoveries:**

1. **32K `maxOutputTokens` in modelUsage is NOT a hard limit.** Model produced 47,689 output tokens
   in a single turn. The 32K figure is informational, not a constraint.

2. **`CLAUDE_CODE_MAX_OUTPUT_TOKENS` env var has NO effect in `--print` mode**, regardless of
   `--setting-sources` or `--settings`. This was a red herring — the model outputs as much as needed.

3. **Compact word format works and is mandatory.** Model correctly used `["word","POS","en",...]`
   arrays when instructed with CRITICAL/mandatory language. `expand_compact_words()` converts
   back to dicts for validation.

4. **JSON quote repair needed.** Chinese/Russian text contains unescaped ASCII `"` inside strings.
   `repair_json_quotes()` fixes these automatically (8 chars repaired in test).

5. **`ReviewWarning` has `category` field, not `check`.** Fixed in `_save_audit()`.

6. **Content quality issues are expected and handled by fix pass.** The 38 errors were all content
   quality (key_terms using transliterations instead of Arabic keys, missing ambiguity_notes),
   not pipeline bugs.

### 14. Multi-Turn Output Handling

When the model DOES produce multi-turn output (observed in earlier tests with verbose format):
- CLI concatenates all turn results into one string
- Result contains markdown code blocks + continuation text between them
- `strip_code_fences()` + `repair_json_quotes()` handle single-turn responses
- Multi-turn responses require special reassembly (not yet needed with mandatory compact format)

## Recommendations

1. ALWAYS use `--setting-sources ""` to avoid loading CLAUDE.md (saves ~7K tokens/call)
2. ALWAYS use `--no-session-persistence` to avoid cluttering session history
3. Use `--output-format json` to get token counts and cost for monitoring
4. Start with 5-10 parallel workers (concurrency confirmed working)
5. Use mandatory compact word format to reduce output tokens
6. Pass system prompt via `--system-prompt` flag, user message via stdin
7. Apply `repair_json_quotes()` to handle unescaped quotes in CJK/Cyrillic text
8. Pipeline version: "claude_cli_p" in ai_attribution
