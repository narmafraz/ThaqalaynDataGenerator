# V4 Pipeline Plan — Cost-Efficient Full Corpus Processing

## Problem Statement

| Metric | Current (v3) | Target (v4) |
|--------|-------------|-------------|
| Cost per 100 hadith (short, <200w) | ~$200 | ~$35 |
| Cost per hadith | ~$2.00 | ~$0.35 |
| Monthly budget (pipeline) | $14,000 | $14,000 |
| Hadith per month at budget | 7,000 | 40,000 |
| Total corpus | ~58,000 | ~58,000 |
| Months to complete | ~8 | 2 |

**Reduction needed: 5.7x**

---

## Root Cause Analysis — Measured From 3,627 Real Responses

### Output field sizes (average per hadith, 112 words avg)

From analyzing all 3,627 existing corpus responses:

| Field | Avg chars | % of output | V4 status |
|-------|----------|-------------|-----------|
| `word_analysis` (11 langs per word) | ~27,000 | **46%** | Replace with `word_tags` (word+POS only) |
| `chunks[].translations` (11 langs) | ~9,000 | **15%** | Keep (this IS the translation) |
| `translations.*.text` (11 langs, stripped but model generates it) | ~9,000 | **15%** | Don't generate (reconstruct from chunks) |
| `translations.*.{summary,key_terms,seo}` | ~6,000 | 10% | Keep |
| `isnad_matn` | ~2,000 | 3% | Keep |
| Other (tags, topics, content_type, etc.) | ~3,000 | 5% | Keep |
| **Total v3 model output** | **~56,000** | | |
| **Total v4 model output** | **~22,000** | | **59% smaller** |

### Where the v3 money goes

| Component | Est. Cost | % |
|-----------|-----------|---|
| Output tokens (dominant at $15/MTok Sonnet) | ~$1.20 | 60% |
| Input tokens + `claude -p` overhead | ~$0.40 | 20% |
| Fix pass (50% of hadith) | ~$0.40 | 20% |
| **Total blended** | **~$2.00** | |

---

## V4 Architecture — Two Changes (No Model Switch)

### Change 1: Word dictionary instead of per-hadith word_analysis

**Current**: Every hadith generates `word_analysis` — every Arabic word with POS tag + 11-language translations. This is **46% of all output tokens**. The word "قَالَ" gets translated tens of thousands of times.

**V4**: Two-step approach:

**Step A (per-hadith, cheap):** Model outputs `word_tags` — just `[["قَالَ","V"],["عَنْ","PREP"],...]`. No translations. This is ~1,800 chars vs ~27,000 chars per hadith. **93% reduction in this field.**

**Step B (one-time, after all hadith processed):** Collect unique `(word, POS)` pairs corpus-wide. From 3,627 hadith we already see 71,894 unique pairs. At 58K hadith this will plateau at ~100K-150K unique pairs (Zipf's law — most tokens are common words). Translate each unique pair once via batch calls.

**Assembly (zero LLM cost):** For each hadith, look up each `(word, POS)` pair in the dictionary → produce full `word_analysis`.

**Measured savings:** 46% of output → 3.5% of output. Saves ~25,000 chars per hadith.

### Change 2: Chunks-only translations (no duplicate full text)

**Current**: Model generates both `translations.*.text` (full translation per language) AND `chunks[].translations` (paragraph translations per language). These contain the same content — v3 already strips `translations.*.text` and reconstructs from chunks via `reconstruct_fields()`. But **the model still generates both**, wasting ~15% of output tokens.

**V4**: Remove `translations.*.text` from the prompt schema entirely. Model only generates chunk translations. Full text is assembled in postprocessing (already implemented).

**Measured savings:** ~15% of output tokens.

### Combined impact

| | v3 | v4 | Reduction |
|--|----|----|-----------|
| Output chars (avg) | ~56,000 | ~22,000 | **59%** |
| Output tokens (avg, ×1.5) | ~84,000 | ~33,000 | **59%** |

---

## Cost Projections (Staying on `claude -p`)

### With Sonnet (no model change)

| Scenario | $/hadith | 58K total | Months at $14K/mo |
|----------|---------|-----------|-------------------|
| v3 current | $2.00 | $116,000 | 8.3 |
| **v4 conservative (40% savings)** | **$1.20** | **$69,600** | **5.0** |
| **v4 optimistic (55% savings)** | **$0.90** | **$52,200** | **3.7** |

*Sonnet alone doesn't hit the $0.35/hadith target. Output reduction helps but input/overhead costs are fixed per call.*

### With Haiku (model change on `claude -p`)

| Scenario | $/hadith | 58K total | Months at $14K/mo |
|----------|---------|-----------|-------------------|
| **v4+Haiku conservative** | **$0.36** | **$20,880** | **1.5** |
| **v4+Haiku optimistic** | **$0.27** | **$15,660** | **1.1** |

**Haiku + v4 changes hits the target.** $0.27-0.36/hadith is close to the $0.35 goal.

### Word dictionary pass (one-time)

| | Haiku | Sonnet |
|--|-------|--------|
| ~100K-150K unique (word, POS) pairs | $60-120 | $200-400 |
| Batch 50-100 words per call | ~1,500-3,000 calls | same |

### Future: OpenAI API option

If/when switching to OpenAI API (user has key), costs drop dramatically further:

| Model | $/hadith (v4) | 58K total |
|-------|--------------|-----------|
| GPT-4.1-mini (batch) | ~$0.005 | ~$290 |
| GPT-4.1-mini (regular) | ~$0.01 | ~$580 |
| GPT-4o-mini (batch) | ~$0.002 | ~$116 |

---

## Implementation Plan

### Phase 1: Prompt changes — remove word_analysis, make translations.text optional

**Files to modify:**

#### 1a. `app/ai_pipeline.py` — `build_user_message()`

Remove field #4 (word_analysis) from the prompt schema. Replace with:

```
4. "word_tags": (array) One entry per Arabic word: [diacritized_word, POS_tag]
   POS tags: N|V|ADJ|ADV|PREP|CONJ|PRON|DET|PART|INTJ|REL|DEM|NEG|COND|INTERR
   Example: [["قَالَ","V"],["عَنْ","PREP"],["عَلِيِّ","N"],["بْنِ","N"]]
   The words must match diacritized_text exactly, in order.
```

Remove `"text"` from field #9 (translations) output instructions. Change to:

```
9. "translations": Object with keys en, ur, tr, fa, id, bn, es, fr, de, ru, zh. Each:
   {"summary": "...", "key_terms": {...}, "seo_question": "..."}
   NOTE: Do NOT include a "text" field — full translation is reconstructed from chunks.
```

Remove field #13 (similar_content_hints) entirely.

#### 1b. `app/ai_pipeline.py` — `build_system_prompt()`

Remove the `COMMON WORD TRANSLATIONS` section (no longer needed — word translations are corpus-wide).

Remove the `COMPACT_WORD_INSTRUCTIONS` from `verse_processor.py` (no word_analysis output).

#### 1c. `app/ai_pipeline.py` — `validate_result()`

- Accept `word_tags` as alternative to `word_analysis`
- Validate `word_tags` format: array of `[string, string]` pairs, POS in VALID_POS_TAGS
- Skip `word_analysis` validation when `word_tags` present
- `translations.*.text` becomes optional (reconstructed from chunks)
- Remove `similar_content_hints` validation

#### 1d. `app/ai_pipeline.py` — `strip_redundant_fields()` / `reconstruct_fields()`

- `strip_redundant_fields()`: No longer strips `diacritized_text` (can't reconstruct without word_analysis). Still strips `chunks[].arabic_text` (reconstructible from word_tags + diacritized_text if word boundaries match).
- `reconstruct_fields()`: Add reconstruction of `translations.*.text` from chunks (already implemented). Add reconstruction of `word_analysis` from `word_tags` + word dictionary (new).

#### 1e. `app/pipeline_cli/verse_processor.py`

- Remove `COMPACT_WORD_INSTRUCTIONS` constant
- Update `VersePlan` to track whether this is a word_tags or word_analysis response
- Update postprocessing to handle `word_tags` format

### Phase 2: Review check adaptation

**File: `app/ai_pipeline_review.py`**

Checks that need updating:

| Check | Change |
|-------|--------|
| `word_count_mismatch` | Compare `len(word_tags)` vs Arabic word count instead of `len(word_analysis)` |
| `word_text_mismatch` | Compare `word_tags[i][0]` vs diacritized_text words |
| `narrator_word_range_mismatch` | Use `word_tags` for word lookup instead of `word_analysis` |
| `chunk_translation_mismatch` | No change needed (already compares chunks) |
| Arabic echo check | Skip (no word-level translations to check) |

Checks that stay the same: translation length ratio, European diacritics, Quran self-reference, missing isnad chunk, back-reference detection, key terms disparity.

### Phase 3: Word dictionary infrastructure (new module)

**New file: `app/pipeline_cli/word_dictionary.py`**

```python
def extract_unique_words(responses_dir: str) -> dict[tuple[str,str], int]:
    """Scan all v4 responses, collect unique (word, POS) pairs with frequency."""

def build_dictionary_prompts(words: dict, batch_size: int = 50) -> list[str]:
    """Build prompts for translating word batches."""

def translate_word_batch(words: list[tuple[str,str]], languages: list[str]) -> dict:
    """Call LLM to translate a batch of words. Returns {(word,POS): {lang: translation}}."""

def load_word_dictionary(path: str) -> dict:
    """Load completed word dictionary from JSON."""

def assemble_word_analysis(word_tags: list, dictionary: dict) -> list:
    """Convert word_tags + dictionary into full word_analysis format."""
```

**Dictionary file format** (`ai-pipeline-data/word_translations_dict_v4.json`):
```json
{
  "قَالَ|V": {"en": "he said", "ur": "کہا", "tr": "dedi", "fa": "گفت", ...},
  "عَنْ|PREP": {"en": "from/about", "ur": "سے", "tr": "-den", "fa": "از", ...},
  ...
}
```

### Phase 4: Assembly pipeline (new module)

**New file: `app/pipeline_cli/word_assembly.py`**

Reads all Track 1 responses with `word_tags`, looks up each word in the dictionary, produces `word_analysis`, and writes updated response files.

Handles missing dictionary entries gracefully (marks as `"???"` for later fill-in or manual review).

### Phase 5: Test updates

- Update all tests in `tests/test_ai_pipeline.py` that validate word_analysis format
- Add tests for word_tags validation
- Add tests for word dictionary extraction and assembly
- Add tests for translations.text reconstruction from chunks
- Ensure existing `strip_redundant_fields()` / `reconstruct_fields()` tests pass

### Phase 6: Pipeline CLI changes

**File: `app/pipeline_cli/pipeline.py`**

- Add `--v4` flag (or make it default) to use word_tags prompt
- Add `--word-dict` subcommand: extract → translate → assemble
- Keep backward compatibility with v3 responses (detect by presence of `word_analysis` vs `word_tags`)

### Phase 7: Production run

1. Run Track 1 on all 58K hadith with v4 prompt (generates word_tags, not word_analysis)
2. Run word dictionary extraction (Python, zero LLM cost)
3. Run word dictionary translation (~$60-400 depending on model)
4. Run assembly (Python, zero LLM cost)
5. Validate all assembled responses

---

## Estimated Implementation Effort

| Phase | Effort | Description |
|-------|--------|-------------|
| Phase 1 | 2-3 hours | Prompt + schema changes (mostly removing code) |
| Phase 2 | 1-2 hours | Review check adaptation |
| Phase 3 | 2-3 hours | Word dictionary module |
| Phase 4 | 1-2 hours | Assembly pipeline |
| Phase 5 | 2-3 hours | Test updates |
| Phase 6 | 1 hour | CLI flag changes |
| Phase 7 | — | Production run |
| **Total** | **~10-14 hours** | |

---

## Summary: What This Gets Us

| | v3 | v4 (Sonnet) | v4 (Haiku) |
|--|----|----|-----|
| Output per hadith | ~56K chars | ~22K chars | ~22K chars |
| Cost per hadith | $2.00 | $0.90-1.20 | **$0.27-0.36** |
| 58K corpus total | $116,000 | $52-70K | **$16-21K** |
| Months at $14K/mo | 8.3 | 3.7-5.0 | **1.1-1.5** |
| Word dictionary (one-time) | included | +$200-400 | +$60-120 |

**Recommendation**: Implement the v4 architectural changes (Phases 1-6), then run with Haiku to hit the budget target. The v4 changes alone save 59% of output, and Haiku gives another 3-5x on top. Combined: **$0.27-0.36/hadith, completing 58K hadith in ~1-1.5 months for ~$16-21K.**

If that's still too expensive, switching to OpenAI API (GPT-4.1-mini) in the future would drop to ~$0.005-0.01/hadith ($290-580 total). That's a separate implementation effort but the v4 architectural changes (word dictionary, chunks-only) carry over and amplify the savings regardless of model.
