# V4 Pipeline Plan — Cost-Efficient Full Corpus Processing

## Problem Statement

| Metric | Current (v3) | Target (v4) |
|--------|-------------|-------------|
| Cost per 100 hadith (short, <200w) | ~$200 | ~$35 |
| Cost per hadith | ~$2.00 | ~$0.35 |
| Monthly budget (pipeline) | $14,000 | $14,000 |
| Hadith per month at budget | 7,000 | 40,000 |
| Total corpus | 80,000 | 80,000 |
| Months to complete | ~12 | 2 |

**Reduction needed: 5.7x**

---

## Root Cause Analysis — Where The Money Goes

### v3 cost breakdown (per short hadith, Sonnet via `claude -p`)

| Component | Tokens | Est. Cost | % |
|-----------|--------|-----------|---|
| System prompt (input) | ~10,000 | ~$0.10 | 5% |
| User message (input) | ~2,000 | ~$0.02 | 1% |
| **Output: translations.text × 11 langs** | **~8,000** | **$0.48** | **24%** |
| **Output: word_analysis × 11 langs** | **~8,000** | **$0.48** | **24%** |
| **Output: chunks × 11 langs (duplicates translations!)** | **~4,000** | **$0.24** | **12%** |
| Output: isnad_matn, tags, topics, etc. | ~2,000 | ~$0.12 | 6% |
| `claude -p` agent wrapper overhead | — | ~$0.10 | 5% |
| **Fix pass (when needed, ~50%)** | ~8,000 | **~$0.38** | **19%** |
| **Subtotal (blended)** | | **~$2.00** | |

### Three fatal inefficiencies

1. **`claude -p` overhead**: The CLI agent wrapper is 4-5x more expensive than a direct API call for the same work.
2. **Massive output duplication**: `translations.text` and `chunks[].translations` contain the same text (v3 already strips `translations.text` and reconstructs it from chunks — but the model still generates both, wasting output tokens).
3. **word_analysis repeated 80K times**: Common words like "قال" (he said), "عن" (from), "الله" (God) appear in nearly every hadith but get re-translated with 11 languages every single time.

---

## Available Models — Pricing Comparison

Since we have an **OpenAI API key** (not Anthropic), here are the options:

| Model | Input $/MTok | Output $/MTok | Batch Input | Batch Output | Quality Tier |
|-------|-------------|--------------|-------------|-------------|-------------|
| GPT-4.1 | $2.00 | $8.00 | $1.00 | $4.00 | High |
| GPT-4o | $2.50 | $10.00 | $1.25 | $5.00 | High |
| **GPT-4.1-mini** | **$0.40** | **$1.60** | **$0.20** | **$0.80** | **Mid (sweet spot)** |
| GPT-4o-mini | $0.15 | $0.60 | $0.075 | $0.30 | Mid |
| GPT-4.1-nano | $0.10 | $0.40 | $0.05 | $0.20 | Low |
| *Claude Sonnet (v3 current)* | *$3.00* | *$15.00* | *N/A* | *N/A* | *High* |
| *Claude Haiku* | *$0.80* | *$4.00* | *N/A* | *N/A* | *Mid* |

**Key takeaway**: GPT-4.1-mini is **2x cheaper than Haiku** on input and **2.5x cheaper** on output. GPT-4o-mini is **5x cheaper than Haiku** on output. With OpenAI Batch API (50% off), savings are enormous.

### Model recommendation

| Use Case | Recommended Model | Rationale |
|----------|-------------------|-----------|
| Track 1: Translations + metadata | **GPT-4.1-mini** | Best quality/cost ratio for multilingual translation |
| Track 1 (if quality issues): | GPT-4.1 | Higher quality, still 2x cheaper than Sonnet |
| Word dictionary: | GPT-4.1-mini or nano | Mechanical word-level translation |
| Additional languages: | GPT-4o-mini (batch) | Pure translation, cheapest viable |

---

## V4 Architecture — Three Key Changes

### Change 1: Generate chunks only, not full translations + chunks

**Current v3**: Model outputs both `translations.en.text` (full translation) AND `chunks[].translations.en` (paragraph translations). The codebase already strips `translations.text` and reconstructs it from chunk concatenation via `reconstruct_fields()`. But the model still wastes tokens generating both.

**V4**: Only ask the model to generate chunks with translations. The `translations.lang.text` field is assembled by concatenating chunk translations in postprocessing (zero cost). We still ask for `translations.lang.summary`, `key_terms`, and `seo_question` separately since those aren't in chunks.

**Savings**: ~30% fewer translation output tokens (no double-generation).

### Change 2: Word analysis as corpus-wide dictionary (not per-hadith)

**Current v3**: Every hadith generates `word_analysis` — an array of every Arabic word with POS tag and 11-language translations. The word "قال" gets translated 40,000+ times across the corpus.

**V4 — Two-step approach:**

**Step A (during Track 1, per-hadith, cheap):** Ask the model to output a `word_tags` list alongside the other fields — just the Arabic words with POS tags, no translations. This is ~50-150 extra output tokens per hadith (just `["قَالَ","V"],["عَنْ","PREP"],...`).

**Step B (once, after Track 1):** Collect all unique `(word, POS)` pairs across the entire corpus. Translate each unique pair once into all target languages. This is a single batch job.

**Assembly (zero LLM cost):** For each hadith, look up each `(word, POS)` pair in the dictionary → produce `word_analysis`.

**Estimated unique words**: Arabic hadith literature has ~30,000-80,000 unique word forms. Even at 80K unique pairs × 15 output tokens × $0.80/MTok (GPT-4.1-mini batch) = **$0.96 total for the entire dictionary**. Compare to v3: $0.48/hadith × 80K = $38,400.

**Savings**: ~$38,000 → ~$1. Essentially eliminates the biggest cost driver.

**Context-dependent translations**: Some words mean different things in context. Mitigation:
- The POS tag disambiguates most cases (same form as noun vs verb)
- For genuinely ambiguous words, use the most common Islamic/hadith-context meaning
- Can flag rare meanings for manual review later
- 90%+ of word tokens in hadith are unambiguous in context

### Change 3: Drop `claude -p`, use OpenAI API directly

**Current v3**: `claude -p` subprocess with JSON parsing. The CLI wrapper adds overhead (agent loop, session management).

**V4**: Direct OpenAI Python SDK calls. Benefits:
- No agent wrapper overhead
- Structured JSON output via `response_format` (no code-fence stripping needed)
- Batch API for 50% discount
- Precise token/cost tracking

---

## Field Plan — What Gets Generated

### Track 1: Per-hadith generation (80K hadith)

| # | Field | In Output? | Notes |
|---|-------|-----------|-------|
| 1 | `chunks` | **Yes** | Paragraph segments with type + translations. This IS the translation. |
| 2 | `tags` | **Yes** | 2-5 tags, tiny output |
| 3 | `content_type` | **Yes** | Single enum value |
| 4 | `topics` | **Yes** | 1-5 topic keys |
| 5 | `isnad_matn` | **Yes** | Narrator chain analysis |
| 6 | `related_quran` | **Yes** | Quran cross-references |
| 7 | `diacritized_text` | **Yes** | Full Arabic with tashkeel |
| 8 | `diacritics_status` | **Yes** | Metadata for #7 |
| 9 | `word_tags` | **Yes (NEW)** | Just `[word, POS]` pairs — no translations. Cheap. |
| 10 | `translations.*.summary` | **Yes** | 1-2 sentence summary per language |
| 11 | `translations.*.key_terms` | **Yes** | 2-4 terms (en only, or per-language if cheap) |
| 12 | `translations.*.seo_question` | **Yes** | 1 sentence (en only) |
| 13 | `key_phrases` | **Yes** | 0-5 Arabic expressions with English translations (cheap, high value) |

**Not generated:**
- `translations.*.text` — reconstructed from chunks (zero cost)
- `word_analysis` — built from word_tags + dictionary (Track 2, near-zero cost)
- `similar_content_hints` — dropped permanently
- `diacritics_changes` — deferred, low value

### Languages in Track 1

| Option | Languages | Output tokens | Recommendation |
|--------|-----------|--------------|----------------|
| **A: 4 core** | en, ur, fa, tr | ~4,000 | Start here, lowest risk |
| B: 7 major | + id, bn, es | ~6,000 | If budget allows |
| C: All 11 | + fr, de, ru, zh | ~8,000 | Track 2 if needed |

**Recommendation**: Start with 4 core languages. At the projected costs, adding more is cheap — the decision should be quality-driven (test whether the model handles all 11 well in a single call).

### Track 2: Word dictionary (one-time job)

| Step | What | Volume | Cost |
|------|------|--------|------|
| Extract unique (word, POS) pairs | Python script, zero LLM | ~50K-80K pairs | $0 |
| Batch-translate all pairs | GPT-4.1-mini batch, 4 langs | ~1.2M output tokens | ~$1 |
| Batch-translate all pairs | GPT-4.1-mini batch, 11 langs | ~3M output tokens | ~$2.50 |
| Assemble word_analysis per hadith | Python script, zero LLM | 80K hadith | $0 |

### Track 3: Additional languages (if Track 1 uses 4 languages)

| Step | What | Cost |
|------|------|------|
| Translate chunks to 7 more languages | GPT-4o-mini batch | ~$1,500-3,000 |
| Translate summaries/key_terms/seo | GPT-4o-mini batch | ~$500-1,000 |

---

## Cost Projections

### Track 1 — Per-hadith cost estimate

Assumptions: short hadith (<200 words), GPT-4.1-mini, 4 languages

| Component | Tokens | Direction |
|-----------|--------|-----------|
| System prompt (trimmed) | ~3,000 | input |
| User message (Arabic + English ref + metadata) | ~1,500 | input |
| **Total input** | **~4,500** | |
| Chunk translations (4 langs × ~100 words each) | ~3,000 | output |
| isnad_matn + narrators | ~500 | output |
| diacritized_text + word_tags | ~500 | output |
| tags + content_type + topics + related_quran | ~200 | output |
| summaries + key_terms + seo (4 langs) | ~600 | output |
| key_phrases | ~100 | output |
| **Total output** | **~4,900** | |

| Pricing | Input cost | Output cost | Total/hadith | Per 100 | 80K corpus |
|---------|-----------|-------------|-------------|---------|-----------|
| GPT-4.1-mini (regular) | $0.0018 | $0.0078 | **$0.0096** | **$0.96** | **$770** |
| GPT-4.1-mini (batch) | $0.0009 | $0.0039 | **$0.0048** | **$0.48** | **$385** |
| GPT-4o-mini (batch) | $0.0003 | $0.0015 | **$0.0018** | **$0.18** | **$144** |

**With 2x safety margin** (retries, longer hadith, errors):

| Model | 80K corpus (with margin) | vs v3 ($160K) |
|-------|-------------------------|--------------|
| GPT-4.1-mini (regular) | **~$1,540** | **104x cheaper** |
| GPT-4.1-mini (batch) | **~$770** | **208x cheaper** |
| GPT-4o-mini (batch) | **~$290** | **552x cheaper** |

### Track 2 — Word dictionary

| | Cost |
|--|------|
| 80K unique (word, POS) pairs × 4 languages | ~$1-2 |
| 80K unique (word, POS) pairs × 11 languages | ~$2-5 |

### Track 3 — Additional 7 languages (optional)

| | Cost |
|--|------|
| 80K hadith chunks × 7 langs (GPT-4o-mini batch) | ~$1,500-3,000 |

### Total budget

| Component | Estimated Cost |
|-----------|---------------|
| Track 1: Core (4 langs, GPT-4.1-mini batch) | ~$770 |
| Track 1: Safety margin (2x) | ~$770 |
| Track 2: Word dictionary (11 langs) | ~$5 |
| Track 3: Additional 7 languages | ~$2,000 |
| Quality testing (100-verse pilot) | ~$5 |
| **Grand total (all 80K, all 11 languages)** | **~$3,550** |

**This is 45x cheaper than v3 and fits comfortably in 1 month, let alone 2.**

Even with 5x safety margin on everything: ~$8,000. Still well under $14K/month.

---

## Implementation Plan

### Phase 0: Quality Pilot (1 day)

Before building anything, validate that GPT-4.1-mini produces acceptable quality.

1. Take 20 existing sample verses (already have v3 Sonnet output for comparison)
2. Call OpenAI API manually with the new slim prompt
3. Compare translation quality, diacritization, isnad accuracy
4. Test on short (<50 words), medium (50-200), and long (>200) hadith
5. If quality is poor, test GPT-4.1 (2.5x more expensive, still cheap)

**Cost of pilot: ~$0.20** (20 verses × $0.01)

### Phase 1: API Integration (2-3 days dev work)

Replace `claude -p` subprocess with OpenAI Python SDK.

```python
from openai import OpenAI

client = OpenAI()  # Uses OPENAI_API_KEY env var

async def call_openai(
    system_prompt: str,
    user_message: str,
    model: str = "gpt-4.1-mini",
    max_tokens: int = 8192,
) -> dict:
    response = client.chat.completions.create(
        model=model,
        max_tokens=max_tokens,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
    )
    return {
        "result": response.choices[0].message.content,
        "input_tokens": response.usage.prompt_tokens,
        "output_tokens": response.usage.completion_tokens,
    }
```

**Key changes to pipeline.py:**
- New `--backend openai|openai-batch|claude-p` flag
- New `call_openai()` and `submit_openai_batch()` functions
- Keep `call_claude()` as fallback
- OpenAI's `response_format: {"type": "json_object"}` gives reliable JSON (no code-fence stripping needed)

**For Batch API:**
```python
# Write JSONL file with all requests
# Upload via client.files.create()
# Submit via client.batches.create()
# Poll for completion
# Download results
```

### Phase 2: Prompt Redesign (1 day)

**New slim system prompt (~2,000 tokens, down from ~10,000):**
- Remove word_analysis/compact array instructions (biggest section)
- Remove few-shot examples (unnecessary for simpler output)
- Keep: glossary, topic taxonomy, narrator templates reference
- Add: chunks-only instructions, word_tags format
- Add: explicit "do NOT output translations.text — only chunk translations"

**New output schema:**
```json
{
  "diacritized_text": "Arabic with full tashkeel...",
  "diacritics_status": "added",
  "word_tags": [["قَالَ","V"],["عَنْ","PREP"],["عَلِيِّ","N"]],
  "chunks": [
    {
      "chunk_type": "isnad",
      "word_start": 0, "word_end": 15,
      "translations": {
        "en": "...", "ur": "...", "fa": "...", "tr": "..."
      }
    },
    {
      "chunk_type": "body",
      "word_start": 15, "word_end": 45,
      "translations": {
        "en": "...", "ur": "...", "fa": "...", "tr": "..."
      }
    }
  ],
  "translations": {
    "en": {"summary": "...", "key_terms": {"taqwa": "God-consciousness"}, "seo_question": "..."},
    "ur": {"summary": "..."},
    "fa": {"summary": "..."},
    "tr": {"summary": "..."}
  },
  "tags": ["theology", "ethics"],
  "content_type": "ethical_teaching",
  "topics": ["divine_knowledge"],
  "isnad_matn": {
    "has_chain": true,
    "narrators": [{"name_ar": "...", "name_en": "...", "role": "narrator", "confidence": "definite"}]
  },
  "related_quran": [{"surah": 2, "ayah": 255, "relationship": "thematic"}],
  "key_phrases": [{"arabic": "...", "english": "...", "category": "theological_concept"}]
}
```

### Phase 3: Validation & Review Adaptation (1 day)

Update `ai_pipeline.py` validation and `ai_pipeline_review.py` review checks:
- Accept `word_tags` field (list of `[word, POS]` pairs)
- Skip `word_analysis` validation (deferred to assembly)
- Accept missing `translations.*.text` (reconstructed from chunks)
- Adapt length_ratio checks to use chunk translations
- Keep isnad, Arabic echo, diacritics checks

### Phase 4: Production Run — Track 1 (1-2 days wall time)

**Mode A — Regular API (fast):**
- 50 concurrent requests (OpenAI allows higher concurrency than Claude)
- ~80K hadith × ~3s per call ÷ 50 workers = ~1.3 hours
- Cost: ~$770 (GPT-4.1-mini)

**Mode B — Batch API (cheapest):**
- Submit all 80K as JSONL
- ~24 hour turnaround
- Cost: ~$385 (GPT-4.1-mini, 50% off)

### Phase 5: Word Dictionary Build (1 day)

1. **Extract**: Python script scans all Track 1 responses, collects unique `(word, POS)` pairs
2. **Translate**: Submit as OpenAI Batch — each pair gets translations in target languages
   - Prompt: "Translate this Arabic word in hadith context: [word] (POS: [tag]). Output JSON: {en, ur, fa, tr}"
   - Batch 100 words per API call to amortize input tokens
3. **Assemble**: Python script builds `word_analysis` for each hadith from word_tags + dictionary
4. **Merge**: Add `word_analysis` to each response file

### Phase 6: Additional Languages (optional, ~$2,000)

If/when needed:
1. For each hadith, send Arabic text + English chunk translations
2. Ask model to translate chunks into id, bn, es, fr, de, ru, zh
3. Use GPT-4o-mini batch (cheapest for pure translation)
4. Merge into existing response files

### Phase 7: Angular UI Integration (parallel)

Update frontend to handle v4 schema gracefully:
- `translations.*.text` may be absent → reconstruct from chunks (existing `reconstruct_fields()` logic)
- `word_analysis` may arrive later → UI shows chunks without word-by-word initially
- Language count may start at 4 → language selector shows only available languages
- Progressive enhancement as Track 2/3 data arrives

---

## Timeline

| Week | Activity | Cost |
|------|----------|------|
| Week 1, Day 1 | Phase 0: Quality pilot (20 verses) | $0.20 |
| Week 1, Days 2-4 | Phase 1-3: API integration + prompt redesign + validation updates | $0 (dev work) |
| Week 1, Day 5 | Phase 4: Production run Track 1 (all 80K) | $400-1,500 |
| Week 2, Day 1 | Phase 5: Word dictionary build | $5 |
| Week 2, Days 2-3 | Phase 5: Assembly + validation | $0 (dev work) |
| Week 2, Day 4+ | Phase 6: Additional languages (if desired) | $2,000 |
| Weeks 1-3 | Phase 7: Angular UI updates (parallel) | $0 (dev work) |
| **Total** | | **$2,500-3,500** |

**All 80K hadith, all 11 languages, complete in 2-3 weeks for ~$3,500.**

---

## Risk Mitigation

### GPT-4.1-mini quality for Arabic/Islamic content
- **Risk**: Weaker diacritization, narrator recognition, or Islamic terminology than Claude Sonnet
- **Mitigation**: Phase 0 pilot on 20 verses with manual quality review
- **Fallback**: GPT-4.1 (full) at $2/$8 — still 7x cheaper than v3
- **Nuclear fallback**: Use `claude -p` with Haiku + slim prompt (~$0.15-0.25/hadith, ~$16K for 80K)

### Word dictionary ambiguity
- **Risk**: Same word form has different meanings in different hadith contexts
- **Mitigation**: POS tags disambiguate most cases. For remaining ambiguity, use most common hadith-context meaning. Can flag and fix rare cases later.
- **Improvement**: During Track 1, model can optionally output a `word_sense` hint for polysemous words

### Long hadith (>200 words)
- **Risk**: Output may exceed max_tokens, translations may be truncated
- **Mitigation**: Set max_tokens to 16K for >200-word hadith. GPT-4.1-mini supports 32K output. Even at 16K output tokens, cost is only ~$0.026 per long hadith (GPT-4.1-mini batch).
- **No chunking needed**: Unlike v3 which chunked for word_analysis, v4 Track 1 doesn't generate word_analysis per-hadith.

### OpenAI Batch API turnaround
- **Risk**: May take up to 24 hours per batch
- **Mitigation**: Submit early, process results next day. For urgency, use regular API (~2x cost, still incredibly cheap).

### Existing v3 responses (3,628 complete)
- **No re-processing needed**: v3 responses are a superset of v4 output
- Extract v4 fields from existing responses and mark as complete
- word_analysis from v3 can be kept as-is (already has all 11 languages)

---

## Comparison: v3 vs v4

| | v3 (current) | v4 (proposed) |
|--|-------------|--------------|
| **Model** | Claude Sonnet via `claude -p` | GPT-4.1-mini via OpenAI API |
| **Cost/hadith** | ~$2.00 | ~$0.01-0.05 |
| **80K corpus** | ~$160,000 | ~$3,500 |
| **Time to complete** | ~12 months | ~2-3 weeks |
| **word_analysis** | Per-hadith (11 langs × every word) | Corpus-wide dictionary (translate each word once) |
| **Translations** | Full text + chunks (duplicated) | Chunks only (text reconstructed) |
| **Languages** | 11 in one call | 4 core + 7 deferred |
| **Fix pass** | ~50% need Sonnet fix pass | Retry with same model (cheap) |
| **Fields** | 13 | 10 core + 3 assembled |

---

## Decisions Needed

1. **Phase 0 results**: Does GPT-4.1-mini produce acceptable Arabic diacritization and translation quality? (This determines the model choice.)

2. **Which 4 core languages?** Recommended: en, ur, fa, tr. Adjust if your audience differs.

3. **All 11 languages in Track 1 or defer 7?** At the projected costs, doing all 11 in Track 1 only adds ~$500-1,000. May be worth it if the model handles it well.

4. **Batch API or regular API?** Batch is 50% off but 24h turnaround. Given the tiny costs, regular API for speed may be preferable.

5. **Keep key_phrases?** Yes — at these costs, the ~100 extra output tokens per hadith adds <$10 total for 80K hadith.
