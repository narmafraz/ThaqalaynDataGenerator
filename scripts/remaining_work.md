# Remaining Work — Pipeline v3 Optimizations

## Completed This Session

- **Item 5**: Key terms prompt fix — Arabic examples in `build_user_message()`, CRITICAL instruction
- **Item 3**: Trim system prompt — empty EXAMPLES removed, key_phrases 30→15, prompt 14,222 chars
- **Item 1**: `--json-schema` structured output — schema in `output_schema.py` (6,210 chars), `call_claude()` updated with `json_schema`, `fallback_model`, `max_budget_usd` params, `postprocess_verse()` accepts `parsed_dict`
- **Item 6**: Pipeline status tool — `pipeline_status.py` with `--audit` flag

## Remaining Items

### 1. Item 4: Remove `similar_content_hints` (needs user decision)
- Remove field 13 from `build_user_message()` (line 492-498 in `ai_pipeline.py`)
- Remove from `output_schema.py` `required` list and `properties`
- Remove from `validate_result()` if validated there (check lines after 1163)
- Saves ~50-100 output tokens/verse

### 2. E2E Validation of --json-schema
- Run 3-5 real verses through the updated pipeline with `--json-schema`
- Verify `structured_output` is correctly parsed
- Compare quality to non-schema output
- Test: `python -m app.pipeline_cli.pipeline --single /books/al-kafi:1:1:1:3 --workers 1`

### 3. Update test_e2e_flow.py
- Update `call_claude()` in test script to match new pipeline.py signature
- Add `--json-schema` and `--fallback-model` flags
- Read `structured_output` from response

### 4. Production Readiness
- Test with 20+ verses covering short/medium/long hadiths + Quran
- Tune worker count (try 10-15 instead of 5)
- First 1000-verse batch on Al-Kafi volume 1
- Monitor with `pipeline_status.py`

### 5. Language Count Optimization (deferred, needs product decision)
- Currently 11 languages per verse
- Reducing to 5 priority languages (en, ur, tr, fa, ar) would save $3-5K
- Remaining 6 languages could be generated in a cheaper second pass

## Key Files Modified
- `app/ai_pipeline.py` — Items 5+3 (prompt changes)
- `app/pipeline_cli/pipeline.py` — Item 1 (schema, fallback, budget)
- `app/pipeline_cli/verse_processor.py` — Item 1 (parsed_dict param)
- `app/pipeline_cli/output_schema.py` — NEW (Item 1)
- `app/pipeline_cli/pipeline_status.py` — NEW (Item 6)
