"""Phase 4: Multi-language translation using cheap LLM model.

Translates EN chunk translations and metadata to 10 other languages.
Uses GPT-5-mini (or similar cheap model) via OpenAI API.
"""

import json
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

NON_EN_LANGUAGES = ["ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"]

LANGUAGE_NAMES = {
    "ur": "Urdu", "tr": "Turkish", "fa": "Farsi/Persian",
    "id": "Indonesian", "bn": "Bengali", "es": "Spanish",
    "fr": "French", "de": "German", "ru": "Russian", "zh": "Chinese (Simplified)",
}


def build_translation_prompt(
    chunks: List[dict],
    en_summary: str,
    en_seo_question: str,
    arabic_text: str = "",
) -> tuple:
    """Build system and user prompts for multi-language translation.

    Args:
        chunks: List of chunk dicts, each with translations.en
        en_summary: English summary from translations.en.summary
        en_seo_question: English SEO question
        arabic_text: Original Arabic text for context

    Returns:
        (system_prompt, user_message) tuple
    """
    system = """You are a professional translator specializing in Islamic religious texts.
Translate the provided English passages to 10 languages: Urdu, Turkish, Farsi, Indonesian, Bengali, Spanish, French, German, Russian, Chinese (Simplified).

RULES:
- Preserve Islamic terminology (salat, wudu, zakat) unless the target language has established equivalents
- Transliterate narrator names — do not translate proper nouns
- Preserve honorifics (peace be upon him, etc.) in each language's convention
- Be faithful — do not add commentary
- For Chinese: do not use spaces between words
- Output valid JSON only"""

    # Build user message with chunks and metadata
    user_parts = []

    if arabic_text:
        user_parts.append(f"Original Arabic (for context, do NOT translate from Arabic — translate from English):\n{arabic_text}\n")

    user_parts.append("Translate the following English texts to 10 languages.\n")
    user_parts.append("Output a JSON object with this structure:")
    user_parts.append("""{
  "chunks": [
    {"translations": {"ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."}},
    ...
  ],
  "summary": {"ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."},
  "seo_question": {"ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."}
}""")

    user_parts.append("\n--- Texts to translate ---\n")

    for i, chunk in enumerate(chunks):
        en_text = ""
        if isinstance(chunk.get("translations"), dict):
            en_text = chunk["translations"].get("en", "")
        user_parts.append(f"Chunk {i+1} ({chunk.get('chunk_type', 'body')}): {en_text}")

    user_parts.append(f"\nSummary: {en_summary}")
    user_parts.append(f"SEO Question: {en_seo_question}")

    return system, "\n".join(user_parts)


def merge_translations(result: dict, translation_response: dict) -> dict:
    """Merge Phase 4 translation response into the pipeline result.

    Args:
        result: Current pipeline result dict (has EN-only translations)
        translation_response: Parsed JSON from translation LLM call

    Returns:
        Updated result dict with all 11 languages
    """
    # Merge chunk translations
    chunks_translations = translation_response.get("chunks", [])
    for i, chunk in enumerate(result.get("chunks", [])):
        if i < len(chunks_translations):
            chunk_trans = chunks_translations[i].get("translations", {})
            if "translations" not in chunk:
                chunk["translations"] = {}
            for lang in NON_EN_LANGUAGES:
                if lang in chunk_trans:
                    chunk["translations"][lang] = chunk_trans[lang]

    # Merge verse-level translations (summary, seo_question)
    summary_trans = translation_response.get("summary", {})
    seo_trans = translation_response.get("seo_question", {})

    translations = result.get("translations", {})
    for lang in NON_EN_LANGUAGES:
        if lang not in translations:
            translations[lang] = {}
        if lang in summary_trans:
            translations[lang]["summary"] = summary_trans[lang]
        if lang in seo_trans:
            translations[lang]["seo_question"] = seo_trans[lang]
        # key_terms for non-EN languages: empty dict (Phase 2 handles EN key_terms)
        if "key_terms" not in translations[lang]:
            translations[lang]["key_terms"] = {}

    result["translations"] = translations
    return result


def _fill_empty_translations(result: dict) -> None:
    """Fill all non-EN languages with empty strings as fallback.

    Mutates result in-place. Used when the translation call fails
    or the response cannot be parsed, so downstream code always sees
    all 11 languages present.
    """
    translations = result.get("translations", {})
    for lang in NON_EN_LANGUAGES:
        if lang not in translations:
            translations[lang] = {"summary": "", "seo_question": "", "key_terms": {}}
        else:
            translations[lang].setdefault("summary", "")
            translations[lang].setdefault("seo_question", "")
            translations[lang].setdefault("key_terms", {})
        for chunk in result.get("chunks", []):
            if "translations" not in chunk:
                chunk["translations"] = {}
            if lang not in chunk["translations"]:
                chunk["translations"][lang] = ""
    result["translations"] = translations


def _strip_code_fences(raw: str) -> str:
    """Strip markdown code fences from LLM output."""
    raw = raw.strip()
    if raw.startswith("```"):
        first_nl = raw.index("\n") if "\n" in raw else 3
        raw = raw[first_nl + 1:]
        if raw.rstrip().endswith("```"):
            raw = raw.rstrip()[:-3].rstrip()
    return raw


CHUNK_BATCH_SIZE = 4  # Max chunks per translation API call


def _build_batch_prompt(
    batch_chunks: List[dict],
    en_summary: str,
    en_seo_question: str,
    arabic_text: str,
    include_metadata: bool,
    batch_index: int,
) -> tuple:
    """Build translation prompt for a batch of chunks.

    Args:
        batch_chunks: Subset of chunks to translate.
        en_summary: EN summary (only included if include_metadata is True).
        en_seo_question: EN SEO question (only if include_metadata).
        arabic_text: Full Arabic text for context.
        include_metadata: Whether to include summary/seo_question.
        batch_index: 0-based batch number (for labeling).

    Returns:
        (system_prompt, user_message) tuple.
    """
    system = """You are a professional translator specializing in Islamic religious texts.
Translate the provided English passages to 10 languages: Urdu, Turkish, Farsi, Indonesian, Bengali, Spanish, French, German, Russian, Chinese (Simplified).

RULES:
- Preserve Islamic terminology (salat, wudu, zakat) unless the target language has established equivalents
- Transliterate narrator names — do not translate proper nouns
- Preserve honorifics (peace be upon him, etc.) in each language's convention
- Be faithful — do not add commentary
- For Chinese: do not use spaces between words
- Output valid JSON only"""

    user_parts = []

    if arabic_text:
        user_parts.append(
            f"Original Arabic (for context, do NOT translate from Arabic — translate from English):\n{arabic_text}\n"
        )

    user_parts.append("Translate the following English texts to 10 languages.\n")

    # Build expected JSON structure
    json_struct = '{\n  "chunks": [\n'
    json_struct += ",\n".join(
        '    {"translations": {"ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."}}'
        for _ in batch_chunks
    )
    json_struct += "\n  ]"
    if include_metadata:
        json_struct += ',\n  "summary": {"ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."}'
        json_struct += ',\n  "seo_question": {"ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."}'
    json_struct += "\n}"

    user_parts.append(f"Output a JSON object with this structure:\n{json_struct}")
    user_parts.append("\n--- Texts to translate ---\n")

    for i, chunk in enumerate(batch_chunks):
        en_text = ""
        if isinstance(chunk.get("translations"), dict):
            en_text = chunk["translations"].get("en", "")
        user_parts.append(f"Chunk {i+1} ({chunk.get('chunk_type', 'body')}): {en_text}")

    if include_metadata:
        user_parts.append(f"\nSummary: {en_summary}")
        user_parts.append(f"SEO Question: {en_seo_question}")

    return system, "\n".join(user_parts)


PER_LANG_WORKERS = 16  # Spark max-num-seqs ceiling
PER_LANG_SYSTEM_PROMPT = """You are a professional translator specializing in Islamic religious texts.
Preserve Islamic terminology (salat, wudu, zakat) unless the target language has established equivalents.
Transliterate narrator names — do not translate proper nouns.
Preserve honorifics (peace be upon him, etc.) in each language's convention.
Be faithful — do not add commentary.
For Chinese: do not use spaces between words.
Output valid JSON only."""

LANG_FULL_NAMES = {
    "ur": "Urdu", "tr": "Turkish", "fa": "Farsi/Persian",
    "id": "Indonesian", "bn": "Bengali", "es": "Spanish",
    "fr": "French", "de": "German", "ru": "Russian",
    "zh": "Chinese (Simplified)",
}


def _per_lang_chunk_schema() -> dict:
    return {
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
        "additionalProperties": False,
    }


def _per_lang_meta_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "summary": {"type": "string"},
            "seo_question": {"type": "string"},
        },
        "required": ["summary", "seo_question"],
        "additionalProperties": False,
    }


def _per_lang_chunk_user(en_text: str, lang_name: str, arabic_context: str = "") -> str:
    parts = []
    if arabic_context:
        parts.append(
            f"Original Arabic (for context, translate from the English):\n{arabic_context}\n"
        )
    parts.append(f"Translate the following English passage into {lang_name}.")
    parts.append('Output JSON of the form: {"text": "..."}')
    parts.append(f"\nEnglish: {en_text}")
    return "\n".join(parts)


def _per_lang_meta_user(en_summary: str, en_seo: str, lang_name: str) -> str:
    return (
        f"Translate the following English texts into {lang_name}.\n"
        'Output JSON of the form: {"summary": "...", "seo_question": "..."}\n\n'
        f"English summary: {en_summary}\n"
        f"English SEO question: {en_seo}"
    )


async def _translate_chunks_per_language(
    result: dict,
    model: str,
    arabic_text: str,
    verse_id: Optional[str],
    raw_archive_dir: Optional[str],
) -> dict:
    """Spark-optimised path: N×10 small per-(chunk,lang) calls + 10 meta calls.

    See PHASE4_OPENWEIGHT_BENCHMARK.md round 4 — this approach hits 99.5%
    parse rate and ~2.4× faster wall time than the batched approach on
    Spark Qwen 3.6 with strict JSON-schema response_format.

    The cost is 10× the prompt tokens (system prompt repeated per call),
    but on Spark electricity is the only cost so this is irrelevant.
    """
    import asyncio
    from app.pipeline_cli.openai_backend import call_openai, archive_raw_response

    chunks = result.get("chunks", [])
    en_trans = result.get("translations", {}).get("en", {})
    en_summary = en_trans.get("summary", "")
    en_seo = en_trans.get("seo_question", "")

    chunk_schema = _per_lang_chunk_schema()
    meta_schema = _per_lang_meta_schema()
    sem = asyncio.Semaphore(PER_LANG_WORKERS)

    async def _call_with_retry(system, user, schema, name, max_tokens):
        """One call, one retry on parse failure. Qwen occasionally emits
        valid JSON followed by a `}` loop that consumes max_tokens; a fresh
        call usually succeeds."""
        for attempt in range(2):
            async with sem:
                cr = await call_openai(
                    system, user, model=model,
                    max_output_tokens=max_tokens,
                    response_format={
                        "type": "json_schema",
                        "json_schema": {"name": name, "schema": schema, "strict": True},
                    },
                )
            if "error" in cr:
                if attempt == 0:
                    continue
                return cr
            # Try to parse — if fail, retry once
            try:
                json.loads(_strip_code_fences(cr.get("result", "")))
                return cr
            except (json.JSONDecodeError, ValueError):
                if attempt == 0:
                    logger.info("Phase4(per-lang) retry: parse fail on %s", name)
                    continue
                return cr
        return cr

    async def call_chunk_lang(chunk_idx: int, lang: str) -> tuple:
        en_text = ""
        ct = chunks[chunk_idx].get("translations", {}) or {}
        if isinstance(ct, dict):
            en_text = ct.get("en", "") or ""
        user = _per_lang_chunk_user(
            en_text, LANG_FULL_NAMES[lang],
            arabic_context=arabic_text if chunk_idx == 0 else "",
        )
        # max_tokens=600 chosen per SPARK_OPTIMIZATION_LOG.md Round D:
        # tighter budget halves the `}`-loop wasted-token problem and reduces
        # parse failures (single-language chunk translations are ~150-400
        # tokens; 600 is enough for the longest, tight enough to clip degenerate
        # outputs early).
        cr = await _call_with_retry(
            PER_LANG_SYSTEM_PROMPT, user, chunk_schema,
            "chunk_translation", max_tokens=600,
        )
        return ("chunk", chunk_idx, lang, cr)

    async def call_meta_lang(lang: str) -> tuple:
        user = _per_lang_meta_user(en_summary, en_seo, LANG_FULL_NAMES[lang])
        # max_tokens=400 per Round D — summary+seo_question in one language
        # is consistently ~100-200 tokens.
        cr = await _call_with_retry(
            PER_LANG_SYSTEM_PROMPT, user, meta_schema,
            "meta_translation", max_tokens=400,
        )
        return ("meta", None, lang, cr)

    tasks = []
    for lang in NON_EN_LANGUAGES:
        for i in range(len(chunks)):
            tasks.append(call_chunk_lang(i, lang))
        tasks.append(call_meta_lang(lang))

    call_results = await asyncio.gather(*tasks)

    total_cost = 0.0
    total_tokens = 0
    total_input_tokens = 0
    total_cache_read = 0
    n_failed = 0
    actual_model: Optional[str] = None  # server-reported canonical name

    for kind, idx, lang, cr in call_results:
        total_cost += cr.get("cost", 0) or 0
        total_tokens += cr.get("output_tokens", 0) or 0
        total_input_tokens += cr.get("input_tokens", 0) or 0
        total_cache_read += cr.get("cache_read_tokens", 0) or 0
        if actual_model is None and cr.get("model"):
            actual_model = cr.get("model")

        if "error" in cr:
            n_failed += 1
            logger.warning("Phase4(per-lang) %s/%s failed: %s",
                           kind, lang, cr.get("error", "")[:120])
            continue

        try:
            parsed = json.loads(_strip_code_fences(cr.get("result", "")))
        except (json.JSONDecodeError, ValueError) as e:
            n_failed += 1
            archive_raw_response(raw_archive_dir, verse_id,
                                 f"phase4.{kind}.c{idx}.{lang}",
                                 cr.get("result", ""))
            logger.warning("Phase4(per-lang) %s/%s parse fail: %s",
                           kind, lang, e)
            continue

        if kind == "chunk":
            if "translations" not in chunks[idx]:
                chunks[idx]["translations"] = {}
            chunks[idx]["translations"][lang] = parsed.get("text", "")
        elif kind == "meta":
            translations = result.get("translations", {})
            if lang not in translations:
                translations[lang] = {}
            translations[lang]["summary"] = parsed.get("summary", "")
            translations[lang]["seo_question"] = parsed.get("seo_question", "")
            translations[lang].setdefault("key_terms", {})
            result["translations"] = translations

    _fill_empty_translations(result)
    result["_phase4_cost"] = total_cost
    result["_phase4_tokens"] = total_tokens
    result["_phase4_input_tokens"] = total_input_tokens
    result["_phase4_cache_read_tokens"] = total_cache_read
    result["_phase4_calls_failed"] = n_failed
    result["_phase4_mode"] = "per_language"
    if actual_model:
        result["_phase4_actual_model"] = actual_model
    return result


async def translate_chunks(
    result: dict,
    model: str = "gpt-5-mini",
    arabic_text: str = "",
    verse_id: Optional[str] = None,
    raw_archive_dir: Optional[str] = None,
) -> dict:
    """Run Phase 4 translation on a pipeline result.

    Splits chunks into batches of CHUNK_BATCH_SIZE to avoid output
    truncation on long verses. The first batch also translates the
    verse-level summary and seo_question.

    For Spark/Qwen models, automatically switches to per-language mode
    which hits 99.5% reliability on Spark vs ~94% for batched (see
    PHASE4_OPENWEIGHT_BENCHMARK.md round 4).

    Args:
        result: Pipeline result with EN-only translations
        model: OpenAI model to use for translation
        arabic_text: Original Arabic text for context

    Returns:
        Updated result dict with all 11 languages, plus cost metadata
    """
    from app.pipeline_cli.openai_backend import call_openai, is_spark_model

    # Spark/Qwen path uses per-language calls for better reliability
    if is_spark_model(model):
        return await _translate_chunks_per_language(
            result, model, arabic_text, verse_id, raw_archive_dir,
        )

    chunks = result.get("chunks", [])
    en_trans = result.get("translations", {}).get("en", {})
    en_summary = en_trans.get("summary", "")
    en_seo = en_trans.get("seo_question", "")

    total_cost = 0.0
    total_tokens = 0
    total_input_tokens = 0
    total_cache_read_tokens = 0
    actual_model: Optional[str] = None

    # Split chunks into batches
    batches = []
    for i in range(0, len(chunks), CHUNK_BATCH_SIZE):
        batches.append(chunks[i:i + CHUNK_BATCH_SIZE])

    if not batches:
        batches = [[]]  # Still need to translate summary/seo

    for batch_idx, batch_chunks in enumerate(batches):
        include_metadata = (batch_idx == 0)  # summary/seo in first batch only

        if not batch_chunks and not include_metadata:
            continue

        system, user = _build_batch_prompt(
            batch_chunks, en_summary, en_seo, arabic_text,
            include_metadata, batch_idx,
        )

        cr = await call_openai(system, user, model=model)

        if "error" in cr:
            logger.error("Phase 4 translation batch %d failed: %s",
                         batch_idx, cr["error"])
            continue

        total_cost += cr.get("cost", 0)
        total_tokens += cr.get("output_tokens", 0)
        total_input_tokens += cr.get("input_tokens", 0)
        total_cache_read_tokens += cr.get("cache_read_tokens", 0)
        if actual_model is None and cr.get("model"):
            actual_model = cr.get("model")

        # Parse response
        try:
            raw = _strip_code_fences(cr.get("result", ""))
            trans_data = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as e:
            # Persist the raw text we already paid for so it can be salvaged
            # offline. The other batches still run; missing translations get
            # caught by validate_result's empty-string check (commit e84f106).
            from app.pipeline_cli.openai_backend import archive_raw_response
            archive_raw_response(raw_archive_dir, verse_id,
                                 f"phase4.batch{batch_idx}",
                                 cr.get("result", ""))
            logger.error("Phase 4 translation batch %d JSON parse failed: %s",
                         batch_idx, e)
            continue

        # Merge chunk translations at correct offsets
        chunk_offset = batch_idx * CHUNK_BATCH_SIZE
        batch_chunk_trans = trans_data.get("chunks", [])
        for j, chunk_trans in enumerate(batch_chunk_trans):
            abs_idx = chunk_offset + j
            if abs_idx < len(chunks):
                ct = chunk_trans.get("translations", {})
                if "translations" not in chunks[abs_idx]:
                    chunks[abs_idx]["translations"] = {}
                for lang in NON_EN_LANGUAGES:
                    if lang in ct:
                        chunks[abs_idx]["translations"][lang] = ct[lang]

        # Merge verse-level translations (first batch only)
        if include_metadata:
            summary_trans = trans_data.get("summary", {})
            seo_trans = trans_data.get("seo_question", {})
            translations = result.get("translations", {})
            for lang in NON_EN_LANGUAGES:
                if lang not in translations:
                    translations[lang] = {}
                if lang in summary_trans:
                    translations[lang]["summary"] = summary_trans[lang]
                if lang in seo_trans:
                    translations[lang]["seo_question"] = seo_trans[lang]
                if "key_terms" not in translations[lang]:
                    translations[lang]["key_terms"] = {}
            result["translations"] = translations

    # Fill any languages still missing after all batches
    _fill_empty_translations(result)

    # Attach cost metadata
    result["_phase4_cost"] = total_cost
    result["_phase4_tokens"] = total_tokens
    result["_phase4_input_tokens"] = total_input_tokens
    result["_phase4_cache_read_tokens"] = total_cache_read_tokens
    if actual_model:
        result["_phase4_actual_model"] = actual_model

    return result
