"""JSON Schema for pipeline structured output via --json-schema flag.

The schema enforces the 13-field output format so Claude returns valid
JSON matching our structure. Arabic-key enforcement and content-level
validation remain in postprocessing (schema can only validate types/shapes).
"""

from app.ai_pipeline import (
    VALID_CHUNK_TYPES,
    VALID_CONTENT_TYPES,
    VALID_DIACRITICS_STATUS,
    VALID_IDENTITY_CONFIDENCE,
    VALID_LANGUAGE_KEYS,
    VALID_NARRATOR_ROLES,
    VALID_PHRASE_CATEGORIES,
    VALID_POS_TAGS,
    VALID_TAGS,
)

# Language keys as a sorted list for consistent schema generation
_LANG_KEYS = sorted(VALID_LANGUAGE_KEYS)


def _translation_entry_schema() -> dict:
    """Schema for a single language's translation entry (text, summary, key_terms, seo_question)."""
    return {
        "type": "object",
        "properties": {
            "text": {"type": "string"},
            "summary": {"type": "string"},
            "key_terms": {
                "type": "object",
                "additionalProperties": {"type": "string"},
            },
            "seo_question": {"type": "string"},
        },
        "required": ["text", "summary", "key_terms", "seo_question"],
    }


def _narrator_schema() -> dict:
    """Schema for a single narrator in isnad_matn.narrators."""
    return {
        "type": "object",
        "properties": {
            "name_ar": {"type": "string"},
            "name_en": {"type": "string"},
            "role": {"type": "string", "enum": sorted(VALID_NARRATOR_ROLES)},
            "position": {"type": "integer"},
            "identity_confidence": {"type": "string", "enum": sorted(VALID_IDENTITY_CONFIDENCE)},
            "ambiguity_note": {"type": ["string", "null"]},
            "known_identity": {"type": ["string", "null"]},
            "word_ranges": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "word_start": {"type": "integer"},
                        "word_end": {"type": "integer"},
                    },
                    "required": ["word_start", "word_end"],
                },
            },
        },
        "required": ["name_ar", "name_en", "role", "position", "identity_confidence"],
    }


def _chunk_schema() -> dict:
    """Schema for a single chunk."""
    return {
        "type": "object",
        "properties": {
            "chunk_type": {"type": "string", "enum": sorted(VALID_CHUNK_TYPES)},
            "arabic_text": {"type": "string"},
            "word_start": {"type": "integer"},
            "word_end": {"type": "integer"},
            "translations": {
                "type": "object",
                "properties": {lang: {"type": "string"} for lang in _LANG_KEYS},
                "required": _LANG_KEYS,
            },
        },
        "required": ["chunk_type", "arabic_text", "word_start", "word_end", "translations"],
    }


def build_output_schema() -> dict:
    """Build the full JSON Schema for the pipeline output.

    Returns a dict suitable for json.dumps() and passing to --json-schema.
    """
    # Word analysis: compact arrays ["word","POS","en","ur",...] — 13 string elements
    word_entry_schema = {
        "type": "array",
        "items": {"type": "string"},
    }

    schema = {
        "type": "object",
        "properties": {
            "diacritized_text": {"type": "string"},
            "diacritics_status": {"type": "string", "enum": sorted(VALID_DIACRITICS_STATUS)},
            "diacritics_changes": {"type": "array", "items": {"type": "string"}},
            "word_analysis": {
                "type": "array",
                "items": word_entry_schema,
            },
            "tags": {
                "type": "array",
                "items": {"type": "string", "enum": sorted(VALID_TAGS)},
            },
            "content_type": {"type": "string", "enum": sorted(VALID_CONTENT_TYPES)},
            "related_quran": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "ref": {"type": "string"},
                        "relationship": {"type": "string", "enum": ["explicit", "thematic"]},
                        "word_start": {"type": "integer"},
                        "word_end": {"type": "integer"},
                    },
                    "required": ["ref", "relationship"],
                },
            },
            "isnad_matn": {
                "type": "object",
                "properties": {
                    "isnad_ar": {"type": "string"},
                    "matn_ar": {"type": "string"},
                    "has_chain": {"type": "boolean"},
                    "narrators": {
                        "type": "array",
                        "items": _narrator_schema(),
                    },
                },
                "required": ["isnad_ar", "matn_ar", "has_chain", "narrators"],
            },
            "translations": {
                "type": "object",
                "properties": {lang: _translation_entry_schema() for lang in _LANG_KEYS},
                "required": _LANG_KEYS,
            },
            "chunks": {
                "type": "array",
                "items": _chunk_schema(),
            },
            "topics": {
                "type": "array",
                "items": {"type": "string"},
            },
            "key_phrases": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "phrase_ar": {"type": "string"},
                        "phrase_en": {"type": "string"},
                        "category": {"type": "string", "enum": sorted(VALID_PHRASE_CATEGORIES)},
                    },
                    "required": ["phrase_ar", "phrase_en", "category"],
                },
            },
            "similar_content_hints": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "description": {"type": "string"},
                        "theme": {"type": "string"},
                    },
                    "required": ["description", "theme"],
                },
            },
        },
        "required": [
            "diacritized_text", "diacritics_status", "diacritics_changes",
            "word_analysis", "tags", "content_type", "related_quran",
            "isnad_matn", "translations", "chunks", "topics",
            "key_phrases", "similar_content_hints",
        ],
    }

    return schema
