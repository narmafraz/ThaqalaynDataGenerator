"""Phase 1: Reduced AI prompt for core fields only.

Asks the LLM for diacritization, POS tagging, chunking, EN translation,
thematic Quran refs, and basic isnad/matn separation. All other fields
(narrators, topics, tags, key_phrases, key_terms, 10 non-EN translations)
are handled by Phase 2 (programmatic) and Phase 4 (translation).
"""

import json
from typing import Optional

from app.ai_pipeline import PipelineRequest, load_glossary


def build_phase1_system_prompt(glossary: Optional[dict] = None) -> str:
    """Build a lighter system prompt for Phase 1 core generation.

    Omits: topic taxonomy, key phrases reference, 10-language translation
    instructions, narrator linking instructions. ~60% smaller than monolithic.
    """
    if glossary is None:
        glossary = load_glossary()

    # Format glossary compactly
    glossary_lines = ["Arabic | English | Urdu | Turkish | Farsi"]
    for term in glossary.get("terms", []):
        glossary_lines.append(
            f"{term['ar']} | {term['en']} | {term.get('ur', '')} | "
            f"{term.get('tr', '')} | {term.get('fa', '')}"
        )
    glossary_table = "\n".join(glossary_lines)

    return f"""You are a specialist in Shia Islamic scholarly texts analyzing hadith from the Four Books and other primary Shia sources.

RULES:
- Preserve all honorifics (عليه السلام, صلى الله عليه وآله وسلم, etc.)
- Use established Islamic terminology (see GLOSSARY)
- Be faithful to Shia scholarly tradition
- Translate faithfully — no commentary or interpretation
- Reproduce Quran quotes exactly — never paraphrase
- Transliterate narrator names consistently (do not translate)
- This text is classical Arabic (fusha qadima)
- Output valid JSON only

GLOSSARY:
{glossary_table}"""


def build_phase1_user_message(request: PipelineRequest) -> str:
    """Build the user message for Phase 1 — only core fields.

    Requests 6 fields instead of 12:
    1. diacritized_text + diacritics_changes
    2. word_tags
    3. chunks (EN-only translations)
    4. translations.en (summary + seo_question only)
    5. related_quran (thematic refs only)
    6. isnad_matn (isnad_ar, matn_ar, has_chain — no narrators)
    """
    parts = [f"Arabic text: {request.arabic_text}"]

    if request.english_text:
        parts.append(f"English reference translation: {request.english_text}")

    parts.append(f"Book: {request.book_name}")
    parts.append(f"Chapter: {request.chapter_title}")

    if request.hadith_number is not None:
        parts.append(f"Hadith number: {request.hadith_number}")

    chain = request.existing_narrator_chain
    parts.append(f"Existing narrator chain: {chain if chain else 'null'}")

    parts.append("")
    parts.append("""Generate a JSON object with these fields:

1. "diacritized_text": (string) Full Arabic text with complete tashkeel.
2. "diacritics_status": (enum) "added"|"completed"|"validated"|"corrected"
3. "diacritics_changes": (array) Corrections made. Empty [] if "added"/"validated".
4. "word_tags": (array) Per word: [diacritized_word, POS_tag]
   POS: N|V|ADJ|ADV|PREP|CONJ|PRON|DET|PART|INTJ|REL|DEM|NEG|COND|INTERR
   Words must match diacritized_text exactly. Every word must have full tashkeel.
5. "chunks": (array) Paragraph-level segmentation with EN-only translations.
   Each: {"chunk_type": "isnad"|"opening"|"body"|"quran_quote"|"closing",
          "word_start": int, "word_end": int,
          "translations": {"en": "..."}}
   RULES: Sequential, non-overlapping, complete (0 to len(word_tags)). At least 1 chunk.
6. "translations": {"en": {"summary": "2-3 sentences", "seo_question": "..."}}
7. "related_quran": (array) [{"ref": "surah:ayah", "relationship": "thematic"}] or []
   Only include thematic connections. Do not scan for explicit [S:V] refs.
8. "isnad_matn": {"isnad_ar": "...", "matn_ar": "...", "has_chain": boolean}
   Separate narrator chain from body text. No narrators array needed.""")

    return "\n".join(parts)


def parse_phase1_response(result_dict: dict) -> dict:
    """Normalize a Phase 1 response for downstream processing.

    Ensures all expected Phase 1 fields exist with correct types.
    Does NOT add Phase 2/4 fields — those are added by programmatic_enrich()
    and translate_chunks().
    """
    normalized = {}

    # Copy Phase 1 fields with defaults
    normalized["diacritized_text"] = result_dict.get("diacritized_text", "")
    normalized["diacritics_status"] = result_dict.get("diacritics_status", "added")
    normalized["diacritics_changes"] = result_dict.get("diacritics_changes", [])
    normalized["word_tags"] = result_dict.get("word_tags", [])

    # Chunks: ensure EN-only translations
    chunks = result_dict.get("chunks", [])
    normalized["chunks"] = chunks

    # translations.en only
    translations = result_dict.get("translations", {})
    if isinstance(translations, dict) and "en" in translations:
        normalized["translations"] = {"en": translations["en"]}
    else:
        normalized["translations"] = {"en": {"summary": "", "seo_question": ""}}

    # related_quran (thematic only from Phase 1)
    normalized["related_quran"] = result_dict.get("related_quran", [])

    # isnad_matn (basic, no narrators)
    isnad = result_dict.get("isnad_matn", {})
    normalized["isnad_matn"] = {
        "isnad_ar": isnad.get("isnad_ar", ""),
        "matn_ar": isnad.get("matn_ar", ""),
        "has_chain": isnad.get("has_chain", False),
        "narrators": [],  # Added by Phase 2
    }

    return normalized
