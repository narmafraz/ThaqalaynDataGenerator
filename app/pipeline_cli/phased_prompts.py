"""Phase 1: Reduced AI prompt for core fields only.

Asks the LLM for diacritization, POS tagging, chunking, EN translation,
thematic Quran refs, basic isnad/matn separation, topics, tags, and
content_type. Remaining fields (narrators, key_phrases, key_terms,
10 non-EN translations) are handled by Phase 2 (programmatic) and
Phase 4 (translation).
"""

import json
from typing import Optional

from app.ai_pipeline import (
    PipelineRequest,
    load_glossary,
    load_topic_taxonomy,
    _format_topic_taxonomy,
)


def build_phase1_system_prompt(
    glossary: Optional[dict] = None,
    topic_taxonomy: Optional[dict] = None,
) -> str:
    """Build a lighter system prompt for Phase 1 core generation.

    Includes topic taxonomy for topics/tags/content_type assignment.
    Omits: key phrases reference, 10-language translation instructions,
    narrator linking instructions. ~50% smaller than monolithic.
    """
    if glossary is None:
        glossary = load_glossary()
    if topic_taxonomy is None:
        topic_taxonomy = load_topic_taxonomy()

    # Format glossary compactly
    glossary_lines = ["Arabic | English | Urdu | Turkish | Farsi"]
    for term in glossary.get("terms", []):
        glossary_lines.append(
            f"{term['ar']} | {term['en']} | {term.get('ur', '')} | "
            f"{term.get('tr', '')} | {term.get('fa', '')}"
        )
    glossary_table = "\n".join(glossary_lines)

    taxonomy_section = ""
    if topic_taxonomy and topic_taxonomy.get("taxonomy"):
        taxonomy_table = _format_topic_taxonomy(topic_taxonomy)
        taxonomy_section = f"""

TOPIC TAXONOMY (for field #9 "topics"):
Assign 1-3 Level 2 topic keys from this controlled vocabulary. Use ONLY the topic keys listed below.
{taxonomy_table}"""

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
{glossary_table}{taxonomy_section}"""


def build_phase1_user_message(request: PipelineRequest) -> str:
    """Build the user message for Phase 1 — core fields + classification.

    Requests 9 fields instead of 12:
    1. diacritized_text + diacritics_changes
    2. word_tags
    3. chunks (EN-only translations)
    4. translations.en (summary + seo_question only)
    5. related_quran (thematic refs only)
    6. isnad_matn (isnad_ar, matn_ar, has_chain — no narrators)
    7. tags
    8. content_type
    9. topics
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
5. "tags": (array of 2-5 enums) theology|ethics|jurisprudence|worship|quran_commentary|prophetic_tradition|family|social_relations|knowledge|dua|afterlife|history|economy|governance
6. "content_type": (enum) legal_ruling|ethical_teaching|narrative|prophetic_tradition|quranic_commentary|supplication|creedal|eschatological|biographical|theological|exhortation|cosmological
7. "chunks": (array) Paragraph-level segmentation with EN-only translations.
   Each: {"chunk_type": "isnad"|"opening"|"body"|"quran_quote"|"closing",
          "word_start": int, "word_end": int,
          "translations": {"en": "..."}}
   RULES: Sequential, non-overlapping, complete (0 to len(word_tags)). At least 1 chunk.
8. "translations": {"en": {"summary": "2-3 sentences", "seo_question": "..."}}
9. "related_quran": (array) [{"ref": "surah:ayah", "relationship": "thematic"}] or []
   Only include thematic connections. Do not scan for explicit [S:V] refs.
10. "isnad_matn": {"isnad_ar": "...", "matn_ar": "...", "has_chain": boolean}
   Separate narrator chain from body text. No narrators array needed.
11. "topics": (array of 1-5 strings) Level 2 topic keys from the TOPIC TAXONOMY in the system prompt.""")

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

    # Topics, tags, content_type (LLM-generated in Phase 1)
    normalized["topics"] = result_dict.get("topics", [])
    normalized["tags"] = result_dict.get("tags", [])
    normalized["content_type"] = result_dict.get("content_type", "")

    # isnad_matn (basic, no narrators)
    isnad = result_dict.get("isnad_matn", {})
    normalized["isnad_matn"] = {
        "isnad_ar": isnad.get("isnad_ar", ""),
        "matn_ar": isnad.get("matn_ar", ""),
        "has_chain": isnad.get("has_chain", False),
        "narrators": [],  # Added by Phase 2
    }

    return normalized
