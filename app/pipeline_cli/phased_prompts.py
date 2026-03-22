"""Phase 1: Reduced AI prompt for core fields only.

Asks the LLM for chunked segmentation with diacritized Arabic text,
EN translation, classification (topics/tags/content_type), thematic
Quran refs, has_chain boolean, and key_terms. Remaining fields
(narrators, key_phrases, word_tags, diacritized_text, isnad_matn,
diacritics_status/changes, 10 non-EN translations) are derived by
Phase 2 (programmatic) and Phase 4 (translation).
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
    narrator linking instructions, word-level POS tagging instructions.
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

TOPIC TAXONOMY (for "topics" field):
Assign 1-5 Level 2 topic keys from this CLOSED vocabulary. You MUST use ONLY the exact keys from the "Level 2 Topic Key" column below. Do NOT invent new keys, do NOT use dotted notation, do NOT paraphrase.
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
- All Arabic text in the output must have COMPLETE tashkeel (diacritics) on every word
- Output valid JSON only

GLOSSARY:
{glossary_table}{taxonomy_section}"""


def build_phase1_user_message(request: PipelineRequest) -> str:
    """Build the user message for Phase 1 — 7 core fields.

    Fields requested:
    1. chunks (with arabic_text + EN translations)
    2. tags
    3. content_type
    4. translations.en (summary + seo_question + key_terms)
    5. related_quran
    6. has_chain
    7. topics
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

1. "has_chain": (boolean) Whether this text has a narrator chain (isnad).
2. "tags": (array of 2-5 enums) theology|ethics|jurisprudence|worship|quran_commentary|prophetic_tradition|family|social_relations|knowledge|dua|afterlife|history|economy|governance
3. "content_type": (enum) legal_ruling|ethical_teaching|narrative|prophetic_tradition|quranic_commentary|supplication|creedal|eschatological|biographical|theological|exhortation|cosmological
4. "chunks": (array) Paragraph-level segmentation of the text.
   Each chunk: {"chunk_type": "isnad"|"opening"|"body"|"quran_quote"|"closing",
                "arabic_text": "fully diacritized Arabic text for this chunk",
                "translations": {"en": "English translation of this chunk"}}
   RULES:
   - At least 1 chunk. Every word of the original text must appear in exactly one chunk.
   - arabic_text must be the COMPLETE Arabic text for that segment with FULL tashkeel on every word.
   - Segment at natural boundaries: isnad→matn transition, topic shifts, Quran quotes, opening/closing formulae.
   - If has_chain is true, the narrator chain MUST be in one or more "isnad" chunks.
5. "translations": {"en": {"summary": "2-3 sentences explaining the verse's meaning and significance",
                           "seo_question": "A natural question this verse answers",
                           "key_terms": {"Arabic term with diacritics": "English definition in context", ...}}}
   key_terms: 3-8 important Arabic terms from the text with contextual English definitions.
   Keys MUST be Arabic words with full diacritics taken from the text.
6. "related_quran": (array) [{"ref": "surah:ayah", "relationship": "thematic"}] or []
   Only include thematic connections to Quran verses. Do not scan for explicit [S:V] refs.
7. "topics": (array of 1-5 strings) Pick ONLY from this closed set of valid topic keys:
   abrogation, ahlulbayt_virtues, anger_control, backbiting, barzakh, charity, community, companions, consultation, death_dying, dhikr, divine_attributes, divine_decree, divine_justice, divine_knowledge, etiquette, etiquette_of_dua, events, fasting, fasting_rulings, financial_law, forbidding_evil, friendship, ghadir, gratitude, hadith_sciences, hajj, halal_haram, honesty, hospitality, humility, ignorance, imamate, imams_biography, inheritance, intercession, judicial_rulings, justice_system, karbala, kinship, leadership, marriage_family_law, miracles, mosque_etiquette, neighbors, night_prayer, occasions_of_revelation, oppression, orphans, paradise_hell, parenting, patience, poverty_wealth, prayer_rulings, prophethood, prophetic_character, prophets, quran_interpretation_method, quran_recitation, quran_virtues, reasoning, reckoning, religious_authority, repentance, resurrection, rights_of_others, rights_of_rulers, ritual_purity, salat, scholars_virtues, seeking_forgiveness, seeking_knowledge, seeking_refuge, signs_of_end, sincerity, specific_supplications, spousal_rights, sunnah, tafsir_specific_verse, tawhid, teaching, times_for_dua, trade_ethics, trust, usury, womens_rights, work_livelihood, zakat_khums
   CRITICAL: Use ONLY keys from the list above. Do NOT use tag names (theology, ethics, etc.) as topics — those are different fields.""")

    return "\n".join(parts)


def parse_phase1_response(result_dict: dict) -> dict:
    """Normalize a Phase 1 response for downstream processing.

    Ensures all expected Phase 1 fields exist with correct types.
    Does NOT add Phase 2/4 fields — those are added by programmatic_enrich()
    and translate_chunks().
    """
    normalized = {}

    # has_chain: top-level boolean
    normalized["has_chain"] = bool(result_dict.get("has_chain", False))

    # tags, content_type, topics
    normalized["tags"] = result_dict.get("tags", [])
    normalized["content_type"] = result_dict.get("content_type", "")
    normalized["topics"] = result_dict.get("topics", [])

    # Chunks: must have arabic_text and translations
    chunks = result_dict.get("chunks", [])
    for chunk in chunks:
        chunk.setdefault("arabic_text", "")
        chunk.setdefault("translations", {})
        # Strip word_start/word_end if LLM included them — Phase 2 adds correct ones
        chunk.pop("word_start", None)
        chunk.pop("word_end", None)
    normalized["chunks"] = chunks

    # translations.en with key_terms
    translations = result_dict.get("translations", {})
    if isinstance(translations, dict) and "en" in translations:
        en = translations["en"] if isinstance(translations["en"], dict) else {}
        normalized["translations"] = {"en": {
            "summary": en.get("summary", ""),
            "seo_question": en.get("seo_question", ""),
            "key_terms": en.get("key_terms", {}),
        }}
    else:
        normalized["translations"] = {
            "en": {"summary": "", "seo_question": "", "key_terms": {}}
        }

    # related_quran (thematic only from Phase 1)
    normalized["related_quran"] = result_dict.get("related_quran", [])

    return normalized
