"""Phase 2: Programmatic enrichment — derive fields from existing data without AI.

Each function is independent and testable. The orchestrator ``programmatic_enrich()``
calls all of them and merges results into a complete pipeline result.
"""

import logging
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

from app.arabic_normalization import strip_tashkeel
from app.ai_pipeline import (
    VALID_TAGS,
    VALID_CONTENT_TYPES,
    VALID_TOPICS,
    VALID_PHRASE_CATEGORIES,
    VALID_LANGUAGE_KEYS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

QURAN_QUOTE_RE = re.compile(r"[\[\(](\d+):(\d+)[\]\)]")

# Common isnad particles and stop-words to exclude from key_terms.
_ISNAD_STOP_WORDS = frozenset(
    {
        "عن",
        "من",
        "بن",
        "ابن",
        "قال",
        "أن",
        "الله",
        "عليه",
        "السلام",
        "إلى",
        "أبي",
        "أبو",
        "في",
        "ما",
        "لا",
        "إن",
        "على",
        "هو",
        "هي",
        "ذلك",
        "كان",
        "عند",
        "لم",
        "بل",
        "ثم",
        "أو",
        "لي",
        "له",
        "هذا",
    }
)

# POS tags considered content-bearing for key_terms extraction.
_CONTENT_POS = frozenset({"N", "V", "ADJ"})

# Maximum key terms per language returned by enrich_key_terms.
_MAX_KEY_TERMS = 5


# ---------------------------------------------------------------------------
# 1. Narrator enrichment
# ---------------------------------------------------------------------------


def enrich_narrators(
    arabic_text: str,
    existing_chain: Optional[str],
    narrator_templates: Optional[Dict[str, Any]],
    registry: Optional[Any],
) -> dict:
    """Derive ``isnad_matn`` from existing narrator chain data and the registry.

    Args:
        arabic_text: Full Arabic text of the verse/hadith.
        existing_chain: Pre-existing narrator chain string from ThaqalaynData
            (e.g. the ``thaqalaynSanad`` field). May be ``None``.
        narrator_templates: Narrator templates dict keyed by canonical_id (str)
            or name. Each value has ``name_en``, ``role``, ``known_identity``,
            ``canonical_id``, etc.
        registry: A :class:`~app.narrator_registry.NarratorRegistry` instance
            (or ``None`` if unavailable).

    Returns:
        An ``isnad_matn`` dict with keys ``isnad_ar``, ``matn_ar``,
        ``has_chain``, and ``narrators``.
    """
    empty_result = {
        "isnad_ar": "",
        "matn_ar": arabic_text or "",
        "has_chain": False,
        "narrators": [],
    }

    if not existing_chain or not existing_chain.strip():
        return empty_result

    # Lazy import to avoid circular dependency at module level.
    try:
        from app.narrator_linker import split_narrator_names
        from app.narrator_registry import NarratorRegistry
    except ImportError:
        logger.warning("narrator_linker or narrator_registry not available")
        return empty_result

    isnad_ar = existing_chain.strip()

    # Derive matn_ar: the remainder of arabic_text after the chain.
    matn_ar = arabic_text or ""
    if isnad_ar and arabic_text:
        # Try to find where the chain ends in the full text.
        stripped_chain = strip_tashkeel(isnad_ar)
        stripped_full = strip_tashkeel(arabic_text)
        idx = stripped_full.find(stripped_chain)
        if idx != -1:
            # matn starts after the chain
            end_pos = idx + len(stripped_chain)
            # Map back to original text (approximate — same length after strip).
            matn_ar = arabic_text[end_pos:].strip()

    # Split and resolve names.
    names = split_narrator_names(isnad_ar, use_undiacritized=True)
    if not names:
        return {
            "isnad_ar": isnad_ar,
            "matn_ar": matn_ar,
            "has_chain": True,
            "narrators": [],
        }

    narrators: List[dict] = []
    for position, name_ar in enumerate(names, start=1):
        entry: dict = {
            "name_ar": name_ar.strip(),
            "name_en": "",
            "role": "narrator",
            "position": position,
            "identity_confidence": "ambiguous",
            "ambiguity_note": "Not resolved against canonical narrator registry",
            "known_identity": None,
            "canonical_id": None,
        }

        canonical_id: Optional[int] = None
        if registry is not None and isinstance(registry, NarratorRegistry):
            preceding = [n.strip() for n in names[:position - 1]]
            canonical_id = registry.resolve(name_ar.strip(), preceding_names=preceding)

        if canonical_id is not None:
            entry["canonical_id"] = canonical_id
            entry["identity_confidence"] = "definite"

            # Enrich from registry.
            narrator_data = registry.get_narrator(canonical_id)
            if narrator_data:
                entry["name_en"] = narrator_data.get("canonical_name_en", "")
                entry["role"] = narrator_data.get("role", "narrator")
                entry["known_identity"] = narrator_data.get("known_identity")

        # Override / supplement from templates if available.
        if narrator_templates and canonical_id is not None:
            tmpl = narrator_templates.get(str(canonical_id))
            if tmpl:
                entry["name_en"] = tmpl.get("name_en", entry["name_en"]) or entry["name_en"]
                entry["role"] = tmpl.get("role", entry["role"]) or entry["role"]
                entry["known_identity"] = (
                    tmpl.get("known_identity", entry["known_identity"])
                    or entry["known_identity"]
                )

        narrators.append(entry)

    return {
        "isnad_ar": isnad_ar,
        "matn_ar": matn_ar,
        "has_chain": True,
        "narrators": narrators,
    }


# ---------------------------------------------------------------------------
# 2. Explicit Quran references
# ---------------------------------------------------------------------------


def enrich_explicit_quran_refs(
    english_text: Optional[str],
    arabic_text: Optional[str] = None,
) -> List[dict]:
    """Extract explicit Quran references of the form ``(surah:ayah)`` or ``[surah:ayah]``.

    Searches both the English and Arabic texts for patterns like ``(2:255)``
    or ``[112:1]``.

    Args:
        english_text: English translation text to scan.
        arabic_text: Arabic text to scan (optional second source).

    Returns:
        De-duplicated list of ``{"ref": "surah:ayah", "relationship": "explicit"}``.
    """
    seen: set = set()
    results: List[dict] = []

    for text in (english_text, arabic_text):
        if not text:
            continue
        for match in QURAN_QUOTE_RE.finditer(text):
            surah, ayah = match.group(1), match.group(2)
            ref = f"{surah}:{ayah}"
            if ref not in seen:
                seen.add(ref)
                results.append({"ref": ref, "relationship": "explicit"})

    return results


# ---------------------------------------------------------------------------
# 3. Key phrases
# ---------------------------------------------------------------------------


def enrich_key_phrases(
    arabic_text: Optional[str],
    phrases_dict: Optional[dict],
) -> List[dict]:
    """Match known key phrases against the Arabic text of a verse.

    Uses diacritics-insensitive comparison via :func:`strip_tashkeel`.

    Args:
        arabic_text: The Arabic text to search within.
        phrases_dict: Dictionary with a ``"phrases"`` key containing a list of
            phrase objects, each having ``phrase_ar``, ``phrase_en``, and
            ``category``.

    Returns:
        List of matching phrase dicts (``phrase_ar``, ``phrase_en``,
        ``category``).
    """
    if not arabic_text or not phrases_dict:
        return []

    phrases = phrases_dict.get("phrases")
    if not phrases or not isinstance(phrases, list):
        return []

    stripped_text = strip_tashkeel(arabic_text)
    results: List[dict] = []

    for phrase in phrases:
        if not isinstance(phrase, dict):
            continue
        phrase_ar = phrase.get("phrase_ar", "")
        if not phrase_ar:
            continue

        # Check both diacritized and stripped versions.
        if phrase_ar in arabic_text or strip_tashkeel(phrase_ar) in stripped_text:
            category = phrase.get("category", "")
            if category and category in VALID_PHRASE_CATEGORIES:
                results.append(
                    {
                        "phrase_ar": phrase_ar,
                        "phrase_en": phrase.get("phrase_en", ""),
                        "category": category,
                    }
                )

    return results


# ---------------------------------------------------------------------------
# 4. Key terms
# ---------------------------------------------------------------------------


def enrich_key_terms(
    word_tags: Optional[List[list]],
    word_dictionary: Optional[dict],
) -> Dict[str, Dict[str, str]]:
    """Derive per-language key terms from word_tags and the word dictionary.

    Selects content words (nouns, verbs, adjectives) that are not common
    isnad particles, then looks each up in *word_dictionary*.

    Args:
        word_tags: List of ``[word, POS]`` pairs from v4 pipeline output.
        word_dictionary: Mapping of ``"word|POS"`` to a dict of language
            translations, e.g. ``{"en": "prayer", "ur": "نماز", ...}``.

    Returns:
        Dict keyed by language code, each mapping Arabic term to its
        translation. At most :data:`_MAX_KEY_TERMS` per language.
    """
    if not word_tags or not word_dictionary:
        return {}

    # Collect content words (de-duped, preserving first-occurrence order).
    seen_words: set = set()
    content_words: List[Tuple[str, str]] = []

    for tag_pair in word_tags:
        if not isinstance(tag_pair, (list, tuple)) or len(tag_pair) < 2:
            continue
        word, pos = tag_pair[0], tag_pair[1]
        if pos not in _CONTENT_POS:
            continue
        stripped = strip_tashkeel(word)
        if stripped in _ISNAD_STOP_WORDS:
            continue
        if stripped in seen_words:
            continue
        seen_words.add(stripped)
        content_words.append((word, pos))

    if not content_words:
        return {}

    # Build per-language key terms from dictionary lookups.
    result: Dict[str, Dict[str, str]] = {}

    for word, pos in content_words:
        key = f"{word}|{pos}"
        translations = word_dictionary.get(key)
        if not translations or not isinstance(translations, dict):
            # Try stripped version as fallback.
            key_stripped = f"{strip_tashkeel(word)}|{pos}"
            translations = word_dictionary.get(key_stripped)
            if not translations or not isinstance(translations, dict):
                continue

        for lang, trans in translations.items():
            if lang not in VALID_LANGUAGE_KEYS:
                continue
            if not trans or not isinstance(trans, str):
                continue
            lang_dict = result.setdefault(lang, {})
            if len(lang_dict) >= _MAX_KEY_TERMS:
                continue
            if word not in lang_dict:
                lang_dict[word] = trans

    return result


# ---------------------------------------------------------------------------
# 5. Diacritics status
# ---------------------------------------------------------------------------


def enrich_diacritics_status(
    original_arabic: Optional[str],
    diacritized_text: Optional[str],
) -> str:
    """Determine the diacritization status by comparing original and diacritized text.

    Args:
        original_arabic: The original Arabic text (may lack diacritics).
        diacritized_text: The fully diacritized Arabic text from Phase 1.

    Returns:
        One of ``"validated"``, ``"added"``, or ``"corrected"``.
    """
    if not original_arabic or not diacritized_text:
        return "added"

    if original_arabic == diacritized_text:
        return "validated"

    # Check if the original has any tashkeel at all.
    stripped_original = strip_tashkeel(original_arabic)
    if stripped_original == original_arabic:
        # No diacritics were present in the original.
        return "added"

    return "corrected"


# ---------------------------------------------------------------------------
# 6. Topics, tags, and content_type
# ---------------------------------------------------------------------------


def enrich_topics_and_tags(
    english_text: Optional[str],
    chapter_title: Optional[str],
    book_name: Optional[str],
    taxonomy: Optional[dict],
) -> Tuple[List[str], List[str], str]:
    """Derive topics, tags, and content_type from text + taxonomy heuristics.

    Args:
        english_text: English translation text.
        chapter_title: Title of the containing chapter.
        book_name: Slug of the book (e.g. ``"al-kafi"``, ``"quran"``).
        taxonomy: The ``tag_topic_mapping.json`` data with keys
            ``keyword_to_topics``, ``keyword_to_tags``,
            ``chapter_to_content_type``, ``default_content_type_by_book``,
            and ``tag_to_topics``.

    Returns:
        Tuple of ``(topics, tags, content_type)`` where *topics* and *tags*
        are lists of up to 3 validated strings, and *content_type* is a single
        validated string.
    """
    book_name = book_name or ""

    # Defaults when taxonomy is unavailable.
    if taxonomy is None:
        default_ct = _default_content_type(book_name)
        return ([], [], default_ct)

    keyword_to_topics: dict = taxonomy.get("keyword_to_topics", {})
    keyword_to_tags: dict = taxonomy.get("keyword_to_tags", {})
    chapter_to_ct: dict = taxonomy.get("chapter_to_content_type", {})
    default_ct_by_book: dict = taxonomy.get("default_content_type_by_book", {})

    # Build a single searchable blob (lowercased).
    blob = " ".join(
        part.lower()
        for part in (english_text or "", chapter_title or "")
        if part
    )

    # --- Topics ---
    topic_counts: Counter = Counter()
    for keyword, topics in keyword_to_topics.items():
        if not isinstance(keyword, str):
            continue
        if keyword.lower() in blob:
            for t in topics:
                if isinstance(t, str):
                    topic_counts[t] += 1

    # Take top 3 validated topics.
    topics: List[str] = []
    for t, _count in topic_counts.most_common():
        if VALID_TOPICS and t not in VALID_TOPICS:
            continue
        topics.append(t)
        if len(topics) >= 3:
            break

    # --- Tags ---
    tag_counts: Counter = Counter()
    for keyword, tags in keyword_to_tags.items():
        if not isinstance(keyword, str):
            continue
        if keyword.lower() in blob:
            for tag in tags:
                if isinstance(tag, str):
                    tag_counts[tag] += 1

    tags: List[str] = []
    for tag, _count in tag_counts.most_common():
        if tag not in VALID_TAGS:
            continue
        tags.append(tag)
        if len(tags) >= 3:
            break

    # --- Content type ---
    content_type = ""
    if chapter_title:
        ct_lower = chapter_title.lower()
        for ct_key, ct_val in chapter_to_ct.items():
            if isinstance(ct_key, str) and ct_key.lower() in ct_lower:
                content_type = ct_val
                break

    if not content_type:
        content_type = default_ct_by_book.get(book_name, "")

    if not content_type or content_type not in VALID_CONTENT_TYPES:
        content_type = _default_content_type(book_name)

    return (topics, tags, content_type)


def _default_content_type(book_name: str) -> str:
    """Return a sensible default content_type based on book name."""
    if book_name == "quran":
        return "quranic_commentary"
    return "narrative"


# ---------------------------------------------------------------------------
# 7. Orchestrator
# ---------------------------------------------------------------------------


def programmatic_enrich(
    phase1_result: dict,
    request: Any,
    narrator_templates: Optional[Dict[str, Any]] = None,
    registry: Optional[Any] = None,
    word_dict: Optional[dict] = None,
    phrases_dict: Optional[dict] = None,
    taxonomy: Optional[dict] = None,
) -> dict:
    """Orchestrate all Phase 2 programmatic enrichments.

    Takes the Phase 1 AI-generated result and enriches it with deterministic,
    data-driven fields that do not require further AI calls.

    Args:
        phase1_result: Dict from Phase 1 containing at minimum
            ``diacritized_text``, ``diacritics_changes``, ``word_tags``,
            ``chunks``, ``translations`` (with ``en``), ``related_quran``
            (thematic only), and ``isnad_matn`` (partial — ``isnad_ar``,
            ``matn_ar``, ``has_chain`` but no ``narrators``).
        request: An object (or duck-typed namespace) with attributes
            ``arabic_text``, ``english_text``, ``book_name``,
            ``chapter_title``, and ``existing_narrator_chain``.
        narrator_templates: Narrator templates dict (keyed by canonical ID
            string).
        registry: A :class:`~app.narrator_registry.NarratorRegistry` instance.
        word_dict: Word dictionary mapping ``"word|POS"`` to per-language
            translations.
        phrases_dict: Key phrases dictionary (``{"phrases": [...]}``) for
            phrase matching.
        taxonomy: Tag/topic mapping from ``tag_topic_mapping.json``.

    Returns:
        A complete result dict with all 12 pipeline fields, ready for
        ``validate_result()``.
    """
    if phase1_result is None:
        phase1_result = {}

    # Extract request attributes defensively.
    arabic_text = getattr(request, "arabic_text", "") or ""
    english_text = getattr(request, "english_text", "") or ""
    book_name = getattr(request, "book_name", "") or ""
    chapter_title = getattr(request, "chapter_title", "") or ""
    existing_chain = getattr(request, "existing_narrator_chain", None)

    # Start with a copy of Phase 1 output.
    result = dict(phase1_result)

    # --- Narrators (merge into isnad_matn) ---
    narrator_enrichment = enrich_narrators(
        arabic_text, existing_chain, narrator_templates, registry
    )
    existing_isnad = result.get("isnad_matn") or {}
    merged_isnad = {
        "isnad_ar": existing_isnad.get("isnad_ar") or narrator_enrichment["isnad_ar"],
        "matn_ar": existing_isnad.get("matn_ar") or narrator_enrichment["matn_ar"],
        "has_chain": existing_isnad.get("has_chain", False)
        or narrator_enrichment["has_chain"],
        "narrators": narrator_enrichment["narrators"],
    }
    result["isnad_matn"] = merged_isnad

    # --- Explicit Quran references (merge with Phase 1 thematic refs) ---
    explicit_refs = enrich_explicit_quran_refs(english_text, arabic_text)
    existing_quran = result.get("related_quran") or []
    # De-duplicate across explicit and thematic.
    seen_refs: set = set()
    merged_quran: List[dict] = []
    for ref_obj in explicit_refs + existing_quran:
        ref_key = ref_obj.get("ref", "")
        if ref_key and ref_key not in seen_refs:
            seen_refs.add(ref_key)
            merged_quran.append(ref_obj)
    result["related_quran"] = merged_quran

    # --- Topics, tags, content_type ---
    topics, tags, content_type = enrich_topics_and_tags(
        english_text, chapter_title, book_name, taxonomy
    )
    if "topics" not in result or not result["topics"]:
        result["topics"] = topics
    if "tags" not in result or not result["tags"]:
        result["tags"] = tags
    if "content_type" not in result or not result["content_type"]:
        result["content_type"] = content_type

    # --- Key phrases ---
    matched_phrases = enrich_key_phrases(arabic_text, phrases_dict)
    if "key_phrases" not in result or not result["key_phrases"]:
        result["key_phrases"] = matched_phrases

    # --- Key terms (merge into translations.*.key_terms) ---
    word_tags = result.get("word_tags")
    enriched_terms = enrich_key_terms(word_tags, word_dict)
    if enriched_terms:
        translations = result.setdefault("translations", {})
        for lang, terms_dict in enriched_terms.items():
            lang_data = translations.setdefault(lang, {})
            if isinstance(lang_data, dict):
                existing_kt = lang_data.get("key_terms")
                if not existing_kt or not isinstance(existing_kt, dict):
                    lang_data["key_terms"] = terms_dict
                else:
                    # Merge — Phase 2 fills gaps, doesn't overwrite.
                    for ar_term, trans in terms_dict.items():
                        if ar_term not in existing_kt:
                            existing_kt[ar_term] = trans

    # --- Diacritics status ---
    diacritized = result.get("diacritized_text", "")
    if "diacritics_status" not in result or not result["diacritics_status"]:
        result["diacritics_status"] = enrich_diacritics_status(
            arabic_text, diacritized
        )

    # Ensure all 12 fields are present with sensible defaults.
    result.setdefault("diacritized_text", arabic_text)
    result.setdefault("diacritics_status", "added")
    result.setdefault("diacritics_changes", [])
    result.setdefault("word_tags", [])
    result.setdefault("isnad_matn", merged_isnad)
    result.setdefault("translations", {})
    result.setdefault("chunks", [])
    result.setdefault("related_quran", [])
    result.setdefault("seo_question", {})
    result.setdefault("key_terms", {})
    result.setdefault("topics", [])
    result.setdefault("key_phrases", [])
    result.setdefault("tags", [])
    result.setdefault("content_type", _default_content_type(book_name))

    # Fix: ensure translations.*.key_terms exists (required by validate_result).
    for lang_key, lang_data in result.get("translations", {}).items():
        if isinstance(lang_data, dict):
            lang_data.setdefault("key_terms", {})

    # Fix: reconstruct chunks[].arabic_text from word_tags if missing.
    word_tags = result.get("word_tags", [])
    for chunk in result.get("chunks", []):
        if "arabic_text" not in chunk and word_tags:
            ws = chunk.get("word_start", 0)
            we = chunk.get("word_end", 0)
            chunk["arabic_text"] = " ".join(
                wt[0] if isinstance(wt, (list, tuple)) else str(wt)
                for wt in word_tags[ws:we]
            )

    # Fix: diacritics_status consistency — if changes exist, can't be "added".
    status = result.get("diacritics_status", "")
    changes = result.get("diacritics_changes", [])
    if status == "added" and isinstance(changes, list) and len(changes) > 0:
        result["diacritics_status"] = "corrected"

    return result
