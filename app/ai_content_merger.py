"""Merge AI-generated content into the static JSON data files.

This module runs as a post-processing step after all other data generation.
It reads validated AI response files from SOURCE_DATA_DIR/ai-content/{subdir}/responses/,
transforms them into a lean zero-duplication format, and injects them into the
generated JSON files in DESTINATION_DIR.

Zero duplication means:
- diacritized_text NOT stored (reconstruct from word_analysis[].word)
- chunks[].arabic_text NOT stored (reconstruct from word_analysis[word_start:word_end])
- translations[lang].text NOT stored (reconstruct by concatenating chunks[].translations[lang])
- NO AI text in verse.translations (Angular reads from ai.chunks directly)
"""

import json
import logging
import os
from typing import Dict, List, Optional

from app.config import (
    AI_RESPONSES_DIR,
    DEFAULT_DESTINATION_DIR,
    JSON_ENCODING,
    JSON_ENSURE_ASCII,
    JSON_INDENT,
    SOURCE_DATA_DIR,
)
from app.narrator_registry import NarratorRegistry

logger = logging.getLogger(__name__)

AI_LANGUAGES = ["en", "ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"]

AI_TRANSLATION_ENTRIES = {
    "en.ai": {"id": "en.ai", "lang": "en", "name": "AI Translation", "source": "ai", "model": "claude-opus-4-6"},
    "ur.ai": {"id": "ur.ai", "lang": "ur", "name": "\u0627\u06d2 \u0622\u0626\u06cc \u062a\u0631\u062c\u0645\u06c1", "source": "ai", "model": "claude-opus-4-6"},
    "tr.ai": {"id": "tr.ai", "lang": "tr", "name": "Yapay Zeka \u00c7evirisi", "source": "ai", "model": "claude-opus-4-6"},
    "fa.ai": {"id": "fa.ai", "lang": "fa", "name": "\u062a\u0631\u062c\u0645\u0647 \u0647\u0648\u0634 \u0645\u0635\u0646\u0648\u0639\u06cc", "source": "ai", "model": "claude-opus-4-6"},
    "id.ai": {"id": "id.ai", "lang": "id", "name": "Terjemahan AI", "source": "ai", "model": "claude-opus-4-6"},
    "bn.ai": {"id": "bn.ai", "lang": "bn", "name": "\u098f\u0986\u0987 \u0985\u09a8\u09c1\u09ac\u09be\u09a6", "source": "ai", "model": "claude-opus-4-6"},
    "es.ai": {"id": "es.ai", "lang": "es", "name": "Traducci\u00f3n IA", "source": "ai", "model": "claude-opus-4-6"},
    "fr.ai": {"id": "fr.ai", "lang": "fr", "name": "Traduction IA", "source": "ai", "model": "claude-opus-4-6"},
    "de.ai": {"id": "de.ai", "lang": "de", "name": "KI-\u00dcbersetzung", "source": "ai", "model": "claude-opus-4-6"},
    "ru.ai": {"id": "ru.ai", "lang": "ru", "name": "\u0418\u0418-\u043f\u0435\u0440\u0435\u0432\u043e\u0434", "source": "ai", "model": "claude-opus-4-6"},
    "zh.ai": {"id": "zh.ai", "lang": "zh", "name": "AI\u7ffb\u8bd1", "source": "ai", "model": "claude-opus-4-6"},
}

AI_TRANSLATION_IDS = sorted(AI_TRANSLATION_ENTRIES.keys())


def load_ai_responses(responses_dir: Optional[str] = None) -> Dict[str, dict]:
    """Load all AI response files from the responses directory.

    Returns dict mapping verse_path -> {"ai_attribution": ..., "result": ...}.
    Skips malformed files with warnings.
    """
    if responses_dir is None:
        responses_dir = AI_RESPONSES_DIR

    lookup = {}
    if not os.path.isdir(responses_dir):
        logger.info("AI responses directory not found: %s — skipping AI merge", responses_dir)
        return lookup

    for filename in sorted(os.listdir(responses_dir)):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(responses_dir, filename)
        try:
            with open(filepath, "r", encoding=JSON_ENCODING) as f:
                wrapper = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Skipping malformed AI response file %s: %s", filename, e)
            continue

        verse_path = wrapper.get("verse_path")
        if not verse_path or "result" not in wrapper:
            logger.warning("Skipping AI response file %s: missing verse_path or result", filename)
            continue

        attribution = wrapper.get("ai_attribution", {})
        lookup[verse_path] = {
            "ai_attribution": attribution,
            "result": wrapper["result"],
        }

    logger.info("Loaded %d AI response files from %s", len(lookup), responses_dir)
    return lookup


def build_lean_ai_content(result: dict, attribution: dict) -> dict:
    """Restructure AI pipeline result into lean zero-duplication format.

    Transformations:
    - v3 (word_analysis present): keep word_analysis. Drop diacritized_text
      and chunks[].arabic_text — both reconstruct from word_analysis ranges.
    - v4 (no word_analysis): keep chunks[].arabic_text (Phase 1 LLM
      canonical, must be preserved per persistence rule). Drop
      diacritized_text and word_tags — both are Phase 2-derived from
      chunks and have no consumer (Angular reads chunks[].arabic_text
      directly; word_tags has placeholder POS and no UI consumer).
    - Remove: isnad_matn.isnad_ar and isnad_matn.matn_ar (unused in UI;
      reconstructable from chunks where chunk_type=="isnad")
    - Remove: diacritics_changes (unused in UI)
    - Remove: similar_content_hints (unused in UI, no longer generated in v4)
    - Dissolve translations[lang] into: summaries[lang], key_terms[lang], seo_questions[lang]
    - Remove: top-level translations dict entirely
    - Add: ai_attribution

    Returns the lean ai dict ready for injection into a verse.
    """
    ai = {}

    # Add attribution
    if isinstance(attribution, str):
        # Legacy format: just the model name as a string
        clean_attribution = {"model": attribution}
    elif isinstance(attribution, dict):
        clean_attribution = {
            k: v for k, v in attribution.items()
            if k in ("model", "generated_date", "pipeline_version")
        }
    else:
        clean_attribution = {}
    if clean_attribution:
        ai["ai_attribution"] = clean_attribution

    # Copy word_analysis as-is (v3 format: per-word objects with translations)
    has_word_analysis = "word_analysis" in result
    if has_word_analysis:
        ai["word_analysis"] = result["word_analysis"]

    # v4 (no word_analysis): word_tags and diacritized_text are Phase 2
    # derived. Both reconstruct from chunks[].arabic_text. Neither has a
    # production consumer that wouldn't be better served by reading chunks
    # directly. Drop both. v3 already drops diacritized_text below, and v3
    # never had word_tags.

    # Copy chunks — strip arabic_text only for v3 (it can be reconstructed
    # from word_analysis via word_start/word_end). For v4, chunks[].arabic_text
    # IS the LLM canonical (Phase 1 produces it directly) and must be kept.
    if "chunks" in result:
        lean_chunks = []
        for chunk in result["chunks"]:
            if has_word_analysis:
                lean_chunk = {k: v for k, v in chunk.items() if k != "arabic_text"}
            else:
                lean_chunk = dict(chunk)
            lean_chunks.append(lean_chunk)
        ai["chunks"] = lean_chunks

    # Copy isnad_matn, stripping isnad_ar and matn_ar (unused in UI)
    if "isnad_matn" in result:
        ai["isnad_matn"] = {
            k: v for k, v in result["isnad_matn"].items()
            if k not in ("isnad_ar", "matn_ar")
        }

    # Copy related_quran as-is
    if "related_quran" in result:
        ai["related_quran"] = result["related_quran"]

    # Copy topics as-is
    if "topics" in result:
        ai["topics"] = result["topics"]

    # Copy tags as-is
    if "tags" in result:
        ai["tags"] = result["tags"]

    # Copy content_type as-is
    if "content_type" in result:
        ai["content_type"] = result["content_type"]

    # Copy key_phrases as-is
    if "key_phrases" in result:
        ai["key_phrases"] = result["key_phrases"]

    # NOTE: similar_content_hints intentionally NOT copied (unused in UI, removed in v4)

    # Dissolve translations into summaries, key_terms, seo_questions
    translations = result.get("translations", {})
    if translations:
        summaries = {}
        key_terms = {}
        seo_questions = {}
        for lang, entry in translations.items():
            if isinstance(entry, dict):
                if "summary" in entry:
                    summaries[lang] = entry["summary"]
                if "key_terms" in entry:
                    key_terms[lang] = entry["key_terms"]
                if "seo_question" in entry:
                    seo_questions[lang] = entry["seo_question"]
        if summaries:
            ai["summaries"] = summaries
        if key_terms:
            ai["key_terms"] = key_terms
        if seo_questions:
            ai["seo_questions"] = seo_questions

    # Copy diacritics_status only (diacritics_changes is unused in UI)
    if "diacritics_status" in result:
        ai["diacritics_status"] = result["diacritics_status"]

    # NOTE: diacritized_text and word_tags intentionally NOT copied — both
    # are Phase 2-derived from chunks[].arabic_text. Angular consumers that
    # historically read diacritized_text need a chunks-based fallback (#4).
    # NOTE: diacritics_changes intentionally NOT copied (unused in UI)

    return ai


def split_ai_per_lang(ai: dict) -> tuple:
    """Split a lean `ai` dict into (base_ai, {lang: per_lang_ai}).

    Base keeps language-agnostic fields and adds two registries the loader
    needs without paying for the per-language payload:
    - `available_languages`: sorted list of langs that have any per-lang
      content (summary/seo_question/key_terms/chunk translation).
    - `key_terms_keys`: canonical Arabic-term ordering across langs; sister
      files' `key_terms` use these keys.

    Per-lang dicts carry: summary, seo_question, key_terms (keyed by the
    same Arabic terms as base.key_terms_keys), and chunks (index-aligned
    with base.chunks, one entry per chunk; each entry holds `translation`
    when present for that lang).

    Returns (base_ai, per_lang). per_lang is empty if no per-language
    fields were found.
    """
    summaries = ai.get("summaries") or {}
    seo_questions = ai.get("seo_questions") or {}
    key_terms = ai.get("key_terms") or {}
    chunks = ai.get("chunks") or []
    word_analysis = ai.get("word_analysis") or []

    langs = set()
    if isinstance(summaries, dict):
        langs.update(summaries.keys())
    if isinstance(seo_questions, dict):
        langs.update(seo_questions.keys())
    if isinstance(key_terms, dict):
        langs.update(key_terms.keys())
    for chunk in chunks:
        if isinstance(chunk, dict):
            chunk_translations = chunk.get("translations") or {}
            if isinstance(chunk_translations, dict):
                langs.update(chunk_translations.keys())
    for entry in word_analysis:
        if isinstance(entry, dict):
            entry_translation = entry.get("translation") or {}
            if isinstance(entry_translation, dict):
                langs.update(entry_translation.keys())
    available_languages = sorted(l for l in langs if l in AI_LANGUAGES)

    seen_keys: List[str] = []
    seen_set = set()
    if isinstance(key_terms, dict):
        for lang in available_languages:
            lang_dict = key_terms.get(lang)
            if isinstance(lang_dict, dict):
                for term in lang_dict.keys():
                    if term not in seen_set:
                        seen_keys.append(term)
                        seen_set.add(term)

    base_ai: dict = {}
    for k, v in ai.items():
        if k in ("summaries", "seo_questions", "key_terms"):
            continue
        if k == "chunks":
            stripped_chunks = []
            for chunk in v or []:
                if isinstance(chunk, dict):
                    stripped_chunks.append(
                        {ck: cv for ck, cv in chunk.items() if ck != "translations"}
                    )
                else:
                    stripped_chunks.append(chunk)
            base_ai["chunks"] = stripped_chunks
        elif k == "word_analysis":
            stripped_entries = []
            for entry in v or []:
                if isinstance(entry, dict):
                    stripped_entries.append(
                        {ek: ev for ek, ev in entry.items() if ek != "translation"}
                    )
                else:
                    stripped_entries.append(entry)
            base_ai["word_analysis"] = stripped_entries
        else:
            base_ai[k] = v
    if available_languages:
        base_ai["available_languages"] = available_languages
    if seen_keys:
        base_ai["key_terms_keys"] = seen_keys

    per_lang: Dict[str, dict] = {}
    for lang in available_languages:
        entry: dict = {}
        if isinstance(summaries, dict) and lang in summaries:
            entry["summary"] = summaries[lang]
        if isinstance(seo_questions, dict) and lang in seo_questions:
            entry["seo_question"] = seo_questions[lang]
        lang_terms = key_terms.get(lang) if isinstance(key_terms, dict) else None
        if isinstance(lang_terms, dict) and lang_terms:
            entry["key_terms"] = lang_terms
        lang_chunks: list = []
        any_translation = False
        for chunk in chunks:
            if isinstance(chunk, dict):
                chunk_translations = chunk.get("translations") or {}
                if isinstance(chunk_translations, dict) and lang in chunk_translations:
                    lang_chunks.append(chunk_translations[lang])
                    any_translation = True
                    continue
            lang_chunks.append(None)
        if any_translation:
            entry["chunks"] = lang_chunks
        lang_words: list = []
        any_word_translation = False
        for word_entry in word_analysis:
            if isinstance(word_entry, dict):
                word_translation = word_entry.get("translation") or {}
                if isinstance(word_translation, dict) and lang in word_translation:
                    lang_words.append(word_translation[lang])
                    any_word_translation = True
                    continue
            lang_words.append(None)
        if any_word_translation:
            entry["word_analysis"] = lang_words
        if entry:
            per_lang[lang] = entry

    return base_ai, per_lang


def _write_verse_detail_split(file_path: str, doc: dict, per_lang: Dict[str, dict]) -> None:
    """Write the base verse_detail file + per-language sister files.

    Cleans up stale sisters: removes any `{base}.{lang}.json` whose lang
    is in `AI_LANGUAGES` but not in the new per_lang map. This keeps the
    on-disk set in sync when a language is dropped from a verse.
    """
    with open(file_path, "w", encoding=JSON_ENCODING) as f:
        json.dump(doc, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT, sort_keys=True)

    if not file_path.endswith(".json"):
        return
    base_no_ext = file_path[:-5]
    verse_index = doc.get("index", "")
    verse_path = f"/books/{verse_index}" if verse_index else ""

    for lang, content in per_lang.items():
        sister_doc = {
            "ai": content,
            "lang": lang,
            "path": verse_path,
        }
        sister_path = f"{base_no_ext}.{lang}.json"
        with open(sister_path, "w", encoding=JSON_ENCODING) as f:
            json.dump(sister_doc, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT, sort_keys=True)

    for lang in AI_LANGUAGES:
        if lang in per_lang:
            continue
        stale_path = f"{base_no_ext}.{lang}.json"
        try:
            os.remove(stale_path)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning("Could not remove stale sister %s: %s", stale_path, e)


def rebuild_narrator_chain_parts_from_ai(verse: dict, ai_result: dict) -> bool:
    """Rebuild verse['narrator_chain']['parts'] using AI-side narrator data.

    Why: the legacy parse pipeline (process_all_narrators) sets parts using
    regex extraction on the raw chain text. Quality depends on the chain
    text's diacritization and on the regex's ability to identify narrator
    boundaries. Where it fails, the entire chain ends up as a single plain
    part (no clickability).

    The AI pipeline's Phase 2 enrichment produces a structured narrators[]
    array with each narrator's name_ar + canonical_id (re-resolved by the
    merger via NarratorRegistry just before this call). That's strictly
    higher-quality structural information than the regex can produce.

    This function re-derives parts from that structured data:
    1. Iterate AI narrators in position order
    2. For each, find name_ar in the remaining chain text and split there
    3. Emit a plain part for the "before" segment + a narrator part with
       the canonical_id link

    For existing data where name_ar still has a leading chain verb baked
    in (pre-D-fix responses), strip the verb prefix before search/split so
    the verb stays in the surrounding plain segment rather than the
    clickable narrator text.

    Falls back gracefully: if any narrator's name_ar can't be located in
    the chain, leave the existing parts unchanged. This covers AI/source
    text mismatches, partial chain extractions, etc.

    Returns True if parts were rebuilt, False otherwise.
    """
    isnad_matn = ai_result.get("isnad_matn", {})
    if not isinstance(isnad_matn, dict):
        return False
    if not isnad_matn.get("has_chain"):
        return False
    isnad_ar = (isnad_matn.get("isnad_ar") or "").strip()
    # isnad_ar is a Phase 2-derived field; if it was stripped from the
    # response, rebuild it from chunks where chunk_type == "isnad". The
    # chunks themselves are LLM-original (Phase 1) so this reconstruction
    # is byte-exact when the chunk types are correct.
    if not isnad_ar:
        chunks = ai_result.get("chunks") or []
        isnad_ar = " ".join(
            (c.get("arabic_text") or "").strip()
            for c in chunks
            if isinstance(c, dict) and c.get("chunk_type") == "isnad"
        ).strip()
    if not isnad_ar:
        return False
    narrators = isnad_matn.get("narrators") or []
    if not narrators:
        return False

    # If verse doesn't have a narrator_chain block yet, set up the structure.
    chain = verse.get("narrator_chain") or {}
    if not isinstance(chain, dict):
        return False

    # Lazy-import to avoid circular imports + reuse the same verb-strip
    # logic as the registry lookup.
    from app.narrator_registry import _LEADING_VERB_PREFIXES
    from app.arabic_normalization import normalize_arabic

    def _strip_leading_verb_preserve_diacritics(name: str) -> str:
        """Strip a leading chain verb from a (potentially diacritized) name.

        We can't use canonical_lookup_key here — that returns the
        normalized form, which won't be findable in the original
        diacritized chain text. Instead, normalize just enough to detect
        the verb prefix, then chop the corresponding number of chars from
        the original.
        """
        if not name:
            return name
        normalized = normalize_arabic(name)
        for prefix in _LEADING_VERB_PREFIXES:
            if normalized.startswith(prefix):
                # Walk the original char-by-char in lockstep with the
                # normalized; stop when normalized has consumed `len(prefix)`
                # characters.
                target = len(prefix)
                consumed = 0
                idx = 0
                while idx < len(name) and consumed < target:
                    nch = normalize_arabic(name[idx])
                    if nch:
                        consumed += len(nch)
                    idx += 1
                stripped = name[idx:].lstrip()
                if stripped:
                    return stripped
        return name

    # Build new parts list.
    new_parts: List[dict] = []
    remaining = isnad_ar

    # Validate first: make sure every narrator name can be found in the chain.
    # If any can't, bail and leave existing parts untouched.
    sentinel = remaining
    cleaned_names: List[tuple] = []  # (clean_name, canonical_id)
    for n in narrators:
        if not isinstance(n, dict):
            return False
        name_ar = (n.get("name_ar") or "").strip()
        if not name_ar:
            return False
        clean_name = _strip_leading_verb_preserve_diacritics(name_ar).strip()
        if not clean_name:
            return False
        # Try the cleaned name first; fall back to the original surface form
        # in case the verb-strip removed too much (defense in depth).
        if clean_name in sentinel:
            idx = sentinel.find(clean_name)
            sentinel = sentinel[idx + len(clean_name):]
            cleaned_names.append((clean_name, n.get("canonical_id")))
        elif name_ar in sentinel:
            idx = sentinel.find(name_ar)
            sentinel = sentinel[idx + len(name_ar):]
            cleaned_names.append((name_ar, n.get("canonical_id")))
        else:
            # Can't locate this narrator in the chain — bail.
            return False

    # All narrators located in order; build the parts list.
    for clean_name, canonical_id in cleaned_names:
        idx = remaining.find(clean_name)
        if idx == -1:
            # Should not happen given the validation above, but be defensive.
            return False
        before = remaining[:idx]
        if before:
            new_parts.append({"kind": "plain", "text": before})
        if canonical_id is not None:
            new_parts.append({
                "kind": "narrator",
                "path": f"/people/narrators/{canonical_id}",
                "text": clean_name,
            })
        else:
            # No canonical ID — render as plain (no clickable link)
            new_parts.append({"kind": "plain", "text": clean_name})
        remaining = remaining[idx + len(clean_name):]

    # Anything left after the last narrator (e.g. " قَالَ")
    if remaining:
        new_parts.append({"kind": "plain", "text": remaining})

    chain["parts"] = new_parts
    # Drop redundant text — same optimization the legacy path does
    if "text" in chain:
        chain["text"] = None
    verse["narrator_chain"] = chain
    return True


def merge_ai_into_verse(verse: dict, ai_lookup: Dict[str, dict]) -> bool:
    """If verse's path matches ai_lookup, set verse['ai'] to lean content
    AND rebuild verse['narrator_chain']['parts'] from AI narrators.

    Returns True if AI content was merged.
    """
    verse_path = verse.get("path")
    if not verse_path:
        return False

    ai_data = ai_lookup.get(verse_path)
    if not ai_data:
        return False

    lean_ai = build_lean_ai_content(ai_data["result"], ai_data["ai_attribution"])
    verse["ai"] = lean_ai

    # Rebuild narrator_chain.parts from the (canonical-id-resolved) AI data.
    # This overrides the legacy parse pipeline's regex-based linking with
    # the higher-quality structured AI output. Safe to call on any verse —
    # silently no-ops if AI lacks a chain or narrator names can't be located.
    rebuild_narrator_chain_parts_from_ai(verse, ai_data["result"])

    return True


def _collect_ai_translation_ids(verses: list, ai_lookup: Dict[str, dict]) -> List[str]:
    """Determine which AI translation IDs should be added to verse_translations.

    Returns sorted list of AI translation IDs that have content in at least one verse.
    """
    ai_langs_present = set()
    for verse in verses:
        verse_path = verse.get("path", "")
        ai_data = ai_lookup.get(verse_path)
        if not ai_data:
            continue
        chunks = ai_data["result"].get("chunks", [])
        for chunk in chunks:
            chunk_translations = chunk.get("translations", {})
            ai_langs_present.update(chunk_translations.keys())

    return sorted(f"{lang}.ai" for lang in ai_langs_present if lang in AI_LANGUAGES)


def merge_ai_into_file(file_path: str, ai_lookup: Dict[str, dict]) -> int:
    """Read a JSON data file, merge AI into verses, re-write if changed.

    Handles different file kinds:
    - verse_list: iterate data.verses[], update data.verse_translations
    - verse_detail: merge into data directly (single verse)
    - chapter_list: skip (no verses)

    Returns the number of verses merged.
    """
    try:
        with open(file_path, "r", encoding=JSON_ENCODING) as f:
            doc = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not read %s: %s", file_path, e)
        return 0

    kind = doc.get("kind", "")
    data = doc.get("data", {})
    merge_count = 0

    if kind == "verse_list":
        verses = data.get("verses", [])
        if verses:
            # Legacy format: inline verses
            for verse in verses:
                if merge_ai_into_verse(verse, ai_lookup):
                    merge_count += 1

            if merge_count > 0:
                ai_ids = _collect_ai_translation_ids(verses, ai_lookup)
                if ai_ids:
                    existing = data.get("verse_translations", [])
                    for ai_id in ai_ids:
                        if ai_id not in existing:
                            existing.append(ai_id)
                    data["verse_translations"] = existing
        else:
            # Shell format: verse_refs with paths, no inline verses to merge.
            # Update verse_translations by checking verse_refs paths against ai_lookup.
            verse_refs = data.get("verse_refs", [])
            ref_verses = [{"path": ref["path"]} for ref in verse_refs if "path" in ref]
            if ref_verses:
                ai_ids = _collect_ai_translation_ids(ref_verses, ai_lookup)
                if ai_ids:
                    existing = data.get("verse_translations", [])
                    for ai_id in ai_ids:
                        if ai_id not in existing:
                            existing.append(ai_id)
                    data["verse_translations"] = existing
                    merge_count = 1  # signal that file was modified

    elif kind == "verse_detail":
        # Single verse wrapped in data
        verse = data.get("verse", data)
        if merge_ai_into_verse(verse, ai_lookup):
            merge_count += 1
            # Update verse_translations to include AI IDs
            ai_ids = _collect_ai_translation_ids([verse], ai_lookup)
            if ai_ids:
                existing = data.get("verse_translations", [])
                for ai_id in ai_ids:
                    if ai_id not in existing:
                        existing.append(ai_id)
                data["verse_translations"] = existing
            # Split per-language AI fields into sister files; base file
            # carries language-agnostic content + available_languages
            # + key_terms_keys registries. See PER_LANGUAGE_VERSE_SPLIT.md.
            ai_block = verse.get("ai") or {}
            base_ai, per_lang = split_ai_per_lang(ai_block)
            verse["ai"] = base_ai
            _write_verse_detail_split(file_path, doc, per_lang)
            return merge_count

    elif kind == "chapter_list":
        # No verses to merge
        return 0

    else:
        # Unknown kind — try both patterns
        verses = data.get("verses", [])
        if verses:
            for verse in verses:
                if merge_ai_into_verse(verse, ai_lookup):
                    merge_count += 1

    if merge_count > 0:
        with open(file_path, "w", encoding=JSON_ENCODING) as f:
            json.dump(doc, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT, sort_keys=True)

    return merge_count


def _walk_complete_book(node: dict, ai_lookup: Dict[str, dict]) -> int:
    """Recursively walk a complete book structure, merging AI into verses.

    Complete books have nested chapters containing either more chapters or verses.
    """
    merge_count = 0

    # Check for verses at this level
    verses = node.get("verses", [])
    for verse in verses:
        if merge_ai_into_verse(verse, ai_lookup):
            merge_count += 1

    # Update verse_translations at this level if needed
    if merge_count > 0 and "verse_translations" in node:
        ai_ids = _collect_ai_translation_ids(verses, ai_lookup)
        for ai_id in ai_ids:
            if ai_id not in node["verse_translations"]:
                node["verse_translations"].append(ai_id)

    # Recurse into sub-chapters
    chapters = node.get("chapters", [])
    for chapter in chapters:
        merge_count += _walk_complete_book(chapter, ai_lookup)

    return merge_count


def merge_ai_into_complete_file(file_path: str, ai_lookup: Dict[str, dict]) -> int:
    """Merge AI content into a complete book file (e.g., complete/al-kafi.json).

    These files have a recursive chapter structure inside data.
    """
    try:
        with open(file_path, "r", encoding=JSON_ENCODING) as f:
            doc = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not read complete file %s: %s", file_path, e)
        return 0

    data = doc.get("data", {})
    merge_count = _walk_complete_book(data, ai_lookup)

    if merge_count > 0:
        with open(file_path, "w", encoding=JSON_ENCODING) as f:
            json.dump(doc, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT, sort_keys=True)

    return merge_count


def update_translations_index(dest_dir: Optional[str] = None):
    """Add AI translation entries to index/translations.json.

    Adds 11 entries (en.ai, ur.ai, etc.) with source='ai' metadata.
    Preserves all existing entries.
    """
    if dest_dir is None:
        dest_dir = os.environ.get("DESTINATION_DIR", DEFAULT_DESTINATION_DIR)

    translations_path = os.path.join(dest_dir, "index", "translations.json")

    if not os.path.isfile(translations_path):
        logger.warning("translations.json not found at %s — skipping update", translations_path)
        return

    with open(translations_path, "r", encoding=JSON_ENCODING) as f:
        translations = json.load(f)

    added = 0
    for tid, entry in AI_TRANSLATION_ENTRIES.items():
        if tid not in translations:
            translations[tid] = entry
            added += 1

    if added > 0:
        with open(translations_path, "w", encoding=JSON_ENCODING) as f:
            json.dump(translations, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT, sort_keys=True)
        logger.info("Added %d AI translation entries to translations.json", added)


def resolve_canonical_ids(ai_lookup: Dict[str, dict]) -> int:
    """Resolve canonical_id on all narrators in loaded AI responses.

    Uses NarratorRegistry with chain-context disambiguation. This backfills
    canonical_id for content generated before the pipeline added resolution,
    and re-resolves for content where the registry has been updated.

    Returns the number of narrators that received a canonical_id.
    """
    registry = NarratorRegistry()
    if registry.narrator_count == 0:
        logger.info("Narrator registry empty — skipping canonical_id resolution")
        return 0

    resolved_count = 0
    for verse_path, ai_data in ai_lookup.items():
        result = ai_data.get("result", {})
        narrators = result.get("isnad_matn", {}).get("narrators", [])
        if not narrators:
            continue

        preceding_names: list = []
        for n in narrators:
            if not isinstance(n, dict):
                continue
            name_ar = n.get("name_ar", "").strip()
            if not name_ar:
                preceding_names.append("")
                continue

            canonical_id = registry.resolve(name_ar, preceding_names=preceding_names)
            if canonical_id is not None:
                if n.get("canonical_id") != canonical_id:
                    n["canonical_id"] = canonical_id
                    resolved_count += 1

            preceding_names.append(name_ar)

    return resolved_count


def merge_ai_content(report=None):
    """Main entry point: merge AI content into generated JSON files.

    Steps:
    1. Load AI responses from SOURCE_DATA_DIR
    1b. Resolve canonical_id on all narrators
    2. Walk DESTINATION_DIR/books/ for all JSON files
    3. Merge AI content into each file
    4. Handle complete book files separately
    5. Update translations.json index
    6. Log stats to report
    """
    dest_dir = os.environ.get("DESTINATION_DIR", DEFAULT_DESTINATION_DIR)

    # Step 1: Load AI responses
    ai_lookup = load_ai_responses()
    if not ai_lookup:
        logger.info("No AI content to merge")
        if report is not None:
            report.ai_verses_available = 0
            report.ai_verses_merged = 0
        return

    # Step 1b: Resolve canonical_id on narrators (backfills existing content)
    resolved = resolve_canonical_ids(ai_lookup)
    if resolved:
        logger.info("Resolved canonical_id on %d narrators", resolved)

    if report is not None:
        report.ai_verses_available = len(ai_lookup)

    total_merged = 0
    errors = []

    # Step 2-3: Walk books/ directory for regular JSON files
    books_dir = os.path.join(dest_dir, "books")
    if os.path.isdir(books_dir):
        for root, _dirs, files in os.walk(books_dir):
            # Skip complete/ subdirectory (handled separately)
            if os.path.basename(root) == "complete":
                continue
            for filename in files:
                if not filename.endswith(".json"):
                    continue
                file_path = os.path.join(root, filename)
                try:
                    merged = merge_ai_into_file(file_path, ai_lookup)
                    total_merged += merged
                except Exception as e:
                    error_msg = f"Error merging AI into {file_path}: {e}"
                    logger.warning(error_msg)
                    errors.append(error_msg)

    # Step 4: Handle complete book files
    complete_dir = os.path.join(dest_dir, "books", "complete")
    if os.path.isdir(complete_dir):
        for filename in os.listdir(complete_dir):
            if not filename.endswith(".json"):
                continue
            file_path = os.path.join(complete_dir, filename)
            try:
                merged = merge_ai_into_complete_file(file_path, ai_lookup)
                total_merged += merged
            except Exception as e:
                error_msg = f"Error merging AI into complete file {file_path}: {e}"
                logger.warning(error_msg)
                errors.append(error_msg)

    # Step 5: Update translations index
    update_translations_index(dest_dir)

    # Step 6: Build AI indexes (topics + phrases)
    from app.build_ai_indexes import build_topics_index, build_phrases_index
    build_topics_index(dest_dir)
    build_phrases_index(dest_dir)

    # Step 7: Report
    logger.info(
        "AI content merge complete: %d verses merged (%d available, %d errors)",
        total_merged,
        len(ai_lookup),
        len(errors),
    )

    if report is not None:
        report.ai_verses_merged = total_merged
        report.ai_merge_errors = errors
