"""Build AI content indexes (topics + phrases) from merged verse data.

Scans all merged JSON files in DESTINATION_DIR/books/ to build two index files:
- index/topics.json: inverted index of AI topics → verse paths
- index/phrases.json: inverted index of key phrases → verse paths

Called at the end of merge_ai_content() after all AI data has been injected.
"""

import json
import logging
import os
import re
from typing import Dict, List, Optional, Tuple

from app.config import (
    AI_PIPELINE_DATA_DIR,
    DEFAULT_DESTINATION_DIR,
    JSON_ENCODING,
    JSON_ENSURE_ASCII,
    JSON_INDENT,
)

logger = logging.getLogger(__name__)


def _load_topic_taxonomy() -> dict:
    """Load topic_taxonomy.json from ai-pipeline-data.

    Returns the 'taxonomy' dict mapping L1 keys to their metadata + subtopics.
    """
    taxonomy_path = os.path.join(AI_PIPELINE_DATA_DIR, "topic_taxonomy.json")
    if not os.path.isfile(taxonomy_path):
        logger.warning("topic_taxonomy.json not found at %s", taxonomy_path)
        return {}
    with open(taxonomy_path, "r", encoding=JSON_ENCODING) as f:
        data = json.load(f)
    return data.get("taxonomy", {})


def _build_l2_to_l1_map(taxonomy: dict) -> Dict[str, str]:
    """Build a reverse map from L2 sub-topic key to L1 parent key."""
    l2_to_l1 = {}
    for l1_key, l1_data in taxonomy.items():
        for l2_key in l1_data.get("topics", {}).keys():
            l2_to_l1[l2_key] = l1_key
    return l2_to_l1


def _normalize_arabic(text: str) -> str:
    """Strip Arabic diacritics for use as dictionary keys."""
    # Remove tashkeel (diacritical marks)
    return re.sub(r'[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7-\u06E8]', '', text).strip()


def _walk_json_files(books_dir: str):
    """Yield all JSON file paths under books_dir, skipping complete/ subdirectory."""
    for root, dirs, files in os.walk(books_dir):
        # Skip complete/ subdirectory
        if os.path.basename(root) == "complete":
            continue
        for filename in sorted(files):
            if filename.endswith(".json"):
                yield os.path.join(root, filename)


def _extract_verses(file_path: str) -> List[dict]:
    """Extract verse dicts from a JSON data file.

    Handles verse_list (data.verses[]) and verse_detail (data.verse) kinds.
    """
    try:
        with open(file_path, "r", encoding=JSON_ENCODING) as f:
            doc = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []

    kind = doc.get("kind", "")
    data = doc.get("data", {})

    if kind == "verse_list":
        return data.get("verses", [])
    elif kind == "verse_detail":
        verse = data.get("verse", data)
        return [verse] if isinstance(verse, dict) else []
    return []


def build_topics_index(dest_dir: Optional[str] = None) -> dict:
    """Walk all merged JSON files, collect verse.ai.topics, build inverted index.

    Returns the index dict (also written to index/topics.json).
    """
    if dest_dir is None:
        dest_dir = os.environ.get("DESTINATION_DIR", DEFAULT_DESTINATION_DIR)

    taxonomy = _load_topic_taxonomy()
    l2_to_l1 = _build_l2_to_l1_map(taxonomy)

    # Initialize structure from taxonomy so all categories appear even if empty
    index: Dict[str, Dict[str, dict]] = {}
    for l1_key, l1_data in taxonomy.items():
        index[l1_key] = {}
        for l2_key in l1_data.get("topics", {}).keys():
            index[l1_key][l2_key] = {"count": 0, "paths": []}

    # Walk books/ directory for verse data
    books_dir = os.path.join(dest_dir, "books")
    total_topics = 0
    seen_paths: Dict[str, set] = {}  # l1:l2 -> set of paths (dedup)

    if os.path.isdir(books_dir):
        for file_path in _walk_json_files(books_dir):
            for verse in _extract_verses(file_path):
                ai = verse.get("ai")
                if not ai:
                    continue
                topics = ai.get("topics", [])
                if not topics:
                    continue
                verse_path = verse.get("path", "")
                if not verse_path:
                    continue

                for topic_key in topics:
                    l1_key = l2_to_l1.get(topic_key)
                    if not l1_key:
                        # Topic not in taxonomy — try it as L1 key directly
                        if topic_key in index:
                            continue
                        logger.debug("Unknown topic key '%s' in verse %s", topic_key, verse_path)
                        continue

                    dedup_key = f"{l1_key}:{topic_key}"
                    if dedup_key not in seen_paths:
                        seen_paths[dedup_key] = set()
                    if verse_path in seen_paths[dedup_key]:
                        continue
                    seen_paths[dedup_key].add(verse_path)

                    if topic_key in index.get(l1_key, {}):
                        index[l1_key][topic_key]["count"] += 1
                        index[l1_key][topic_key]["paths"].append(verse_path)
                        total_topics += 1

    # Prune empty categories (L1s with all-zero L2s)
    pruned = {}
    for l1_key, l2s in index.items():
        non_empty = {k: v for k, v in l2s.items() if v["count"] > 0}
        if non_empty:
            pruned[l1_key] = non_empty

    # Write to index/topics.json
    index_dir = os.path.join(dest_dir, "index")
    os.makedirs(index_dir, exist_ok=True)
    topics_path = os.path.join(index_dir, "topics.json")
    with open(topics_path, "w", encoding=JSON_ENCODING) as f:
        json.dump(pruned, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT, sort_keys=True)

    logger.info("Built topics index: %d topic entries across %d L1 categories", total_topics, len(pruned))
    return pruned


def build_phrases_index(dest_dir: Optional[str] = None) -> dict:
    """Walk all merged JSON files, collect verse.ai.key_phrases, build inverted index.

    Returns the index dict (also written to index/phrases.json).
    """
    if dest_dir is None:
        dest_dir = os.environ.get("DESTINATION_DIR", DEFAULT_DESTINATION_DIR)

    index: Dict[str, dict] = {}

    books_dir = os.path.join(dest_dir, "books")
    if os.path.isdir(books_dir):
        for file_path in _walk_json_files(books_dir):
            for verse in _extract_verses(file_path):
                ai = verse.get("ai")
                if not ai:
                    continue
                key_phrases = ai.get("key_phrases", [])
                if not key_phrases:
                    continue
                verse_path = verse.get("path", "")
                if not verse_path:
                    continue

                for kp in key_phrases:
                    phrase_ar = kp.get("phrase_ar", "")
                    if not phrase_ar:
                        continue

                    # Normalize Arabic for key (strip diacritics to avoid duplicates)
                    normalized_key = _normalize_arabic(phrase_ar)
                    if not normalized_key:
                        continue

                    if normalized_key not in index:
                        index[normalized_key] = {
                            "phrase_ar": phrase_ar,  # Keep original with diacritics for display
                            "phrase_en": kp.get("phrase_en", ""),
                            "category": kp.get("category", ""),
                            "paths": [],
                        }

                    # Dedup paths
                    if verse_path not in index[normalized_key]["paths"]:
                        index[normalized_key]["paths"].append(verse_path)

    # Write to index/phrases.json
    index_dir = os.path.join(dest_dir, "index")
    os.makedirs(index_dir, exist_ok=True)
    phrases_path = os.path.join(index_dir, "phrases.json")
    with open(phrases_path, "w", encoding=JSON_ENCODING) as f:
        json.dump(index, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT, sort_keys=True)

    logger.info("Built phrases index: %d unique phrases", len(index))
    return index
