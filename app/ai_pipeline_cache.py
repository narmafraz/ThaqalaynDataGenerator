"""AI pipeline caching for chunked processing intermediates.

Persists structure pass and chunk detail results so that:
- Chunk boundaries (Layer 1) survive glossary/schema changes
- Structural analysis (Layer 2) survives translation-only changes
- Only the layer that actually changed gets regenerated

Cache lives at: ThaqalaynDataSources/ai-content/samples/cache/{verse_id}/
  meta.json       — hashes, versions, timestamps
  structure.json  — structure pass output (Layer 1+2)
  chunk_N.json    — detail pass for chunk N (Layer 3)

Staleness detection:
  Layer 1: Arabic text hash — if changed, everything is stale
  Layer 2: Structure schema version — if changed, structure is stale
  Layer 3: Pipeline version / glossary hash / language keys — chunks stale
"""

import hashlib
import json
import os
import shutil
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from app.ai_pipeline import (
    PIPELINE_VERSION,
    VALID_LANGUAGE_KEYS,
    PipelineRequest,
    load_glossary,
)
from app.ai_pipeline_review import estimate_word_count
from app.config import AI_CONTENT_DIR


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CACHE_FORMAT_VERSION = "1.0.0"
STRUCTURE_SCHEMA_VERSION = "2.0.0"

DEFAULT_CACHE_BASE = os.path.join(AI_CONTENT_DIR, "samples", "cache")


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class CacheMetadata:
    """Metadata for a cached verse's intermediate results."""
    verse_path: str
    arabic_text_hash: str
    pipeline_version: str
    structure_version: str
    glossary_hash: str
    language_keys: List[str]
    model: str
    structure_timestamp: str
    chunk_timestamps: Dict[str, str]
    chunk_count: int
    word_count: int
    cache_format_version: str = CACHE_FORMAT_VERSION


@dataclass
class CacheStaleness:
    """Result of checking if cache is stale."""
    is_stale: bool
    reasons: List[str]
    needs_structure: bool
    needs_chunks: bool
    stale_chunk_indices: List[int]


# ---------------------------------------------------------------------------
# Hashing helpers
# ---------------------------------------------------------------------------

def _hash_text(text: str) -> str:
    """SHA-256 hash of a string, returning hex digest."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _hash_glossary(glossary: Optional[dict] = None) -> str:
    """SHA-256 hash of the glossary JSON content."""
    if glossary is None:
        glossary = load_glossary()
    return _hash_text(json.dumps(glossary, ensure_ascii=False, sort_keys=True))


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _verse_id_from_path(verse_path: str) -> str:
    """Convert verse path to filesystem-safe directory name.

    '/books/al-kafi:1:1:1:1' -> 'al-kafi_1_1_1_1'
    '/books/quran:1:1' -> 'quran_1_1'
    """
    return verse_path.replace("/books/", "").replace(":", "_")


def _cache_dir(verse_path: str, base_dir: Optional[str] = None) -> str:
    """Return the cache directory path for a verse."""
    if base_dir is None:
        base_dir = DEFAULT_CACHE_BASE
    return os.path.join(base_dir, _verse_id_from_path(verse_path))


# ---------------------------------------------------------------------------
# Save functions
# ---------------------------------------------------------------------------

def save_structure_cache(
    verse_path: str,
    request: PipelineRequest,
    structure_result: dict,
    model: str,
    glossary: Optional[dict] = None,
    base_dir: Optional[str] = None,
) -> str:
    """Save a structure pass result to cache.

    Returns the cache directory path.
    """
    cache_path = _cache_dir(verse_path, base_dir)
    os.makedirs(cache_path, exist_ok=True)

    chunk_count = len(structure_result.get("chunks", []))

    meta = CacheMetadata(
        verse_path=verse_path,
        arabic_text_hash=_hash_text(request.arabic_text),
        pipeline_version=PIPELINE_VERSION,
        structure_version=STRUCTURE_SCHEMA_VERSION,
        glossary_hash=_hash_glossary(glossary),
        language_keys=sorted(VALID_LANGUAGE_KEYS),
        model=model,
        structure_timestamp=datetime.now(timezone.utc).isoformat(),
        chunk_timestamps={},
        chunk_count=chunk_count,
        word_count=estimate_word_count(request),
    )

    structure_path = os.path.join(cache_path, "structure.json")
    with open(structure_path, "w", encoding="utf-8") as f:
        json.dump(structure_result, f, ensure_ascii=False, indent=2)

    meta_path = os.path.join(cache_path, "meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(asdict(meta), f, ensure_ascii=False, indent=2)

    return cache_path


def save_chunk_cache(
    verse_path: str,
    chunk_index: int,
    chunk_detail: dict,
    base_dir: Optional[str] = None,
) -> str:
    """Save a chunk detail pass result to cache.

    Returns the chunk cache file path.
    """
    cache_path = _cache_dir(verse_path, base_dir)
    os.makedirs(cache_path, exist_ok=True)

    chunk_file = os.path.join(cache_path, "chunk_{}.json".format(chunk_index))
    with open(chunk_file, "w", encoding="utf-8") as f:
        json.dump(chunk_detail, f, ensure_ascii=False, indent=2)

    # Update meta.json timestamp for this chunk
    meta_path = os.path.join(cache_path, "meta.json")
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta_dict = json.load(f)
        meta_dict["chunk_timestamps"][str(chunk_index)] = (
            datetime.now(timezone.utc).isoformat()
        )
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_dict, f, ensure_ascii=False, indent=2)

    return chunk_file


# ---------------------------------------------------------------------------
# Load functions
# ---------------------------------------------------------------------------

def load_structure_cache(
    verse_path: str,
    base_dir: Optional[str] = None,
) -> Optional[dict]:
    """Load a cached structure pass result. Returns None if missing."""
    cache_path = _cache_dir(verse_path, base_dir)
    structure_path = os.path.join(cache_path, "structure.json")
    if not os.path.exists(structure_path):
        return None
    with open(structure_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_chunk_cache(
    verse_path: str,
    chunk_index: int,
    base_dir: Optional[str] = None,
) -> Optional[dict]:
    """Load a cached chunk detail result. Returns None if missing."""
    cache_path = _cache_dir(verse_path, base_dir)
    chunk_file = os.path.join(cache_path, "chunk_{}.json".format(chunk_index))
    if not os.path.exists(chunk_file):
        return None
    with open(chunk_file, "r", encoding="utf-8") as f:
        return json.load(f)


def load_cache_metadata(
    verse_path: str,
    base_dir: Optional[str] = None,
) -> Optional[dict]:
    """Load cache metadata for a verse. Returns None if missing."""
    cache_path = _cache_dir(verse_path, base_dir)
    meta_path = os.path.join(cache_path, "meta.json")
    if not os.path.exists(meta_path):
        return None
    with open(meta_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Staleness detection
# ---------------------------------------------------------------------------

def check_cache_staleness(
    request: PipelineRequest,
    glossary: Optional[dict] = None,
    base_dir: Optional[str] = None,
) -> CacheStaleness:
    """Check if cached intermediates are stale for a given request.

    Checks in order of layer stability:
    1. Arabic text hash (Layer 1) — everything stale
    2. Structure schema version (Layer 2) — structure stale
    3. Pipeline version (Layer 3) — chunks stale
    4. Glossary hash (Layer 3) — chunks stale
    5. Language keys (Layer 3) — chunks stale
    6. Chunk completeness — specific chunks stale
    """
    meta = load_cache_metadata(request.verse_path, base_dir)

    if meta is None:
        return CacheStaleness(
            is_stale=True,
            reasons=["no cache exists"],
            needs_structure=True,
            needs_chunks=True,
            stale_chunk_indices=[],
        )

    reasons = []
    needs_structure = False
    needs_chunks = False
    stale_indices = []

    # Layer 1: Arabic text changed
    current_hash = _hash_text(request.arabic_text)
    if meta.get("arabic_text_hash") != current_hash:
        reasons.append("arabic text changed")
        needs_structure = True
        needs_chunks = True

    # Layer 2: Structure schema version changed
    if not needs_structure and meta.get("structure_version") != STRUCTURE_SCHEMA_VERSION:
        reasons.append(
            "structure schema version changed: {} -> {}".format(
                meta.get("structure_version"), STRUCTURE_SCHEMA_VERSION
            )
        )
        needs_structure = True
        needs_chunks = True

    # Layer 3: Pipeline version changed
    if not needs_chunks and meta.get("pipeline_version") != PIPELINE_VERSION:
        reasons.append(
            "pipeline version changed: {} -> {}".format(
                meta.get("pipeline_version"), PIPELINE_VERSION
            )
        )
        needs_chunks = True

    # Layer 3: Glossary changed
    if not needs_chunks:
        current_glossary_hash = _hash_glossary(glossary)
        if meta.get("glossary_hash") != current_glossary_hash:
            reasons.append("glossary changed")
            needs_chunks = True

    # Layer 3: Language keys changed
    if not needs_chunks:
        current_langs = sorted(VALID_LANGUAGE_KEYS)
        cached_langs = meta.get("language_keys", [])
        if current_langs != cached_langs:
            reasons.append(
                "language keys changed: {} -> {}".format(cached_langs, current_langs)
            )
            needs_chunks = True

    # Check chunk completeness
    if not needs_structure and not needs_chunks:
        chunk_count = meta.get("chunk_count", 0)
        cache_path = _cache_dir(request.verse_path, base_dir)
        for i in range(chunk_count):
            chunk_file = os.path.join(cache_path, "chunk_{}.json".format(i))
            if not os.path.exists(chunk_file):
                stale_indices.append(i)
                reasons.append("chunk {} cache missing".format(i))

    is_stale = needs_structure or needs_chunks or len(stale_indices) > 0

    return CacheStaleness(
        is_stale=is_stale,
        reasons=reasons,
        needs_structure=needs_structure,
        needs_chunks=needs_chunks,
        stale_chunk_indices=stale_indices,
    )


# ---------------------------------------------------------------------------
# Invalidation
# ---------------------------------------------------------------------------

def invalidate_cache(
    verse_path: str,
    base_dir: Optional[str] = None,
) -> bool:
    """Remove all cached data for a verse. Returns True if cache existed."""
    cache_path = _cache_dir(verse_path, base_dir)
    if os.path.exists(cache_path):
        shutil.rmtree(cache_path)
        return True
    return False


def invalidate_chunks(
    verse_path: str,
    chunk_indices: Optional[List[int]] = None,
    base_dir: Optional[str] = None,
) -> int:
    """Remove chunk caches (all if indices is None). Keeps structure.

    Returns number of chunks removed.
    """
    cache_path = _cache_dir(verse_path, base_dir)
    if not os.path.exists(cache_path):
        return 0

    removed = 0
    if chunk_indices is None:
        for name in os.listdir(cache_path):
            if name.startswith("chunk_") and name.endswith(".json"):
                os.remove(os.path.join(cache_path, name))
                removed += 1
    else:
        for i in chunk_indices:
            chunk_file = os.path.join(cache_path, "chunk_{}.json".format(i))
            if os.path.exists(chunk_file):
                os.remove(chunk_file)
                removed += 1

    return removed


# ---------------------------------------------------------------------------
# Convenience
# ---------------------------------------------------------------------------

def get_cached_or_plan(
    request: PipelineRequest,
    glossary: Optional[dict] = None,
    base_dir: Optional[str] = None,
) -> Tuple[Optional[dict], CacheStaleness]:
    """Determine what work needs to be done for a chunked request.

    Returns:
        (structure_result_or_none, staleness)

        If structure is cached and valid, returns (structure_dict, staleness).
        The staleness tells the caller which chunks need (re)generation.
        If structure is stale, returns (None, staleness).
    """
    staleness = check_cache_staleness(request, glossary, base_dir)

    if staleness.needs_structure:
        return (None, staleness)

    structure = load_structure_cache(request.verse_path, base_dir)
    return (structure, staleness)
