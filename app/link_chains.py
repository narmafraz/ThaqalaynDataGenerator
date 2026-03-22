"""Link hadiths that share identical full narrator chains.

Finds subchains with 3+ narrators appearing in 2-20 hadiths, then adds
bidirectional "Shared Chain" relations between those hadiths.
"""

import logging
from typing import Dict, List, Set

from app.lib_model import get_chapters, get_verses

logger = logging.getLogger(__name__)

RELATION_TYPE = "Shared Chain"
MIN_CHAIN_LENGTH = 3   # Minimum narrators in chain (skip pairs — too common)
MIN_GROUP_SIZE = 2     # At least 2 hadiths sharing the chain
MAX_GROUP_SIZE = 20    # Skip very common chains (noise)


def collect_shared_chains(narrators) -> Dict[str, Set[str]]:
    """Collect unique full chains with 2-20 verse_paths from narrator subchain data.

    Deduplicates chain keys across narrators (same chain appears in multiple
    narrator files).
    """
    seen_keys: Set[str] = set()
    chain_groups: Dict[str, Set[str]] = {}

    for narrator in narrators.values():
        if not narrator.subchains:
            continue
        for chain_key, chain_data in narrator.subchains.items():
            if chain_key in seen_keys:
                continue
            seen_keys.add(chain_key)

            if not chain_data.narrator_ids or len(chain_data.narrator_ids) < MIN_CHAIN_LENGTH:
                continue
            if not chain_data.verse_paths:
                continue
            count = len(chain_data.verse_paths)
            if count < MIN_GROUP_SIZE or count > MAX_GROUP_SIZE:
                continue

            chain_groups[chain_key] = set(chain_data.verse_paths)

    return chain_groups


def build_verse_relations(chain_groups: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    """Build verse_path -> set of related verse_paths from chain groups."""
    relations: Dict[str, Set[str]] = {}
    for paths in chain_groups.values():
        for path in paths:
            if path not in relations:
                relations[path] = set()
            relations[path].update(paths - {path})
    return relations


def apply_shared_chain_relations(books: List, verse_relations: Dict[str, Set[str]]) -> int:
    """Walk book trees and add 'Shared Chain' relations to qualifying verses.

    Returns count of updated verses.
    """
    updated = 0
    for book in books:
        updated += _update_chapter(book, verse_relations)
    return updated


def _update_chapter(chapter, verse_relations: Dict[str, Set[str]]) -> int:
    """Recursively update verses in a chapter tree."""
    updated = 0
    chapters = get_chapters(chapter)
    verses = get_verses(chapter)

    if chapters:
        for sub in chapters:
            updated += _update_chapter(sub, verse_relations)
    elif verses:
        for verse in verses:
            path = verse.path if hasattr(verse, 'path') else verse.get('path')
            if not path or path not in verse_relations:
                continue

            # Get or initialize relations
            if hasattr(verse, 'relations'):
                if not verse.relations:
                    verse.relations = {}
                existing = set(verse.relations.get(RELATION_TYPE, set()))
                verse.relations[RELATION_TYPE] = existing | verse_relations[path]
            else:
                relations = verse.get('relations') or {}
                existing = set(relations.get(RELATION_TYPE, []))
                relations[RELATION_TYPE] = existing | verse_relations[path]
                verse['relations'] = relations

            updated += 1

    return updated
