"""Chapter-level topic linking via TF-IDF similarity.

Finds semantically similar chapters across different books based on their
English titles. Outputs index/related_chapters.json for the Angular UI.

Uses only Python stdlib (no scikit-learn or external dependencies).
"""

import json
import logging
import math
import os
import re
from collections import Counter, defaultdict
from typing import Dict, List, Set, Tuple

from app.book_registry import BOOK_REGISTRY
from app.lib_db import get_dest_path, write_file

logger = logging.getLogger(__name__)

SIMILARITY_THRESHOLD = 0.3
MAX_RELATED = 5

STOP_WORDS = {
    'a', 'an', 'the', 'of', 'on', 'in', 'and', 'or', 'is', 'to',
    'his', 'her', 'from', 'about', 'for', 'that', 'this', 'with', 'by',
    'chapter', 'book', 'regarding', 'concerning', 'section', 'part',
    'its', 'he', 'who', 'it', 'not', 'has', 'was', 'are', 'been',
    'be', 'at', 'as', 'but', 'if', 'no', 'their', 'them', 'they',
    'which', 'what', 'when', 'where', 'how', 'all', 'one', 'him',
}


def _get_book_slug(path: str) -> str:
    """Extract book slug from path like /books/al-kafi:1:1:1."""
    raw = path.lstrip('/books/')
    # Actually, strip the /books/ prefix properly
    if path.startswith('/books/'):
        raw = path[7:]
    return raw.split(':')[0]


def _get_book_display_name(slug: str) -> str:
    """Look up display name from BOOK_REGISTRY."""
    for config in BOOK_REGISTRY:
        if config.slug == slug:
            return config.titles.get('en', slug.replace('-', ' ').title())
    return slug.replace('-', ' ').title()


def _tokenize(title: str) -> List[str]:
    """Tokenize a chapter title: lowercase, split, remove stop words."""
    title = title.lower()
    words = re.findall(r"[a-z']+", title)
    return [w for w in words if w not in STOP_WORDS and len(w) > 1]


def _build_tfidf(
    docs: Dict[str, List[str]],
) -> Dict[str, Dict[str, float]]:
    """Build TF-IDF vectors for a set of documents.

    Args:
        docs: Mapping of doc_id -> list of tokens.

    Returns:
        Mapping of doc_id -> {term: tfidf_weight}.
    """
    n_docs = len(docs)
    if n_docs == 0:
        return {}

    # Document frequency: how many docs contain each term
    df: Counter = Counter()
    for tokens in docs.values():
        unique = set(tokens)
        for term in unique:
            df[term] += 1

    # TF-IDF per document
    vectors: Dict[str, Dict[str, float]] = {}
    for doc_id, tokens in docs.items():
        if not tokens:
            continue
        tf = Counter(tokens)
        vec = {}
        for term, count in tf.items():
            idf = math.log(n_docs / (df[term] + 1)) + 1
            vec[term] = (count / len(tokens)) * idf
        vectors[doc_id] = vec

    return vectors


def _cosine_sim(a: Dict[str, float], b: Dict[str, float]) -> float:
    """Compute cosine similarity between two sparse vectors."""
    # Use the smaller dict for iteration
    if len(a) > len(b):
        a, b = b, a

    dot = sum(a[k] * b[k] for k in a if k in b)
    if dot == 0:
        return 0.0

    mag_a = math.sqrt(sum(v * v for v in a.values()))
    mag_b = math.sqrt(sum(v * v for v in b.values()))
    if mag_a == 0 or mag_b == 0:
        return 0.0

    return dot / (mag_a * mag_b)


def link_related_chapters() -> None:
    """Compute chapter similarity and write index/related_chapters.json."""
    # Load English book index
    index_path = get_dest_path('/index/books.en')
    if not os.path.exists(index_path):
        logger.warning("books.en.json not found, skipping chapter linking")
        return

    with open(index_path, 'r', encoding='utf-8') as f:
        books_index = json.load(f)

    # Filter to Chapter-type entries, skip Quran
    chapters: Dict[str, str] = {}  # path -> title
    for path, entry in books_index.items():
        if entry.get('part_type') != 'Chapter':
            continue
        if path.startswith('/books/quran'):
            continue
        title = entry.get('title', '')
        if title:
            chapters[path] = title

    logger.info("Found %d chapter titles for similarity computation", len(chapters))

    if len(chapters) < 2:
        return

    # Tokenize all titles
    docs: Dict[str, List[str]] = {}
    for path, title in chapters.items():
        tokens = _tokenize(title)
        if tokens:
            docs[path] = tokens

    logger.info("%d chapters have usable tokens after stop-word removal", len(docs))

    # Build TF-IDF vectors
    vectors = _build_tfidf(docs)

    # Compute pairwise similarities (only cross-book)
    paths = list(vectors.keys())
    related: Dict[str, List[dict]] = {}

    for i, path_a in enumerate(paths):
        slug_a = _get_book_slug(path_a)
        candidates = []

        for j, path_b in enumerate(paths):
            if i == j:
                continue
            slug_b = _get_book_slug(path_b)
            if slug_a == slug_b:
                continue

            sim = _cosine_sim(vectors[path_a], vectors[path_b])
            if sim >= SIMILARITY_THRESHOLD:
                candidates.append((path_b, sim))

        if candidates:
            # Sort by score descending, take top N
            candidates.sort(key=lambda x: -x[1])
            top = candidates[:MAX_RELATED]
            related[path_a] = [
                {
                    'path': p,
                    'title': chapters.get(p, ''),
                    'book': _get_book_display_name(_get_book_slug(p)),
                    'score': round(s, 2),
                }
                for p, s in top
            ]

    logger.info(
        "Chapter linking: %d chapters have related chapters in other books",
        len(related),
    )

    write_file('/index/related_chapters', related)
