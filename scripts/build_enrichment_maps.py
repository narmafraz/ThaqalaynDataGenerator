#!/usr/bin/env python3
"""Build tag/topic/content_type mapping from existing AI responses.

Analyzes AI-generated responses to build statistical keyword->topic/tag mappings
for use by the programmatic enrichment phase (Phase 2).

Usage:
    python scripts/build_enrichment_maps.py [--responses-dir DIR]

Output:
    ai-pipeline-data/tag_topic_mapping.json
"""

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import AI_PIPELINE_DATA_DIR, AI_CONTENT_DIR

# Windows console UTF-8 fix
sys.stdout.reconfigure(encoding="utf-8")

OUTPUT_FILE = os.path.join(AI_PIPELINE_DATA_DIR, "tag_topic_mapping.json")

# ---------------------------------------------------------------------------
# Baseline mapping -- used as the default if no AI responses are found,
# and as the foundation that statistical data is merged into.
# ---------------------------------------------------------------------------

BASELINE_MAPPING = {
    "version": "1.0.0",
    "keyword_to_topics": {
        "prayer": ["salat", "prayer_rulings"],
        "fasting": ["fasting", "fasting_rulings"],
        "knowledge": ["seeking_knowledge", "scholars_virtues"],
        "scholar": ["scholars_virtues", "seeking_knowledge"],
        "imam": ["imamate", "religious_authority"],
        "unity": ["tawhid", "divine_attributes"],
        "patience": ["patience"],
        "repentance": ["repentance"],
        "charity": ["financial_law", "zakat_khums"],
        "marriage": ["marriage_family_law"],
        "paradise": ["paradise_hell"],
        "hell": ["paradise_hell"],
        "death": ["resurrection", "barzakh"],
        "judgment": ["resurrection", "divine_justice"],
        "quran": ["quran_recitation", "quran_commentary"],
        "sin": ["repentance", "forbidding_evil"],
        "god": ["tawhid", "divine_attributes"],
        "allah": ["tawhid", "divine_attributes"],
        "prophet": ["prophethood", "prophetic_traditions"],
        "pilgrimage": ["hajj"],
        "hajj": ["hajj"],
        "zakat": ["zakat_khums", "financial_law"],
        "khums": ["zakat_khums", "financial_law"],
        "fast": ["fasting", "fasting_rulings"],
        "ablution": ["ritual_purity"],
        "purity": ["ritual_purity"],
        "worship": ["salat", "dhikr"],
        "mosque": ["mosque_etiquette", "salat"],
        "night prayer": ["night_prayer"],
        "wudu": ["ritual_purity"],
        "inheritance": ["inheritance"],
        "divorce": ["marriage_family_law"],
        "testimony": ["judicial_rulings"],
        "halal": ["halal_haram"],
        "haram": ["halal_haram"],
        "backbiting": ["backbiting"],
        "anger": ["anger_control"],
        "honesty": ["honesty"],
        "gratitude": ["gratitude"],
        "sincerity": ["sincerity"],
        "humility": ["humility"],
        "companion": ["companions"],
        "battle": ["battles_events"],
        "karbala": ["karbala"],
        "ghadir": ["ghadir"],
        "imamate": ["imamate"],
        "wilaya": ["imamate"],
        "intercession": ["intercession"],
        "dream": ["dreams_visions"],
        "supplication": ["dua_etiquette", "specific_duas"],
        "dua": ["dua_etiquette", "specific_duas"],
        "food": ["halal_haram", "dietary_rulings"],
        "trade": ["commercial_law"],
        "business": ["commercial_law"],
        "forgive": ["repentance", "divine_attributes"],
        "mercy": ["divine_attributes"],
        "justice": ["divine_justice"],
        "creation": ["creation_narratives"],
        "angel": ["angels_jinn"],
        "jinn": ["angels_jinn"],
        "family": ["family_relations"],
        "neighbor": ["rights_of_others"],
        "child": ["family_relations"],
        "parent": ["family_relations"],
        "obedience": ["obedience_to_god"],
        "heaven": ["paradise_hell"],
        "fire": ["paradise_hell"],
        "trust": ["sincerity"],
        "truth": ["honesty"],
        "lie": ["honesty", "backbiting"],
        "wealth": ["financial_law", "asceticism"],
        "poverty": ["asceticism"],
        "world": ["asceticism"],
        "heart": ["spiritual_purification"],
        "soul": ["spiritual_purification"],
        "remembrance": ["dhikr"],
    },
    "keyword_to_tags": {
        "prayer": ["worship", "jurisprudence"],
        "fasting": ["worship", "jurisprudence"],
        "knowledge": ["knowledge"],
        "scholar": ["knowledge"],
        "imam": ["theology"],
        "unity": ["theology"],
        "patience": ["ethics"],
        "repentance": ["ethics"],
        "charity": ["jurisprudence", "ethics"],
        "marriage": ["jurisprudence", "family"],
        "paradise": ["afterlife"],
        "hell": ["afterlife"],
        "death": ["afterlife"],
        "judgment": ["afterlife", "theology"],
        "quran": ["quran_commentary"],
        "sin": ["ethics"],
        "god": ["theology"],
        "allah": ["theology"],
        "prophet": ["prophetic_tradition", "history"],
        "pilgrimage": ["worship", "jurisprudence"],
        "worship": ["worship"],
        "backbiting": ["ethics", "social_relations"],
        "anger": ["ethics"],
        "honesty": ["ethics"],
        "supplication": ["dua"],
        "dua": ["dua"],
        "food": ["jurisprudence"],
        "trade": ["economy"],
        "family": ["family"],
        "governance": ["governance"],
        "forgive": ["ethics", "theology"],
        "mercy": ["theology"],
        "justice": ["theology", "governance"],
        "creation": ["theology"],
        "angel": ["theology"],
        "obedience": ["ethics", "theology"],
        "wealth": ["economy", "ethics"],
        "soul": ["ethics"],
        "remembrance": ["worship", "dua"],
    },
    "chapter_to_content_type": {
        "the book of reason and ignorance": "theological",
        "the book of excellence of knowledge": "ethical_teaching",
        "the book of oneness of god": "theological",
        "the book of proof": "creedal",
        "the book of faith and disbelief": "creedal",
        "the book of supplication": "supplication",
        "the book of prayer": "legal_ruling",
        "the book of fasting": "legal_ruling",
        "the book of hajj": "legal_ruling",
        "the book of zakat": "legal_ruling",
        "the book of jihad": "legal_ruling",
        "the book of marriage": "legal_ruling",
        "the book of divorce": "legal_ruling",
        "the book of inheritance": "legal_ruling",
        "the book of testimony": "legal_ruling",
        "the book of judgments": "legal_ruling",
        "the book of penalties": "legal_ruling",
        "the book of manners": "ethical_teaching",
        "the book of food and drink": "legal_ruling",
        "the book of clothing": "legal_ruling",
        "the book of animals": "legal_ruling",
    },
    "default_content_type_by_book": {
        "al-kafi": "narrative",
        "quran": "quranic_commentary",
        "tahdhib-al-ahkam": "legal_ruling",
        "al-istibsar": "legal_ruling",
        "man-la-yahduruhu-al-faqih": "legal_ruling",
        "nahj-al-balagha": "exhortation",
        "al-sahifa-al-sajjadiyya": "supplication",
        "kitab-al-irshad": "biographical",
        "al-amali-mufid": "narrative",
        "al-amali-saduq": "narrative",
        "al-amali-tusi": "narrative",
    },
    "tag_to_topics": {
        "theology": [
            "tawhid", "divine_attributes", "divine_justice",
            "imamate", "prophethood", "divine_decree", "divine_knowledge",
        ],
        "ethics": [
            "patience", "honesty", "humility", "rights_of_others",
            "repentance", "gratitude", "sincerity", "forbidding_evil",
            "anger_control", "backbiting",
        ],
        "jurisprudence": [
            "ritual_purity", "prayer_rulings", "fasting_rulings",
            "financial_law", "marriage_family_law", "halal_haram",
            "judicial_rulings", "inheritance",
        ],
        "worship": [
            "salat", "fasting", "hajj", "zakat_khums", "dhikr",
            "quran_recitation", "night_prayer", "mosque_etiquette",
        ],
        "knowledge": [
            "seeking_knowledge", "scholars_virtues", "teaching",
            "reasoning", "ignorance", "religious_authority",
        ],
        "afterlife": ["resurrection", "paradise_hell", "barzakh"],
        "dua": ["dua_etiquette", "specific_duas", "istighfar"],
        "history": [
            "prophets_stories", "companions", "battles_events",
            "karbala", "ghadir",
        ],
        "quran_commentary": ["quran_commentary_general"],
        "prophetic_tradition": ["prophetic_traditions"],
        "family": ["family_relations", "marriage_family_law"],
        "social_relations": ["rights_of_others", "community"],
        "economy": ["commercial_law", "financial_law"],
        "governance": ["political_authority", "justice_system"],
    },
}

# Stopwords to skip when building keyword co-occurrence counts
STOPWORDS = frozenset({
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "being", "have", "has", "had", "do", "does", "did", "will", "would",
    "shall", "should", "may", "might", "can", "could", "not", "no", "nor",
    "so", "if", "then", "than", "that", "this", "these", "those", "it",
    "its", "he", "him", "his", "she", "her", "they", "them", "their",
    "we", "us", "our", "you", "your", "who", "whom", "which", "what",
    "when", "where", "how", "why", "as", "about", "up", "out", "into",
    "through", "during", "before", "after", "above", "below", "between",
    "under", "again", "further", "once", "here", "there", "all", "each",
    "every", "both", "few", "more", "most", "other", "some", "such",
    "only", "own", "same", "also", "very", "just", "said", "says",
    "one", "two", "upon", "over", "while",
})

# Minimum co-occurrence count before a keyword-topic/tag association is kept
MIN_COOCCURRENCE = 3

# Maximum number of topics/tags to associate with a single keyword
MAX_ASSOCIATIONS = 5


def tokenize(text: str) -> list[str]:
    """Lowercase and split English text into word tokens, filtering stopwords."""
    words = re.findall(r"[a-z]+", text.lower())
    return [w for w in words if w not in STOPWORDS and len(w) > 2]


def collect_response_dirs(base_dir: str | None) -> list[str]:
    """Return a list of response directories to scan."""
    dirs = []
    if base_dir:
        if os.path.isdir(base_dir):
            dirs.append(base_dir)
        return dirs

    # Scan known subdirectories under ai-content/
    for subdir in ("corpus", "samples", "benchmarks"):
        resp_dir = os.path.join(AI_CONTENT_DIR, subdir, "responses")
        if os.path.isdir(resp_dir):
            dirs.append(resp_dir)
    return dirs


def extract_english_text(result: dict) -> str:
    """Extract all English text from a response result for keyword analysis.

    Pulls from translations.en.summary, chunk translations, and key_terms.
    """
    parts = []

    # translations.en.summary
    translations = result.get("translations", {})
    en = translations.get("en", {})
    if isinstance(en, dict):
        summary = en.get("summary", "")
        if summary:
            parts.append(summary)
        # seo_question (may be a string or dict with language keys)
        seo = en.get("seo_question", "")
        if isinstance(seo, str) and seo:
            parts.append(seo)
        elif isinstance(seo, dict):
            en_seo = seo.get("en", "")
            if en_seo:
                parts.append(en_seo)

    # chunk translations (en)
    for chunk in result.get("chunks", []):
        if not isinstance(chunk, dict):
            continue
        chunk_translations = chunk.get("translations", {})
        if not isinstance(chunk_translations, dict):
            continue
        en_chunk = chunk_translations.get("en", "")
        if isinstance(en_chunk, str) and en_chunk:
            parts.append(en_chunk)

    # key_terms (en values)
    key_terms = result.get("key_terms", {})
    if isinstance(key_terms, dict):
        for _arabic, lang_map in key_terms.items():
            if isinstance(lang_map, dict):
                en_term = lang_map.get("en", "")
                if en_term:
                    parts.append(en_term)

    return " ".join(parts)


def load_responses(response_dirs: list[str]) -> list[dict]:
    """Load all valid AI response JSON files from the given directories."""
    responses = []
    for resp_dir in response_dirs:
        for fname in os.listdir(resp_dir):
            if not fname.endswith(".json"):
                continue
            fpath = os.path.join(resp_dir, fname)
            try:
                with open(fpath, encoding="utf-8") as f:
                    data = json.load(f)
                result = data.get("result")
                if result and isinstance(result, dict):
                    responses.append({
                        "verse_path": data.get("verse_path", ""),
                        "result": result,
                    })
            except (json.JSONDecodeError, OSError):
                continue
    return responses


def build_cooccurrence_maps(
    responses: list[dict],
) -> tuple[
    dict[str, Counter],
    dict[str, Counter],
]:
    """Build keyword -> Counter(topic) and keyword -> Counter(tag) maps.

    For each response, extract English text tokens and the response's
    topics/tags, then increment co-occurrence counts for every
    (keyword, topic) and (keyword, tag) pair.
    """
    keyword_topic_counts: dict[str, Counter] = defaultdict(Counter)
    keyword_tag_counts: dict[str, Counter] = defaultdict(Counter)

    for resp in responses:
        result = resp["result"]
        topics = result.get("topics", [])
        tags = result.get("tags", [])

        if not topics and not tags:
            continue

        text = extract_english_text(result)
        keywords = set(tokenize(text))

        for kw in keywords:
            for topic in topics:
                keyword_topic_counts[kw][topic] += 1
            for tag in tags:
                keyword_tag_counts[kw][tag] += 1

    return keyword_topic_counts, keyword_tag_counts


def merge_statistical_into_baseline(
    baseline: dict,
    keyword_topic_counts: dict[str, Counter],
    keyword_tag_counts: dict[str, Counter],
) -> dict:
    """Merge statistically discovered associations into the baseline mapping.

    Only associations that meet MIN_COOCCURRENCE are added.  Existing baseline
    entries are preserved; new entries are appended.
    """
    mapping = json.loads(json.dumps(baseline))  # deep copy

    # Merge keyword_to_topics
    kw_topics = mapping["keyword_to_topics"]
    for kw, counter in keyword_topic_counts.items():
        top_topics = [
            topic
            for topic, count in counter.most_common(MAX_ASSOCIATIONS)
            if count >= MIN_COOCCURRENCE
        ]
        if not top_topics:
            continue
        if kw in kw_topics:
            # Add new topics not already present
            existing = set(kw_topics[kw])
            for t in top_topics:
                if t not in existing:
                    kw_topics[kw].append(t)
                    # Cap at MAX_ASSOCIATIONS
                    if len(kw_topics[kw]) >= MAX_ASSOCIATIONS:
                        break
        else:
            kw_topics[kw] = top_topics

    # Merge keyword_to_tags
    kw_tags = mapping["keyword_to_tags"]
    for kw, counter in keyword_tag_counts.items():
        top_tags = [
            tag
            for tag, count in counter.most_common(MAX_ASSOCIATIONS)
            if count >= MIN_COOCCURRENCE
        ]
        if not top_tags:
            continue
        if kw in kw_tags:
            existing = set(kw_tags[kw])
            for t in top_tags:
                if t not in existing:
                    kw_tags[kw].append(t)
                    if len(kw_tags[kw]) >= MAX_ASSOCIATIONS:
                        break
        else:
            kw_tags[kw] = top_tags

    return mapping


def print_stats(
    responses: list[dict],
    keyword_topic_counts: dict[str, Counter],
    keyword_tag_counts: dict[str, Counter],
) -> None:
    """Print summary statistics about what was discovered."""
    print(f"Responses analyzed: {len(responses)}")

    # Count unique topics and tags seen
    all_topics: set[str] = set()
    all_tags: set[str] = set()
    for resp in responses:
        result = resp["result"]
        for t in result.get("topics", []):
            all_topics.add(t)
        for t in result.get("tags", []):
            all_tags.add(t)

    print(f"Unique topics seen: {len(all_topics)}")
    print(f"Unique tags seen: {len(all_tags)}")

    # Count keyword associations above threshold
    kw_topic_above = sum(
        1
        for counter in keyword_topic_counts.values()
        for count in counter.values()
        if count >= MIN_COOCCURRENCE
    )
    kw_tag_above = sum(
        1
        for counter in keyword_tag_counts.values()
        for count in counter.values()
        if count >= MIN_COOCCURRENCE
    )
    print(
        f"Keyword-topic associations (>={MIN_COOCCURRENCE} co-occurrences): "
        f"{kw_topic_above}"
    )
    print(
        f"Keyword-tag associations (>={MIN_COOCCURRENCE} co-occurrences): "
        f"{kw_tag_above}"
    )

    # Top 20 most common keywords by total co-occurrence
    total_counts: Counter = Counter()
    for kw, counter in keyword_topic_counts.items():
        total_counts[kw] += sum(counter.values())
    for kw, counter in keyword_tag_counts.items():
        total_counts[kw] += sum(counter.values())

    print("\nTop 20 keywords by co-occurrence frequency:")
    for kw, count in total_counts.most_common(20):
        print(f"  {kw}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build tag/topic/content_type mapping from AI responses."
    )
    parser.add_argument(
        "--responses-dir",
        type=str,
        default=None,
        help="Path to a specific responses directory to scan. "
        "If omitted, scans corpus/, samples/, and benchmarks/ under ai-content/.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print statistics but do not write the output file.",
    )
    args = parser.parse_args()

    response_dirs = collect_response_dirs(args.responses_dir)

    if not response_dirs:
        print("No response directories found. Writing baseline mapping only.")
        if not args.dry_run:
            os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(BASELINE_MAPPING, f, ensure_ascii=False, indent=2)
            print(f"Wrote baseline mapping to {OUTPUT_FILE}")
        return

    print(f"Scanning response directories: {response_dirs}")
    responses = load_responses(response_dirs)

    if not responses:
        print("No valid responses found. Writing baseline mapping only.")
        if not args.dry_run:
            os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(BASELINE_MAPPING, f, ensure_ascii=False, indent=2)
            print(f"Wrote baseline mapping to {OUTPUT_FILE}")
        return

    print(f"Loaded {len(responses)} responses. Building co-occurrence maps...")
    keyword_topic_counts, keyword_tag_counts = build_cooccurrence_maps(responses)

    print_stats(responses, keyword_topic_counts, keyword_tag_counts)

    mapping = merge_statistical_into_baseline(
        BASELINE_MAPPING, keyword_topic_counts, keyword_tag_counts
    )

    if args.dry_run:
        print("\n[Dry run] Would write mapping to:", OUTPUT_FILE)
        new_kw_topics = set(mapping["keyword_to_topics"]) - set(
            BASELINE_MAPPING["keyword_to_topics"]
        )
        new_kw_tags = set(mapping["keyword_to_tags"]) - set(
            BASELINE_MAPPING["keyword_to_tags"]
        )
        print(f"New keyword_to_topics entries: {len(new_kw_topics)}")
        print(f"New keyword_to_tags entries: {len(new_kw_tags)}")
        if new_kw_topics:
            print("Sample new topic keywords:", sorted(new_kw_topics)[:10])
        if new_kw_tags:
            print("Sample new tag keywords:", sorted(new_kw_tags)[:10])
    else:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)
        print(f"\nWrote mapping to {OUTPUT_FILE}")
        new_kw_topics = set(mapping["keyword_to_topics"]) - set(
            BASELINE_MAPPING["keyword_to_topics"]
        )
        new_kw_tags = set(mapping["keyword_to_tags"]) - set(
            BASELINE_MAPPING["keyword_to_tags"]
        )
        print(f"  Baseline keyword_to_topics: {len(BASELINE_MAPPING['keyword_to_topics'])}")
        print(f"  Final keyword_to_topics: {len(mapping['keyword_to_topics'])}")
        print(f"  New entries from responses: {len(new_kw_topics)}")
        print(f"  Baseline keyword_to_tags: {len(BASELINE_MAPPING['keyword_to_tags'])}")
        print(f"  Final keyword_to_tags: {len(mapping['keyword_to_tags'])}")
        print(f"  New entries from responses: {len(new_kw_tags)}")


if __name__ == "__main__":
    main()
