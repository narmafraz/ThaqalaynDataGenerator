"""Biography enrichment pipeline for narrator data.

Orchestrates the full workflow of:
1. Loading narrator index from ThaqalaynData
2. Matching narrator names to WikiShia articles
3. Scraping biography data from WikiShia
4. Generating English transliterations
5. Writing enriched narrator JSON files

Usage:
    from app.wikishia.biography import enrich_narrators
    enrich_narrators()  # Runs the full pipeline
"""

import json
import logging
import os
from typing import Dict, List, Optional

from app.lib_db import load_json, write_file
from app.wikishia.name_matching import MatchResult, NameMatcher
from app.wikishia.scraper import BiographyData, WikiShiaScraper
from app.wikishia.transliteration import transliterate_arabic

logger = logging.getLogger(__name__)

# Path to manual name mappings
MANUAL_MAPPING_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "manual_mappings.json"
)

# Path to cached biography data
BIOGRAPHY_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "raw", "wikishia"
)


def load_narrator_index() -> Dict[int, str]:
    """Load narrator index from ThaqalaynData.

    Returns:
        Dict mapping narrator ID to Arabic name.
    """
    try:
        index_data = load_json("/people/narrators/index")
        narrator_data = index_data.get("data", {})
        return {
            int(nid): info["titles"]["ar"]
            for nid, info in narrator_data.items()
            if "titles" in info and "ar" in info["titles"]
        }
    except Exception as e:
        logger.error("Failed to load narrator index: %s", e)
        return {}


def load_biography_cache() -> Dict[str, dict]:
    """Load cached biography data from disk.

    Returns:
        Dict mapping WikiShia title to biography dict.
    """
    cache_file = os.path.join(BIOGRAPHY_CACHE_DIR, "biographies.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_biography_cache(cache: Dict[str, dict]):
    """Save biography cache to disk."""
    os.makedirs(BIOGRAPHY_CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(BIOGRAPHY_CACHE_DIR, "biographies.json")
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    logger.info("Saved %d biographies to cache", len(cache))


def enrich_narrator_json(narrator_id: int, biography: Optional[dict],
                         transliteration: Optional[str]) -> bool:
    """Add biography and transliteration data to a narrator's JSON file.

    Loads the existing narrator JSON, adds biography fields, and writes back.

    Args:
        narrator_id: Narrator ID.
        biography: Biography dict from BiographyData.to_dict(), or None.
        transliteration: English transliteration string, or None.

    Returns:
        True if the file was updated, False on error.
    """
    narrator_path = "/people/narrators/{}".format(narrator_id)
    try:
        narrator_json = load_json(narrator_path)
    except Exception as e:
        logger.warning("Could not load narrator %d: %s", narrator_id, e)
        return False

    data = narrator_json.get("data", {})
    modified = False

    # Add transliteration to titles
    if transliteration:
        if "titles" not in data:
            data["titles"] = {}
        if "en" not in data["titles"] or not data["titles"]["en"]:
            data["titles"]["en"] = transliteration
            modified = True

    # Add biography fields
    if biography:
        for key in ("birth_date", "death_date", "era", "reliability",
                     "teachers", "students", "biography_summary",
                     "biography_source", "wikishia_url"):
            if key in biography and biography[key]:
                data[key] = biography[key]
                modified = True

    if modified:
        narrator_json["data"] = data
        write_file(narrator_path, narrator_json)
        return True

    return False


def enrich_narrator_index_with_transliterations(
    transliterations: Dict[int, str]
):
    """Add English transliterations to the narrator index file.

    Updates each narrator entry in index.json with an 'en' title.

    Args:
        transliterations: Dict mapping narrator ID to English transliteration.
    """
    try:
        index_json = load_json("/people/narrators/index")
    except Exception as e:
        logger.error("Could not load narrator index: %s", e)
        return

    data = index_json.get("data", {})
    updated = 0

    for nid_str, entry in data.items():
        nid = int(nid_str)
        if nid in transliterations:
            if "titles" not in entry:
                entry["titles"] = {}
            if "en" not in entry["titles"] or not entry["titles"]["en"]:
                entry["titles"]["en"] = transliterations[nid]
                updated += 1

    index_json["data"] = data
    write_file("/people/narrators/index", index_json)
    logger.info("Updated %d narrator index entries with English names", updated)


def run_matching_pipeline(
    narrator_names: Dict[int, str],
    wikishia_titles: Optional[List[str]] = None,
    manual_mapping_path: str = MANUAL_MAPPING_PATH,
) -> Dict[int, MatchResult]:
    """Run the 5-step name matching pipeline.

    Args:
        narrator_names: Dict mapping narrator ID to Arabic name.
        wikishia_titles: Optional list of WikiShia titles. If None, uses sample data.
        manual_mapping_path: Path to manual mapping JSON file.

    Returns:
        Dict mapping narrator ID to MatchResult.
    """
    matcher = NameMatcher()
    matcher.load_narrator_names(narrator_names)

    if wikishia_titles:
        matcher.load_wikishia_titles(wikishia_titles)

    if os.path.exists(manual_mapping_path):
        matcher.load_manual_mapping_file(manual_mapping_path)

    return matcher.run_pipeline()


def enrich_narrators(
    scrape: bool = False,
    wikishia_titles: Optional[List[str]] = None,
):
    """Run the full narrator biography enrichment pipeline.

    This is the main entry point for the biography enrichment feature.

    Args:
        scrape: If True, actually scrape WikiShia (requires network access).
                If False, use cached/sample data only.
        wikishia_titles: Optional list of WikiShia article titles for matching.
    """
    logger.info("Starting narrator biography enrichment pipeline")

    # Step 1: Load narrator index
    narrator_names = load_narrator_index()
    if not narrator_names:
        logger.error("No narrator names loaded, aborting")
        return

    logger.info("Loaded %d narrator names", len(narrator_names))

    # Step 2: Generate transliterations for all names
    from app.wikishia.transliteration import transliterate_narrator_index
    transliterations = transliterate_narrator_index(narrator_names)
    logger.info("Generated %d transliterations", len(transliterations))

    # Step 3: Run name matching pipeline
    match_results = run_matching_pipeline(narrator_names, wikishia_titles)
    matched = {nid: r for nid, r in match_results.items() if r.matched_title}
    logger.info("Matched %d/%d narrators to WikiShia articles",
                len(matched), len(narrator_names))

    # Step 4: Load/scrape biography data
    bio_cache = load_biography_cache()
    if scrape and matched:
        scraper = WikiShiaScraper()
        for nid, result in matched.items():
            title = result.matched_title
            if title not in bio_cache:
                logger.info("Scraping biography for: %s", title)
                bio = scraper.get_biography(title)
                if bio:
                    bio_cache[title] = bio.to_dict()
        save_biography_cache(bio_cache)

    # Step 5: Write enriched narrator data
    enriched = 0
    for nid in narrator_names:
        bio_dict = None
        if nid in matched:
            title = matched[nid].matched_title
            if title in bio_cache:
                bio_dict = bio_cache[title]

        transliteration = transliterations.get(nid)
        if enrich_narrator_json(nid, bio_dict, transliteration):
            enriched += 1

    logger.info("Enriched %d narrator files", enriched)

    # Step 6: Update narrator index with transliterations
    enrich_narrator_index_with_transliterations(transliterations)

    logger.info("Narrator biography enrichment complete")
