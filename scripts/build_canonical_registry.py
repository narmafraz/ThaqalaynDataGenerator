"""Bootstrap script to build canonical_narrators.json from existing data.

Inputs:
- ThaqalaynData/people/narrators/index.json (4,860 narrator entries)
- ThaqalaynDataSources/ai-pipeline-data/narrator_templates.json (1,074 entries with metadata)

Algorithm:
1. Load all narrator entries (id -> Arabic name, narration count)
2. Normalize each name via normalize_arabic()
3. Group by normalized form -> collapses diacritical variants
4. For each cluster, check narrator_templates.json for metadata
5. Merge clusters that share the same known_identity
6. Assign canonical IDs ordered by decreasing narration count
7. Write canonical_narrators.json

Usage:
    cd ThaqalaynDataGenerator
    source .venv/Scripts/activate
    PYTHONPATH="$PWD:$PWD/app" SOURCE_DATA_DIR="../ThaqalaynDataSources/" python scripts/build_canonical_registry.py
"""

import json
import logging
import os
import sys

# Fix Windows console encoding
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.arabic_normalization import normalize_arabic
from app.config import AI_PIPELINE_DATA_DIR, SOURCE_DATA_DIR

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DESTINATION_DIR = os.environ.get("DESTINATION_DIR", "../ThaqalaynData/")
NARRATOR_INDEX_PATH = os.path.join(DESTINATION_DIR, "people", "narrators", "index.json")
TEMPLATES_PATH = os.path.join(AI_PIPELINE_DATA_DIR, "narrator_templates.json")
OUTPUT_PATH = os.path.join(AI_PIPELINE_DATA_DIR, "canonical_narrators.json")


def load_narrator_index(path: str) -> dict:
    """Load narrator index. Returns {id_int: {"ar": name, "narrations": count}}."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    narrators = {}
    for id_str, entry in data.get("data", {}).items():
        narrators[int(id_str)] = {
            "ar": entry["titles"]["ar"],
            "narrations": entry.get("narrations", 0),
            "narrated_from": entry.get("narrated_from", 0),
            "narrated_to": entry.get("narrated_to", 0),
        }
    return narrators


def load_templates(path: str) -> dict:
    """Load narrator templates. Returns {ar_name: template_entry}."""
    if not os.path.isfile(path):
        logger.warning("Templates file not found: %s", path)
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("narrators", {})


def build_registry(narrator_index: dict, templates: dict) -> dict:
    """Build canonical narrator registry from existing data.

    Returns dict ready for JSON serialization.
    """
    # Step 1: Group by normalized form
    # normalized_form -> [(old_id, ar_name, narrations)]
    clusters = {}
    for old_id, entry in narrator_index.items():
        ar_name = entry["ar"]
        narrations = entry["narrations"]
        normalized = normalize_arabic(ar_name)

        if normalized not in clusters:
            clusters[normalized] = []
        clusters[normalized].append((old_id, ar_name, narrations))

    logger.info("Grouped %d narrators into %d normalized clusters",
                len(narrator_index), len(clusters))

    # Step 2: Build template lookup by normalized form
    template_by_normalized = {}
    for ar_name, template in templates.items():
        normalized = normalize_arabic(ar_name)
        template_by_normalized[normalized] = {
            "ar_name": ar_name,
            **template,
        }

    # Step 3: Merge clusters that share the same known_identity
    # known_identity -> list of normalized forms that should merge
    identity_to_clusters = {}
    cluster_identity = {}  # normalized_form -> known_identity

    for normalized, members in clusters.items():
        # Check if any member matches a template
        template = template_by_normalized.get(normalized)
        if template and template.get("known_identity"):
            identity = template["known_identity"]
            cluster_identity[normalized] = identity
            if identity not in identity_to_clusters:
                identity_to_clusters[identity] = []
            identity_to_clusters[identity].append(normalized)

    # Now merge clusters with same identity
    merged_clusters = {}  # merge_key -> {members: [...], template: ..., identity: ...}
    merged_by_normalized = {}  # normalized_form -> merge_key

    for identity, norm_forms in identity_to_clusters.items():
        if len(norm_forms) > 1:
            # Multiple normalized forms for same person — merge
            merge_key = norm_forms[0]
            for nf in norm_forms:
                merged_by_normalized[nf] = merge_key
        else:
            merged_by_normalized[norm_forms[0]] = norm_forms[0]

    # Build final merged cluster list
    for normalized, members in clusters.items():
        merge_key = merged_by_normalized.get(normalized, normalized)
        if merge_key not in merged_clusters:
            merged_clusters[merge_key] = {
                "members": [],
                "template": None,
                "identity": cluster_identity.get(merge_key),
            }
        merged_clusters[merge_key]["members"].extend(members)

        # Attach template if available
        template = template_by_normalized.get(normalized)
        if template and not merged_clusters[merge_key]["template"]:
            merged_clusters[merge_key]["template"] = template

    logger.info("After identity-based merging: %d canonical narrators", len(merged_clusters))

    # Step 4: Assign canonical IDs ordered by total narration count (desc)
    scored = []
    for merge_key, cluster in merged_clusters.items():
        total_narrations = sum(m[2] for m in cluster["members"])
        scored.append((total_narrations, merge_key, cluster))

    scored.sort(key=lambda x: (-x[0], x[1]))

    # Step 5: Build output
    narrators_out = {}
    for canonical_id_zero, (total_narrations, merge_key, cluster) in enumerate(scored):
        canonical_id = canonical_id_zero + 1
        members = cluster["members"]
        template = cluster["template"]

        # Pick canonical name: highest narration count variant
        members.sort(key=lambda m: -m[2])
        canonical_ar = members[0][1]

        # Collect all variant names (dedup, preserve order)
        seen_variants = set()
        variants_ar = []
        for _, ar_name, _ in members:
            if ar_name != canonical_ar and ar_name not in seen_variants:
                variants_ar.append(ar_name)
                seen_variants.add(ar_name)

        # Get English name and role from template
        canonical_en = None
        role = "narrator"
        disambiguation_context = None

        if template:
            canonical_en = template.get("name_en")
            role = template.get("role", "narrator")
            confidence = template.get("identity_confidence", "")
            if confidence == "ambiguous":
                # Mark for manual disambiguation
                disambiguation_context = f"NEEDS_REVIEW: {template.get('known_identity', 'unknown')}"

        old_ids = sorted(set(m[0] for m in members))

        entry = {
            "canonical_name_ar": canonical_ar,
            "canonical_name_en": canonical_en,
            "role": role,
            "variants_ar": variants_ar,
            "disambiguation_context": disambiguation_context,
            "old_ids": old_ids,
        }
        narrators_out[str(canonical_id)] = entry

    result = {
        "version": "1.0.0",
        "last_id": len(narrators_out),
        "narrators": narrators_out,
    }

    return result


def main():
    logger.info("Loading narrator index from %s", NARRATOR_INDEX_PATH)
    narrator_index = load_narrator_index(NARRATOR_INDEX_PATH)
    logger.info("Loaded %d narrators", len(narrator_index))

    logger.info("Loading templates from %s", TEMPLATES_PATH)
    templates = load_templates(TEMPLATES_PATH)
    logger.info("Loaded %d templates", len(templates))

    registry = build_registry(narrator_index, templates)

    logger.info("Writing registry to %s", OUTPUT_PATH)
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)

    logger.info("Done! %d canonical narrators (from %d originals)",
                registry["last_id"], len(narrator_index))

    # Print top 20 for review
    print("\nTop 20 narrators by narration count:")
    for id_str, entry in list(registry["narrators"].items())[:20]:
        ar = entry["canonical_name_ar"]
        en = entry.get("canonical_name_en") or "?"
        variants = len(entry.get("variants_ar", []))
        old = len(entry.get("old_ids", []))
        print(f"  #{id_str}: {ar} ({en}) — {variants} variants, {old} old IDs")


if __name__ == "__main__":
    main()
