import logging
import os
import re
from typing import Dict, List, Set

from fastapi.encoders import jsonable_encoder

from app.lib_bs4 import get_contents, is_rtl_tag
from app.lib_db import (delete_folder, insert_chapter, load_chapter, load_json,
                        write_file)
from app.lib_model import ProcessingReport, get_chapters, get_default_report, get_verses
from app.models import Chapter, Language, PartType, Translation, Verse
from app.models.people import ChainVerses, Narrator, NarratorIndex
from app.models.quran import NarratorChain, SpecialText
from app.narrator_linker import (
    extract_isnad_text, split_narrator_names, resolve_narrators,
    build_chain_parts, link_verse_narrators, strip_html,
)
from app.narrator_registry import NarratorRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# History:
# 1st commit detects 10455 narrators
# 2nd commit detects 6222 narrators
# 3rd commit detects 5744 narrators
# 4th commit detects 4860 narrators
# Revisit:
# http://localhost:4200/#/books/al-kafi:1:3:1#h5
# http://localhost:4200/#/books/al-kafi:5:1:4#h1 (first text is weird)

SPAN_PATTERN = re.compile(u"</?span[^>]*>")
NARRATOR_SPLIT_PATTERN = re.compile(u" (?:عَمَّنْ سَمِعَ|وَ سَمِعْتُ|وَ|جَمِيعاً عَنْ|جَمِيعاً عَنِ|عَنْ|عَنِ|إِلَى|قَالَ حَدَّثَنِي|عَمَّنْ|مِمَّنْ|مِنْهُمْ|رَفَعَهُ عَنْ|رَفَعَهُ إِلَى|فِي حَدِيثِ|رَفَعَهُ أَنَّ|رَفَعَهُ) ")
SKIP_PREFIX_PATTERN = re.compile(u"^([\\d\\s-]*|أخْبَرَنَا|أَخْبَرَنَا|وَ)* ")
NARRATORS_TEXT_PATTERN = re.compile(u"(.*?) (قَالَ|فِي هَذِهِ الْآيَةِ|يَرْفَعُهُ قَالَ|رَفَعَهُ قَالَ|فَكَانَ مِنْ سُؤَالِهِ أَنْ قَالَ|فِي قَوْلِ|فِي قَوْلِهِ|فِي|أَنَّ|يَقُولُ|فَقَالَ|مِثْلَهُ)")
NARRATORS_TEXT_FAILOVER_PATTERN = re.compile(u"(.*?\\( عليهم? السلام \\))")
NARRATORS_TEXT_CONTINUE_PATTERN = re.compile(u"\\s*(حَدَّثَنِي)\\s")

def extract_narrators(hadith: Verse, report: ProcessingReport = None) -> List[str]:
    if report is None:
        report = get_default_report()
    narrators = []
    first_line = hadith.text[0]

    # Step 1: Extract from beginning to the first qaal
    narrators_text_match = NARRATORS_TEXT_PATTERN.match(first_line)
    if not narrators_text_match:
        narrators_text_match = NARRATORS_TEXT_FAILOVER_PATTERN.match(first_line)
    if narrators_text_match:
        while narrators_text_match:
            if len(narrators_text_match.groups()) > 1:
                ending_phrase_len = len(narrators_text_match.groups()[-1]) + 1
            else: # in case of failover pattern we don't have anything to remove
                ending_phrase_len = 0
            end_index = narrators_text_match.end(0)
            if NARRATORS_TEXT_CONTINUE_PATTERN.match(first_line, end_index):
                narrators_text_match = NARRATORS_TEXT_PATTERN.match(first_line, end_index)
            else:
                break
        # Modify first line to only contain narrators
        narrators_text = first_line[:end_index]
        hadith_text = first_line[end_index:]
        hadith.text[0] = hadith_text
        if not hadith.narrator_chain:
            hadith.narrator_chain = NarratorChain()
            hadith.narrator_chain.parts = []
        hadith.narrator_chain.text = narrators_text

        # Step 2: trim unwanted prefixes/suffix
        if ending_phrase_len > 0:
            narrators_with_prefix = narrators_text[:-ending_phrase_len]
        else: # in case of failover pattern we don't have anything to remove
            narrators_with_prefix = narrators_text
        if narrators_with_prefix:
            narrators_without_prefix = SKIP_PREFIX_PATTERN.sub('', narrators_with_prefix)
            # Step 3: split the text to get narrators
            narrators = NARRATOR_SPLIT_PATTERN.split(narrators_without_prefix)
    else:
        logger.warning("Could not find narrators for %s", hadith.path)
        report.narrations_without_narrators += 1

    return narrators

def assign_narrator_id(narrators, narrator_index: NarratorIndex):
    narrator_ids = []

    for narrator in narrators:
        if narrator in narrator_index.name_id:
            index = narrator_index.name_id[narrator]
        else:
            index = narrator_index.last_id + 1
            narrator_index.last_id = index
            narrator_index.name_id[narrator] = index
            narrator_index.id_name[index] = narrator
        narrator_ids.append(index)

    return narrator_ids

def add_narrator_links(hadith: Verse, narrator_ids, narrator_index: NarratorIndex):
    if not hadith.narrator_chain or not hadith.narrator_chain.text:
        return
    
    line = hadith.narrator_chain.text
    for id in narrator_ids:
        narrator = narrator_index.id_name[id]
        beforeafter = line.split(narrator, 1)
        if len(beforeafter) != 2:
            raise Exception("Could not split " + hadith.path + " into two parts when splitting by " + narrator)
        before = beforeafter[0]
        line = beforeafter[1]

        if before:
            beforePart = SpecialText()
            beforePart.kind = "plain"
            beforePart.text = before
            hadith.narrator_chain.parts.append(beforePart)

        narratorPart = SpecialText()
        narratorPart.kind = "narrator"
        narratorPart.text = narrator
        narratorPart.path = f"/people/narrators/{id}"
        hadith.narrator_chain.parts.append(narratorPart)
    
    lastPart = SpecialText()
    lastPart.kind = "plain"
    lastPart.text = line
    hadith.narrator_chain.parts.append(lastPart)

def getCombinations(lst) -> Dict[int, List[List[int]]]:
    """Generate only full chain and direct pairs (not all subsequences).

    Full chains preserve the complete transmission record.
    Direct pairs preserve narrated_from/narrated_to metadata accuracy.
    """
    result = {}

    # Full chain
    if len(lst) > 1:
        full_key = '-'.join(str(n) for n in lst)
        for n in lst:
            if n not in result:
                result[n] = []
            result[n].append((full_key, lst))

    # Direct pairs (consecutive narrators only — needed for metadata)
    for i in range(len(lst) - 1):
        pair = [lst[i], lst[i + 1]]
        pair_key = '-'.join(str(n) for n in pair)
        if pair_key != '-'.join(str(n) for n in lst):  # skip if same as full chain
            for n in pair:
                if n not in result:
                    result[n] = []
                result[n].append((pair_key, pair))

    return result

def update_narrators(hadith: Verse, narrator_ids, narrators: Dict[int, Narrator], narrator_index: NarratorIndex) -> List[Narrator]:
    narrator_id_to_subchain_ids = getCombinations(narrator_ids)

    for id in narrator_ids:
        narrator = load_narrator(id, narrator_index, narrators)
        narrator.verse_paths.add(hadith.path)
        if id in narrator_id_to_subchain_ids:
            subchains = narrator_id_to_subchain_ids[id]
            for (subchain_key, subchain_ids) in subchains:
                if subchain_key not in narrator.subchains:
                    cv = ChainVerses()
                    cv.narrator_ids = subchain_ids
                    cv.verse_paths = set()
                    narrator.subchains[subchain_key] = cv
                narrator.subchains[subchain_key].verse_paths.add(hadith.path)

def process_chapter_verses(chapter: Chapter, narrator_index, narrators, report: ProcessingReport = None):
    if report is None:
        report = get_default_report()
    for hadith in chapter.verses:
        # Ran into issues with /books/al-kafi:7:3:15#h5
        if len(hadith.text) < 1:
            logger.warning("No Arabic text found in %s", hadith.path)
            continue
        hadith.text[0] = SPAN_PATTERN.sub("", hadith.text[0])
        try:
            narrator_names = extract_narrators(hadith, report)
            narrator_ids = assign_narrator_id(narrator_names, narrator_index)
            add_narrator_links(hadith, narrator_ids, narrator_index)
            update_narrators(hadith, narrator_ids, narrators, narrator_index)
            if hadith.narrator_chain:
                hadith.narrator_chain.text = None
        except Exception as e:
            logger.error('Ran into exception with hadith at ' + hadith.path)
            raise e

def process_chapter(kafi: Chapter, narrator_index, narrators: Dict[int, Narrator], report: ProcessingReport = None):
    if report is None:
        report = get_default_report()
    chapters = get_chapters(kafi)
    verses = get_verses(kafi)
    if chapters:
        for chapter in chapters:
            process_chapter(chapter, narrator_index, narrators, report)
    elif verses:
        process_chapter_verses(kafi, narrator_index, narrators, report)
    else:
        logger.info("Couldn't find anything to process in %s", kafi)

    return narrators

def load_narrator(narrator_id: int, narrator_index: NarratorIndex, narrators: Dict[int, Narrator]) -> Narrator:
    if narrator_id in narrators:
        return narrators[narrator_id]

    narrator_path = f"/people/narrators/{narrator_id}"
    try:
        narrator_json = load_json(narrator_path)
        if 'data' in narrator_json:
            narrator_json = narrator_json['data']
        narrator = Narrator(**narrator_json)
    except:
        narrator = Narrator()
        narrator_name = narrator_index.id_name[narrator_id]
        narrator.titles = {}
        narrator.titles[Language.AR.value] = narrator_name
        narrator.verse_paths = set()
        narrator.index = narrator_id
        narrator.path = narrator_path
        narrator.subchains = {}

    narrators[narrator_id] = narrator

    return narrator

def insert_narrators(narrators: Dict[int, Narrator]):
    for narrator in narrators.values():
        obj = {
            "index": narrator.index,
            "kind": "person_content",
            'data': jsonable_encoder(narrator)
        }
        logger.info(f"Inserting /people/narrators/{narrator.index}")
        write_file(f"/people/narrators/{narrator.index}", obj)

def load_narrator_index() -> NarratorIndex:
    try:
        narrator_index = load_json("/people/narrators/index")['data']
    except:
        narrator_index = {}

    narrators = NarratorIndex() 
    narrators.id_name = {int(k):v['titles']['ar'] for (k,v) in narrator_index.items()}
    narrators.name_id = {v: k for k, v in narrators.id_name.items()}
    narrators.last_id = max(narrators.id_name.keys(), default=0)

    return narrators

def compose_narrator_metadata(name: str, narrator: Narrator) -> dict:
    result = {}
    result['titles'] = {}
    result['titles'][Language.AR.value] = name
    two_chains = [x for x in narrator.subchains.values() if len(x.narrator_ids) == 2]
    narrated_to = [x for x in two_chains if x.narrator_ids[0] == narrator.index]
    narrated_from = [x for x in two_chains if x.narrator_ids[1] == narrator.index]
    result['narrations'] = len(narrator.verse_paths)
    result['narrated_from'] = len(narrated_from)
    result['narrated_to'] = len(narrated_to)
    return result

def insert_narrator_index(narrator_index: NarratorIndex, narrators: Dict[int, Narrator]):
    narrators_with_metadata = {id:compose_narrator_metadata(name, narrators[id]) for (id, name) in narrator_index.id_name.items()}
    obj = {
        "index": 'people',
        "kind": "person_list",
        'data': narrators_with_metadata
    }
    write_file("/people/narrators/index", obj)

def kafi_narrators(report: ProcessingReport = None):
    if report is None:
        report = get_default_report()
    # reset narrators
    delete_folder("/people/narrators")
    narrator_index = load_narrator_index()
    narrators = {}

    kafi = load_chapter("/books/complete/al-kafi")
    process_chapter(kafi, narrator_index, narrators, report)
    logger.info("Number of narrations without narrators: %d", report.narrations_without_narrators)

    insert_narrators(narrators)
    insert_narrator_index(narrator_index, narrators)
    insert_chapter(kafi)
    write_file("/books/complete/al-kafi", jsonable_encoder(kafi))


# ── New unified narrator processing using NarratorRegistry ──────────────


def _process_book_with_registry(
    book: Chapter,
    registry: NarratorRegistry,
    narrators: Dict[int, Narrator],
    narrator_id_name: Dict[int, str],
    report: ProcessingReport,
    use_undiacritized: bool = False,
):
    """Process a single book's narrator chains using the canonical registry.

    Walks the book recursively, extracting and linking narrators for each verse.
    """
    chapters = get_chapters(book)
    verses = get_verses(book)

    if chapters:
        for chapter in chapters:
            _process_book_with_registry(
                chapter, registry, narrators, narrator_id_name, report, use_undiacritized
            )
    elif verses:
        for hadith in verses:
            if not hadith.text or len(hadith.text) < 1:
                continue
            if hadith.path is None:
                # Colophons and editorial notes (e.g. "هَذَا آخِرُ كِتَابِ")
                # are parsed as verses but have no index/path — skip them
                continue
            hadith.text[0] = strip_html(hadith.text[0])
            try:
                canonical_ids = link_verse_narrators(
                    hadith, registry, use_undiacritized=use_undiacritized
                )
                if not canonical_ids:
                    report.narrations_without_narrators += 1

                # Update narrator tracking (verse_paths, subchains)
                for cid in canonical_ids:
                    narrator = _get_or_create_narrator(cid, registry, narrators, narrator_id_name)
                    narrator.verse_paths.add(hadith.path)

                # Build subchains
                narrator_id_to_subchain_ids = getCombinations(canonical_ids)
                for cid in canonical_ids:
                    narrator = narrators[cid]
                    if cid in narrator_id_to_subchain_ids:
                        for (subchain_key, subchain_ids) in narrator_id_to_subchain_ids[cid]:
                            if subchain_key not in narrator.subchains:
                                cv = ChainVerses()
                                cv.narrator_ids = subchain_ids
                                cv.verse_paths = set()
                                narrator.subchains[subchain_key] = cv
                            narrator.subchains[subchain_key].verse_paths.add(hadith.path)
            except Exception as e:
                logger.error("Narrator extraction error at %s: %s", hadith.path, e)


def _get_or_create_narrator(
    canonical_id: int,
    registry: NarratorRegistry,
    narrators: Dict[int, Narrator],
    narrator_id_name: Dict[int, str],
) -> Narrator:
    """Get or create a Narrator object for a canonical ID."""
    if canonical_id in narrators:
        return narrators[canonical_id]

    narrator = Narrator()
    narrator.index = canonical_id
    narrator.path = f"/people/narrators/{canonical_id}"
    narrator.titles = {}

    # Get names from registry
    ar_name = registry.get_name_ar(canonical_id)
    en_name = registry.get_name_en(canonical_id)
    if ar_name:
        narrator.titles[Language.AR.value] = ar_name
        narrator_id_name[canonical_id] = ar_name
    if en_name:
        narrator.titles[Language.EN.value] = en_name

    narrator.verse_paths = set()
    narrator.subchains = {}

    narrators[canonical_id] = narrator
    return narrator


def _insert_narrator_index_registry(
    narrators: Dict[int, Narrator],
    narrator_id_name: Dict[int, str],
):
    """Write narrator index using canonical names."""
    narrators_with_metadata = {}
    for cid, narrator in narrators.items():
        name = narrator_id_name.get(cid, narrator.titles.get(Language.AR.value, ""))
        narrators_with_metadata[cid] = compose_narrator_metadata(name, narrator)
        # Add English name to metadata if available
        en_name = narrator.titles.get(Language.EN.value)
        if en_name:
            narrators_with_metadata[cid]["titles"][Language.EN.value] = en_name

    obj = {
        "index": "people",
        "kind": "person_list",
        "data": narrators_with_metadata,
    }
    write_file("/people/narrators/index", obj)


def process_all_narrators(report: ProcessingReport = None):
    """Process narrators across ALL books using the canonical narrator registry.

    Replaces kafi_narrators() in the pipeline. Uses NarratorRegistry for
    consolidated IDs and narrator_linker for extraction.

    Steps:
    1. Load NarratorRegistry from canonical_narrators.json
    2. Delete /people/narrators/ folder
    3. Walk ALL books (al-kafi first, then thaqalayn_api books, then ghbook books)
    4. Extract and link narrators for each verse
    5. Write narrator files + narrator index
    6. Re-save complete book files with updated narrator_chain.parts
    """
    if report is None:
        report = get_default_report()

    registry = NarratorRegistry()
    if registry.narrator_count == 0:
        logger.warning("Narrator registry is empty — falling back to kafi_narrators()")
        kafi_narrators(report)
        return

    logger.info("Loaded narrator registry: %d canonical narrators", registry.narrator_count)

    # Reset narrators
    delete_folder("/people/narrators")

    narrators: Dict[int, Narrator] = {}
    narrator_id_name: Dict[int, str] = {}

    # Process Al-Kafi (well-diacritized, no undiacritized fallback needed)
    try:
        kafi = load_chapter("/books/complete/al-kafi")
        _process_book_with_registry(kafi, registry, narrators, narrator_id_name, report, use_undiacritized=False)
        logger.info("Al-Kafi: processed, %d narrators found so far", len(narrators))
    except Exception as e:
        logger.error("Failed to process Al-Kafi narrators: %s", e)

    # Process other complete books (thaqalayn_api + ghbook)
    dest_dir = os.environ.get("DESTINATION_DIR", "../ThaqalaynData/")
    complete_dir = os.path.join(dest_dir, "books", "complete")
    complete_books = {}

    if os.path.isdir(complete_dir):
        for filename in sorted(os.listdir(complete_dir)):
            if not filename.endswith(".json"):
                continue
            book_slug = filename[:-5]  # Remove .json
            if book_slug in ("al-kafi", "quran"):
                continue  # Already processed or skip Quran

            try:
                book = load_chapter(f"/books/complete/{book_slug}")
                _process_book_with_registry(
                    book, registry, narrators, narrator_id_name, report,
                    use_undiacritized=True,  # Non-Kafi books may have less diacritization
                )
                complete_books[book_slug] = book
                logger.info("Processed %s, %d narrators total", book_slug, len(narrators))
            except Exception as e:
                logger.error("Failed to process %s narrators: %s", book_slug, e)

    logger.info("Total narrators found across all books: %d", len(narrators))
    logger.info("Narrations without narrators: %d", report.narrations_without_narrators)

    # Add shared chain relations before writing files
    from app.link_chains import collect_shared_chains, build_verse_relations, apply_shared_chain_relations
    chain_groups = collect_shared_chains(narrators)
    logger.info("Found %d shared chains (3+ narrators, 2-20 occurrences)", len(chain_groups))
    verse_relations = build_verse_relations(chain_groups)
    all_books = [kafi] + list(complete_books.values())
    updated = apply_shared_chain_relations(all_books, verse_relations)
    logger.info("Added 'Shared Chain' relations to %d verses", updated)

    # Write narrator files
    insert_narrators(narrators)
    _insert_narrator_index_registry(narrators, narrator_id_name)

    # Re-save Al-Kafi with updated narrator_chain.parts
    try:
        insert_chapter(kafi)
        write_file("/books/complete/al-kafi", jsonable_encoder(kafi))
    except Exception as e:
        logger.error("Failed to re-save Al-Kafi: %s", e)

    # Re-save other complete books
    for book_slug, book in complete_books.items():
        try:
            insert_chapter(book)
            write_file(f"/books/complete/{book_slug}", jsonable_encoder(book))
        except Exception as e:
            logger.error("Failed to re-save %s: %s", book_slug, e)

    generate_featured_narrators()


# Map of known Imam kunyas/titles to their canonical English names.
# Order matters: more specific patterns MUST come before generic ones.
_IMAM_LABELS = [
    # Specific compound names first
    ("عَلِيِّ بْنِ الْحُسَيْنِ", "Imam al-Sajjad"),
    ("الْحُسَيْنِ بْنِ عَلِيٍّ", "Imam al-Husayn"),
    ("الْحَسَنِ بْنِ عَلِيٍّ", "Imam al-Hasan"),
    ("مُوسَى بْنِ جَعْفَرٍ", "Imam al-Kadhim"),
    ("مُوسَى بْنُ جَعْفَرٍ", "Imam al-Kadhim"),
    ("جَعْفَرِ بْنِ مُحَمَّدٍ", "Imam al-Sadiq"),
    ("عَلِيِّ بْنِ مُوسَى", "Imam al-Ridha"),
    # Specific kunyas (with qualifiers)
    ("أَبِي جَعْفَرٍ الثَّانِي", "Imam al-Jawad"),
    ("أَبِي جَعْفَرٍ الْأَوَّلِ", "Imam al-Baqir"),
    ("أَبِي الْحَسَنِ الثَّالِثِ", "Imam al-Hadi"),
    ("أَبِي الْحَسَنِ الْعَسْكَرِيِّ", "Imam al-Askari"),
    ("صَاحِبِ الْعَسْكَرِ", "Imam al-Askari"),
    ("أَبِي الْحَسَنِ الرِّضَا", "Imam al-Ridha"),
    ("أَبِي الْحَسَنِ مُوسَى", "Imam al-Kadhim"),
    ("أَبِي الْحَسَنِ الْأَوَّلِ", "Imam al-Kadhim"),
    ("أَبِي الْحَسَنِ الْمَاضِي", "Imam al-Kadhim"),
    ("الْعَبْدِ الصَّالِحِ", "Imam al-Kadhim"),
    ("عَبْدٍ صَالِحٍ", "Imam al-Kadhim"),
    # Generic kunyas
    ("أَبِي عَبْدِ اللَّهِ", "Imam al-Sadiq"),
    ("الصَّادِقِ", "Imam al-Sadiq"),
    ("أَبِي جَعْفَرٍ", "Imam al-Baqir"),
    ("الرِّضَا", "Imam al-Ridha"),
    ("أَبِي إِبْرَاهِيمَ", "Imam al-Kadhim"),
    ("أَبِي الْحَسَنِ", "Imam (Abu al-Hasan)"),
    ("أَمِيرِ الْمُؤْمِنِينَ", "Amir al-Mu'minin"),
    # Very generic — last resort
    ("الْحُسَيْنِ", "Imam al-Husayn"),
    ("عَلِيٍّ", "Imam Ali"),
]


def generate_featured_narrators():
    """Generate people/narrators/featured.json with Imam data from the narrator index."""
    import unicodedata

    try:
        index_data = load_json("/people/narrators/index")['data']
    except Exception:
        logger.warning("Could not load narrator index for featured generation")
        return

    # Normalize combining characters so diacritic order doesn't matter
    def nfc(s):
        return unicodedata.normalize('NFC', s)

    nfc_labels = [(nfc(pat), label) for pat, label in _IMAM_LABELS]

    # Find narrators with عليه السلام in their title (Imam marker)
    imam_entries = []
    for nid_str, info in index_data.items():
        title_ar = info.get('titles', {}).get('ar', '')
        if 'عليه السلام' not in title_ar:
            continue
        narrations = info.get('narrations', 0)
        if narrations < 1:
            continue

        # Determine English label from known patterns (check longest patterns first)
        title_nfc = nfc(title_ar)
        label_en = None
        for pattern, label in nfc_labels:
            if pattern in title_nfc:
                label_en = label
                break

        if not label_en:
            label_en = "Imam"

        imam_entries.append({
            "id": int(nid_str),
            "name_ar": title_ar,
            "name_en": label_en,
            "narrations": narrations,
        })

    # Sort by narrations descending, pick top entry per English label for featured list
    imam_entries.sort(key=lambda x: -x["narrations"])
    seen_labels = set()
    featured = []
    for entry in imam_entries:
        if entry["name_en"] not in seen_labels:
            seen_labels.add(entry["name_en"])
            featured.append(entry)

    # Also include all entries (for badge detection on narrator pages)
    all_imam_ids = {e["id"]: {"name_en": e["name_en"], "name_ar": e["name_ar"]} for e in imam_entries}

    obj = {
        "index": "featured",
        "kind": "person_list",
        "data": {
            "featured": featured,
            "imam_ids": all_imam_ids,
        }
    }
    write_file("/people/narrators/featured", obj)
    logger.info("Generated featured narrators: %d featured, %d total imam entries", len(featured), len(all_imam_ids))

