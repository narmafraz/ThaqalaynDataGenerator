import itertools
import logging
import re
from typing import Dict, List, Set

from fastapi.encoders import jsonable_encoder

from app.lib_bs4 import get_contents, is_rtl_tag
from app.lib_db import (delete_file, insert_chapter, load_chapter, load_json,
                        write_file)
from app.lib_model import SEQUENCE_ERRORS, get_chapters, get_verses
from app.models import Chapter, Language, PartType, Translation, Verse
from app.models.people import ChainVerses, Narrator, NarratorIndex
from app.models.quran import NarratorChain, SpecialText

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# History:
# 1st commit detects 10455 narrators
# 2nd commit detects 6222 narrators
# Revisit:
# http://localhost:4200/#/books/al-kafi:1:3:1#h5

SPAN_PATTERN = re.compile(u"</?span[^>]*>")
NARRATOR_SPLIT_PATTERN = re.compile(u" (?:وَ|جَمِيعاً عَنْ|جَمِيعاً عَنِ|عَنْ|عَنِ|إِلَى|قَالَ حَدَّثَنِي|عَمَّنْ|مِمَّنْ|مِنْهُمْ) ")
SKIP_PREFIX_PATTERN = re.compile(u"^([\\d\\s-]*|أخْبَرَنَا|أَخْبَرَنَا)* ")
NARRATORS_TEXT_PATTERN = re.compile(u"(.*?) قَالَ")
NARRATORS_TEXT_CONTINUE_PATTERN = re.compile(u"\\s*(حَدَّثَنِي)\\s")

def extract_narrators(hadith: Verse) -> List[str]:
    narrators = []
    first_line = hadith.text[0]

    # Step 1: Extract from beginning to the first qaal
    narrators_text_match = NARRATORS_TEXT_PATTERN.match(first_line)
    if narrators_text_match:
        while narrators_text_match:
            end_index = narrators_text_match.end(0)
            if NARRATORS_TEXT_CONTINUE_PATTERN.match(first_line, end_index):
                narrators_text_match = NARRATORS_TEXT_PATTERN.match(first_line, end_index)
            else:
                break
        narrators_text = first_line[:end_index]
        hadith_text = first_line[end_index:]
        hadith.text[0] = hadith_text
        if not hadith.narrator_chain:
            hadith.narrator_chain = NarratorChain()
            hadith.narrator_chain.parts = []
        hadith.narrator_chain.text = narrators_text

        # Step 2: trim unwanted prefixes
        narrators_with_prefix = narrators_text[:-6]
        if narrators_with_prefix:
            narrators_without_prefix = SKIP_PREFIX_PATTERN.sub('', narrators_with_prefix)
            # Step 3: split the text to get narrators
            narrators = NARRATOR_SPLIT_PATTERN.split(narrators_without_prefix)
    else:
        logger.warn("Could not find narrators for %s", hadith.path)

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
    result = {}

    for i, j in itertools.combinations(range(len(lst) + 1), 2):
        combi = lst[i:j]
        combi_key = '-'.join(str(n) for n in combi)
        if len(combi) > 1:
            for n in combi:
                if n not in result:
                    result[n] = []
                result[n].append((combi_key, combi))

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

def process_chapter_verses(chapter: Chapter, narrator_index, narrators):
    for hadith in chapter.verses:
        # Ran into issues with /books/al-kafi:7:3:15#h5
        if len(hadith.text) < 1:
            logger.warn("No Arabic text found in %s", hadith.path)
            continue
        hadith.text[0] = SPAN_PATTERN.sub("", hadith.text[0])
        try:
            narrator_names = extract_narrators(hadith)
            narrator_ids = assign_narrator_id(narrator_names, narrator_index)
            add_narrator_links(hadith, narrator_ids, narrator_index)
            update_narrators(hadith, narrator_ids, narrators, narrator_index)
        except Exception as e:
            logger.error('Ran into exception with hadith at ' + hadith.path)
            raise e

def process_chapter(kafi: Chapter, narrator_index, narrators: Dict[int, Narrator]):
    chapters = get_chapters(kafi)
    verses = get_verses(kafi)
    if chapters:
        for chapter in chapters:
            process_chapter(chapter, narrator_index, narrators)
    elif verses:
        process_chapter_verses(kafi, narrator_index, narrators)
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

def load_narrator_index() -> NarratorIndex:
    try:
        narrator_index = load_json("/people/narrators/index")['data']
    except:
        narrator_index = {}

    narrators = NarratorIndex() 
    narrators.id_name = narrator_index
    narrators.name_id = {v: k for k, v in narrator_index.items()}
    narrators.last_id = max(narrator_index.keys(), default=0)

    return narrators

def insert_narrators(narrators: Dict[int, Narrator]):
    for narrator in narrators.values():
        obj = {
            "index": narrator.index,
            "kind": "person_content",
            'data': jsonable_encoder(narrator)
        }
        write_file(f"/people/narrators/{narrator.index}", obj)

def insert_narrator_index(narrator_index: NarratorIndex):
    obj = {
        "index": 'people',
        "kind": "person_list",
        'data': jsonable_encoder(narrator_index.id_name)
    }
    write_file("/people/narrators/index", obj)

def kafi_narrators():
    # reset narrators
    delete_file("/people/narrators/index")
    narrator_index = load_narrator_index()
    narrators = {}

    kafi = load_chapter("/books/complete/al-kafi")
    process_chapter(kafi, narrator_index, narrators)

    insert_narrators(narrators)
    insert_narrator_index(narrator_index)
    insert_chapter(kafi)
    # write_file("/books/complete/al-kafi", jsonable_encoder(kafi))

