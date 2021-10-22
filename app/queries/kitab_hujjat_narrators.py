# Aim is to get Quran verses in the first 30 chapters of Kitab al Hujjat of Al-Kafi

import csv
import glob
import json
import logging
import os
import re
from pprint import pprint
from typing import Dict, List, Tuple

from app.lib_db import load_chapter, load_json
from app.models.people import ChainVerses, Narrator, NarratorIndex
from app.models.quran import Chapter, Verse
from fastapi.encoders import jsonable_encoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ABIH = "أَبِيهِ"
ABU = "أَبِي"
IMAM_NAMES = [
	"أَبِي الْحَسَنِ ( عليه السلام )",
	"أَبِي الْحَسَنِ الرِّضَا ( عليه السلام )",
	"أَبِي الْحَسَنِ مُوسَى ( عليه السلام )",
	"أَبِي جَعْفَرٍ ( عليه السلام )",
	"أَبِي عَبْدِ اللَّهِ ( عليه السلام )",
	"أَحَدِهِمَا ( عليهما السلام )",
	"أَمِيرِ الْمُؤْمِنِينَ ( صلوات الله عليه )",
	"الْعَبْدِ الصَّالِحِ ( عليه السلام )",
	"بَعْضِ أَصْحَابِ أَمِيرِ الْمُؤْمِنِينَ ( عليه السلام )",
	"كَانَ مَكِيناً عِنْدَ الرِّضَا ( عليه السلام )",
	"يَثِقُ بِهِ مِنْ أَصْحَابِ أَمِيرِ الْمُؤْمِنِينَ ( عليه السلام )"
]

class NarratorMetadata():
	id: int
	narration_indices: List[int] = []
	inverted_positions_in_chains: List[int] = []

class NarratorResult():
	narrator_pairs: List[Tuple[str, str]] = []
	ahadith: List[Verse] = []
	narrator_metadata: Dict[str, NarratorMetadata] = {}
	next_narrator_id = 0

def query_chapter(chapter: Chapter, results: NarratorResult):
	for hadith in chapter.verses:
		parts = hadith.narrator_chain.parts
		narrator_parts = filter(lambda p: p.kind == 'narrator', parts)
		narrator_names = list(map(lambda p: p.text, narrator_parts))

		if narrator_names[-1] in IMAM_NAMES:
			narrator_names = narrator_names[:-1]
		abih_index = narrator_names.index(ABIH) if ABIH in narrator_names else -1
		if abih_index >= 0:
			narrator_names[abih_index] = ABU + " " +  narrator_names[abih_index-1]

		narrator_names_len = len(narrator_names)
		ahadith_len = len(results.ahadith)
		for narrator_index, narrator_name in enumerate(narrator_names):
			if narrator_name not in results.narrator_metadata:
				results.narrator_metadata[narrator_name] = NarratorMetadata()
				results.narrator_metadata[narrator_name].id = results.next_narrator_id
				results.next_narrator_id += 1

			results.narrator_metadata[narrator_name].narration_indices.append(ahadith_len)
			results.narrator_metadata[narrator_name].inverted_positions_in_chains.append(narrator_names_len - narrator_index)

		narrator_pairs = zip(narrator_names, narrator_names[1:])
		results.narrator_pairs.extend(narrator_pairs)
		results.ahadith.append(hadith)

def query_book() -> NarratorResult:
	results = NarratorResult()
	for chno in range(1, 31):
		chapter = load_chapter(f"/books/al-kafi/1/4/{chno}")
		query_chapter(chapter, results)
	return results

def add_node(nodes, results: NarratorResult, narrator_name, counts):
	id = results.narrator_metadata[narrator_name].id

	if id not in nodes:
		node = {}
		node['id'] = id
		node['label'] = narrator_name
		nodes[id] = node

	if id in counts:
		counts[id] += 1
	else:
		counts[id] = 1
	

def add_edge(edges, results: NarratorResult, n1_name, n2_name):
	id1 = results.narrator_metadata[n1_name].id
	id2 = results.narrator_metadata[n2_name].id
	edge_id = f"{id1}-{id2}"
	if edge_id not in edges:
		edge = {}
		edge['from'] = id1
		edge['to'] = id2
		edge['value'] = 1
		edge['arrows'] = 'from'
		edges[edge_id] = edge
	else:
		edge = edges[edge_id]
		edge['value'] += 1
		edge['label'] = f"{edge['value']} narrations"

def visjsify(narrator_index: Dict[int, Narrator], results: NarratorResult):
	nodes = {}
	edges = {}
	from_counts = {}
	to_counts = {}
	for result in results.narrator_pairs:
		n1_name = result[0]
		n2_name = result[1]

		add_node(nodes, results, n1_name, to_counts)
		add_node(nodes, results, n2_name, from_counts)
		add_edge(edges, results, n1_name, n2_name)

	for k,v in nodes.items():
		v['label'] = f"{v['label']}\n[{from_counts[k] if k in from_counts else 0}, {to_counts[k] if k in to_counts else 0}]"
	return list(nodes.values()), list(edges.values())

def main():
	with open("app\queries\graph_template.html", 'r', encoding='utf-8') as templatefile:
		template = templatefile.read()
	narrator_index = load_json("/people/narrators/index")['data']
	results = query_book()
	(nodes, edges) = visjsify(narrator_index, results)

	htmlfile = template.replace('%%nodes%%', json.dumps(nodes, indent=2)).replace('%%edges%%', json.dumps(edges, indent=2)).replace('%%tabledata%%', "[]")
	with open("kitab_hujjat_narrators.html", 'w', newline='', encoding='utf-8') as f:
		f.write(htmlfile)

main()
