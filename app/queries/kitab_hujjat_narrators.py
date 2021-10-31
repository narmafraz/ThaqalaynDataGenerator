# Aim is to get Quran verses in the first 30 chapters of Kitab al Hujjat of Al-Kafi

import csv
import glob
import json
import logging
import os
import re
from pprint import pprint
from statistics import mean
from typing import Dict, List, Tuple

from app.lib_db import load_chapter, load_json
from app.models.people import ChainVerses, Narrator, NarratorIndex
from app.models.quran import Chapter, Verse
from colour import Color
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_CHAPTER=1
END_CHAPTER=10

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

class NarratorMetadata(BaseModel):
	id: int
	narration_indices: List[int] = []
	inverted_positions_in_chains: List[int] = []

class NarratorResult(BaseModel):
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
				results.narrator_metadata[narrator_name] = NarratorMetadata(id = results.next_narrator_id)
				# results.narrator_metadata[narrator_name].id = results.next_narrator_id
				results.next_narrator_id += 1

			results.narrator_metadata[narrator_name].narration_indices.append(ahadith_len)
			results.narrator_metadata[narrator_name].inverted_positions_in_chains.append(narrator_names_len - narrator_index)

		narrator_pairs = zip(narrator_names, narrator_names[1:])
		results.narrator_pairs.extend(narrator_pairs)
		results.ahadith.append(hadith)

def query_book() -> NarratorResult:
	results = NarratorResult()
	for chno in range(START_CHAPTER, END_CHAPTER+1):
		chapter = load_chapter(f"/books/al-kafi/1/4/{chno}")
		query_chapter(chapter, results)
	return results

def add_node(nodes, results: NarratorResult, narrator_name, counts):
	meta = results.narrator_metadata[narrator_name]
	id = meta.id

	if id not in nodes:
		node = {}
		node['id'] = id
		node['label'] = narrator_name
		node['m'] = round(mean(meta.inverted_positions_in_chains))
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

	max_steps = max(map(lambda n: n['m'], nodes.values()))
	from_colour = Color("#ffd89b")
	to_colour = Color("#19547b")
	gradients = list(from_colour.range_to(to_colour, max_steps+1))

	for k,v in nodes.items():
		from_count = from_counts[k] if k in from_counts else 0
		to_count = to_counts[k] if k in to_counts else 0

		v['label'] = f"{v['label']}\n[{from_count}, {to_count}]"
		v['value'] = from_count + to_count
		v['color'] = gradients[v['m']].get_hex_l()
		v['to'] = to_count
		v['fro'] = from_count

	return list(nodes.values()), list(edges.values()), gradients

def path_anchor(path):
	k = path.rfind(":");
	return path[:k] + "#h" + path[k+1:]

def main():
	with open("app\queries\graph_template.html", 'r', encoding='utf-8') as templatefile:
		template = templatefile.read()
	narrator_index = load_json("/people/narrators/index")['data']
	
	results = query_book()

	(nodes, edges, gradients) = visjsify(narrator_index, results)
	hadith_data = [{
		'text': ' '.join(h.text),
		'chain': h.narrator_chain.text,
		'path': path_anchor(h.path)
	} for h in results.ahadith]
	id_to_narrations = {x.id:x.narration_indices for x in results.narrator_metadata.values()}

	gradients_len = len(gradients)
	gradients_width = int(100 / gradients_len)
	gradients_dups = [val.get_hex_l() for val in gradients for _ in (0, 1)]
	gradients_widths = [gradients_width*i for i in range(1, gradients_len)]
	gradients_width_dups = [val for val in gradients_widths for _ in (0, 1)]
	gradients_width_dups.append(100)
	gradients_pairs = zip(gradients_dups[1:], gradients_width_dups)
	gradients_str = ", ".join([f"{p[0]} {p[1]}%" for p in gradients_pairs])

	htmlfile = template\
		.replace('%%nodes%%', json.dumps(nodes, indent=2))\
		.replace('%%edges%%', json.dumps(edges, indent=2))\
		.replace('%%hadithdata%%', json.dumps(hadith_data, indent=2))\
		.replace('%%narratornarrationsdata%%', json.dumps(id_to_narrations, indent=2))\
		.replace('%%imamnamesdata%%', json.dumps(IMAM_NAMES, indent=2))\
		.replace('%%gradients%%', gradients_str)\
		.replace('%%START_CHAPTER%%', str(START_CHAPTER))\
		.replace('%%END_CHAPTER%%', str(END_CHAPTER))
	
	with open("kitab_hujjat_narrators.html", 'w', newline='', encoding='utf-8') as f:
		f.write(htmlfile)

main()
