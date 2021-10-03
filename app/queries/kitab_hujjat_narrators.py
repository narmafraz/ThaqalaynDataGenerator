# Aim is to get Quran verses in the first 30 chapters of Kitab al Hujjat of Al-Kafi

import csv
import glob
import json
import logging
import os
import re
from pprint import pprint
from typing import Dict

from app.lib_db import load_chapter, load_json
from app.models.people import ChainVerses, Narrator, NarratorIndex
from fastapi.encoders import jsonable_encoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def query_chapter(chapter):
	results = []
	for hadith in chapter.verses:
		parts = hadith.narrator_chain.parts
		narrator_parts = filter(lambda p: p.kind == 'narrator', parts)
		narrator_names = list(map(lambda p: p.text, narrator_parts))
		narrator_pairs = zip(narrator_names, narrator_names[1:])
		results.extend(narrator_pairs)
	
	return results

def query_book():
	results = []
	for chno in range(1, 31):
		chapter = load_chapter(f"/books/al-kafi/1/4/{chno}")
		result = query_chapter(chapter)
		results.extend(result)
	return results

def add_node(nodes, name_id_map, narrator_index, narrator_name, counts):
	id = name_id_map[narrator_name]

	if id not in nodes:
		node = {}
		node['id'] = id
		node['value'] = narrator_index[str(id)]['narrations']
		node['label'] = narrator_name
		nodes[id] = node

	if id in counts:
		counts[id] += 1
	else:
		counts[id] = 1
	

def add_edge(edges, name_id_map, narrator_index, n1_name, n2_name):
	id1 = name_id_map[n1_name]
	id2 = name_id_map[n2_name]
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

def visjsify(narrator_index: Dict[int, Narrator], results):
	nodes = {}
	edges = {}
	from_counts = {}
	to_counts = {}
	name_id_map = {v['titles']['ar']:int(k) for (k,v) in narrator_index.items()}
	for result in results:
		n1_name = result[0]
		n2_name = result[1]

		add_node(nodes, name_id_map, narrator_index, n1_name, to_counts)
		add_node(nodes, name_id_map, narrator_index, n2_name, from_counts)
		add_edge(edges, name_id_map, narrator_index, n1_name, n2_name)

	for k,v in nodes.items():
		v['label'] = f"{v['label']}\n[{from_counts[k] if k in from_counts else 0}, {to_counts[k] if k in to_counts else 0}]"
	return list(nodes.values()), list(edges.values())

def main():
	with open("app\queries\graph_template.html", 'r', encoding='utf-8') as templatefile:
		template = templatefile.read()
	narrator_index = load_json("/people/narrators/index")['data']
	results = query_book()
	(nodes, edges) = visjsify(narrator_index, results)

	htmlfile = template.replace('%%nodes%%', json.dumps(nodes, indent=2)).replace('%%edges%%', json.dumps(edges, indent=2))
	with open("kitab_hujjat_narrators.html", 'w', newline='', encoding='utf-8') as f:
		f.write(htmlfile)

main()
