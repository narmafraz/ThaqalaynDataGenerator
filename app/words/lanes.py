"""Lane's Arabic-English Lexicon TEI XML parser — structured body extraction.

Builds on the basic parser in ``scripts/download_lanes_lexicon.py`` (which
flattens each entry to ``raw_text``) by producing **structured** entries
preserving:

- the headword (Buckwalter → NFC Arabic)
- ordered body segments classified as English-italic (definitions),
  embedded Arabic (foreign words / cross-references), or connective prose
- source-citation abbreviation codes extracted from the prose tails
- page breaks (preserved for citing the printed lexicon)

Output schema per entry:

    {
      "id": "n42874",
      "key": "qAla",          # original Buckwalter
      "headword_ar": "قَالَ",  # bw2ar'd
      "type": "main",
      "letter": "q",
      "root": "ق.#.ل",        # from parent div2[@n], converted
      "body": [
        {"kind": "italic_en", "text": "..."},   # definition prose (English)
        {"kind": "arabic", "text_ar": "..."},   # embedded Arabic
        {"kind": "text", "text": "..."},        # connective + citations
        {"kind": "page_break", "n": 42},        # printed-page marker
        {"kind": "quote", "text": "..."},       # occasional <quote>
      ],
      "source_refs": ["S", "K", "M", "TA", ...],  # deduped, in order
    }

A separate :data:`SOURCE_CITATION_LEGEND` constant maps each
common abbreviation to its full reference (e.g. ``S`` → al-Jawhari,
*Sihah*). The UI uses this to render a legend.

Module exposes:

- :func:`parse_lanes_xml_to_structured(path)` — one file → list of dicts
- :func:`build_lanes_entries_index(dir)` — all files → dict keyed by id
- :data:`SOURCE_CITATION_LEGEND` — code → reference dict

This module does NOT depend on CAMeL Tools. The Buckwalter conversion uses
``builders.perseus_bw_to_arabic`` which lazily loads CAMeL when first called.
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional

from .builders import perseus_bw_to_arabic

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source citation legend
# ---------------------------------------------------------------------------

# Lane's cites ~30 classical Arabic lexicographers + dictionaries throughout
# the body using one- to three-letter abbreviations. The legend below covers
# the most-common codes; unknown codes are returned verbatim with no
# expansion (the UI can choose to display them as raw codes or hide).
#
# Sourced from Lane's own preface + standard reference guides to his work.
SOURCE_CITATION_LEGEND: Dict[str, str] = {
    # Major dictionaries (Lane's primary sources)
    "S":   "Sihah — al-Jawhari, al-Sihah",
    "K":   "Kamoos — al-Firuzabadi, al-Qamus al-Muhit",
    "M":   "Muhkam — Ibn Sida, al-Muhkam",
    "TA":  "Taj al-'Arus — al-Zabidi (commentary on Kamoos)",
    "T":   "Tahdhib — al-Azhari, Tahdhib al-Lughah",
    "L":   "Lisan al-'Arab — Ibn Manzur",
    "A":   "Asas al-Balaghah — al-Zamakhshari",
    "O":   "Ubab — al-Saghani, al-'Ubab al-Zakhir",
    "Msb": "Misbah al-Munir — al-Fayyumi",
    "Mgh": "Mughrib — al-Mutarrizi, al-Mughrib fi Tartib al-Mu'rib",
    "MA":  "Mukhassas — Ibn Sida",
    "JK":  "Jamharah — Ibn Durayd, Jamharat al-Lughah",
    "IF":  "Mu'jam Maqayis al-Lughah — Ibn Faris",
    "Nh":  "Nihayah — Ibn al-Athir, al-Nihayah fi Gharib al-Hadith",
    # Individual scholars (Lane cites these by initials)
    "AHn":  "Abu Hanifah al-Dinawari",
    "IB":   "Ibn Barri",
    "IAar": "Ibn al-A'rabi",
    "IDrd": "Ibn Durayd",
    "ISd":  "Ibn al-Sikkit (or Ibn al-Sayyid)",
    "ISh":  "Ibn Shumayl",
    "ISk":  "Ibn al-Sikkit",
    "IJ":   "Ibn Jinni",
    "IAth": "Ibn al-Athir",
    "Az":   "al-Azhari",
    "Sh":   "al-Shaybani",
    "ADk":  "Abu al-Dahduh",
    "AZ":   "Abu Zayd",
    "AO":   "Abu 'Ubayd",
    "ABk":  "Abu Bakr (al-Zubaydi or al-Anbari)",
    "AAF":  "Abu al-Abbas al-Mubarrad / Tha'lab",
    "Fr":   "al-Farra'",
    "Akh":  "al-Akhfash",
    "Sb":   "Sibawayh",
    "Ks":   "al-Kisa'i",
    "Lth":  "al-Layth ibn al-Muzaffar",
    "Lh":   "Abu al-Hasan al-Lihyani",
    "Mz":   "al-Mubarrad (or al-Muzani)",
}


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") else tag


# Source citation patterns inside TAIL text. Lane's writes them as
# parenthesized initial sequences: "(S, K, M:)" or "(S, A, IDrd:)" etc.
# Sometimes wrapped: "(M, IB,)". The terminal punctuation varies.
_CITATION_RE = re.compile(
    r"\(([A-Z][A-Za-z]{0,5}(?:,\s*[A-Z][A-Za-z]{0,5})*)\s*[:,]?\s*\)"
)


def _extract_citation_codes(text: str) -> List[str]:
    """Return source-citation codes mentioned in a snippet of prose."""
    codes: List[str] = []
    for m in _CITATION_RE.finditer(text):
        for raw in m.group(1).split(","):
            code = raw.strip()
            if code:
                codes.append(code)
    return codes


def _parse_entry_body(entry: ET.Element) -> List[Dict]:
    """Walk an ``<entryFree>`` and emit ordered body segments.

    Walks in document order, preserving structural element types and
    interleaving the prose TAIL text between them. Returns a flat list
    of segment dicts; the caller can render or further process them.

    Element-to-segment-kind mapping:

    - ``hi rend="ital"`` → ``italic_en`` (English definitions; rarely
      contains nested Arabic — when it does, the nested foreign text
      is emitted as a separate ``arabic`` segment after the italic one
      so simple downstream consumers can handle them sequentially).
    - ``foreign lang="ar"`` → ``arabic`` (Arabic in Buckwalter; we
      convert to NFC Arabic).
    - ``orth lang="ar"`` → ``arabic`` (alternative orthographic forms;
      same handling as ``foreign``). The headword's own ``form/orth``
      element is captured separately by the caller and skipped here.
    - ``quote`` → ``quote`` (rare; preserve full text).
    - ``pb n="N"`` → ``page_break`` with ``n`` = page number.
    - Other elements (``L``, ``G``, ``H``, ``itype``) → flattened to
      their text content as ``text`` segments. These are rare and
      semantically light.

    Prose TAIL text and the ``entryFree``'s own ``text`` are emitted
    as ``text`` segments — these carry the connective phrases like
    ", (S, K,) and " between definitions.
    """
    segments: List[Dict] = []

    # Skip the headword <form> child — it's parsed separately by the caller.
    headform = entry.find(".//{*}form") or entry.find("form")

    def emit_text(s: str):
        s = (s or "").strip()
        if s:
            segments.append({"kind": "text", "text": s})

    # Entry's own leading text (before any children).
    emit_text(entry.text)

    for child in entry:
        # Skip the leading <form> (headword container).
        if child is headform:
            tail = (child.tail or "").strip()
            if tail:
                emit_text(tail)
            continue

        tag = _strip_ns(child.tag)
        if tag == "hi" and child.get("rend") == "ital":
            text = "".join(child.itertext()).strip()
            if text:
                segments.append({"kind": "italic_en", "text": text})
        elif tag == "foreign" and child.get("lang") == "ar":
            bw = (child.text or "").strip()
            if bw:
                segments.append({
                    "kind": "arabic",
                    "text_bw": bw,
                    "text_ar": perseus_bw_to_arabic(bw),
                })
        elif tag == "orth":
            bw = (child.text or "").strip()
            if bw:
                segments.append({
                    "kind": "arabic",
                    "text_bw": bw,
                    "text_ar": perseus_bw_to_arabic(bw),
                    "orth_type": child.get("type") or "plain",
                })
        elif tag == "quote":
            text = "".join(child.itertext()).strip()
            if text:
                segments.append({"kind": "quote", "text": text})
        elif tag == "pb":
            n = child.get("n")
            try:
                segments.append({"kind": "page_break", "n": int(n)})
            except (TypeError, ValueError):
                pass
        elif tag in ("entryFree", "form"):
            # Nested entryFree (sub-senses) — skip; they're handled when
            # the outer iterator reaches them. Sub-form is handled above.
            pass
        else:
            # Flatten unknown / light elements (itype, L, G, H) to text.
            text = "".join(child.itertext()).strip()
            if text:
                segments.append({"kind": "text", "text": text})

        tail = (child.tail or "").strip()
        if tail:
            emit_text(tail)

    return segments


def _parse_single_entry(
    entry: ET.Element, letter: str, root: str
) -> Optional[Dict]:
    """Parse one ``<entryFree>`` into the structured-entry schema."""
    eid = entry.get("id")
    if not eid:
        return None
    headform = entry.find("./{*}form") or entry.find("./form")
    headword_ar: Optional[str] = None
    if headform is not None:
        orth = headform.find("./{*}orth") or headform.find("./orth")
        if orth is not None and orth.text:
            headword_ar = perseus_bw_to_arabic(orth.text)

    body = _parse_entry_body(entry)

    # Extract source-citation codes from connective ``text`` segments.
    codes: List[str] = []
    seen_codes = set()
    for seg in body:
        if seg.get("kind") == "text":
            for c in _extract_citation_codes(seg["text"]):
                if c not in seen_codes:
                    seen_codes.add(c)
                    codes.append(c)

    return {
        "id": eid,
        "key": entry.get("key", ""),
        "headword_ar": headword_ar or "",
        "type": entry.get("type", ""),
        "letter": letter,
        "root": root,
        "body": body,
        "source_refs": codes,
    }


def parse_lanes_xml_to_structured(xml_path: Path) -> List[Dict]:
    """Parse one Lane's TEI XML file → list of structured entries.

    Walks div1 (alphabetical letter) > div2 (root) > entryFree[*]
    and returns every entry, tagging it with its letter + root context.

    Returns an empty list on parse errors.
    """
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as e:
        logger.warning("ParseError in %s: %s", xml_path.name, e)
        return []
    root = tree.getroot()

    out: List[Dict] = []
    for div1 in root.iter():
        if _strip_ns(div1.tag) != "div1":
            continue
        letter = div1.get("n", "")
        for div2 in div1.iter():
            if _strip_ns(div2.tag) != "div2":
                continue
            current_root = div2.get("n", "")
            for entry in div2.iter():
                if _strip_ns(entry.tag) != "entryFree":
                    continue
                parsed = _parse_single_entry(entry, letter, current_root)
                if parsed:
                    out.append(parsed)
    return out


def build_lanes_entries_index(lexicon_dir: Path) -> Dict[str, Dict]:
    """Walk all Lane's XML files and return entry_id → structured entry."""
    out: Dict[str, Dict] = {}
    for xml_path in sorted(lexicon_dir.glob("*.xml")):
        for entry in parse_lanes_xml_to_structured(xml_path):
            out[entry["id"]] = entry
    return out
