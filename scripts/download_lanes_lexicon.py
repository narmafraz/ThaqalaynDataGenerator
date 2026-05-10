"""Download Lane's Arabic-English Lexicon (TEI XML from Perseus).

Source: https://github.com/laneslexicon/lexicon_xml — public-domain Perseus
Digital Library XML files with amendments. 37 files organized by Arabic
letter chapter (b0.xml, l1.xml, etc.).

License: Public domain (original lexicon 1863-93; Perseus digitization
is also public domain).

Per DECISION_LOG D047: chose Perseus XML over JSON forks because the
XML retains the original Lane markup (cross-references, source
citations like S=Sihah, K=Kamoos), the cleanest authoritative source.
JSON forks tend to lose this structural metadata.

Format: TEI XML with hierarchy:
  <div1 type="alphabetical letter" n="b">
    <div2 type="root">                  # one Arabic root
      <entryFree id="..." key="..." type="main">
        <form>
          <orth lang="ar">..</orth>      # Arabic head form
        </form>
        ...
        # body: definitions, examples, cross-references with
        # <hi rend="ital">, <quote>, etc.
      </entryFree>

Idempotent: skips download per file when size on disk matches.

Usage:
    python scripts/download_lanes_lexicon.py [--force]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_DEST = (
    PROJECT_ROOT / ".." / "ThaqalaynWordSources" / "sources" / "lanes-lexicon"
).resolve()

GITHUB_API_URL = (
    "https://api.github.com/repos/laneslexicon/lexicon_xml/contents/"
)
RAW_BASE_URL = "https://raw.githubusercontent.com/laneslexicon/lexicon_xml/master/"


def list_xml_files() -> List[str]:
    """Fetch the list of .xml files in the repo root via GitHub API."""
    req = urllib.request.Request(
        GITHUB_API_URL,
        headers={
            "User-Agent": "ThaqalaynWords/1.0",
            "Accept": "application/vnd.github.v3+json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))
    return [entry["name"] for entry in data if entry.get("name", "").endswith(".xml")]


def download_xml_file(filename: str, dest_dir: Path, *, force: bool = False) -> bool:
    """Download one XML file. Returns True if downloaded."""
    dest = dest_dir / filename
    if dest.exists() and not force:
        logger.info("  exists: %s (skip)", filename)
        return False
    url = RAW_BASE_URL + filename
    req = urllib.request.Request(url, headers={"User-Agent": "ThaqalaynWords/1.0"})
    logger.info("  downloading %s ...", filename)
    with urllib.request.urlopen(req, timeout=60) as response:
        data = response.read()
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        f.write(data)
    logger.info("  wrote %s (%d KB)", filename, len(data) // 1024)
    return True


# ---------------------------------------------------------------------------
# Parser — extract structured entries from TEI XML
# ---------------------------------------------------------------------------

# TEI XML files use the TEI namespace
_TEI_NS = "http://www.tei-c.org/ns/1.0"


def _strip_namespace(tag: str) -> str:
    """Remove namespace prefix from an XML tag."""
    if tag.startswith("{"):
        return tag.split("}", 1)[1]
    return tag


def _element_text(elem: ET.Element) -> str:
    """Recursively gather all text from an element, preserving order."""
    parts: List[str] = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.append(_element_text(child))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)


def parse_xml_file(xml_path: Path) -> List[Dict]:
    """Parse a Lane's XML file. Returns list of entry dicts.

    Each entry:
        {
            "id": str,               # entryFree @id
            "key": str,               # entryFree @key (headword in romanization)
            "type": str,              # 'main', etc.
            "letter": str,            # division letter
            "root": str,              # closest <div2 n="...">
            "orth_ar": str,           # primary Arabic headform
            "raw_text": str,          # full text content of the entry
        }
    """
    entries: List[Dict] = []
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as e:
        logger.warning("Parse error in %s: %s", xml_path.name, e)
        return []
    root = tree.getroot()

    # Walk down: TEI → text → body → div1 (letter) → div2 (root) → entryFree
    # The XML may or may not declare the TEI namespace; we strip prefixes.
    def find_letter_divs(node):
        for child in node.iter():
            if _strip_namespace(child.tag) == "div1":
                yield child

    for div1 in find_letter_divs(root):
        letter = div1.get("n", "")
        for div2 in div1.iter():
            if _strip_namespace(div2.tag) != "div2":
                continue
            current_root = div2.get("n", "")
            for entry in div2.iter():
                if _strip_namespace(entry.tag) != "entryFree":
                    continue
                # Capture entry fields
                eid = entry.get("id", "")
                ekey = entry.get("key", "")
                etype = entry.get("type", "")
                # Find first <orth> (Arabic headword)
                orth_ar = ""
                for sub in entry.iter():
                    if _strip_namespace(sub.tag) == "orth":
                        if sub.get("lang") == "ar":
                            orth_ar = (sub.text or "").strip()
                            break
                # Gather full text content (deep)
                raw_text = _element_text(entry).strip()
                # Collapse internal whitespace
                raw_text = re.sub(r"\s+", " ", raw_text)
                entries.append({
                    "id": eid,
                    "key": ekey,
                    "type": etype,
                    "letter": letter,
                    "root": current_root,
                    "orth_ar": orth_ar,
                    "raw_text": raw_text,
                })
    return entries


def build_root_index(all_entries: List[Dict]) -> Dict[str, List[Dict]]:
    """Group entries by their root (the TEI 'div2 n' attribute)."""
    by_root: Dict[str, List[Dict]] = {}
    for e in all_entries:
        root = e.get("root")
        if not root:
            continue
        by_root.setdefault(root, []).append(e)
    return by_root


def build_orth_index(all_entries: List[Dict]) -> Dict[str, List[str]]:
    """Group entry IDs by their Arabic head-form (orth_ar)."""
    by_orth: Dict[str, List[str]] = {}
    for e in all_entries:
        orth = e.get("orth_ar")
        if not orth:
            continue
        by_orth.setdefault(orth, []).append(e.get("id", ""))
    return by_orth


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--force", action="store_true",
                        help="Force re-download all files")
    parser.add_argument("--parse-only", action="store_true",
                        help="Skip downloads; just parse existing files")
    args = parser.parse_args()

    dest_dir: Path = args.dest
    logger.info("Destination: %s", dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    # ----- Discover + download phase -----
    if not args.parse_only:
        logger.info("Fetching file list from GitHub ...")
        try:
            xml_files = list_xml_files()
        except Exception as e:
            logger.error("Couldn't list files: %s", e)
            sys.exit(1)
        logger.info("Found %d XML files", len(xml_files))
        for fname in xml_files:
            download_xml_file(fname, dest_dir, force=args.force)

    # ----- Parse phase -----
    xml_paths = sorted(dest_dir.glob("*.xml"))
    if not xml_paths:
        logger.error("No XML files in %s", dest_dir)
        sys.exit(1)
    logger.info("Parsing %d XML files ...", len(xml_paths))
    all_entries: List[Dict] = []
    for xp in xml_paths:
        entries = parse_xml_file(xp)
        logger.info("  %s: %d entries", xp.name, len(entries))
        all_entries.extend(entries)
    logger.info("Total entries: %d", len(all_entries))

    # Build indexes
    by_root = build_root_index(all_entries)
    by_orth = build_orth_index(all_entries)
    logger.info("  %d unique roots", len(by_root))
    logger.info("  %d unique Arabic head-forms", len(by_orth))

    # Persist
    entries_out = dest_dir / "parsed_entries.json"
    root_index_out = dest_dir / "root_index.json"
    orth_index_out = dest_dir / "orth_index.json"
    with open(entries_out, "w", encoding="utf-8") as f:
        json.dump(all_entries, f, ensure_ascii=False, indent=None, separators=(",", ":"))
    logger.info("Wrote %s (%d MB)", entries_out, entries_out.stat().st_size // 1_000_000)
    with open(root_index_out, "w", encoding="utf-8") as f:
        json.dump(by_root, f, ensure_ascii=False, indent=None, separators=(",", ":"))
    logger.info("Wrote %s", root_index_out)
    with open(orth_index_out, "w", encoding="utf-8") as f:
        json.dump(by_orth, f, ensure_ascii=False, indent=None, separators=(",", ":"))
    logger.info("Wrote %s", orth_index_out)


if __name__ == "__main__":
    main()
