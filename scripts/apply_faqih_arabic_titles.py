"""Apply scraped Arabic chapter titles to Man La Yahduruhu al-Faqih data files.

Reads faqih_arabic_chapter_titles.json and patches:
1. The modular chapter_list files (books/man-la-yahduruhu-al-faqih/{vol}.json)
2. The shell chapter files (books/man-la-yahduruhu-al-faqih/{vol}/{ch}.json)
3. The index files (index/books.ar.json)
"""

import json
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
_SOURCE_DIR = os.environ.get(
    "SOURCE_DATA_DIR",
    os.path.join(_PROJECT_ROOT, "..", "ThaqalaynDataSources"),
)
_DEST_DIR = os.environ.get(
    "DESTINATION_DIR",
    os.path.join(_PROJECT_ROOT, "..", "ThaqalaynData"),
)

TITLES_PATH = os.path.join(
    _SOURCE_DIR, "ai-pipeline-data", "faqih_arabic_chapter_titles.json"
)


def main():
    with open(TITLES_PATH, "r", encoding="utf-8") as f:
        ar_titles = json.load(f)
    print(f"Loaded {len(ar_titles)} Arabic titles")

    patched_total = 0

    # Patch modular chapter_list files
    for vol in range(1, 5):
        path = os.path.join(_DEST_DIR, "books", "man-la-yahduruhu-al-faqih", f"{vol}.json")
        if not os.path.exists(path):
            continue

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        patched = 0
        for chapter in data["data"]["chapters"]:
            en_title = chapter["titles"].get("en", "")
            if en_title in ar_titles and "ar" not in chapter["titles"]:
                chapter["titles"]["ar"] = ar_titles[en_title]
                patched += 1

        if patched > 0:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
            print(f"  Vol {vol}: patched {patched} chapters")
            patched_total += patched

    # Patch shell chapter files (verse_list kind)
    # These are at books/man-la-yahduruhu-al-faqih/{vol}/{ch}.json
    # Map English titles to Arabic using the en index
    en_index_path = os.path.join(_DEST_DIR, "index", "books.en.json")
    with open(en_index_path, "r", encoding="utf-8") as f:
        en_index = json.load(f)

    patched_shell = 0
    book_dir = os.path.join(_DEST_DIR, "books", "man-la-yahduruhu-al-faqih")
    for path, entry in en_index.items():
        if not path.startswith("/books/man-la-yahduruhu-al-faqih"):
            continue
        en_title = entry.get("title", "")
        if en_title not in ar_titles:
            continue
        # Convert path to filesystem: /books/man-la-yahduruhu-al-faqih:1:5 -> books/man-la-yahduruhu-al-faqih/1/5.json
        rel = path.replace(":", "/").lstrip("/") + ".json"
        file_path = os.path.join(_DEST_DIR, rel)
        if not os.path.exists(file_path):
            continue
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        titles = data.get("data", {}).get("titles", {})
        if "ar" not in titles:
            titles["ar"] = ar_titles[en_title]
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
            patched_shell += 1

    if patched_shell > 0:
        print(f"  Shell chapter files: patched {patched_shell}")
        patched_total += patched_shell

    # Patch index/books.ar.json
    ar_index_path = os.path.join(_DEST_DIR, "index", "books.ar.json")
    if os.path.exists(ar_index_path):
        with open(ar_index_path, "r", encoding="utf-8") as f:
            ar_index = json.load(f)

        # Load English index to map paths to English titles
        en_index_path = os.path.join(_DEST_DIR, "index", "books.en.json")
        with open(en_index_path, "r", encoding="utf-8") as f:
            en_index = json.load(f)

        patched_idx = 0
        for path, entry in en_index.items():
            if not path.startswith("/books/man-la-yahduruhu-al-faqih"):
                continue
            en_title = entry.get("title", "")
            if en_title in ar_titles:
                if path not in ar_index:
                    ar_index[path] = {}
                if "title" not in ar_index[path] or not ar_index[path]["title"]:
                    ar_index[path]["title"] = ar_titles[en_title]
                    ar_index[path]["part_type"] = entry.get("part_type", "Chapter")
                    if "local_index" in entry:
                        ar_index[path]["local_index"] = entry["local_index"]
                    patched_idx += 1

        if patched_idx > 0:
            with open(ar_index_path, "w", encoding="utf-8") as f:
                json.dump(ar_index, f, ensure_ascii=False, indent=2, sort_keys=True)
            print(f"  Arabic index: patched {patched_idx} entries")
            patched_total += patched_idx

    print(f"\nTotal patched: {patched_total}")


if __name__ == "__main__":
    main()
