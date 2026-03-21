"""Translate Arabic chapter titles to English using claude -p.

Usage:
    # Step 1: Extract titles that need translation
    python scripts/translate_chapter_titles.py extract

    # Step 2: Review the prompt (printed to stdout)
    python scripts/translate_chapter_titles.py prompt

    # Step 3: Run translation via claude -p
    python scripts/translate_chapter_titles.py translate

    # Step 4: Apply translations to the data (updates parser lookup + regenerates)
    python scripts/translate_chapter_titles.py apply

Stores translations in:
    ThaqalaynDataSources/ai-pipeline-data/chapter_title_translations.json
"""

import argparse
import json
import os
import subprocess
import sys

sys.stdout.reconfigure(encoding="utf-8")

# ── Paths ──────────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)  # ThaqalaynDataGenerator/

SOURCE_DATA_DIR = os.environ.get(
    "SOURCE_DATA_DIR",
    os.path.join(PROJECT_DIR, "..", "ThaqalaynDataSources"),
)
DESTINATION_DIR = os.environ.get(
    "DESTINATION_DIR",
    os.path.join(PROJECT_DIR, "..", "ThaqalaynData"),
)

AI_PIPELINE_DATA_DIR = os.path.join(SOURCE_DATA_DIR, "ai-pipeline-data")
TRANSLATIONS_FILE = os.path.join(
    AI_PIPELINE_DATA_DIR, "chapter_title_translations.json"
)
PROMPT_FILE = os.path.join(SCRIPT_DIR, "chapter_title_prompt.txt")

INDEX_AR = os.path.join(DESTINATION_DIR, "index", "books.ar.json")
INDEX_EN = os.path.join(DESTINATION_DIR, "index", "books.en.json")

# Books whose chapters come from ghbook_parser with Arabic-only titles
BOOKS_NEEDING_TRANSLATION = ["al-istibsar", "tahdhib-al-ahkam"]


# ── Helpers ────────────────────────────────────────────────────────────────


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Wrote {path}")


def extract_titles_needing_translation() -> dict[str, list[dict]]:
    """Find chapters in AR index that have no real English title.

    Returns {book_slug: [{path, ar_title}, ...]}
    """
    ar_index = load_json(INDEX_AR)
    en_index = load_json(INDEX_EN)

    by_book: dict[str, list[dict]] = {}

    for path, ar_entry in ar_index.items():
        # Only process target books
        book_slug = None
        for slug in BOOKS_NEEDING_TRANSLATION:
            if path.startswith(f"/books/{slug}:"):
                book_slug = slug
                break
        if not book_slug:
            continue

        # Only chapters (not Book or Volume level)
        if ar_entry.get("part_type") != "Chapter":
            continue

        ar_title = ar_entry.get("title", "")
        if not ar_title:
            continue

        # Check if EN index has a real translation (not just Arabic copy)
        en_entry = en_index.get(path, {})
        en_title = en_entry.get("title", "")

        # If EN title is missing or identical to Arabic, it needs translation
        if not en_title or en_title == ar_title:
            by_book.setdefault(book_slug, []).append({
                "path": path,
                "ar_title": ar_title,
            })

    # Sort by path within each book for stable ordering
    for slug in by_book:
        by_book[slug].sort(key=lambda x: x["path"])

    return by_book


# ── Commands ───────────────────────────────────────────────────────────────


def cmd_extract(args: argparse.Namespace) -> None:
    """Extract and display titles that need translation."""
    by_book = extract_titles_needing_translation()

    total = 0
    for slug, titles in sorted(by_book.items()):
        print(f"\n{slug}: {len(titles)} chapters need English titles")
        if args.verbose:
            for t in titles[:10]:
                print(f"  {t['path']}: {t['ar_title']}")
            if len(titles) > 10:
                print(f"  ... and {len(titles) - 10} more")
        total += len(titles)

    print(f"\nTotal: {total} chapter titles need translation")


def build_prompt(by_book: dict[str, list[dict]]) -> str:
    """Build the translation prompt for claude -p."""
    # Collect all titles into a numbered list grouped by book
    lines = []
    title_index = []  # Track (number, path, ar_title) for response parsing

    num = 0
    for slug in sorted(by_book.keys()):
        titles = by_book[slug]
        lines.append(f"\n## {slug}")
        for t in titles:
            num += 1
            ar = t["ar_title"]
            lines.append(f"{num}. {ar}")
            title_index.append((num, t["path"], ar))

    numbered_titles = "\n".join(lines)

    prompt = f"""You are a specialist in Twelver Shia Islamic scholarly texts. Translate the following Arabic chapter titles ("bab" headings) from two of the Four Books (al-Kutub al-Arba'a) of Shia hadith: al-Istibsar and Tahdhib al-Ahkam, both by Shaykh al-Tusi.

## Context

These are chapter headings from classical Shia jurisprudence (fiqh) collections. The text is in classical Arabic (fusha qadima) — vocabulary and syntax differ from Modern Standard Arabic. Be faithful to Shia scholarly tradition in terminology and interpretation. These books cover Shia fiqh rulings on worship, transactions, marriage, inheritance, criminal law, and other topics specific to Ja'fari jurisprudence.

## Instructions

1. Each title follows the pattern: `NUMBER- بَابُ TOPIC` (or `NUMBER - بَابُ TOPIC`).
2. Translate only the topic part after "باب". Render the result as: `Chapter on ...` or `Chapter of ...`.
3. Preserve the original Arabic number prefix exactly as-is (e.g., "1-", "47-", "129-"). Place it before "Chapter": `1- Chapter of ...`.
4. For titles without "باب" (e.g., section headers like "أَبْوَابُ الزِّيَادَاتِ" or introductions like "تمهيد"), translate them naturally (e.g., "Supplementary Chapters on...", "Introduction").
5. Use established Islamic terminology — do not translate terms that have standard transliterations:
   - صلاة = salat (prayer)
   - وضوء = wudu (ablution)
   - غسل = ghusl (ritual bath)
   - زكاة = zakat
   - خمس = khums (one-fifth tax)
   - حج = hajj
   - صوم = sawm (fasting)
   - جنابة = janaba (major ritual impurity)
   - تيمّم = tayammum (dry ablution)
   - نكاح = marriage
   - طلاق = divorce
   - ميراث = inheritance
   - حدود = hudud (legal punishments)
   - قصاص = qisas (retribution)
   - ديات = diyat (blood money)
   - متعة = mut'a (temporary marriage)
   - تقيّة = taqiyya (precautionary dissimulation)
   - إمام = Imam (when referring to the Twelve Imams)
6. Transliterate proper names — do not translate them (e.g., "مِنًى" = "Mina", "عَرَفَات" = "Arafat", "الكوفة" = "Kufa"). Preserve honorifics conceptually but do not include them in the title.
7. Aim for concise, readable English. If the Arabic is very long (a full sentence describing a legal scenario), condense the legal issue while keeping the key terms.
8. This is classical Arabic — note that some words have different meanings than in MSA.

## Style reference (from Al-Kafi chapter titles in the same project)

بَابُ فَرْضِ الْعِلْمِ وَ وُجُوبِ طَلَبِهِ وَ الْحَثِّ عَلَيْهِ => 1- Chapter on the Obligation of Knowledge, the Duty to Seek It, and the Urging Upon It
بَابُ النَّهْيِ عَنِ الْقَوْلِ بِغَيْرِ عِلْمٍ => 11- Chapter on the Forbiddance of Speaking Without Knowledge
بَابُ التَّقْلِيدِ => 18- Chapter on Taqlid (Emulation)
بَابُ النَّوَادِرِ => 16- The Miscellaneous

## Output format

Return one translation per line, matching the input numbering. No JSON, no extra text, no blank lines.

1. 1- Chapter on the Amount of Water That Is Not Made Impure by Anything
2. 10- Chapter on ...
3. 121- Chapter on ...

## Titles to translate ({num} total)
{numbered_titles}
"""
    return prompt, title_index


def cmd_prompt(args: argparse.Namespace) -> None:
    """Build and display the prompt without running it."""
    by_book = extract_titles_needing_translation()
    if not by_book:
        print("No titles need translation.")
        return

    prompt, title_index = build_prompt(by_book)

    # Save prompt to file for review
    with open(PROMPT_FILE, "w", encoding="utf-8") as f:
        f.write(prompt)

    total = sum(len(v) for v in by_book.values())
    print(f"Prompt saved to: {PROMPT_FILE}")
    print(f"Titles to translate: {total}")
    print(f"Prompt length: {len(prompt):,} characters (~{len(prompt) // 4:,} tokens)")
    print(f"\nPreview (first 80 lines):\n")
    for line in prompt.split("\n")[:80]:
        print(line)
    if prompt.count("\n") > 80:
        print(f"\n... ({prompt.count(chr(10)) - 80} more lines, see {PROMPT_FILE})")


def run_claude_p(prompt: str, model: str = "sonnet") -> str:
    """Run claude -p with a prompt, handling Windows encoding issues.

    Returns the response text.
    """
    proc = subprocess.Popen(
        ["claude", "-p", "--output-format", "json", "--model", model],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout_bytes, stderr_bytes = proc.communicate(
        input=prompt.encode("utf-8"),
        timeout=900,  # 15 minute timeout per batch
    )
    stdout_text = stdout_bytes.decode("utf-8", errors="replace")
    stderr_text = stderr_bytes.decode("utf-8", errors="replace")

    if proc.returncode != 0:
        print(f"claude -p failed (exit code {proc.returncode}):")
        print(stderr_text)
        raise RuntimeError("claude -p failed")

    # Parse claude's JSON output wrapper
    try:
        claude_output = json.loads(stdout_text)
        return claude_output.get("result", stdout_text)
    except json.JSONDecodeError:
        return stdout_text


def parse_line_responses(response_text: str) -> dict[int, str]:
    """Parse line-by-line output: 'N. translation text' -> {N: text}."""
    import re
    results = {}
    for line in response_text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^(\d+)\.\s+(.+)$", line)
        if m:
            results[int(m.group(1))] = m.group(2)
    return results


def cmd_translate(args: argparse.Namespace) -> None:
    """Run translation via claude -p, one batch per book."""
    by_book = extract_titles_needing_translation()
    if not by_book:
        print("No titles need translation.")
        return

    total = sum(len(v) for v in by_book.values())
    model = args.model
    print(f"Translating {total} chapter titles via claude -p --model {model}")
    print(f"Splitting into {len(by_book)} batches (one per book)\n")

    all_translations = {}

    # Load existing translations if any
    if os.path.exists(TRANSLATIONS_FILE):
        all_translations = load_json(TRANSLATIONS_FILE)
        print(f"Loaded {len(all_translations)} existing translations")

    for slug in sorted(by_book.keys()):
        book_titles = {slug: by_book[slug]}
        prompt, title_index = build_prompt(book_titles)
        count = len(by_book[slug])

        print(f"\n--- {slug}: {count} titles ---")
        print(f"Prompt: ~{len(prompt) // 4:,} tokens")

        response_text = run_claude_p(prompt, model=model)

        # Save raw response
        response_file = os.path.join(
            SCRIPT_DIR, f"chapter_title_response_{slug}.txt"
        )
        with open(response_file, "w", encoding="utf-8") as f:
            f.write(response_text)
        print(f"Response saved to {response_file}")

        # Parse
        translations_by_num = parse_line_responses(response_text)

        matched = 0
        missing = []
        for num, path, ar_title in title_index:
            if num in translations_by_num:
                all_translations[ar_title] = translations_by_num[num]
                matched += 1
            else:
                missing.append((num, path, ar_title))

        print(f"Matched: {matched}/{count}")
        if missing:
            print(f"Missing: {len(missing)}")
            for num, path, ar in missing[:5]:
                print(f"  {num}. {path}: {ar}")

    save_json(TRANSLATIONS_FILE, all_translations)
    print(f"\nTotal: {len(all_translations)} translations saved to {TRANSLATIONS_FILE}")
    print("Next step: run 'py scripts/translate_chapter_titles.py apply'")


def cmd_apply(args: argparse.Namespace) -> None:
    """Apply translations: update index files and chapter JSON files."""
    if not os.path.exists(TRANSLATIONS_FILE):
        print(f"No translations file found at {TRANSLATIONS_FILE}")
        print("Run 'translate' command first.")
        sys.exit(1)

    translations = load_json(TRANSLATIONS_FILE)
    print(f"Loaded {len(translations)} translations")

    # Load AR and EN indexes
    ar_index = load_json(INDEX_AR)
    en_index = load_json(INDEX_EN)

    updated_index = 0
    updated_files = 0

    for path, ar_entry in ar_index.items():
        # Only target books
        book_slug = None
        for slug in BOOKS_NEEDING_TRANSLATION:
            if path.startswith(f"/books/{slug}:"):
                book_slug = slug
                break
        if not book_slug:
            continue

        if ar_entry.get("part_type") != "Chapter":
            continue

        ar_title = ar_entry.get("title", "")
        en_translation = translations.get(ar_title)
        if not en_translation:
            continue

        # Update EN index
        if path not in en_index:
            en_index[path] = {}
        en_index[path]["title"] = en_translation
        en_index[path]["part_type"] = ar_entry.get("part_type", "Chapter")
        if "local_index" in ar_entry:
            en_index[path]["local_index"] = ar_entry["local_index"]
        updated_index += 1

        # Update individual chapter JSON file
        # Path like /books/al-istibsar:1:1 -> books/al-istibsar/1/1.json
        rel_path = path[1:]  # strip leading /
        file_path = os.path.join(
            DESTINATION_DIR,
            rel_path.replace(":", "/") + ".json",
        )
        if os.path.exists(file_path):
            chapter_data = load_json(file_path)
            if "data" in chapter_data and "titles" in chapter_data["data"]:
                chapter_data["data"]["titles"]["en"] = en_translation
                save_json(file_path, chapter_data)
                updated_files += 1

    # Write updated EN index
    save_json(INDEX_EN, en_index)

    # Update volume/book list files (these embed chapter titles for the chapter-list view)
    updated_list_files = 0
    for slug in BOOKS_NEEDING_TRANSLATION:
        # Find all volume/book list files for this slug
        book_dir = os.path.join(DESTINATION_DIR, "books", slug)
        if not os.path.isdir(book_dir):
            continue
        # Walk all JSON files that contain a chapters[] array
        for root, dirs, files in os.walk(book_dir):
            for fname in files:
                if not fname.endswith(".json"):
                    continue
                fpath = os.path.join(root, fname)
                data = load_json(fpath)
                inner = data.get("data", data)
                chapters = inner.get("chapters")
                if not chapters or not isinstance(chapters, list):
                    continue
                changed = False
                for ch in chapters:
                    titles = ch.get("titles", {})
                    ar_title = titles.get("ar", "")
                    if ar_title and ar_title in translations and "en" not in titles:
                        titles["en"] = translations[ar_title]
                        changed = True
                if changed:
                    save_json(fpath, data)
                    updated_list_files += 1

    print(f"\nDone:")
    print(f"  Updated EN index entries: {updated_index}")
    print(f"  Updated chapter JSON files: {updated_files}")
    print(f"  Updated volume/list files: {updated_list_files}")


# ── Main ───────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Translate Arabic chapter titles to English"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # extract
    p_extract = subparsers.add_parser(
        "extract", help="Show titles that need translation"
    )
    p_extract.add_argument(
        "-v", "--verbose", action="store_true", help="Show sample titles"
    )
    p_extract.set_defaults(func=cmd_extract)

    # prompt
    p_prompt = subparsers.add_parser(
        "prompt", help="Build and display the translation prompt"
    )
    p_prompt.set_defaults(func=cmd_prompt)

    # translate
    p_translate = subparsers.add_parser(
        "translate", help="Run translation via claude -p"
    )
    p_translate.add_argument(
        "--model", default="sonnet",
        help="Claude model to use (default: sonnet)",
    )
    p_translate.set_defaults(func=cmd_translate)

    # apply
    p_apply = subparsers.add_parser(
        "apply", help="Apply translations to data files"
    )
    p_apply.set_defaults(func=cmd_apply)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
