"""Scrape Shia tafsirs from altafsir.com.

altafsir.com is an Arabic tafsir site from the Royal Aal al-Bayt Institute.
Shia tafsirs are under madhab ID 4.

Key insights from reverse-engineering the site:
- Content is in the initial HTML response (no AJAX needed)
- Encoding is windows-1256 (not UTF-8)
- Commentary text is inside <Font class='TextResultArabic'><font color=black>...</font></Font>
- A single URL returns one "page" of tafsir. Long commentary is paginated with
  &Page=N&Size=1 query parameters. Max page count is discoverable from
  InnerLink_onchange(tafsir, page, 1) links in the HTML.
- Multiple ayahs often share the same commentary block (e.g., al-Mizan discusses
  Fatiha verses 1-5 together as one block). We detect this by content hash.

Usage:
    python app/scrapers/scrape_altafsir.py --list
    python app/scrapers/scrape_altafsir.py --tafsir 56 --surah 1   # al-Mizan, Fatiha
    python app/scrapers/scrape_altafsir.py --tafsir 38             # al-Qummi, all surahs
    python app/scrapers/scrape_altafsir.py                         # All tafsirs, all surahs
    python app/scrapers/scrape_altafsir.py --dry-run

Rate-limited to 0.5 sec/request to be respectful.
"""

import argparse
import hashlib
import html as htmllib
import json
import os
import re
import sys
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.stdout.reconfigure(encoding="utf-8")

# Reusable session with connection pooling and retries
_session: requests.Session | None = None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; ThaqalaynScraper/1.0; academic)",
            "Accept-Language": "ar",
        })
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=5, pool_maxsize=5))
        _session = s
    return _session

# Shia tafsirs on altafsir.com (madhab ID 4), verified from live page 2026-04-23
SHIA_TAFSIRS = {
    3: {
        "edition_id": "ar.majma",
        "name_ar": "مجمع البيان في تفسير القرآن",
        "name_en": "Majma' al-Bayan fi Tafsir al-Quran",
        "author_ar": "الفضل بن الحسن الطبرسي",
        "author_en": "Al-Fadl ibn al-Hasan al-Tabarsi",
        "death": "548 AH",
    },
    38: {
        "edition_id": "ar.qummi",
        "name_ar": "تفسير القرآن",
        "name_en": "Tafsir al-Qummi",
        "author_ar": "علي بن إبراهيم القمي",
        "author_en": "Ali ibn Ibrahim al-Qummi",
        "death": "329 AH",
    },
    39: {
        "edition_id": "ar.tibyan",
        "name_ar": "التبيان الجامع لعلوم القرآن",
        "name_en": "Al-Tibyan al-Jami' li-Ulum al-Quran",
        "author_ar": "الشيخ الطوسي",
        "author_en": "Sheikh al-Tusi",
        "death": "460 AH",
    },
    40: {
        "edition_id": "ar.sadra",
        "name_ar": "تفسير صدر المتألهين",
        "name_en": "Tafsir Sadr al-Muta'allihin",
        "author_ar": "صدر المتألهين الشيرازي",
        "author_en": "Mulla Sadra al-Shirazi",
        "death": "1059 AH",
    },
    41: {
        "edition_id": "ar.safi.altafsir",
        "name_ar": "الصافي في تفسير كلام الله الوافي",
        "name_en": "Al-Safi fi Tafsir Kalam Allah al-Wafi",
        "author_ar": "الفيض الكاشاني",
        "author_en": "Al-Fayz al-Kashani",
        "death": "1090 AH",
    },
    42: {
        "edition_id": "ar.saadah",
        "name_ar": "تفسير بيان السعادة في مقامات العبادة",
        "name_en": "Bayan al-Sa'adah fi Maqamat al-Ibadah",
        "author_ar": "سلطان محمد الجنابذي",
        "author_en": "Sultan Muhammad al-Junabadhi",
        "death": "1327 AH",
    },
    56: {
        "edition_id": "ar.mizan.altafsir",
        "name_ar": "الميزان في تفسير القرآن",
        "name_en": "Al-Mizan fi Tafsir al-Quran",
        "author_ar": "العلامة الطباطبائي",
        "author_en": "Allamah Tabatabai",
        "death": "1402 AH",
    },
    110: {
        "edition_id": "ar.burhan",
        "name_ar": "البرهان في تفسير القرآن",
        "name_en": "Al-Burhan fi Tafsir al-Quran",
        "author_ar": "هاشم البحراني",
        "author_en": "Hashim al-Bahrani",
        "death": "1107 AH",
    },
}

# Number of ayahs per surah (standard Quran)
AYAH_COUNTS = [
    7, 286, 200, 176, 120, 165, 206, 75, 129, 109,
    123, 111, 43, 52, 99, 128, 111, 110, 98, 135,
    112, 78, 118, 64, 77, 227, 93, 88, 69, 60,
    34, 30, 73, 54, 45, 83, 182, 88, 75, 85,
    54, 53, 89, 59, 37, 35, 38, 29, 18, 45,
    60, 49, 62, 55, 78, 96, 29, 22, 24, 13,
    14, 11, 11, 18, 12, 12, 30, 52, 52, 44,
    28, 28, 20, 56, 40, 31, 50, 40, 46, 42,
    29, 19, 36, 25, 22, 17, 19, 26, 30, 20,
    15, 21, 11, 8, 8, 19, 5, 8, 8, 11,
    11, 8, 3, 9, 5, 4, 7, 3, 6, 3,
    5, 4, 5, 6,
]

OUTPUT_DIR = os.path.join(
    os.environ.get("SOURCE_DATA_DIR", "../ThaqalaynDataSources/"),
    "scraped", "altafsir_com",
)

BASE_URL = "https://www.altafsir.com/Tafasir.asp"
NOT_FOUND_MARKER = "تفسير هذه الآية غير موجود"
RATE_LIMIT_SEC = 0.2

# Regex patterns
PAGE_LINK_RE = re.compile(r"InnerLink_onchange\(\d+,(\d+),\d+\)")
COMMENTARY_RE = re.compile(
    r"<[Ff]ont class=['\"]?TextResultArabic['\"]?><font color=['\"]?black['\"]?>(.*?)</font></[Ff]ont>",
    re.DOTALL,
)
AYAH_TEXT_RE = re.compile(
    r"id=['\"]?AyahText['\"]?[^>]*>(.*?)</h2>",
    re.DOTALL | re.IGNORECASE,
)
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def clean_text(raw: str) -> str:
    """Strip HTML tags, decode entities, normalize whitespace."""
    text = TAG_RE.sub(" ", raw)
    text = htmllib.unescape(text)
    text = WS_RE.sub(" ", text).strip()
    return text


def fetch_page(tafsir_id: int, surah: int, ayah: int, page: int = 1) -> str | None:
    """Fetch one page of tafsir HTML. Returns None on failure."""
    url = (
        f"{BASE_URL}?tMadhNo=4&tTafsirNo={tafsir_id}&tSoraNo={surah}"
        f"&tAyahNo={ayah}&tDisplay=yes&Page={page}&Size=1&LanguageId=1"
    )
    try:
        resp = get_session().get(url, timeout=30)
        resp.raise_for_status()
        # Server returns windows-1256 encoded Arabic
        return resp.content.decode("windows-1256", errors="replace")
    except requests.exceptions.RequestException as e:
        print(f"      ERROR fetching {url}: {e}")
        return None


def parse_page(html: str) -> dict:
    """Extract tafsir content from HTML page. Returns dict with text, pages, ayah_range."""
    if NOT_FOUND_MARKER in html:
        return {"text": "", "max_page": 1, "ayah_range": "", "has_tafsir": False}

    # Find max page number from InnerLink_onchange(tafsir, N, 1) links
    pages = PAGE_LINK_RE.findall(html)
    max_page = max(map(int, pages)) if pages else 1

    # Extract commentary blocks
    commentary_blocks = COMMENTARY_RE.findall(html)
    # Filter out very short blocks (headers, navigation)
    commentary_texts = []
    for block in commentary_blocks:
        cleaned = clean_text(block)
        if len(cleaned) > 50:  # Skip tiny blocks (likely headers)
            commentary_texts.append(cleaned)

    text = "\n\n".join(commentary_texts)

    # Extract ayah range from AyahText header
    ayah_match = AYAH_TEXT_RE.search(html)
    ayah_range = clean_text(ayah_match.group(1)) if ayah_match else ""

    return {
        "text": text,
        "max_page": max_page,
        "ayah_range": ayah_range,
        "has_tafsir": True,
    }


def fetch_full_tafsir(tafsir_id: int, surah: int, ayah: int) -> dict:
    """Fetch all pages of tafsir for one ayah, combined into single text block."""
    first = fetch_page(tafsir_id, surah, ayah, page=1)
    time.sleep(RATE_LIMIT_SEC)
    if not first:
        return {"text": "", "has_tafsir": False, "ayah_range": ""}

    parsed = parse_page(first)
    if not parsed["has_tafsir"]:
        return {"text": "", "has_tafsir": False, "ayah_range": ""}

    all_text = [parsed["text"]]
    for page_num in range(2, parsed["max_page"] + 1):
        page_html = fetch_page(tafsir_id, surah, ayah, page=page_num)
        time.sleep(RATE_LIMIT_SEC)
        if page_html:
            page_parsed = parse_page(page_html)
            if page_parsed["text"]:
                all_text.append(page_parsed["text"])

    return {
        "text": "\n\n".join(all_text),
        "has_tafsir": True,
        "ayah_range": parsed["ayah_range"],
    }


def text_hash(text: str) -> str:
    """Short hash for content deduplication."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:12]


def scrape_surah(tafsir_id: int, surah: int, dry_run: bool = False) -> dict:
    """Scrape all ayahs of one surah for one tafsir, deduplicating by content.

    Returns dict with:
        blocks: [str] — unique commentary blocks
        ayahs: [{ayah: int, block: int, range: str}]
    """
    ayah_count = AYAH_COUNTS[surah - 1]
    blocks: list[str] = []
    block_hashes: dict[str, int] = {}  # hash -> block_idx
    ayahs: list[dict] = []
    last_page1_hash: str | None = None
    last_block_idx: int | None = None

    for ayah_num in range(1, ayah_count + 1):
        # Optimization: fetch page 1 first. If its hash matches the previous ayah's
        # page-1 hash, we're looking at the same block — reuse it without fetching
        # additional pages. This avoids redundant fetches for large commentary blocks
        # that span many ayahs.
        first_html = fetch_page(tafsir_id, surah, ayah_num, page=1)
        time.sleep(RATE_LIMIT_SEC)
        if not first_html:
            continue

        parsed_first = parse_page(first_html)
        if not parsed_first["has_tafsir"]:
            continue

        page1_hash = text_hash(parsed_first["text"])

        if page1_hash == last_page1_hash and last_block_idx is not None:
            # Same block as previous ayah — reuse
            ayahs.append({"ayah": ayah_num, "block": last_block_idx})
            continue

        # New block — fetch remaining pages
        all_text = [parsed_first["text"]]
        for page_num in range(2, parsed_first["max_page"] + 1):
            page_html = fetch_page(tafsir_id, surah, ayah_num, page=page_num)
            time.sleep(RATE_LIMIT_SEC)
            if page_html:
                page_parsed = parse_page(page_html)
                if page_parsed["text"]:
                    all_text.append(page_parsed["text"])

        full_text = "\n\n".join(all_text)
        full_hash = text_hash(full_text)

        # Dedupe against earlier blocks (in case a block reappears)
        if full_hash in block_hashes:
            block_idx = block_hashes[full_hash]
        else:
            block_idx = len(blocks)
            blocks.append(full_text)
            block_hashes[full_hash] = block_idx

        ayahs.append({"ayah": ayah_num, "block": block_idx})
        last_page1_hash = page1_hash
        last_block_idx = block_idx

    return {"blocks": blocks, "ayahs": ayahs}


def scrape_tafsir(tafsir_id: int, tafsir_info: dict,
                  surahs: list[int] | None = None, dry_run: bool = False,
                  resume: bool = True) -> dict:
    """Scrape all surahs for one tafsir."""
    target_surahs = surahs or list(range(1, 115))
    tafsir_dir = os.path.join(OUTPUT_DIR, str(tafsir_id))
    if not dry_run:
        os.makedirs(tafsir_dir, exist_ok=True)

    total_ayahs = 0
    total_blocks = 0
    total_skipped_surahs = 0

    for surah_num in target_surahs:
        surah_file = os.path.join(tafsir_dir, f"{surah_num}.json")

        # Resume: skip if file exists. Many tafsirs are sparse (not every ayah
        # has a commentary), so we can't use ayah count as a completion check.
        # Pass --no-resume to re-scrape.
        if resume and os.path.exists(surah_file):
            try:
                existing = json.load(open(surah_file, encoding="utf-8"))
                total_ayahs += len(existing.get("ayahs", []))
                total_blocks += len(existing.get("blocks", []))
                total_skipped_surahs += 1
                continue
            except (OSError, json.JSONDecodeError):
                pass

        print(f"    Surah {surah_num} ({AYAH_COUNTS[surah_num - 1]} ayahs)...", flush=True)
        t0 = time.time()
        result = scrape_surah(tafsir_id, surah_num, dry_run=dry_run)
        elapsed = time.time() - t0

        if not result["ayahs"]:
            print(f"      No tafsir found (elapsed {elapsed:.1f}s)")
            continue

        print(f"      {len(result['ayahs'])} ayahs, {len(result['blocks'])} unique blocks "
              f"(elapsed {elapsed:.1f}s)")

        if not dry_run:
            surah_data = {
                "tafsir_id": tafsir_id,
                "edition_id": tafsir_info["edition_id"],
                "surah": surah_num,
                "blocks": result["blocks"],
                "ayahs": result["ayahs"],
            }
            with open(surah_file, "w", encoding="utf-8") as f:
                json.dump(surah_data, f, ensure_ascii=False, indent=2)

        total_ayahs += len(result["ayahs"])
        total_blocks += len(result["blocks"])

    return {
        "total_ayahs": total_ayahs,
        "total_blocks": total_blocks,
        "skipped_surahs": total_skipped_surahs,
    }


def main():
    parser = argparse.ArgumentParser(description="Scrape Shia tafsirs from altafsir.com")
    parser.add_argument("--tafsir", type=int, help="Specific tafsir ID")
    parser.add_argument("--surah", type=int, help="Specific surah")
    parser.add_argument("--list", action="store_true", help="List available tafsirs")
    parser.add_argument("--dry-run", action="store_true", help="Don't save files")
    parser.add_argument("--no-resume", action="store_true", help="Re-scrape already-saved surahs")
    args = parser.parse_args()

    if args.list:
        print("Available Shia tafsirs on altafsir.com:")
        for tid, info in SHIA_TAFSIRS.items():
            print(f"  ID {tid}: {info['name_en']} — {info['author_en']} (d. {info['death']})")
            print(f"         edition_id: {info['edition_id']}")
        return

    tafsirs = {args.tafsir: SHIA_TAFSIRS[args.tafsir]} if args.tafsir else SHIA_TAFSIRS
    surahs = [args.surah] if args.surah else None

    print(f"Altafsir.com Shia Tafsir Scraper")
    print(f"  Output: {OUTPUT_DIR}")
    print(f"  Tafsirs: {len(tafsirs)}")
    print(f"  Rate limit: {RATE_LIMIT_SEC}s/request")
    if args.dry_run:
        print(f"  DRY RUN")
    print()

    for tid, info in tafsirs.items():
        print(f"  [{tid}] {info['name_en']} ({info['edition_id']})...")
        t0 = time.time()
        stats = scrape_tafsir(tid, info, surahs=surahs, dry_run=args.dry_run,
                              resume=not args.no_resume)
        elapsed = time.time() - t0
        print(f"    Done: {stats['total_ayahs']} ayahs, {stats['total_blocks']} blocks, "
              f"{stats['skipped_surahs']} surahs resumed (elapsed {elapsed:.1f}s)")
        print()


if __name__ == "__main__":
    main()
