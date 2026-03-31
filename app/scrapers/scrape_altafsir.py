"""Scrape Shia tafsirs from altafsir.com.

altafsir.com is an ASP.NET site from the Royal Aal al-Bayt Institute that hosts
100+ tafsirs in Arabic. Shia tafsirs are under madhab ID 4.

Uses urllib (no Playwright needed — the content is returned in the HTML when
tDisplay=yes is set in the URL parameters).

Usage:
    python app/scrapers/scrape_altafsir.py
    python app/scrapers/scrape_altafsir.py --tafsir 38   # al-Qummi only
    python app/scrapers/scrape_altafsir.py --surah 1     # Surah 1 only
    python app/scrapers/scrape_altafsir.py --list         # List available tafsirs
    python app/scrapers/scrape_altafsir.py --dry-run      # Don't save files

Rate-limited to 1 request per second to be respectful to the academic resource.
"""

import argparse
import json
import os
import sys
import time
import urllib.request
from html.parser import HTMLParser

sys.stdout.reconfigure(encoding="utf-8")

# Shia tafsirs available on altafsir.com (madhab ID 4)
SHIA_TAFSIRS = {
    38: {
        "name_ar": "تفسير القمي",
        "name_en": "Tafsir al-Qummi",
        "author_ar": "علي بن إبراهيم القمي",
        "author_en": "Ali ibn Ibrahim al-Qummi",
        "death": "329 AH",
    },
    39: {
        "name_ar": "التبيان في تفسير القرآن",
        "name_en": "Al-Tibyan fi Tafsir al-Quran",
        "author_ar": "الشيخ الطوسي",
        "author_en": "Sheikh al-Tusi",
        "death": "460 AH",
    },
    41: {
        "name_ar": "تفسير الصافي",
        "name_en": "Tafsir as-Safi",
        "author_ar": "الفيض الكاشاني",
        "author_en": "Fayz Kashani",
        "death": "1091 AH",
    },
    42: {
        "name_ar": "تفسير نور الثقلين",
        "name_en": "Tafsir Nur al-Thaqalayn",
        "author_ar": "عبد علي الحويزي",
        "author_en": "Abd Ali al-Huwayzi",
        "death": "1112 AH",
    },
    56: {
        "name_ar": "الميزان في تفسير القرآن",
        "name_en": "Al-Mizan fi Tafsir al-Quran",
        "author_ar": "العلامة الطباطبائي",
        "author_en": "Allamah Tabatabai",
        "death": "1402 AH",
    },
    110: {
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
    "tafsir", "altafsir",
)

BASE_URL = "https://www.altafsir.com/Tafasir.asp"


class TafsirContentParser(HTMLParser):
    """Extract tafsir text from altafsir.com HTML response."""

    def __init__(self):
        super().__init__()
        self.in_disp_frame = False
        self.depth = 0
        self.text_parts = []
        self._current_tag = None

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        # Look for the DispFrame div which contains tafsir content
        if tag == "div" and attrs_dict.get("id") == "TextFrame":
            self.in_disp_frame = True
            self.depth = 1
            return
        if self.in_disp_frame:
            if tag == "div":
                self.depth += 1
            if tag == "br":
                self.text_parts.append("\n")
            if tag == "p":
                self.text_parts.append("\n")

    def handle_endtag(self, tag):
        if self.in_disp_frame:
            if tag == "div":
                self.depth -= 1
                if self.depth <= 0:
                    self.in_disp_frame = False
            if tag == "p":
                self.text_parts.append("\n")

    def handle_data(self, data):
        if self.in_disp_frame:
            self.text_parts.append(data)

    def get_text(self) -> str:
        return "".join(self.text_parts).strip()


def fetch_ayah_tafsir(tafsir_id: int, surah: int, ayah: int) -> str:
    """Fetch tafsir for a specific ayah from altafsir.com."""
    params = (
        f"?tMadhNo=4&tTafsirNo={tafsir_id}&tSoraNo={surah}"
        f"&tAyahNo={ayah}&tDisplay=yes&UserProfile=0&LanguageId=1"
    )
    url = BASE_URL + params

    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; ThaqalaynBot/1.0; academic research)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ar,en;q=0.5",
    })

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"    ERROR fetching surah {surah} ayah {ayah}: {e}")
        return ""

    parser = TafsirContentParser()
    parser.feed(html)
    return parser.get_text()


def scrape_tafsir(tafsir_id: int, tafsir_info: dict, surahs: list[int] | None = None,
                  dry_run: bool = False) -> dict:
    """Scrape an entire tafsir edition from altafsir.com.

    Returns stats dict.
    """
    name = tafsir_info["name_en"]
    target_surahs = surahs or list(range(1, 115))

    tafsir_dir = os.path.join(OUTPUT_DIR, str(tafsir_id))
    if not dry_run:
        os.makedirs(tafsir_dir, exist_ok=True)

    total_ayahs = 0
    empty_ayahs = 0

    for surah_num in target_surahs:
        ayah_count = AYAH_COUNTS[surah_num - 1]
        surah_file = os.path.join(tafsir_dir, f"{surah_num}.json")

        # Skip if already scraped
        if os.path.exists(surah_file) and not dry_run:
            existing = json.load(open(surah_file, encoding="utf-8"))
            if len(existing.get("ayahs", [])) >= ayah_count:
                print(f"    Surah {surah_num}: already scraped ({ayah_count} ayahs)")
                total_ayahs += ayah_count
                continue

        print(f"    Surah {surah_num}: scraping {ayah_count} ayahs...")
        ayahs = []

        for ayah_num in range(1, ayah_count + 1):
            text = fetch_ayah_tafsir(tafsir_id, surah_num, ayah_num)
            if text:
                ayahs.append({"ayah": ayah_num, "text": text})
                total_ayahs += 1
            else:
                empty_ayahs += 1

            # Rate limit: 1 request per second
            time.sleep(1.0)

        if not dry_run and ayahs:
            surah_data = {
                "tafsir_id": tafsir_id,
                "tafsir_name": name,
                "surah": surah_num,
                "ayahs": ayahs,
            }
            with open(surah_file, "w", encoding="utf-8") as f:
                json.dump(surah_data, f, ensure_ascii=False, indent=2)
            print(f"      Saved {len(ayahs)} ayahs to {surah_file}")

    return {"total_ayahs": total_ayahs, "empty_ayahs": empty_ayahs}


def main():
    parser = argparse.ArgumentParser(description="Scrape Shia tafsirs from altafsir.com")
    parser.add_argument("--tafsir", type=int, help="Specific tafsir ID to scrape")
    parser.add_argument("--surah", type=int, help="Specific surah to scrape")
    parser.add_argument("--list", action="store_true", help="List available tafsirs")
    parser.add_argument("--dry-run", action="store_true", help="Don't save files")
    args = parser.parse_args()

    if args.list:
        print("Available Shia tafsirs on altafsir.com:")
        for tid, info in SHIA_TAFSIRS.items():
            print(f"  ID {tid}: {info['name_en']} ({info['name_ar']}) — {info['author_en']} (d. {info['death']})")
        return

    tafsirs_to_scrape = {args.tafsir: SHIA_TAFSIRS[args.tafsir]} if args.tafsir else SHIA_TAFSIRS
    surahs = [args.surah] if args.surah else None

    print(f"Altafsir.com Shia Tafsir Scraper")
    print(f"  Output: {OUTPUT_DIR}")
    print(f"  Tafsirs: {len(tafsirs_to_scrape)}")
    if args.dry_run:
        print(f"  DRY RUN")
    print()

    for tid, info in tafsirs_to_scrape.items():
        print(f"  [{tid}] {info['name_en']} ({info['author_en']})...")
        stats = scrape_tafsir(tid, info, surahs=surahs, dry_run=args.dry_run)
        print(f"    Done: {stats['total_ayahs']} ayahs, {stats['empty_ayahs']} empty")
        print()

    print("Done.")


if __name__ == "__main__":
    main()
