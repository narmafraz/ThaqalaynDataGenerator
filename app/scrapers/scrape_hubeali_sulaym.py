"""
Scrape the Book of Sulaym ibn Qays from hubeali.com.

The book is on a single page with 39 hadiths, each marked with
<h2 id="hadith-N"> headings. Arabic text is in <span class="arabic-auto">.

Source: https://hubeali.com/the-book-of-sulaym-bin-qays-al-hilali/

Usage:
    cd ThaqalaynDataGenerator
    source .venv/Scripts/activate
    python app/scrapers/scrape_hubeali_sulaym.py
"""

import json
import os
import re
import time
import urllib.request

try:
    from bs4 import BeautifulSoup
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "beautifulsoup4"])
    from bs4 import BeautifulSoup

SOURCE_URL = "https://hubeali.com/the-book-of-sulaym-bin-qays-al-hilali/"
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "raw", "hubeali_com", "book-of-sulaym"
)


def fetch_page():
    """Download the full page HTML."""
    print("Fetching page from {}...".format(SOURCE_URL))
    req = urllib.request.Request(SOURCE_URL)
    req.add_header("User-Agent", "ThaqalaynDataGenerator/1.0")
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
        # Try UTF-8 first, fall back to latin-1 (which never fails)
        try:
            html = raw.decode("utf-8")
        except UnicodeDecodeError:
            html = raw.decode("utf-8", errors="replace")
    print("  Fetched {} bytes".format(len(html)))
    return html


def parse_hadiths(html):
    """Parse the HTML to extract hadiths with Arabic and English text."""
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", class_="entry-content")
    if not content:
        print("ERROR: Could not find .entry-content div")
        return []

    # Find all h2 elements that are hadith headings
    hadith_headings = []
    for h2 in content.find_all("h2"):
        h2_id = h2.get("id", "")
        h2_text = h2.get_text(strip=True)
        if h2_id.startswith("hadith-") or re.match(r"HADITH\s+\d+", h2_text):
            match = re.search(r"(\d+)", h2_text)
            if match:
                hadith_headings.append((int(match.group(1)), h2))

    print("  Found {} hadith headings".format(len(hadith_headings)))

    hadiths = []
    for i, (num, heading) in enumerate(hadith_headings):
        # Collect all elements between this heading and the next hadith heading
        next_heading = hadith_headings[i + 1][1] if i + 1 < len(hadith_headings) else None

        arabic_parts = []
        english_parts = []
        sub_headings = []

        element = heading.find_next_sibling()
        while element and element != next_heading:
            if element.name == "h2":
                # Check if this is the next hadith heading
                el_text = element.get_text(strip=True)
                if re.match(r"HADITH\s+\d+", el_text):
                    break
                # Otherwise it's a sub-heading within the hadith
                sub_headings.append(el_text)
            elif element.name == "p":
                # Check for Arabic spans
                arabic_spans = element.find_all("span", class_="arabic-auto")
                if arabic_spans:
                    for span in arabic_spans:
                        text = span.get_text(strip=True)
                        if text:
                            arabic_parts.append(text)
                    # Also check for English text outside Arabic spans
                    # Remove Arabic spans temporarily to get English-only text
                    el_copy = BeautifulSoup(str(element), "html.parser")
                    for span in el_copy.find_all("span", class_="arabic-auto"):
                        span.decompose()
                    eng_text = el_copy.get_text(strip=True)
                    if eng_text and len(eng_text) > 5:
                        english_parts.append(eng_text)
                else:
                    text = element.get_text(strip=True)
                    if text:
                        english_parts.append(text)

            element = element.find_next_sibling()

        hadith = {
            "id": num,
            "arabicText": "\n\n".join(arabic_parts),
            "englishText": "\n\n".join(english_parts),
            "subHeadings": sub_headings,
        }
        hadiths.append(hadith)
        print("  Hadith {}: {} Arabic paragraphs, {} English paragraphs".format(
            num, len(arabic_parts), len(english_parts)))

    return hadiths


def save_data(hadiths, raw_html):
    """Save scraped data to files."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Save raw HTML
    html_path = os.path.join(OUTPUT_DIR, "page.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(raw_html)
    print("Saved raw HTML to {}".format(html_path))

    # Save parsed hadiths
    output_path = os.path.join(OUTPUT_DIR, "hadiths.json")
    data = {
        "source": "hubeali.com",
        "source_url": SOURCE_URL,
        "book": "The Book of Sulaym ibn Qays al-Hilali",
        "author": "Sulaym ibn Qays",
        "translator": "HubeAli",
        "total_hadiths": len(hadiths),
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "hadiths": hadiths,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("Saved {} hadiths to {}".format(len(hadiths), output_path))


def main():
    print("=" * 60)
    print("Book of Sulaym ibn Qays Scraper (hubeali.com)")
    print("=" * 60)

    html = fetch_page()
    hadiths = parse_hadiths(html)
    save_data(hadiths, html)

    print("\n" + "=" * 60)
    print("Scraping complete! {} hadiths extracted.".format(len(hadiths)))
    print("=" * 60)


if __name__ == "__main__":
    main()
