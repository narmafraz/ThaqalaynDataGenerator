"""WikiShia MediaWiki API scraper for narrator biographies.

Scrapes narrator biographies from en.wikishia.net using the MediaWiki API.
Extracts biography data including birth/death dates, era, reliability,
teachers/students, and biography summaries.

WikiShia uses the standard MediaWiki API:
- API endpoint: https://en.wikishia.net/api.php
- Search: action=query&list=search&srsearch=<query>
- Page content: action=parse&page=<title>&prop=wikitext
- Categories: action=query&titles=<title>&prop=categories

Usage:
    from app.wikishia.scraper import WikiShiaScraper

    scraper = WikiShiaScraper()
    results = scraper.search_narrator("Muhammad ibn Ya'qub al-Kulayni")
    bio = scraper.get_biography("Muhammad ibn Ya'qub al-Kulayni")
"""

import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

WIKISHIA_API_URL = "https://en.wikishia.net/api.php"
DELAY_BETWEEN_REQUESTS = 1.0  # Be respectful to WikiShia servers
USER_AGENT = "ThaqalaynDataGenerator/1.0 (narrator-biography-enrichment)"


class BiographyData:
    """Structured biography data extracted from a WikiShia article."""

    def __init__(self):
        self.title: str = ""
        self.birth_date: Optional[str] = None
        self.death_date: Optional[str] = None
        self.era: Optional[str] = None
        self.reliability: Optional[str] = None
        self.teachers: List[str] = []
        self.students: List[str] = []
        self.biography_summary: Optional[str] = None
        self.biography_source: str = "WikiShia"
        self.wikishia_url: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        result = {}
        if self.birth_date:
            result["birth_date"] = self.birth_date
        if self.death_date:
            result["death_date"] = self.death_date
        if self.era:
            result["era"] = self.era
        if self.reliability:
            result["reliability"] = self.reliability
        if self.teachers:
            result["teachers"] = self.teachers
        if self.students:
            result["students"] = self.students
        if self.biography_summary:
            result["biography_summary"] = self.biography_summary
        result["biography_source"] = self.biography_source
        if self.wikishia_url:
            result["wikishia_url"] = self.wikishia_url
        return result


def _make_api_request(params: dict) -> Optional[dict]:
    """Make a request to the WikiShia MediaWiki API.

    Args:
        params: Query parameters for the API request.

    Returns:
        Parsed JSON response, or None on error.
    """
    params["format"] = "json"
    url = WIKISHIA_API_URL + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status == 200:
                data = resp.read().decode("utf-8")
                return json.loads(data)
            else:
                logger.warning("WikiShia API returned status %d for %s", resp.status, url)
                return None
    except urllib.error.HTTPError as e:
        logger.warning("WikiShia API HTTP error %d for %s", e.code, url)
        return None
    except Exception as e:
        logger.error("WikiShia API error for %s: %s", url, e)
        return None


class WikiShiaScraper:
    """Scraper for WikiShia narrator biographies using MediaWiki API."""

    def __init__(self, delay: float = DELAY_BETWEEN_REQUESTS):
        self.delay = delay
        self._last_request_time = 0.0

    def _throttle(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_request_time = time.time()

    def search_narrator(self, query: str, limit: int = 5) -> List[dict]:
        """Search WikiShia for articles matching a narrator name.

        Args:
            query: Search query (narrator name).
            limit: Maximum number of results.

        Returns:
            List of search result dicts with 'title', 'snippet', 'pageid'.
        """
        self._throttle()
        params = {
            "action": "query",
            "list": "search",
            "srsearch": query,
            "srlimit": str(limit),
            "srnamespace": "0",  # Main namespace only
        }
        response = _make_api_request(params)
        if not response or "query" not in response:
            return []
        return response["query"].get("search", [])

    def get_page_wikitext(self, title: str) -> Optional[str]:
        """Fetch raw wikitext content of a WikiShia page.

        Args:
            title: Page title.

        Returns:
            Raw wikitext string, or None if page doesn't exist.
        """
        self._throttle()
        params = {
            "action": "parse",
            "page": title,
            "prop": "wikitext",
        }
        response = _make_api_request(params)
        if not response or "parse" not in response:
            return None
        return response["parse"].get("wikitext", {}).get("*")

    def get_page_categories(self, title: str) -> List[str]:
        """Get categories for a WikiShia page.

        Args:
            title: Page title.

        Returns:
            List of category names.
        """
        self._throttle()
        params = {
            "action": "query",
            "titles": title,
            "prop": "categories",
            "cllimit": "50",
        }
        response = _make_api_request(params)
        if not response or "query" not in response:
            return []
        pages = response["query"].get("pages", {})
        for page in pages.values():
            cats = page.get("categories", [])
            return [c.get("title", "").replace("Category:", "") for c in cats]
        return []

    def get_biography(self, title: str) -> Optional[BiographyData]:
        """Fetch and parse biography data for a narrator from WikiShia.

        Args:
            title: WikiShia article title.

        Returns:
            BiographyData object, or None if page doesn't exist or parsing fails.
        """
        wikitext = self.get_page_wikitext(title)
        if not wikitext:
            return None

        bio = BiographyData()
        bio.title = title
        bio.wikishia_url = "https://en.wikishia.net/view/" + urllib.parse.quote(title.replace(" ", "_"))

        bio.birth_date = _extract_birth_date(wikitext)
        bio.death_date = _extract_death_date(wikitext)
        bio.era = _extract_era(wikitext)
        bio.reliability = _extract_reliability(wikitext)
        bio.teachers = _extract_teachers(wikitext)
        bio.students = _extract_students(wikitext)
        bio.biography_summary = _extract_summary(wikitext)

        return bio


# --- Wikitext parsing helpers ---

def _extract_infobox_field(wikitext: str, field_name: str) -> Optional[str]:
    """Extract a field from a MediaWiki infobox template.

    Handles patterns like:
        | Birth = 250 AH
        |Birth= 250/864
    """
    # Match field name = value, stopping at next field, template end, or blank line
    pattern = re.compile(
        r'\|\s*' + re.escape(field_name) + r'\s*=([^\n]*?)(?:\n|$)',
        re.IGNORECASE
    )
    match = pattern.search(wikitext)
    if match:
        value = match.group(1).strip()
        # Strip wikitext markup
        value = re.sub(r'\[\[([^\]|]*\|)?([^\]]*)\]\]', r'\2', value)
        value = re.sub(r"'''?", '', value)
        value = value.strip()
        if value and value.lower() not in ('unknown', 'n/a', ''):
            return value
    return None


def _extract_birth_date(wikitext: str) -> Optional[str]:
    """Extract birth date from wikitext infobox."""
    for field in ("Birth", "Born", "Birth Date", "birth_date", "birth"):
        result = _extract_infobox_field(wikitext, field)
        if result:
            return result
    return None


def _extract_death_date(wikitext: str) -> Optional[str]:
    """Extract death date from wikitext infobox."""
    for field in ("Death", "Died", "Death Date", "death_date", "death",
                  "Demise", "Death/Martyrdom"):
        result = _extract_infobox_field(wikitext, field)
        if result:
            return result
    return None


def _extract_era(wikitext: str) -> Optional[str]:
    """Extract era/period from wikitext infobox."""
    for field in ("Era", "Period", "era"):
        result = _extract_infobox_field(wikitext, field)
        if result:
            return result
    return None


def _extract_reliability(wikitext: str) -> Optional[str]:
    """Extract reliability rating from wikitext.

    WikiShia articles often mention reliability in the body text
    using terms like 'thiqah' (trustworthy), 'da'if' (weak), etc.
    """
    # Check infobox first
    for field in ("Reliability", "Grade", "Status", "Authenticity"):
        result = _extract_infobox_field(wikitext, field)
        if result:
            return result

    # Look for reliability mentions in text
    reliability_patterns = [
        (r'[Tt]hiqah?\b', "Thiqah (Trustworthy)"),
        (r'[Tt]rustworthiness|[Tt]rustworthy', "Thiqah (Trustworthy)"),
        (r"[Dd]a['`]if\b", "Da'if (Weak)"),
        (r'[Mm]ajhul\b', "Majhul (Unknown)"),
        (r'[Mm]uwath+aq\b', "Muwaththaq (Reliable)"),
        (r'[Hh]asan\b.*(?:narrator|hadith|tradition)', "Hasan (Good)"),
    ]
    for pattern, label in reliability_patterns:
        if re.search(pattern, wikitext):
            return label

    return None


def _extract_teachers(wikitext: str) -> List[str]:
    """Extract list of teachers/masters from wikitext infobox."""
    for field in ("Teachers", "Masters", "Professors", "teachers"):
        result = _extract_infobox_field(wikitext, field)
        if result:
            # Split by common delimiters
            names = re.split(r'[,،;]\s*|\n\*\s*|\n-\s*|<br\s*/?>',
                             result, flags=re.IGNORECASE)
            return [n.strip() for n in names if n.strip()]
    return []


def _extract_students(wikitext: str) -> List[str]:
    """Extract list of students from wikitext infobox."""
    for field in ("Students", "Pupils", "students"):
        result = _extract_infobox_field(wikitext, field)
        if result:
            names = re.split(r'[,،;]\s*|\n\*\s*|\n-\s*|<br\s*/?>',
                             result, flags=re.IGNORECASE)
            return [n.strip() for n in names if n.strip()]
    return []


def _extract_summary(wikitext: str) -> Optional[str]:
    """Extract first meaningful paragraph as biography summary.

    Skips infoboxes, categories, and templates at the start of the page.
    """
    # Remove templates/infoboxes (balanced {{ }})
    cleaned = _strip_templates(wikitext)
    # Remove category tags
    cleaned = re.sub(r'\[\[Category:[^\]]*\]\]', '', cleaned)
    # Remove file/image links
    cleaned = re.sub(r'\[\[(File|Image):[^\]]*\]\]', '', cleaned, flags=re.IGNORECASE)

    # Split into paragraphs and find first substantial one
    paragraphs = cleaned.split('\n\n')
    for para in paragraphs:
        # Clean up wiki markup
        text = re.sub(r'\[\[([^\]|]*\|)?([^\]]*)\]\]', r'\2', para)
        text = re.sub(r"'''?", '', text)
        text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL)
        text = re.sub(r'<ref[^/]*/>', '', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = text.strip()

        # Skip short or heading-only text
        if len(text) > 50 and not text.startswith('=='):
            return text[:1000]  # Cap at 1000 chars

    return None


def _strip_templates(wikitext: str) -> str:
    """Remove top-level {{ }} template blocks from wikitext."""
    result = []
    depth = 0
    i = 0
    while i < len(wikitext):
        if i + 1 < len(wikitext) and wikitext[i:i+2] == '{{':
            depth += 1
            i += 2
        elif i + 1 < len(wikitext) and wikitext[i:i+2] == '}}':
            depth = max(0, depth - 1)
            i += 2
        else:
            if depth == 0:
                result.append(wikitext[i])
            i += 1
    return ''.join(result)
