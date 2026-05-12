"""Parse hawramani.com classical-lexicon HTML dumps.

hawramani.com aggregates entries from multiple classical Arabic lexicons
on a single page per Arabic head-word. This module walks dumped HTML
files and extracts:

- the head-word (the page's primary Arabic word)
- per-lexicon entry blocks: lexicon name (EN + AR), permalink, sanitized
  body HTML

HTML structure (per page):

    <div class="dictionary-entry-container">          # one per headword variant
      <div class="dictionary-entry-title-wrapper">
        <h1 class="dictionary-entry-title"><dfn>{HEADWORD}</dfn></h1>
        <div class="description-of-entry">Entries on X in N dictionaries by...</div>
      </div>
      <div class="dictionary-entry-content">
        <div class="definition-container dictionary_{LEXICON_ID}">
          <div class="entry-meta">
            <div class="credits">
              <a href="...">
                {LEXICON_EN_NAME}
                <span class="ar">{LEXICON_AR_NAME}</span>
              </a>
            </div>
            <div class="sectionperma">{PERMALINK}</div>
          </div>
          <div class="definition">{BODY_HTML}</div>     # actual lexicon content
        </div>
        ... more definition-container divs (one per lexicon)
      </div>
    </div>

Output schema per file (one HTML file = one page = potentially multiple
headwords with their lexicon entries):

    {
      "fetched_slug": "قال",
      "url": "https://arabiclexicon.hawramani.com/قال/",
      "headwords": [
        {
          "headword_ar": "قال",
          "summary": "Entries on قال in 6 dictionaries by ...",
          "entries": [
            {
              "lexicon_id": "dictionary_48",
              "lexicon_en": "Sultan Qaboos Encyclopedia of Arab Names",
              "lexicon_ar": "موسوعة السلطان قابوس...",
              "permalink": "https://arabiclexicon.hawramani.com/?p=440#4b7660",
              "body_html": "<sanitized HTML>"
            },
            ...
          ]
        },
        ...
      ]
    }

Sanitization: only a whitelist of safe inline / structural tags + the
``href`` attribute on ``<a>`` and the ``lang``/``class`` attributes are
preserved. ``<script>``, ``<style>``, ``<iframe>``, event handlers and
``data-*`` attributes are all dropped. The body is parsed with the
stdlib ``html.parser`` so no third-party dependency is required.
"""
from __future__ import annotations

import logging
import re
import urllib.parse
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lexicon legend — id → (English name, Arabic name)
# ---------------------------------------------------------------------------

# Empirically derived from hawramani's CSS classes (``dictionary_N``)
# and credit blocks across many pages. Covers the 38 lexicons we
# observed in the pre-flight scrape. UI can render the (en, ar) tuple
# beside each ``classical_definitions.entries[].lexicon_id`` for
# attribution. New IDs are discovered automatically by the parser and
# will appear in output even if absent from this legend — the legend
# is purely a display-side convenience.
LEXICON_LEGEND: Dict[str, Dict[str, str]] = {
    "dictionary_1":  {"en": "Ibn Manẓūr, Lisān al-ʿArab (d. 1311 CE)",
                      "ar": "لسان العرب لابن منظور"},
    "dictionary_3":  {"en": "Al-Khalīl b. Aḥmad al-Farāhīdī, Kitāb al-ʿAin (d. c. 786 CE)",
                      "ar": "كتاب العين للخليل بن أحمد الفراهيدي"},
    "dictionary_4":  {"en": "Abū ʿUbayd al-Qāsim bin Salām al-Harawī, Gharīb al-Ḥadīth",
                      "ar": "غريب الحديث لأبي عبيد القاسم بن سلام"},
    "dictionary_5":  {"en": "Ghulām Thaʿlab, al-ʿAsharāt fī Gharīb al-Lugha (d. 957 CE)",
                      "ar": "العشرات في غريب اللغة لمحمد بن عبد الواحد"},
    "dictionary_6":  {"en": "Ismāʿīl bin Ḥammād al-Jawharī, Tāj al-Lugha wa Ṣiḥāḥ al-ʿArabiyya (Sihah)",
                      "ar": "تاج اللغة وصِحاح العربية للجوهري"},
    "dictionary_7":  {"en": "Ibn Fāris, Maqāyīs al-Lugha (d. 1004 CE)",
                      "ar": "مقاييس اللغة لابن فارس"},
    "dictionary_8":  {"en": "Ibn Sīda al-Mursī, Al-Muḥkam wa-l-Muḥīṭ al-Aʿẓam (d. 1066 CE)",
                      "ar": "المحكم والمحيط الأعظم لابن سيده الأندلسي"},
    "dictionary_9":  {"en": "Al-Zamakhsharī, Asās al-Balāgha (d. 1143 CE)",
                      "ar": "أساس البلاغة للزمخشري"},
    "dictionary_10": {"en": "Abū Mūsā al-Madīnī, al-Majmūʿ al-Mughīth fī Gharīb al-Qurʾān wa-l-Ḥadīth",
                      "ar": "المجموع المغيث في غريبي القرآن والحديث لأبي موسى المديني"},
    "dictionary_11": {"en": "Ibn al-Athīr al-Jazarī, al-Nihāya fī Gharīb al-Ḥadīth (d. 1210 CE)",
                      "ar": "النهاية في غريب الحديث لابن الأثير الجزري"},
    "dictionary_12": {"en": "Al-Muṭarrizī, al-Mughrib fī Tartīb al-Muʿrib (d. 1213 CE)",
                      "ar": "المغرب في ترتيب المعرب للمُطَرِّزي"},
    "dictionary_13": {"en": "Al-Ṣaghānī, al-Shawārid (d. 1252 CE)",
                      "ar": "الشوارد للصغاني"},
    "dictionary_14": {"en": "Zayn al-Dīn al-Razī, Mukhtār al-Ṣiḥāḥ (d. 1266 CE)",
                      "ar": "مختار الصحاح للرازي"},
    "dictionary_15": {"en": "Ibn Mālik, al-Alfāẓ al-Mukhtalifa fī l-Maʿānī al-Muʾtalifa",
                      "ar": "الألفاظ المختلفة في المعاني المؤتلفة لابن مالك"},
    "dictionary_16": {"en": "Abu Ḥayyān al-Gharnāṭī, Tuḥfat al-Arīb bi-mā fī l-Qurʾān min al-Gharīb",
                      "ar": "تحفة الأريب بما في القرآن من الغريب لأبي حيان"},
    "dictionary_17": {"en": "Al-Fayyūmī, Al-Miṣbāḥ al-Munīr fī Gharīb al-Sharḥ al-Kabīr",
                      "ar": "المصباح المنير للفيّومي"},
    "dictionary_18": {"en": "Al-Sharīf al-Jurjānī, Kitāb al-Taʿrīfāt (d. 1413 CE)",
                      "ar": "كتاب التعريفات للشريف الجرجاني"},
    "dictionary_19": {"en": "Firuzabadi, al-Qāmūs al-Muḥīṭ (Kamoos) (d. 1414 CE)",
                      "ar": "القاموس المحيط للفيروزآبادي"},
    "dictionary_20": {"en": "Al-Suyūṭī, Muʿjam Maqālīd al-ʿUlūm fī l-Ḥudūd wa-l-Rusūm",
                      "ar": "معجم مقاليد العلوم للسيوطي"},
    "dictionary_21": {"en": "Muḥammad al-Fattinī, Majmaʿ Biḥār al-Anwār fī Gharīb al-Tanzīl wa-l-Akhbār",
                      "ar": "مجمع بحار الأنوار للفَتِّنيّ"},
    "dictionary_22": {"en": "Al-Munāwī, al-Tawqīf ʿalā Muhimmāt al-Taʿārīf (d. 1622 CE)",
                      "ar": "التوقيف على مهمات التعاريف للمناوي"},
    "dictionary_23": {"en": "Aḥmadnagarī, Dastūr al-ʿUlamāʾ, or Jāmiʿ al-ʿUlūm fī Iṣṭilāḥāt al-Funūn",
                      "ar": "دستور العلماء للأحمدنكري"},
    "dictionary_24": {"en": "Al-Tahānawī, Kashshāf Iṣṭilāḥāt al-Funūn wa-l-ʿUlūm (d. 1777 CE)",
                      "ar": "كشّاف اصطلاحات الفنون والعلوم للتهانوي"},
    "dictionary_25": {"en": "Murtaḍa al-Zabīdī, Tāj al-ʿArūs fī Jawāhir al-Qamūs (d. 1791 CE)",
                      "ar": "تاج العروس لمرتضى الزبيدي"},
    "dictionary_27": {"en": "Al-Barakatī, al-Taʿrīfāt al-Fiqhīya (d. 1975 CE)",
                      "ar": "التعريفات الفقهيّة للبركتي"},
    "dictionary_29": {"en": "Ibn al-Tustarī al-Kātib, al-Mudhakkar wa-l-Muʾannath",
                      "ar": "المذكر والمؤنث لابن التستري الكاتب"},
    "dictionary_31": {"en": "Al-Rāghib al-Isfahānī, al-Mufradāt fī Gharīb al-Qurʾān (d. c. 1109 CE)",
                      "ar": "المفردات في غريب القرآن للراغب الأصفهاني"},
    "dictionary_32": {"en": "Reinhart Dozy, Supplément aux dictionnaires arabes",
                      "ar": "تكملة المعاجم العربية لرينهارت دوزي"},
    "dictionary_36": {"en": "Al-Ṣāḥib bin ʿAbbād, Al-Muḥīṭ fī l-Lugha (d. c. 995 CE)",
                      "ar": "المحيط في اللغة للصاحب بن عباد"},
    "dictionary_37": {"en": "Al-Ṣaghānī, al-ʿUbāb al-Dhākhir wa-l-Lubāb al-Fākhir (d. 1252 CE)",
                      "ar": "العباب الزاخر للصغاني"},
    "dictionary_38": {"en": "Hamiduddin Farahi, Mufradāt al-Qurʾān (d. 1930 CE)",
                      "ar": "مفردات القرآن للفراهي"},
    "dictionary_39": {"en": "ʿAbdullāh ibn ʿAbbās, Gharīb al-Qurʾān fī Shiʿr al-ʿArab",
                      "ar": "غريب القرآن في شعر العرب لعبد الله بن عباس"},
    "dictionary_40": {"en": "Al-Suyūṭī, al-Muhadhdhib fī-mā Waqaʿa fi l-Qurʾān min al-Muʿarrab",
                      "ar": "المهذب فيما وقع في القرآن من المعرب للسيوطي"},
    "dictionary_46": {"en": "Dictionary of Arabic Baby Names (2009)",
                      "ar": ""},
    "dictionary_48": {"en": "Sultan Qaboos Encyclopedia of Arab Names (Sultan Qaboos University, 1985)",
                      "ar": "موسوعة السلطان قابوس لأسماء العرب"},
    "dictionary_49": {"en": "Arabic-English Lexicon by Edward William Lane (d. 1876 CE)",
                      "ar": "المعجم العربي الإنجليزي لإدوارد وليام لين"},
    "dictionary_51": {"en": "Habib Anthony Salmone, An Advanced Learner's Arabic-English Dictionary",
                      "ar": "حبيب سالمون، قاموس عربي انجليزي متقدم للمتعلمين"},
    "dictionary_52": {"en": "Yāqūt al-Ḥamawī, Muʿjam al-Buldān (d. 1229 CE)",
                      "ar": "معجم البلدان لياقوت الحموي"},
}


# ---------------------------------------------------------------------------
# HTML sanitizer — allowlist approach
# ---------------------------------------------------------------------------

# Tags retained verbatim in sanitized output.
_ALLOWED_TAGS = {
    "p", "br", "b", "strong", "i", "em", "u", "span", "div", "a",
    "ul", "ol", "li", "sup", "sub",
    "h3", "h4", "h5", "h6",
    "blockquote", "q", "cite",
    "dfn", "abbr",
}

# Per-tag attribute allowlist. Anything not listed is dropped.
_ALLOWED_ATTRS = {
    "a": {"href", "lang", "title"},
    "span": {"lang", "class"},     # class=ar marks Arabic spans
    "div": {"lang", "class"},
    "p": {"lang", "dir"},
    "abbr": {"title"},
    # Default for all other tags: no attributes allowed.
}

# Self-closing tags (HTML5 void elements).
_VOID_TAGS = {"br", "hr", "img"}


class _HTMLSanitizer(HTMLParser):
    """Stream sanitizer: allowlist tags + attrs; drop script/style/etc.

    Preserves the structural order of inline content. Drops the entire
    subtree of a disallowed tag (``script``, ``style``, ``iframe``).
    Strips event handlers (``on*`` attrs) and javascript: URLs.
    """

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._out: List[str] = []
        # Stack of currently-open "blocked" tags. While > 0 we drop
        # everything (including nested children).
        self._drop_depth = 0

    @property
    def result(self) -> str:
        return "".join(self._out)

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style", "iframe", "object", "embed",
                   "form", "input", "button", "link", "meta", "select",
                   "textarea", "svg", "canvas"}:
            self._drop_depth += 1
            return
        if self._drop_depth:
            return
        if tag not in _ALLOWED_TAGS:
            # Drop the tag itself but keep its content (open-fold).
            return
        attr_str = self._render_attrs(tag, attrs)
        if tag in _VOID_TAGS:
            self._out.append(f"<{tag}{attr_str}>")
        else:
            self._out.append(f"<{tag}{attr_str}>")

    def handle_endtag(self, tag):
        if tag in {"script", "style", "iframe", "object", "embed",
                   "form", "input", "button", "link", "meta", "select",
                   "textarea", "svg", "canvas"}:
            if self._drop_depth:
                self._drop_depth -= 1
            return
        if self._drop_depth:
            return
        if tag not in _ALLOWED_TAGS or tag in _VOID_TAGS:
            return
        self._out.append(f"</{tag}>")

    def handle_startendtag(self, tag, attrs):
        # Treat <br/> as <br> etc.
        if tag in _VOID_TAGS and tag in _ALLOWED_TAGS:
            if not self._drop_depth:
                attr_str = self._render_attrs(tag, attrs)
                self._out.append(f"<{tag}{attr_str}>")
            return
        # Otherwise fall through to start+end handling.
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_data(self, data):
        if self._drop_depth:
            return
        self._out.append(self._escape_text(data))

    def handle_entityref(self, name):
        # convert_charrefs=True means most refs are already resolved
        # into data; this is a defensive fallback.
        if self._drop_depth:
            return
        self._out.append(f"&{name};")

    def _render_attrs(self, tag: str, attrs: list) -> str:
        allowed = _ALLOWED_ATTRS.get(tag, set())
        parts: List[str] = []
        for name, value in attrs:
            if not name or name.startswith("on") or name.startswith("data-"):
                continue  # event handlers and data-* dropped wholesale
            if name not in allowed:
                continue
            if value is None:
                value = ""
            if name == "href":
                # Drop javascript:/data:/vbscript: URLs.
                v_low = value.strip().lower()
                if (v_low.startswith("javascript:") or
                    v_low.startswith("vbscript:") or
                    v_low.startswith("data:")):
                    continue
            esc = self._escape_attr(value)
            parts.append(f' {name}="{esc}"')
        return "".join(parts)

    @staticmethod
    def _escape_text(s: str) -> str:
        return (s.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;"))

    @staticmethod
    def _escape_attr(s: str) -> str:
        return (s.replace("&", "&amp;")
                 .replace('"', "&quot;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;"))


def sanitize_html(raw_html: str) -> str:
    """Return a sanitized version of an HTML fragment.

    Whitelists structural/inline tags safe for ``[innerHTML]`` rendering.
    Strips script/style/iframe (and their subtrees), event handlers,
    data-* attributes, and javascript: URLs.
    """
    p = _HTMLSanitizer()
    p.feed(raw_html)
    p.close()
    out = p.result
    # Collapse runs of whitespace inside the HTML to a single space —
    # hawramani's HTML has lots of indentation noise that doesn't
    # affect rendering.
    out = re.sub(r"\s{2,}", " ", out)
    return out.strip()


# ---------------------------------------------------------------------------
# Page parser (BeautifulSoup-based, walks div structure)
# ---------------------------------------------------------------------------


def parse_hawramani_page(html: str, fetched_slug: str) -> Dict:
    """Parse one dumped hawramani HTML file → structured dict.

    Walks the page's DOM via BeautifulSoup, identifying:
    - each ``<div class="dictionary-entry-container">`` (one per headword variant)
    - inside, each ``<div class="definition-container dictionary_N">`` (one per
      lexicon)
    - within that, the lexicon credits, permalink, and ``<div class="definition">``
      body (sanitized via :func:`sanitize_html`)

    Args:
        html: The raw page HTML.
        fetched_slug: The Arabic slug used to fetch the page (e.g. ``قال``).

    Returns:
        A dict with the schema documented at the module level. If the
        page has no entries (rare — should be filtered by the scraper's
        miss-detection), returns ``{}``.
    """
    soup = BeautifulSoup(html, "html.parser")
    headwords: List[Dict] = []

    for container in soup.find_all("div", class_="dictionary-entry-container"):
        title_el = container.find("h1", class_="dictionary-entry-title")
        if not title_el:
            continue
        dfn = title_el.find("dfn")
        headword_ar = (dfn.get_text(" ", strip=True) if dfn else
                       title_el.get_text(" ", strip=True))
        if not headword_ar:
            continue

        summary_el = container.find("div", class_="description-of-entry")
        summary = summary_el.get_text(" ", strip=True) if summary_el else ""

        entries: List[Dict] = []
        # Each per-lexicon block.
        for def_container in container.find_all("div", class_="definition-container"):
            # Lexicon ID is the second class on the div (e.g. dictionary_48).
            classes = def_container.get("class") or []
            lexicon_id = next(
                (c for c in classes if c.startswith("dictionary_")),
                "",
            )

            # Credits — author + lexicon name in EN + AR.
            credits_el = def_container.find("div", class_="credits")
            lexicon_en, lexicon_ar = "", ""
            if credits_el:
                anchor = credits_el.find("a")
                if anchor:
                    ar_span = anchor.find("span", class_="ar")
                    if ar_span:
                        lexicon_ar = ar_span.get_text(" ", strip=True)
                        ar_span.extract()  # remove so EN text is clean
                    lexicon_en = anchor.get_text(" ", strip=True)

            # Permalink — text inside <div class="sectionperma">.
            perma_el = def_container.find("div", class_="sectionperma")
            permalink = perma_el.get_text(" ", strip=True) if perma_el else ""

            # Body — content of <div class="definition">.
            body_el = def_container.find("div", class_="definition")
            body_html = sanitize_html(body_el.decode_contents()) if body_el else ""

            entries.append({
                "lexicon_id": lexicon_id,
                "lexicon_en": lexicon_en,
                "lexicon_ar": lexicon_ar,
                "permalink": permalink,
                "body_html": body_html,
            })

        headwords.append({
            "headword_ar": headword_ar,
            "summary": summary,
            "entries": entries,
        })

    if not headwords:
        return {}
    url = (
        "https://arabiclexicon.hawramani.com/"
        + urllib.parse.quote(fetched_slug, safe="") + "/"
    )
    return {
        "fetched_slug": fetched_slug,
        "url": url,
        "headwords": headwords,
    }


def parse_hawramani_dir(
    raw_dir: Path,
) -> Dict[str, Dict]:
    """Walk a directory of dumped HTML files → dict keyed by fetched_slug.

    Skips ``misses.json`` and any non-HTML file. Returns empty dict if
    the directory doesn't exist.
    """
    if not raw_dir.is_dir():
        return {}
    out: Dict[str, Dict] = {}
    for p in raw_dir.glob("*.html"):
        fetched_slug = p.stem
        try:
            with open(p, "r", encoding="utf-8") as f:
                html = f.read()
        except Exception as e:
            logger.warning("Failed to read %s: %s", p.name, e)
            continue
        parsed = parse_hawramani_page(html, fetched_slug)
        if parsed:
            out[fetched_slug] = parsed
    return out
