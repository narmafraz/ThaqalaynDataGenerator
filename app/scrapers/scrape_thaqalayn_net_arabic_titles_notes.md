# thaqalayn.net Arabic Chapter Titles Assessment

## Finding: name_ar IS MOSTLY NULL — Not Viable Beyond Faqih

Date: 2026-04-24

ThaqalaynAPI books are missing Arabic chapter titles in our generated data
(see `index/books.ar.json` vs `index/books.en.json` — 5,199 of 8,870 entries
have no Arabic). The natural place to look for them is thaqalayn.net itself,
since the API's English chapter titles all originate there.

Both data shapes were probed:

1. **Current React app** — embeds sidebar JSON inside HTML pages with
   `name_en` / `name_ar` fields. The scraper at
   `scripts/scrape_thaqalayn_arabic_titles.py` extracts these via the
   `CHAPTER_PATTERN` regex on the double-escaped JSON.
2. **2020 site mirror** at
   `ThaqalaynDataSources/scraped/thaqalayn_net/Thaqalayn/` — static HTML;
   the chapter-title `<h*>` element shows English only, with Arabic only
   appearing inside the hadith bodies, not in chapter headings.

## Per-Book Coverage Probe

Sampled one URL per book and counted populated `name_ar` values vs total:

| Book | non-null / total |
|------|------------------|
| al-khisal (sect 1, sect 2) | 2/6, 2/208 |
| nahj-al-balagha | 3/485 |
| thawab-al-amal | 2/6 |
| al-amali-mufid | 3/108 |
| maani-al-akhbar | 2/28 |
| **man-la-yahduruhu-al-faqih** (vol 1 sect 1) | **88/178** |
| man-la-yahduruhu-al-faqih (other section) | 0/0 |

The 2-3 non-null values per book are the book/section labels (e.g.
"Introduction"), **not** chapter titles. Faqih is the lone exception, and
even there only ~50% of vol 1 has populated Arabic; vols 2-5 are largely
blank.

## Decision

- Keep the generic scraper at `scripts/scrape_thaqalayn_arabic_titles.py`
  but treat thaqalayn.net as a **Faqih-only source**.
- For all other ThaqalaynAPI books, find Arabic chapter titles **elsewhere**.
  Plausible sources (per book family):

  | Book(s) | Suggested Arabic source |
  |---|---|
  | Sheikh Saduq corpus (al-Khisal, al-Amali Saduq, al-Tawhid, Maani al-Akhbar, Uyun, Thawab al-Amal, Sifat/Fadail al-Shia, Kamal al-Din) | ghbook.ir (existing scraper for Tahdhib/Istibsar generalises), al-shia.org, hadith.inoor.ir |
  | Nahj al-Balagha | many — al-shia.org, hubeali.com, al-islam.org |
  | Kitab al-Ghayba (Numani / Tusi), al-Amali Mufid | al-shia.org, ghbook.ir |
  | Kamil al-Ziyarat, Risalat al-Huquq, Kitab al-Mumin, Kitab al-Zuhd | likely ghbook.ir or al-shia.org |
  | Kitab al-Duafa, Mujam al-Ahadith al-Mutabara | rijal/index works — harder to source |

- Do NOT re-investigate thaqalayn.net for Arabic chapter titles for these
  books. The data is not there.

## Architecture Survives

The path-keyed file layout
(`scraped/{source}/arabic_chapter_titles/{slug}.json`) and the parser-side
injector at `app/chapter_translations.py` are source-agnostic. Any future
scraper just needs to write the same shape under a different `{source}/`
subdirectory; the injector unions everything by path during the regen.
