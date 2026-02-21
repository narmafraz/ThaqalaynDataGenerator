# lib.eshia.ir Scraping Assessment

## Finding: IMAGE-BASED SCANS -- Not Viable for Text Scraping

Investigation of lib.eshia.ir (e.g., `lib.eshia.ir/10083` for Tahdhib al-Ahkam)
revealed that the site serves **scanned book page images**, not selectable/parseable
Arabic text.

While the site has a "search" feature, the underlying content delivery is image-based,
making automated text extraction impractical without OCR (which introduces errors in
Arabic diacritics that would defeat the purpose of cross-validation).

## Decision

Replace lib.eshia.ir with **rafed.net page-by-page text** as the cross-validation
source for the Four Books. rafed.net provides:

1. **Word file downloads** (`download_rafed_word.py`) -- bulk Arabic text per volume
2. **Page-by-page rendered text** (`scrape_rafed_text.py`) -- structured Arabic text
   with chapter/page boundaries preserved

## Updated Cross-Validation Matrix

| Book | Source 1 (Primary) | Source 2 (Cross-validation) | Source 3 |
|------|-------------------|----------------------------|----------|
| Al-Kafi | hubeali.com (parsed) | rafed.net (Word/text) | ThaqalaynAPI |
| Man La Yahduruhu al-Faqih | ThaqalaynAPI (scraped) | rafed.net (Word/text) | -- |
| Tahdhib al-Ahkam | ghbook.ir (HTML) | rafed.net (Word/text) | -- |
| al-Istibsar | ghbook.ir (HTML) | rafed.net (Word/text) | -- |

Date: 2026-02-21
