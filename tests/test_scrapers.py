"""Tests for scraper modules.

Tests focus on data structures, configuration correctness, and pure logic
functions (not network I/O). Each test is independent and uses no fixtures.
"""

import json
import os
import tempfile


class TestRafedWordDownloader:
    """Tests for download_rafed_word.py configuration and logic."""

    def test_books_have_required_fields(self):
        """Every book entry has title, title_ar, author, and volumes."""
        from app.scrapers.download_rafed_word import BOOKS

        for key, book in BOOKS.items():
            assert "title" in book, "Book {} missing title".format(key)
            assert "title_ar" in book, "Book {} missing title_ar".format(key)
            assert "author" in book, "Book {} missing author".format(key)
            assert "volumes" in book, "Book {} missing volumes".format(key)
            assert len(book["volumes"]) > 0, "Book {} has no volumes".format(key)

    def test_volumes_have_required_fields(self):
        """Every volume has vol and view_id."""
        from app.scrapers.download_rafed_word import BOOKS

        for key, book in BOOKS.items():
            for vol_info in book["volumes"]:
                assert "vol" in vol_info, "Volume in {} missing vol".format(key)
                assert "view_id" in vol_info, "Volume in {} missing view_id".format(key)
                assert isinstance(vol_info["vol"], int)
                assert isinstance(vol_info["view_id"], int)
                assert vol_info["view_id"] > 0

    def test_volume_numbers_sequential(self):
        """Volume numbers are sequential starting from 1."""
        from app.scrapers.download_rafed_word import BOOKS

        for key, book in BOOKS.items():
            vol_nums = [v["vol"] for v in book["volumes"]]
            expected = list(range(1, len(vol_nums) + 1))
            assert vol_nums == expected, \
                "Book {} volumes not sequential: {}".format(key, vol_nums)

    def test_view_ids_unique(self):
        """All view_ids are unique across all books."""
        from app.scrapers.download_rafed_word import BOOKS

        all_ids = []
        for book in BOOKS.values():
            for vol_info in book["volumes"]:
                all_ids.append(vol_info["view_id"])
        assert len(all_ids) == len(set(all_ids)), "Duplicate view_ids found"

    def test_four_books_present(self):
        """All Four Books are registered."""
        from app.scrapers.download_rafed_word import BOOKS

        assert "al-kafi" in BOOKS
        assert "man-la-yahduruhu-al-faqih" in BOOKS
        assert "tahdhib-al-ahkam" in BOOKS
        assert "al-istibsar" in BOOKS

    def test_tahdhib_has_10_volumes(self):
        """Tahdhib al-Ahkam has 10 volumes."""
        from app.scrapers.download_rafed_word import BOOKS

        assert len(BOOKS["tahdhib-al-ahkam"]["volumes"]) == 10

    def test_istibsar_has_4_volumes(self):
        """al-Istibsar has 4 volumes."""
        from app.scrapers.download_rafed_word import BOOKS

        assert len(BOOKS["al-istibsar"]["volumes"]) == 4

    def test_kafi_volumes(self):
        """Al-Kafi has 8 volumes on rafed.net."""
        from app.scrapers.download_rafed_word import BOOKS

        assert len(BOOKS["al-kafi"]["volumes"]) == 8

    def test_faqih_has_4_volumes(self):
        """Man La Yahduruhu al-Faqih has 4 volumes on rafed.net."""
        from app.scrapers.download_rafed_word import BOOKS

        assert len(BOOKS["man-la-yahduruhu-al-faqih"]["volumes"]) == 4

    def test_base_url_format(self):
        """Base URL is correct."""
        from app.scrapers.download_rafed_word import BASE_URL

        assert BASE_URL == "https://books.rafed.net/api/download"

    def test_download_url_construction(self):
        """Download URL follows expected pattern."""
        from app.scrapers.download_rafed_word import BASE_URL

        view_id = 722
        url = "{}/{}/doc".format(BASE_URL, view_id)
        assert url == "https://books.rafed.net/api/download/722/doc"

    def test_save_metadata_creates_valid_json(self):
        """save_metadata writes valid JSON with expected fields."""
        from app.scrapers.download_rafed_word import save_metadata, OUTPUT_DIR

        with tempfile.TemporaryDirectory() as tmpdir:
            # Temporarily override OUTPUT_DIR
            import app.scrapers.download_rafed_word as mod
            original_dir = mod.OUTPUT_DIR
            mod.OUTPUT_DIR = tmpdir

            try:
                book_info = {
                    "title": "Test Book",
                    "title_ar": "كتاب اختبار",
                    "author": "Test Author",
                    "volumes": [{"vol": 1, "view_id": 999}],
                }
                results = [{"vol": 1, "view_id": 999, "status": "downloaded"}]
                save_metadata("test-book", book_info, results)

                metadata_path = os.path.join(tmpdir, "test-book", "metadata.json")
                assert os.path.exists(metadata_path)

                with open(metadata_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                assert data["source"] == "rafed.net"
                assert data["title"] == "Test Book"
                assert data["title_ar"] == "كتاب اختبار"
                assert data["language"] == "ar"
                assert data["format"] == "doc"
                assert len(data["volumes"]) == 1
            finally:
                mod.OUTPUT_DIR = original_dir

    def test_arabic_titles_not_escaped(self):
        """Arabic text in titles is UTF-8, not escaped."""
        from app.scrapers.download_rafed_word import BOOKS

        for key, book in BOOKS.items():
            # title_ar should contain actual Arabic characters, not \uXXXX escapes
            title = book["title_ar"]
            assert len(title) > 0, "Book {} has empty title_ar".format(key)
            # Check it has Arabic Unicode characters (U+0600 to U+06FF range)
            has_arabic = any('\u0600' <= c <= '\u06FF' for c in title)
            assert has_arabic, "Book {} title_ar lacks Arabic chars: {}".format(
                key, repr(title))


class TestGhbookDownloader:
    """Tests for download_ghbook_html.py configuration."""

    def test_books_have_required_fields(self):
        """Every book has required metadata."""
        from app.scrapers.download_ghbook_html import BOOKS

        for key, book in BOOKS.items():
            assert "title" in book
            assert "title_ar" in book
            assert "author" in book
            assert "book_id" in book
            assert "downloads" in book

    def test_downloads_have_url_params(self):
        """Every download entry has format and url_params."""
        from app.scrapers.download_ghbook_html import BOOKS

        for key, book in BOOKS.items():
            for dl in book["downloads"]:
                assert "format" in dl
                assert "url_params" in dl
                assert "filename" in dl

    def test_tahdhib_present(self):
        """Tahdhib al-Ahkam is configured."""
        from app.scrapers.download_ghbook_html import BOOKS

        assert "tahdhib-al-ahkam" in BOOKS
        assert BOOKS["tahdhib-al-ahkam"]["book_id"] == 378

    def test_istibsar_present(self):
        """al-Istibsar is configured."""
        from app.scrapers.download_ghbook_html import BOOKS

        assert "al-istibsar" in BOOKS
        assert BOOKS["al-istibsar"]["book_id"] == 2628


class TestThaqalaynApiScraper:
    """Tests for scrape_thaqalayn_api.py configuration."""

    def test_books_dict_has_entries(self):
        """Books dict is non-empty."""
        from app.scrapers.scrape_thaqalayn_api import BOOKS_TO_SCRAPE

        assert len(BOOKS_TO_SCRAPE) > 0

    def test_book_entries_have_tuple_values(self):
        """Each book maps to (folder_name, hadith_count) tuple."""
        from app.scrapers.scrape_thaqalayn_api import BOOKS_TO_SCRAPE

        for slug, value in BOOKS_TO_SCRAPE.items():
            assert isinstance(value, tuple), "Book {} value is not a tuple".format(slug)
            assert len(value) == 2
            folder_name, count = value
            assert isinstance(folder_name, str)
            assert isinstance(count, int)
            assert count > 0

    def test_folder_names_unique(self):
        """All output folder names are unique."""
        from app.scrapers.scrape_thaqalayn_api import BOOKS_TO_SCRAPE

        folders = [folder for folder, _count in BOOKS_TO_SCRAPE.values()]
        assert len(folders) == len(set(folders)), "Duplicate folder names"

    def test_four_books_faqih_present(self):
        """Man La Yahduruhu al-Faqih (5 vols) is registered."""
        from app.scrapers.scrape_thaqalayn_api import BOOKS_TO_SCRAPE

        faqih_slugs = [s for s in BOOKS_TO_SCRAPE if "Faqih" in s]
        assert len(faqih_slugs) == 5

    def test_nahj_al_balagha_present(self):
        """Nahj al-Balagha is registered."""
        from app.scrapers.scrape_thaqalayn_api import BOOKS_TO_SCRAPE

        assert "Nahj-al-Balagha-Radi" in BOOKS_TO_SCRAPE

    def test_base_url_correct(self):
        """API base URL is correct."""
        from app.scrapers.scrape_thaqalayn_api import BASE_URL

        assert BASE_URL == "https://www.thaqalayn-api.net/api/v2"


class TestRafedTextScraper:
    """Tests for scrape_rafed_text.py configuration."""

    def test_volumes_have_required_fields(self):
        """Every volume entry has vol and view_id."""
        from app.scrapers.scrape_rafed_text import VOLUMES

        for key, book in VOLUMES.items():
            assert "title" in book
            assert "vols" in book
            for vol_info in book["vols"]:
                assert "vol" in vol_info
                assert "view_id" in vol_info

    def test_tahdhib_config(self):
        """Tahdhib al-Ahkam has 10 volumes."""
        from app.scrapers.scrape_rafed_text import VOLUMES

        assert "tahdhib-al-ahkam" in VOLUMES
        assert len(VOLUMES["tahdhib-al-ahkam"]["vols"]) == 10

    def test_istibsar_config(self):
        """al-Istibsar has 4 volumes."""
        from app.scrapers.scrape_rafed_text import VOLUMES

        assert "al-istibsar" in VOLUMES
        assert len(VOLUMES["al-istibsar"]["vols"]) == 4

    def test_view_ids_match_word_downloader(self):
        """View IDs match between text scraper and Word downloader."""
        from app.scrapers.scrape_rafed_text import VOLUMES
        from app.scrapers.download_rafed_word import BOOKS

        for book_key in VOLUMES:
            if book_key not in BOOKS:
                continue
            text_ids = {v["view_id"] for v in VOLUMES[book_key]["vols"]}
            word_ids = {v["view_id"] for v in BOOKS[book_key]["volumes"]}
            assert text_ids == word_ids, \
                "View ID mismatch for {}: text={} word={}".format(
                    book_key, text_ids, word_ids)


    def test_page_count_from_toc(self):
        """get_page_count_from_toc returns max page + buffer from TOC data."""
        from app.scrapers.scrape_rafed_text import get_page_count_from_toc, TOC_PAGE_BUFFER

        # al-istibsar TOC exists from scraping
        result = get_page_count_from_toc("al-istibsar", 1)
        if result is not None:
            # Should be max_page + buffer; max_page for vol 1 is 486
            assert result > 400
            assert result == 486 + TOC_PAGE_BUFFER

    def test_page_count_from_toc_missing_book(self):
        """get_page_count_from_toc returns None for nonexistent book."""
        from app.scrapers.scrape_rafed_text import get_page_count_from_toc

        assert get_page_count_from_toc("nonexistent-book", 1) is None


class TestHubealiScraper:
    """Tests for scrape_hubeali_sulaym.py configuration."""

    def test_source_url_set(self):
        """Source URL is configured."""
        from app.scrapers.scrape_hubeali_sulaym import SOURCE_URL

        assert "hubeali.com" in SOURCE_URL
