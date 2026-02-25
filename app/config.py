"""Centralized configuration for ThaqalaynDataGenerator.

This module holds project-wide constants and paths that were previously
hardcoded across multiple files. Import from here instead of defining
constants locally.
"""

import os

# ─── Paths ────────────────────────────────────────────────────────────────

# Root directory of the app/ package
APP_DIR = os.path.dirname(os.path.abspath(__file__))

# Source data directory (defaults to sibling ThaqalaynDataSources directory)
DEFAULT_SOURCE_DATA_DIR = "../ThaqalaynDataSources/"
SOURCE_DATA_DIR = os.environ.get("SOURCE_DATA_DIR", DEFAULT_SOURCE_DATA_DIR)

# Raw source data directory
RAW_DIR = os.path.join(SOURCE_DATA_DIR, "scraped")

# AI pipeline data and content directories
AI_PIPELINE_DATA_DIR = os.path.join(SOURCE_DATA_DIR, "ai-pipeline-data")
AI_CONTENT_DIR = os.path.join(SOURCE_DATA_DIR, "ai-content")

# Output destination (defaults to sibling ThaqalaynData directory)
DEFAULT_DESTINATION_DIR = "../ThaqalaynData/"

# ─── Book Identifiers ────────────────────────────────────────────────────

QURAN_BOOK_INDEX = 1
QURAN_BOOK_PATH = "/books/quran"

KAFI_BOOK_INDEX = 2
KAFI_BOOK_PATH = "/books/al-kafi"

# ─── Translation IDs ─────────────────────────────────────────────────────

HUBEALI_TRANSLATION_ID = "en.hubeali"
SARWAR_TRANSLATION_ID = "en.sarwar"

# ─── Data Generation Settings ────────────────────────────────────────────

# JSON output encoding
JSON_ENCODING = "utf-8"
JSON_ENSURE_ASCII = False
JSON_INDENT = 2

# ThaqalaynAPI scraper settings
THAQALAYN_API_BASE_URL = "https://www.thaqalayn-api.net/api/v2"
THAQALAYN_API_DELAY_SECONDS = 0.5


def get_raw_path(*parts: str) -> str:
    """Build an absolute path under the scraped/ data directory.

    Usage:
        get_raw_path("tanzil_net", "quran_simple.txt")
        get_raw_path("hubeali_com", "Al-Kafi-Volume-1")
    """
    return os.path.join(RAW_DIR, *parts)
