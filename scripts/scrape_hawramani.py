"""Scrape hawramani.com classical-lexicon pages for our corpus lemmas.

For each lemma in ThaqalaynWords/lemmas/, fetch
``https://arabiclexicon.hawramani.com/{lemma_slug}/`` and save the
HTML to disk. hawramani aggregates entries from ~5-10 classical
Arabic lexicons per Arabic head-word, so one page download per lemma
gives us multi-lexicon content (Mufradat, Lisan, Qamus, Mu'jam al-
Wasit, Suyuti, Ibn ʿAbbās's Gharīb, etc.) in a single HTTP request.

Output: ``ThaqalaynWordSources/sources/hawramani-classical/raw/{slug}.html``

Idempotent: files already on disk are skipped. Failed fetches (404 +
'Not Found' h1) are recorded in ``misses.json`` so re-runs don't
re-attempt them.

Concurrent fetch with adaptive backoff — starts at 5 simultaneous
connections, slows down if 429/503 errors appear.

Usage:
    # Pre-flight: just the 100 most-frequent lemmas
    python scripts/scrape_hawramani.py --top-n 100

    # Full corpus
    python scripts/scrape_hawramani.py --full

    # Custom rate
    python scripts/scrape_hawramani.py --full --workers 3
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_LEMMAS_DIR = (
    PROJECT_ROOT / ".." / "ThaqalaynWords" / "lemmas"
).resolve()
DEFAULT_OUT_DIR = (
    PROJECT_ROOT / ".." / "ThaqalaynWordSources" / "sources" /
    "hawramani-classical" / "raw"
).resolve()

USER_AGENT = "ThaqalaynWords/1.0 (research; +https://thaqalaynwords.netlify.app/)"
BASE_URL = "https://arabiclexicon.hawramani.com/"

# hawramani's URL rewriter doesn't handle diacritics uniformly — some
# diacritized lemmas resolve, others 404. Strip Arabic diacritics
# (tashkeel) before constructing the URL. But PRESERVE alif/ya/hamza
# variants — those are semantically distinct in hawramani's lookup
# (verified empirically: ``اراد`` 404s, ``أراد`` 200s).
_DIACRITIC_MARKS = set(
    # tanwin / harakat / shadda / sukun / dagger alif / tatweel
    "ًٌٍَُِّْٰـ"
)


def strip_diacritics(s: str) -> str:
    """Remove Arabic diacritic marks. Leave consonants + hamza/ya
    variants intact."""
    return "".join(c for c in s if c not in _DIACRITIC_MARKS)


# Lemma slug → URL path. Strips diacritics before encoding.
def lemma_to_url(lemma_slug: str) -> str:
    bare = strip_diacritics(lemma_slug)
    encoded = urllib.parse.quote(bare, safe="")
    return f"{BASE_URL}{encoded}/"


# ---------------------------------------------------------------------------
# Fetch + classify
# ---------------------------------------------------------------------------

# Markers that distinguish a real hit from a 200-with-no-content page.
# We saw via probing that the 404 page is ~46.6 KB but the server may
# return 200 + a "Not Found"-h1 wrapper in some routes — so we check
# the body, not just the status.
_NOT_FOUND_BODY_MARKER = b"<h1>Not Found</h1>"
_HIT_BODY_MARKER = b'class="dictionary-entry-container"'


class FetchOutcome:
    """Discriminated result of a single fetch attempt."""
    HIT = "hit"           # 200 + has content
    MISS_404 = "miss_404" # 404 or 200-with-no-content
    ERROR = "error"       # network / 5xx / parse error
    SKIPPED = "skipped"   # already on disk


# Global throttle: when 429s appear, raise the per-request floor delay
# for all workers. Adjusted dynamically as the run progresses.
_throttle_lock = threading.Lock()
_min_interval = 0.0  # seconds — increases on 429, decays on success
_last_request_t = 0.0


def _throttle_wait():
    """Block until at least ``_min_interval`` has elapsed since the last
    completed request (across all workers). Cheap per-call lock acquire."""
    global _last_request_t
    with _throttle_lock:
        now = time.monotonic()
        wait = _min_interval - (now - _last_request_t)
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
        _last_request_t = now


def _bump_throttle_on_429():
    """Increase the global per-request delay after a 429."""
    global _min_interval
    with _throttle_lock:
        _min_interval = min(_min_interval * 2 + 0.5, 10.0)


def _ease_throttle_on_success():
    """Slowly relax the per-request delay after sustained success."""
    global _min_interval
    with _throttle_lock:
        if _min_interval > 0:
            _min_interval = max(_min_interval * 0.95, 0.0)


def fetch_one(
    stripped_slug: str, dest_dir: Path, *,
    timeout: float = 30.0,
    max_retries: int = 5,
) -> Tuple[str, str, Optional[str]]:
    """Fetch one diacritic-stripped lemma page; classify; write to disk.

    Implements:
    - Global adaptive throttle (slows everyone on 429, speeds up on
      streaks of success).
    - Per-request retry up to ``max_retries`` on 429 / 5xx / network
      errors, with exponential backoff starting at 2s.
    - Idempotent: file-already-on-disk → SKIPPED with no HTTP.

    Returns (stripped_slug, outcome, error_message).
    """
    fname = _safe_filename(stripped_slug) + ".html"
    dest = dest_dir / fname
    if dest.exists():
        return stripped_slug, FetchOutcome.SKIPPED, None

    url = f"{BASE_URL}{urllib.parse.quote(stripped_slug, safe='')}/"
    last_err = "no attempts"

    for attempt in range(max_retries):
        _throttle_wait()
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                status = resp.status
            # Successful HTTP transaction (whatever the status code).
        except urllib.error.HTTPError as e:
            if e.code == 404:
                _ease_throttle_on_success()  # 404 still counts as "server responded"
                return stripped_slug, FetchOutcome.MISS_404, None
            if e.code == 429:
                _bump_throttle_on_429()
                # Honor Retry-After if present, else exponential backoff.
                retry_after = 0
                try:
                    retry_after = int(e.headers.get("Retry-After", "0"))
                except (TypeError, ValueError):
                    pass
                wait = max(retry_after, 2 ** attempt * 2)
                last_err = f"HTTP 429 (retry in {wait}s)"
                time.sleep(wait)
                continue
            if 500 <= e.code < 600:
                last_err = f"HTTP {e.code}"
                time.sleep(2 ** attempt)
                continue
            return stripped_slug, FetchOutcome.ERROR, f"HTTP {e.code}"
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last_err = f"network: {e}"
            time.sleep(2 ** attempt)
            continue
        except Exception as e:
            return stripped_slug, FetchOutcome.ERROR, f"unexpected: {e}"

        # We got a status — classify by body.
        if status != 200:
            return stripped_slug, FetchOutcome.ERROR, f"HTTP {status}"
        if _NOT_FOUND_BODY_MARKER in body or _HIT_BODY_MARKER not in body:
            _ease_throttle_on_success()
            return stripped_slug, FetchOutcome.MISS_404, None

        # Hit — save to disk.
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            f.write(body)
        _ease_throttle_on_success()
        return stripped_slug, FetchOutcome.HIT, None

    # Out of retries.
    return stripped_slug, FetchOutcome.ERROR, f"retries exhausted: {last_err}"


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def _safe_filename(s: str) -> str:
    """Replace Windows-forbidden filename chars. NFC-Arabic + dashes OK."""
    bad = '<>:"/\\|?*\x00'
    return "".join("_" if c in bad else c for c in s)


def load_lemmas(lemmas_dir: Path) -> List[Tuple[str, int]]:
    """Return list of (lemma_slug, frequency) ordered by descending freq."""
    out: List[Tuple[str, int]] = []
    for p in lemmas_dir.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            continue
        slug = d.get("slug")
        freq = d.get("frequency_in_corpus", 0) or 0
        if slug:
            out.append((slug, freq))
    out.sort(key=lambda t: -t[1])
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--top-n", type=int,
                   help="Pre-flight: only fetch top-N most-frequent lemmas")
    g.add_argument("--full", action="store_true",
                   help="Fetch every lemma in the corpus")
    parser.add_argument("--workers", type=int, default=5,
                        help="Concurrent connections (default 5)")
    parser.add_argument("--lemmas-dir", type=Path, default=DEFAULT_LEMMAS_DIR,
                        help="Path to ThaqalaynWords/lemmas/")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR,
                        help="Where to write raw HTML files")
    args = parser.parse_args()

    if not args.top_n and not args.full:
        parser.error("specify --top-n N or --full")

    lemmas = load_lemmas(args.lemmas_dir)
    if not lemmas:
        logger.error("No lemmas at %s — build word pages first", args.lemmas_dir)
        sys.exit(1)

    selected = lemmas[: args.top_n] if args.top_n else lemmas

    # Dedupe by stripped (diacritic-free) form — many lemmas share the
    # same consonantal skeleton (e.g. نَعَمَ vs نَعِمَ → both نعم) and
    # hawramani serves the same page for both. One fetch per unique
    # stripped form. We keep the highest-frequency lemma as the
    # representative for logging purposes.
    by_stripped: Dict[str, Tuple[str, int]] = {}
    for slug, freq in selected:
        stripped = strip_diacritics(slug)
        existing = by_stripped.get(stripped)
        if existing is None or freq > existing[1]:
            by_stripped[stripped] = (slug, freq)
    unique_fetches = [(stripped, rep[0]) for stripped, rep in by_stripped.items()]
    logger.info(
        "Selected %d / %d lemmas → %d unique stripped forms to fetch "
        "(dedup eliminated %d) (workers=%d)",
        len(selected), len(lemmas), len(unique_fetches),
        len(selected) - len(unique_fetches), args.workers,
    )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    misses_path = args.out_dir / "misses.json"
    known_misses: set = set()
    if misses_path.exists():
        try:
            known_misses = set(json.load(open(misses_path, encoding="utf-8")))
        except Exception:
            pass

    # Filter out known misses from earlier runs (matching on stripped form).
    initial = len(unique_fetches)
    unique_fetches = [
        (stripped, rep) for stripped, rep in unique_fetches
        if stripped not in known_misses
    ]
    skipped_known_miss = initial - len(unique_fetches)
    if skipped_known_miss:
        logger.info("  skipping %d known misses from prior runs",
                    skipped_known_miss)

    stats = {"hit": 0, "miss_404": 0, "error": 0, "skipped": 0}
    errors: Dict[str, str] = {}
    new_misses: List[str] = []
    stats_lock = threading.Lock()
    start_t = time.monotonic()

    def record(outcome: Tuple[str, str, Optional[str]]):
        slug, kind, err = outcome
        with stats_lock:
            stats[kind] += 1
            if kind == FetchOutcome.MISS_404:
                new_misses.append(slug)
            if err:
                errors[slug] = err
            total_done = sum(stats.values())
            if total_done % 200 == 0:
                elapsed = time.monotonic() - start_t
                rate = total_done / elapsed if elapsed > 0 else 0
                logger.info(
                    "  progress: %d/%d  hit=%d miss=%d err=%d skip=%d  %.1f req/s",
                    total_done, len(selected),
                    stats["hit"], stats["miss_404"],
                    stats["error"], stats["skipped"], rate,
                )

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(fetch_one, stripped, args.out_dir): stripped
                for stripped, _rep in unique_fetches
            }
            for fut in as_completed(futures):
                try:
                    record(fut.result())
                except Exception as e:
                    record((futures[fut], FetchOutcome.ERROR, str(e)))
    finally:
        # Persist accumulated misses for resumability.
        all_misses = sorted(known_misses | set(new_misses))
        with open(misses_path, "w", encoding="utf-8") as f:
            json.dump(all_misses, f, ensure_ascii=False, indent=None)

    elapsed = time.monotonic() - start_t
    logger.info("---")
    logger.info("Done in %.1f min", elapsed / 60)
    logger.info("  hit:     %d", stats["hit"])
    logger.info("  miss:    %d (saved to %s)", stats["miss_404"], misses_path.name)
    logger.info("  error:   %d", stats["error"])
    logger.info("  skipped (already on disk): %d", stats["skipped"])
    if errors:
        # Show the first few error reasons for diagnosis.
        sample = list(errors.items())[:5]
        logger.info("  sample errors:")
        for slug, err in sample:
            logger.info("    %s — %s", slug, err)
    if unique_fetches:
        hit_pct = 100 * stats["hit"] / max(1, len(unique_fetches))
        logger.info("Hit rate: %.1f%% of attempted", hit_pct)


if __name__ == "__main__":
    main()
