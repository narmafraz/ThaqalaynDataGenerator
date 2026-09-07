"""Chapter/book title translation into the 10 non-English UI languages
(SPARK_AI_CONTENT_ROADMAP item 5).

Reads `index/books.en.json` (+ Arabic anchors from `books.ar.json`) and
produces `index/books.{lang}.json` files in the same shape, via batched
Spark/Qwen calls with a strict JSON schema. Durable + resumable: every batch
response is persisted under
`ThaqalaynDataSources/ai-content/chapter-titles/{lang}/batch_{i:04d}.json`
and skipped on re-run (delete a file to redo it).

Usage:
  python scripts/translate_chapter_titles.py extract
      Build the work manifest from the built ThaqalaynData index.
  python scripts/translate_chapter_titles.py run --langs fa,ur --sample 30
      Translate (sample first! iterate on prompt quality before full runs).
  python scripts/translate_chapter_titles.py run --langs all --workers 8
      Full corpus, all 10 languages.
  python scripts/translate_chapter_titles.py merge
      Write index/books.{lang}.json for every language with responses.
  python scripts/translate_chapter_titles.py report
      Coverage/validation summary of the responses on disk.
"""
import argparse
import asyncio
import hashlib
import json
import os
import re
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.environ.get("DESTINATION_DIR", os.path.join(ROOT, "..", "ThaqalaynData"))
SOURCES = os.environ.get("SOURCE_DATA_DIR", os.path.join(ROOT, "..", "ThaqalaynDataSources"))
OUT_DIR = os.path.join(SOURCES, "ai-content", "chapter-titles")
MANIFEST = os.path.join(OUT_DIR, "titles_manifest.json")

LANGS = {
    "ur": "Urdu", "tr": "Turkish", "fa": "Farsi (Persian)", "id": "Indonesian",
    "bn": "Bengali", "es": "Spanish", "fr": "French", "de": "German",
    "ru": "Russian", "zh": "Chinese (Simplified)",
}

# Script sanity: expected unicode ranges per language (None = Latin-based).
SCRIPT_RANGES = {
    "ur": r"[؀-ۿ]", "fa": r"[؀-ۿ]",
    "bn": r"[ঀ-৿]", "ru": r"[Ѐ-ӿ]",
    "zh": r"[一-鿿]",
}

BATCH_SIZE = 25

_SYSTEM = """You are a specialist translator of Twelver Shia Islamic scholarly texts. You translate CHAPTER and BOOK TITLES from classical Shia hadith collections (al-Kafi, Tahdhib al-Ahkam, Nahj al-Balagha, etc.) into {language}.

RULES:
- Translate each title into natural, formal {language} as used in Islamic scholarly publishing for {language}-speaking Shia communities.
- Use the established Islamic terminology of {language}: words like salat, wudu, zakat, hajj, jihad, imam, hadith have conventional renderings in {language} — use those, do not invent new ones.
- Keep proper nouns (names of Imams, narrators, places, book names) in their conventional {language} form; transliterate if no convention exists.
- PERSON NAMES must be written in {language}'s own script (e.g. Chinese characters for Chinese, Cyrillic for Russian, Bengali script for Bengali). Never leave a name in Latin letters when {language} uses a different script, and never leave it in Arabic script unless {language} is written in Arabic script.
- The ARABIC original (when given) is authoritative for meaning; the ENGLISH is a reference translation.
- Keep titles concise — these are navigation labels, not explanations.
- TRANSLATE EVERY WORD: never leave an English word untranslated in the output. If a term has no {language} equivalent, transliterate it into {language} script.
- Do not add numbering, punctuation decorations, or commentary.
- Output valid JSON only: {{"items": [{{"i": <index>, "t": "<translated title>"}}, ...]}} with exactly one item per input title, same "i" values."""


def _load_index(name):
    with open(os.path.join(DATA, "index", name), encoding="utf-8") as f:
        return json.load(f)


def cmd_extract(_args):
    en = _load_index("books.en.json")
    try:
        ar = _load_index("books.ar.json")
    except FileNotFoundError:
        ar = {}
    items = []
    for path, entry in sorted(en.items()):
        title = (entry or {}).get("title")
        if not title or not str(title).strip():
            continue
        items.append({
            "path": path,
            "en": str(title).strip(),
            "ar": str((ar.get(path) or {}).get("title") or "").strip() or None,
            "part_type": entry.get("part_type"),
        })
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(MANIFEST, "w", encoding="utf-8") as f:
        json.dump({"count": len(items), "items": items}, f, ensure_ascii=False, indent=1)
    print(f"manifest: {len(items)} titles -> {MANIFEST}")


def _schema(n):
    return {
        "type": "object",
        "properties": {"items": {
            "type": "array", "minItems": n, "maxItems": n,
            "items": {"type": "object",
                      "properties": {"i": {"type": "integer"}, "t": {"type": "string"}},
                      "required": ["i", "t"], "additionalProperties": False}}},
        "required": ["items"], "additionalProperties": False,
    }


def _build_user(batch, lang_name):
    lines = [f"Translate these {len(batch)} titles into {lang_name}:", ""]
    for j, it in enumerate(batch):
        lines.append(f"[{j}] ENGLISH: {it['en']}")
        if it.get("ar"):
            lines.append(f"    ARABIC: {it['ar']}")
    return "\n".join(lines)


def _validate(batch, items, lang):
    """Return (ok, reason). Count, index coverage, non-empty, script sanity."""
    if len(items) != len(batch):
        return False, f"count {len(items)} != {len(batch)}"
    seen = {it.get("i") for it in items}
    if seen != set(range(len(batch))):
        return False, "index coverage mismatch"
    rng = SCRIPT_RANGES.get(lang)
    for it in items:
        t = (it.get("t") or "").strip()
        if not t:
            return False, f"empty translation at i={it.get('i')}"
        if len(t) > 300:
            return False, f"suspiciously long ({len(t)} chars) at i={it.get('i')}"
        if rng:
            if not re.search(rng, t):
                return False, f"no {lang}-script characters at i={it.get('i')}: {t[:40]!r}"
            # Cross-script leak: untranslated English words inside a non-Latin
            # title (e.g. zh sample produced '真主 imposed 的...'). Isolated
            # Latin letters/abbreviations are tolerated; word-runs are not.
            if re.search(r"[A-Za-z]{3,}", t):
                return False, f"latin word leaked into {lang} at i={it.get('i')}: {t[:40]!r}"
    return True, ""


async def _run_lang(lang, items, workers, model):
    from app.pipeline_cli.openai_backend import call_openai
    from app.pipeline_cli.translation_phase import _strip_code_fences

    lang_dir = os.path.join(OUT_DIR, lang)
    os.makedirs(lang_dir, exist_ok=True)
    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    sem = asyncio.Semaphore(workers)
    stats = {"ok": 0, "failed": 0, "skipped": 0}

    async def do_batch(bi, batch):
        out_path = os.path.join(lang_dir, f"batch_{bi:04d}.json")
        key = hashlib.sha1(json.dumps([b["path"] for b in batch]).encode()).hexdigest()[:12]
        if os.path.exists(out_path):
            try:
                prev = json.load(open(out_path, encoding="utf-8"))
                if prev.get("key") == key:
                    stats["skipped"] += 1
                    return
            except Exception:
                pass
        async with sem:
            system = _SYSTEM.format(language=LANGS[lang])
            user = _build_user(batch, LANGS[lang])
            fmt = {"type": "json_schema", "json_schema": {
                "name": "title_translations", "schema": _schema(len(batch)), "strict": True}}
            last = "exhausted"
            for _attempt in range(3):
                cr = await call_openai(system, user, model=model,
                                       max_output_tokens=120 * len(batch) + 256,
                                       response_format=fmt)
                if "error" in cr:
                    last = f"api error: {str(cr['error'])[:120]}"
                    continue
                try:
                    parsed = json.loads(_strip_code_fences(cr.get("result", "")))
                except (json.JSONDecodeError, ValueError) as e:
                    last = f"parse: {e}"
                    continue
                got = parsed.get("items") or []
                ok, reason = _validate(batch, got, lang)
                if not ok:
                    last = reason
                    continue
                by_i = {it["i"]: it["t"].strip() for it in got}
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump({"key": key, "lang": lang,
                               "paths": [b["path"] for b in batch],
                               "titles": [by_i[j] for j in range(len(batch))]},
                              f, ensure_ascii=False, indent=1)
                stats["ok"] += 1
                return
            # Batch exhausted (T=0 retries are deterministic). Fall back to
            # per-title calls with a corrective instruction — a different
            # prompt shape breaks the repeated failure.
            titles = []
            for it in batch:
                one = None
                last_r = ""
                # Sampled retries (temperature) — T=0 repeats the exact failure.
                for temp in (0.0, 0.6, 0.9):
                    cr = await call_openai(
                        _SYSTEM.format(language=LANGS[lang]) +
                        "\n- CRITICAL: a previous attempt left English words untranslated. Every single word must be rendered in the target language/script.",
                        _build_user([it], LANGS[lang]),
                        model=model, max_output_tokens=400, temperature=temp,
                        response_format={"type": "json_schema", "json_schema": {
                            "name": "title_translations", "schema": _schema(1), "strict": True}})
                    if "error" in cr:
                        last_r = str(cr["error"])[:80]
                        continue
                    try:
                        got = json.loads(_strip_code_fences(cr.get("result", ""))).get("items") or []
                    except (json.JSONDecodeError, ValueError) as e:
                        last_r = f"parse: {e}"
                        continue
                    ok, last_r = _validate([it], [{**got[0], "i": 0}] if got else [], lang)
                    if ok:
                        one = got[0]["t"].strip()
                        break
                if one is None:
                    print(f"  fallback still failing [{it['en'][:40]}]: {last_r}", flush=True)
                titles.append(one)
            # Persist whatever succeeded; nulls fall back to the English title
            # at merge time. One stubborn title must not cost its batch.
            n_ok = sum(1 for t in titles if t is not None)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump({"key": key, "lang": lang,
                           "paths": [b["path"] for b in batch],
                           "titles": titles}, f, ensure_ascii=False, indent=1)
            if n_ok == len(titles):
                stats["ok"] += 1
                print(f"  salvaged {lang} batch {bi} via per-title fallback", flush=True)
            else:
                stats["failed"] += 1
                print(f"  {lang} batch {bi}: {n_ok}/{len(titles)} salvaged; "
                      f"last reason: {last}", flush=True)

    await asyncio.gather(*(do_batch(bi, b) for bi, b in enumerate(batches)))
    print(f"{lang}: batches ok={stats['ok']} skipped={stats['skipped']} failed={stats['failed']}")


def cmd_run(args):
    with open(MANIFEST, encoding="utf-8") as f:
        items = json.load(f)["items"]
    if args.sample:
        items = items[:args.sample]
    langs = list(LANGS) if args.langs == "all" else [l.strip() for l in args.langs.split(",")]
    for lang in langs:
        assert lang in LANGS, f"unknown lang {lang}"

    async def _run_all():
        # One event loop for the whole run: per-language asyncio.run() calls
        # left the shared AsyncOpenAI client's pooled connections finalizing
        # on a closed loop ("RuntimeError: Event loop is closed" teardown
        # noise between languages).
        for lang in langs:
            print(f"=== {lang} ({LANGS[lang]}): {len(items)} titles ===", flush=True)
            await _run_lang(lang, items, args.workers, args.model)

    asyncio.run(_run_all())


def _collect(lang):
    lang_dir = os.path.join(OUT_DIR, lang)
    out = {}
    if not os.path.isdir(lang_dir):
        return out
    for fn in sorted(os.listdir(lang_dir)):
        if not fn.endswith(".json"):
            continue
        doc = json.load(open(os.path.join(lang_dir, fn), encoding="utf-8"))
        for path, title in zip(doc["paths"], doc["titles"]):
            if title:  # nulls = untranslatable -> English fallback at merge
                out[path] = title
    return out


def cmd_merge(_args):
    en = _load_index("books.en.json")
    for lang in LANGS:
        got = _collect(lang)
        if not got:
            continue
        # Full file with per-entry English fallback: the frontend's fallback
        # machinery is per-FILE, so a translated index must never have holes.
        out = {}
        translated = 0
        for path, entry in en.items():
            e = dict(entry)
            if path in got:
                e["title"] = got[path]
                translated += 1
            out[path] = e
        fp = os.path.join(DATA, "index", f"books.{lang}.json")
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False)
        print(f"books.{lang}.json: {translated}/{len(en)} translated (rest EN fallback)")


def cmd_report(_args):
    with open(MANIFEST, encoding="utf-8") as f:
        total = json.load(f)["count"]
    for lang in LANGS:
        got = _collect(lang)
        print(f"{lang}: {len(got)}/{total} ({len(got)/total:.1%})")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("extract")
    r = sub.add_parser("run")
    r.add_argument("--langs", default="all")
    r.add_argument("--sample", type=int, default=0)
    r.add_argument("--workers", type=int, default=8)
    r.add_argument("--model", default="qwen36-fast")
    sub.add_parser("merge")
    sub.add_parser("report")
    args = ap.parse_args()
    {"extract": cmd_extract, "run": cmd_run, "merge": cmd_merge,
     "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    main()
