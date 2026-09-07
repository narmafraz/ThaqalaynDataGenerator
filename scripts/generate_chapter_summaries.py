"""Chapter point-summaries with dedup (SPARK_AI_CONTENT_ROADMAP item 6).

For each chapter, produce the distinct POINTS it establishes: narrations
making the same point collapse into one point listing its supporting
narrations. Grounded STRICTLY in the chapter's own narration texts (the AI
English translations from the response files) — the prompt forbids outside
claims, and validation enforces structure.

Durable artifacts: ai-content/corpus/chapter_summaries/{chapter_id}.json,
resumable (skip-if-exists). English first; the 10-language translation of
points is a separate later pass.

Usage:
  python scripts/generate_chapter_summaries.py extract
      Build the chapter -> [verse texts] worklist from responses + Data shells.
  python scripts/generate_chapter_summaries.py run --sample 10 [--book X]
  python scripts/generate_chapter_summaries.py run --book all --workers 6
  python scripts/generate_chapter_summaries.py report
"""
import argparse
import asyncio
import json
import os
import re
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.environ.get("DESTINATION_DIR", os.path.join(ROOT, "..", "ThaqalaynData"))
SOURCES = os.environ.get("SOURCE_DATA_DIR", os.path.join(ROOT, "..", "ThaqalaynDataSources"))
RESPONSES = os.path.join(SOURCES, "ai-content", "corpus", "responses")
OUT_DIR = os.path.join(SOURCES, "ai-content", "corpus", "chapter_summaries")
WORKLIST = os.path.join(OUT_DIR, "worklist.json")

MAX_HADITH = 40          # long chapters deferred to a hierarchical pass
MAX_POINTS = 8
MAX_POINT_CHARS = 300

_SYSTEM = """You analyse chapters of Twelver Shia hadith collections. You are given the numbered narrations of ONE chapter (English translations). Produce the distinct POINTS the chapter establishes.

RULES:
- A "point" is a single teaching, ruling, historical report, or doctrinal claim.
- Narrations making the SAME point in different words collapse into ONE point; list every supporting narration number.
- Ground every point STRICTLY in the given narrations. Never add outside facts, interpretations beyond the texts, or commentary. If the narrations conflict, state the disagreement as its own point.
- 1 to {max_points} points, ordered by how strongly the chapter supports them (most-supported first).
- Each point: one clear English sentence (max ~40 words), no numbering prefix.
- "supports": the narration numbers (from the input) that state or imply the point. Every narration should support at least one point where possible.
- Output valid JSON only: {{"points": [{{"text": "...", "supports": [1, 3]}}, ...]}}"""


def _en_text_from_response(vid):
    fp = os.path.join(RESPONSES, vid + ".json")
    if not os.path.exists(fp):
        return None
    try:
        result = json.load(open(fp, encoding="utf-8")).get("result") or {}
    except (json.JSONDecodeError, OSError):
        return None
    chunks = result.get("chunks") or []
    parts = [((c.get("translations") or {}).get("en") or "").strip()
             for c in chunks if isinstance(c, dict)]
    text = " ".join(p for p in parts if p)
    if not text:
        t = (result.get("translations") or {}).get("en") or {}
        text = (t.get("text") or "").strip()
    return text or None


def cmd_extract(args):
    from app.pipeline_cli.verse_processor import verse_path_to_id
    chapters = []
    skipped_long = skipped_sparse = 0
    for root, dirs, files in os.walk(os.path.join(DATA, "books")):
        if "complete" in root:
            continue
        for f in files:
            if not f.endswith(".json") or "." in f[:-5] or "narrators" in f:
                continue
            fp = os.path.join(root, f)
            try:
                doc = json.load(open(fp, encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            if doc.get("kind") != "verse_list":
                continue
            refs = [r for r in (doc["data"].get("verse_refs") or [])
                    if r.get("part_type") in ("Hadith", "Verse") and r.get("path")]
            if len(refs) < 2:
                continue
            if len(refs) > MAX_HADITH:
                skipped_long += 1
                continue
            items = []
            for r in refs:
                text = _en_text_from_response(verse_path_to_id(r["path"]))
                if text:
                    items.append({"n": r.get("local_index"), "text": text[:2400]})
            # need most of the chapter covered to summarise it honestly
            if len(items) < max(2, int(0.8 * len(refs))):
                skipped_sparse += 1
                continue
            chapters.append({"path": doc.get("index"), "chapter_id":
                             doc.get("index", "").replace(":", "_"),
                             "n_hadith": len(refs), "items": items})
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(WORKLIST, "w", encoding="utf-8") as f:
        json.dump({"count": len(chapters), "chapters": chapters}, f, ensure_ascii=False)
    print(f"worklist: {len(chapters)} chapters "
          f"(skipped: {skipped_long} too long, {skipped_sparse} sparse AI coverage)")


def _schema():
    return {
        "type": "object",
        "properties": {"points": {
            "type": "array", "minItems": 1, "maxItems": MAX_POINTS,
            "items": {"type": "object", "properties": {
                "text": {"type": "string"},
                "supports": {"type": "array", "minItems": 1,
                             "items": {"type": "integer"}}},
                "required": ["text", "supports"], "additionalProperties": False}}},
        "required": ["points"], "additionalProperties": False,
    }


def _validate_seq(n_items, points):
    valid_ns = set(range(1, n_items + 1))
    if not points:
        return False, "no points"
    for p in points:
        t = (p.get("text") or "").strip()
        if not t or len(t) > MAX_POINT_CHARS:
            return False, f"bad point text len={len(t)}"
        if re.match(r"^\d+[.)]", t):
            return False, "point text carries numbering"
        if not p.get("supports") or not set(p["supports"]) <= valid_ns:
            return False, f"supports outside chapter: {p.get('supports')}"
    return True, ""


async def _run(chapters, workers, model):
    from app.pipeline_cli.openai_backend import call_openai
    from app.pipeline_cli.translation_phase import _strip_code_fences
    os.makedirs(OUT_DIR, exist_ok=True)
    sem = asyncio.Semaphore(workers)
    stats = {"ok": 0, "failed": 0, "skipped": 0}

    async def do(ch):
        out = os.path.join(OUT_DIR, ch["chapter_id"] + ".json")
        if os.path.exists(out):
            stats["skipped"] += 1
            return
        async with sem:
            # Present narrations renumbered 1..N: some chapters' local_index
            # values aren't 1-based/contiguous and the model then cites
            # positional numbers anyway ("supports outside chapter" failures,
            # e.g. al-istibsar 3:18). Map back to local_index on save.
            seq_to_n = {i + 1: it["n"] for i, it in enumerate(ch["items"])}
            lines = [f"Chapter with {len(ch['items'])} narrations:", ""]
            for i, it in enumerate(ch["items"]):
                lines.append(f"[{i + 1}] {it['text']}")
                lines.append("")
            fmt = {"type": "json_schema", "json_schema": {
                "name": "chapter_points", "schema": _schema(), "strict": True}}
            last = "exhausted"
            for temp in (0.0, 0.5):
                cr = await call_openai(_SYSTEM.format(max_points=MAX_POINTS),
                                       "\n".join(lines), model=model,
                                       max_output_tokens=2400, temperature=temp,
                                       response_format=fmt)
                if "error" in cr:
                    last = str(cr["error"])[:120]
                    continue
                try:
                    points = json.loads(_strip_code_fences(cr.get("result", ""))).get("points") or []
                except (json.JSONDecodeError, ValueError) as e:
                    last = f"parse: {e}"
                    continue
                ok, last = _validate_seq(len(ch["items"]), points)
                if ok:
                    for p in points:  # map sequential numbers -> local_index
                        p["supports"] = sorted(seq_to_n[s] for s in p["supports"])
                    with open(out, "w", encoding="utf-8") as f:
                        json.dump({"chapter_path": ch["path"],
                                   "kind": "chapter_summary", "model": model,
                                   "n_hadith": ch["n_hadith"],
                                   "points": points}, f, ensure_ascii=False, indent=1)
                    stats["ok"] += 1
                    return
            stats["failed"] += 1
            print(f"  FAILED {ch['chapter_id']}: {last}", flush=True)

    await asyncio.gather(*(do(c) for c in chapters))
    print(f"summaries: ok={stats['ok']} skipped={stats['skipped']} failed={stats['failed']}")


def cmd_run(args):
    chapters = json.load(open(WORKLIST, encoding="utf-8"))["chapters"]
    if args.book and args.book != "all":
        books = {b.strip() for b in args.book.split(",")}
        chapters = [c for c in chapters
                    if (c["path"] or "").split(":")[0] in books]
    if args.sample:
        # spread the sample across books for diverse content
        by_book = {}
        for c in chapters:
            by_book.setdefault(c["path"].split(":")[0], []).append(c)
        sample = []
        while len(sample) < args.sample and any(by_book.values()):
            for b in list(by_book):
                if by_book[b] and len(sample) < args.sample:
                    sample.append(by_book[b].pop(0))
        chapters = sample
    print(f"{len(chapters)} chapters queued")
    asyncio.run(_run(chapters, args.workers, args.model))


def cmd_report(_args):
    total = json.load(open(WORKLIST, encoding="utf-8"))["count"]
    have = len([f for f in os.listdir(OUT_DIR)
                if f.endswith(".json") and f != "worklist.json"])
    print(f"summaries on disk: {have}/{total}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("extract")
    r = sub.add_parser("run")
    r.add_argument("--book", default="all")
    r.add_argument("--sample", type=int, default=0)
    r.add_argument("--workers", type=int, default=6)
    r.add_argument("--model", default="qwen36-fast")
    sub.add_parser("report")
    args = ap.parse_args()
    {"extract": cmd_extract, "run": cmd_run, "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    main()
