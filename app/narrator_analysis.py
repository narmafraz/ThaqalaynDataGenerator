"""Build per-chapter narrator-chain (isnad) analysis sidecars.

Output files: `<DESTINATION_DIR>/books/.../{chapter}.narrators.json`, one per
`verse_list` chapter shell, with `kind == "narrator_analysis"`.

These sidecars power the opt-in "Narrator insights" panel on chapter pages.
They are precomputed at build time so the Angular client downloads a single
small file (a few KB) only when the reader expands the panel — nothing is
loaded otherwise, and no per-verse fan-out is needed in the browser.

Why a two-pass build
--------------------
The interesting analysis (independent transmission paths) requires knowing
which "narrators" are actually *source-Imams* (the authority a report is
narrated *from*) or non-identifying *placeholders* (collective refs like
"a number of our companions", relative refs like "his father"), versus real
*transmitters*. That classification lives in the AI field
``verse.ai.isnad_matn.narrators[].role`` / ``.identity_confidence``, which only
covers ~90% of the corpus. So we first scan every verse to build a
corpus-wide role profile per canonical narrator id, then apply that profile to
every chapter — including hadith that have no AI content of their own.

Two per-verse chain sources, with fallback:
  * ``verse.ai.isnad_matn.narrators`` — ordered, with role/confidence/names
    (preferred; ~90% coverage).
  * ``verse.narrator_chain.parts`` — narrator ids only, but present for ~100%
    of hadith. Used when AI isnad is absent.
"""

from __future__ import annotations

import json
import re
import statistics
from collections import Counter, defaultdict
from pathlib import Path

SKIP_DIRS = {"complete"}  # books/complete/ holds aggregated full-text dumps
CHAIN_PART_TYPES = {"Hadith", "Verse"}

# Roles (from AI isnad_matn) that mark a narrator as the *source* the report is
# narrated from rather than a link in the transmission path. Excluding these
# stops two unrelated hadith from being forced into one cluster merely because
# they both quote, e.g., Imam al-Sadiq.
SOURCE_ROLES = {"source", "imam", "prophet", "infallible"}

# Canonical names (normalized) that are collective or relative placeholders,
# not a single identifiable transmitter. Matched as a fallback when role data
# is unavailable. Kept deliberately small and high-precision.
PLACEHOLDER_NAME_SUBSTRINGS_AR = (
    "عدة من أصحابنا",
    "بعض أصحابنا",
    "عدة من اصحابنا",
    "بعض اصحابنا",
)
PLACEHOLDER_NAME_EXACT_EN = {
    "a number of our companions",
    "some of our companions",
    "his father",
    "my father",
    "a man",
    "someone",
}


def _verse_path_to_relpath(verse_path: str) -> str:
    """`/books/al-kafi:1:1:1:1` -> `al-kafi/1/1/1/1.json` (relative to books/)."""
    p = verse_path
    if p.startswith("/books/"):
        p = p[len("/books/"):]
    elif p.startswith("books/"):
        p = p[len("books/"):]
    return p.replace(":", "/") + ".json"


def _norm(s: str | None) -> str:
    return (s or "").strip().lower()


def _chain_from_verse(verse: dict) -> list[dict]:
    """Return an ordered list of narrator dicts for a verse.

    Each entry: {id:int, name_ar, name_en, role, confidence, ambiguous:bool}.
    Prefers AI isnad_matn; falls back to narrator_chain.parts (ids only).
    Returns [] when no chain is present.
    """
    ai = (verse.get("ai") or {})
    isnad = (ai.get("isnad_matn") or {})
    narrators = isnad.get("narrators")
    if narrators:
        out = []
        for n in narrators:
            cid = n.get("canonical_id")
            if cid is None:
                continue
            conf = _norm(n.get("identity_confidence"))
            out.append(
                {
                    "id": int(cid),
                    "name_ar": n.get("name_ar"),
                    "name_en": n.get("name_en"),
                    "role": _norm(n.get("role")),
                    "confidence": conf,
                    "ambiguous": conf == "ambiguous" or bool(n.get("ambiguity_note")),
                }
            )
        if out:
            return out

    # Fallback: narrator_chain.parts (no role/confidence info)
    nc = verse.get("narrator_chain") or {}
    out = []
    for part in nc.get("parts", []):
        if part.get("kind") != "narrator":
            continue
        path = part.get("path") or ""
        tail = path.rsplit("/", 1)[-1]
        if not tail.isdigit():
            continue
        out.append(
            {
                "id": int(tail),
                "name_ar": part.get("text"),
                "name_en": None,
                "role": "",
                "confidence": "",
                "ambiguous": False,
            }
        )
    return out


def _is_placeholder(entry: dict) -> bool:
    name_en = _norm(entry.get("name_en"))
    if name_en in PLACEHOLDER_NAME_EXACT_EN:
        return True
    name_ar = (entry.get("name_ar") or "")
    return any(sub in name_ar for sub in PLACEHOLDER_NAME_SUBSTRINGS_AR)


class NarratorProfile:
    """Corpus-wide classification of each canonical narrator id.

    Built in pass A; consumed during per-chapter analysis in pass B.
    """

    def __init__(self) -> None:
        self.role_counts: dict[int, Counter] = defaultdict(Counter)
        self.placeholder_ids: set[int] = set()
        self.ambiguous_ids: set[int] = set()
        self.names_ar: dict[int, str] = {}
        self.names_en: dict[int, str] = {}

    def observe(self, entry: dict) -> None:
        nid = entry["id"]
        if entry.get("role"):
            self.role_counts[nid][entry["role"]] += 1
        if entry.get("ambiguous"):
            self.ambiguous_ids.add(nid)
        if _is_placeholder(entry):
            self.placeholder_ids.add(nid)
        if entry.get("name_ar") and nid not in self.names_ar:
            self.names_ar[nid] = entry["name_ar"]
        if entry.get("name_en") and nid not in self.names_en:
            self.names_en[nid] = entry["name_en"]

    def is_source(self, nid: int) -> bool:
        """True if this id is, on balance, a source-Imam across the corpus."""
        rc = self.role_counts.get(nid)
        if not rc:
            return False
        src = sum(rc[r] for r in SOURCE_ROLES if r in rc)
        return src > 0 and src >= sum(rc.values()) / 2

    def is_excluded(self, nid: int) -> bool:
        """Exclude source-Imams and non-identifying placeholders from clustering.

        Ambiguity is *reported* (insight #8) but does NOT exclude a narrator
        here: a transmitter flagged ambiguous in one verse is still a real
        person who legitimately links chains, and globally dropping every such
        id over-fragments the clusters.
        """
        return nid in self.placeholder_ids or self.is_source(nid)

    def name(self, nid: int) -> dict:
        return {
            "id": nid,
            "name_en": self.names_en.get(nid),
            "name_ar": self.names_ar.get(nid),
        }


# Maps Arabic/English grading vocabulary to a normalized class, mirroring the
# Angular `getGradingClass()` so the panel's grading mix uses consistent buckets.
_GRADE_RULES = (
    ("sahih", ("صحيح", "sahih")),
    ("hasan", ("حسن", "hasan")),
    ("da'if", ("ضعيف", "da'if", "daif")),
    ("mu'tabar", ("معتبر", "mu'tabar", "muatabar", "mutabar")),
    ("majhul", ("مجهول", "majhul")),
    ("muwathaq", ("موثق", "muwathaq")),
)
_SPAN_RE = re.compile(r"^(.+?):\s*<span>\s*(.+?)\s*</span>", re.DOTALL)


def _classify_grade(text: str) -> str:
    low = (text or "").lower()
    for label, needles in _GRADE_RULES:
        if any(n in low for n in needles):
            return label
    return "other"


def _gradings_of(verse: dict, data_gradings=None) -> dict[str, str]:
    """Return {scholar -> normalized grade class} for a verse.

    Handles both corpus formats:
      * dict  (ThaqalaynAPI): {"majlisi": "...", "behbudi": "..."}
      * list  (al-Kafi/Sarwar): ["Scholar: <span>term</span> - source", ...]
    Gradings live at `data.gradings`; `data.verse.gradings` mirrors it. Pass
    `data_gradings` when only the data-level copy is available.
    """
    g = verse.get("gradings")
    if not g:
        g = data_gradings
    out: dict[str, str] = {}
    if isinstance(g, dict):
        for k, v in g.items():
            if v:
                out[str(k)] = _classify_grade(str(v))
    elif isinstance(g, list):
        for s in g:
            if not isinstance(s, str):
                continue
            m = _SPAN_RE.match(s)
            if m:
                out[m.group(1).strip()] = _classify_grade(m.group(2))
    return out


# --------------------------------------------------------------------------- #
# Clustering (union-find over shared *real* transmitters)
# --------------------------------------------------------------------------- #
def _cluster(chains: dict[int, set[int]]) -> list[list[int]]:
    """Group hadith local-indices that share at least one transmitter id.

    `chains` maps local_index -> set of (already hub-filtered) narrator ids.
    Returns a list of clusters (each a sorted list of local indices),
    ordered largest-first then by smallest index.
    """
    parent: dict[int, int] = {li: li for li in chains}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        parent[find(a)] = find(b)

    narrator_to_hadith: dict[int, list[int]] = defaultdict(list)
    for li, ids in chains.items():
        for nid in ids:
            narrator_to_hadith[nid].append(li)
    for hadiths in narrator_to_hadith.values():
        first = hadiths[0]
        for other in hadiths[1:]:
            union(first, other)

    groups: dict[int, list[int]] = defaultdict(list)
    for li in chains:
        groups[find(li)].append(li)
    clusters = [sorted(g) for g in groups.values()]
    clusters.sort(key=lambda c: (-len(c), c[0]))
    return clusters


def analyze_chapter(verses: list[dict], profile: NarratorProfile) -> dict:
    """Compute the full insight bundle for one chapter's verses.

    `verses` is the ordered list of verse dicts (only Hadith/Verse parts).
    """
    # Per-hadith extracted data. We track, for each narrator/source/grade, the
    # *list of hadith local_indices* it pertains to (not just a count) so the UI
    # can link through to those hadith. Counts are derived as len(...).
    chains: dict[int, list[dict]] = {}          # local_index -> ordered entries
    clustered_ids: dict[int, set[int]] = {}     # local_index -> hub-filtered ids
    narrator_hadith: dict[int, list[int]] = defaultdict(list)   # nid -> [local_index]
    source_hadith: dict[int, list[int]] = defaultdict(list)     # source nid -> [li]
    ambiguous_hadith: dict[int, list[int]] = defaultdict(list)  # ambiguous nid -> [li]
    grading_hadith: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    chain_lengths: list[int] = []
    ambiguous_chain_count = 0
    no_chain = 0
    ai_covered = 0

    for v in verses:
        li = v.get("local_index")
        if li is None:
            continue
        entries = _chain_from_verse(v)
        if (v.get("ai") or {}).get("isnad_matn", {}).get("narrators"):
            ai_covered += 1
        if not entries:
            no_chain += 1
            continue
        chains[li] = entries
        chain_lengths.append(len(entries))

        # de-duplicate ids within a single chain for frequency/clustering
        seen_ids = set()
        kept = set()
        chain_has_ambiguous = False
        for e in entries:
            nid = e["id"]
            is_new = nid not in seen_ids
            seen_ids.add(nid)
            if is_new and profile.is_source(nid):
                source_hadith[nid].append(li)
            if is_new and (e.get("ambiguous") or nid in profile.ambiguous_ids):
                ambiguous_hadith[nid].append(li)
            if e.get("ambiguous") or nid in profile.ambiguous_ids:
                chain_has_ambiguous = True
            if not profile.is_excluded(nid):
                kept.add(nid)
        for nid in seen_ids:
            narrator_hadith[nid].append(li)
        clustered_ids[li] = kept
        if chain_has_ambiguous:
            ambiguous_chain_count += 1

        for scholar, grade in _gradings_of(v).items():
            grading_hadith[scholar][grade].append(li)

    hadith_count = len(verses)
    analyzed = len(chains)

    # Frequency orderings derived from the hadith-index maps.
    freq = Counter({nid: len(lis) for nid, lis in narrator_hadith.items()})
    source_freq = Counter({nid: len(lis) for nid, lis in source_hadith.items()})
    ambiguous_ids = Counter({nid: len(lis) for nid, lis in ambiguous_hadith.items()})

    # Narrators are referenced by id everywhere below; their names are emitted
    # once in a per-file `narrators` lookup map (see end). `ref()` records an id
    # as referenced and returns it, so the map covers exactly what's used.
    referenced: set[int] = set()

    def ref(nid: int) -> int:
        referenced.add(nid)
        return nid

    # --- #1 independent transmission paths ---------------------------------- #
    clusters = _cluster(clustered_ids)
    cluster_out = []
    for grp in clusters:
        # the transmitters that bind this cluster (shared by >1 member)
        member_ids = Counter()
        for li in grp:
            for nid in clustered_ids[li]:
                member_ids[nid] += 1
        shared = [ref(nid) for nid, c in member_ids.most_common() if c > 1][:5]
        cluster_out.append(
            {
                "size": len(grp),
                "local_indices": grp,
                "shared_ids": shared,
            }
        )

    # --- #2 prolific narrators (real transmitters, excludes sources/placeholders)
    prolific = []
    for nid, c in freq.most_common():
        if profile.is_excluded(nid):
            continue
        prolific.append({"id": ref(nid), "hadith": sorted(narrator_hadith[nid]),
                         "pct": round(c / analyzed, 3) if analyzed else 0})
        if len(prolific) >= 15:
            break

    # --- #3 shared spine: the backbone binding the dominant cluster --------- #
    # Which real transmitters hold the largest cluster together? (i.e. appear
    # in more than one of its members). This explains *why* the big group is
    # one path, and is distinct from the global #2 prolific leaderboard.
    spine = []
    if clusters and len(clusters[0]) > 1:
        biggest = clusters[0]
        backbone: dict[int, list[int]] = defaultdict(list)
        for li in biggest:
            for nid in clustered_ids[li]:
                backbone[nid].append(li)
        spine = [
            {"id": ref(nid), "hadith": sorted(lis),
             "pct": round(len(lis) / len(biggest), 3)}
            for nid, lis in sorted(backbone.items(), key=lambda kv: -len(kv[1]))
            if len(lis) > 1
        ][:8]

    # --- #4 source / Imam distribution -------------------------------------- #
    sources = [
        {"id": ref(nid), "hadith": sorted(source_hadith[nid])}
        for nid, c in source_freq.most_common()
    ]

    # --- #5 chain length distribution --------------------------------------- #
    length_stats = {}
    if chain_lengths:
        hist = Counter(chain_lengths)
        length_stats = {
            "min": min(chain_lengths),
            "max": max(chain_lengths),
            "mean": round(statistics.mean(chain_lengths), 2),
            "median": int(statistics.median(chain_lengths)),
            "histogram": {str(k): hist[k] for k in sorted(hist)},
        }

    # --- #6 grading mix ----------------------------------------------------- #
    # {scholar: {grade: [local_indices]}} — counts are len(...) in the UI.
    gradings = {sch: {g: sorted(lis) for g, lis in grades.items()}
                for sch, grades in grading_hadith.items()}

    # --- #7 corroboration: per source, how many independent paths reach it --- #
    # For each source-Imam, count the clusters whose members cite it.
    corroboration = []
    for sid, _ in source_freq.most_common():
        citing = sorted(li for li, entries in chains.items()
                        if any(e["id"] == sid for e in entries))
        if len(citing) < 2:
            continue
        citing_set = set(citing)
        paths = sum(1 for grp in clusters if citing_set.intersection(grp))
        if paths >= 2:
            corroboration.append(
                {"id": ref(sid), "hadith": citing,
                 "independent_paths": paths}
            )

    # --- #8 ambiguity flags ------------------------------------------------- #
    ambiguity = {
        "chains_with_ambiguous": ambiguous_chain_count,
        "narrators": [
            {"id": ref(nid), "hadith": sorted(ambiguous_hadith[nid])}
            for nid, c in ambiguous_ids.most_common(10)
        ],
    }

    # --- #9 isnad graph (real transmitters as nodes; co-occurrence edges) ---- #
    node_freq = [(nid, c) for nid, c in freq.most_common()
                 if not profile.is_excluded(nid)]
    node_ids = {nid for nid, _ in node_freq}
    nodes = [{"id": ref(nid), "count": c} for nid, c in node_freq]
    edge_counts: Counter = Counter()
    for entries in chains.values():
        ids = [e["id"] for e in entries if e["id"] in node_ids]
        # adjacency edges along the chain (consecutive kept narrators)
        for a, b in zip(ids, ids[1:]):
            if a != b:
                edge_counts[tuple(sorted((a, b)))] += 1
    edges = [{"a": a, "b": b, "weight": w}
             for (a, b), w in edge_counts.most_common()]
    graph = {"nodes": nodes, "edges": edges}

    # Per-file name lookup: each referenced narrator named exactly once, as a
    # compact [en, ar] pair. The frontend resolves ids against this map, so
    # names are never repeated within the file.
    narrators = {}
    for nid in sorted(referenced):
        nm = profile.name(nid)
        narrators[str(nid)] = [nm.get("name_en"), nm.get("name_ar")]

    return {
        "hadith_count": hadith_count,
        "analyzed_count": analyzed,
        "no_chain_count": no_chain,
        "ai_coverage": ai_covered,
        "cluster_basis": "exclude_imams_placeholders",
        "independent_paths": len(clusters),
        "clusters": cluster_out,
        "prolific": prolific,
        "spine": spine,
        "sources": sources,
        "chain_lengths": length_stats,
        "gradings": gradings,
        "corroboration": corroboration,
        "ambiguity": ambiguity,
        "graph": graph,
        "narrators": narrators,
    }


# --------------------------------------------------------------------------- #
# Build orchestration
# --------------------------------------------------------------------------- #
def _load(path: Path) -> dict | None:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _chapter_verse_paths(shell: dict) -> list[str]:
    data = shell.get("data") or {}
    refs = data.get("verse_refs") or []
    return [r["path"] for r in refs
            if r.get("part_type") in CHAIN_PART_TYPES and r.get("path")]


def build_profile(data_root: Path) -> NarratorProfile:
    """Pass A: scan every verse_detail to classify narrators corpus-wide."""
    profile = NarratorProfile()
    books_dir = data_root / "books"
    for book_dir in sorted(books_dir.iterdir()):
        if not book_dir.is_dir() or book_dir.name in SKIP_DIRS:
            continue
        for jp in book_dir.rglob("*.json"):
            # only base verse_detail files (skip *.en.json etc. and shells)
            if jp.name.count(".") > 1:
                continue
            doc = _load(jp)
            if not doc or doc.get("kind") != "verse_detail":
                continue
            verse = (doc.get("data") or {}).get("verse") or {}
            for entry in _chain_from_verse(verse):
                profile.observe(entry)
    return profile


def build_chapter_sidecars(
    data_root: Path, profile: NarratorProfile, only_book: str | None = None
) -> list[Path]:
    """Pass B: write a `{chapter}.narrators.json` next to each shell.

    `only_book` restricts output to a single book slug (the profile is still
    built corpus-wide by the caller for accurate classification).
    """
    books_dir = data_root / "books"
    written: list[Path] = []
    for book_dir in sorted(books_dir.iterdir()):
        if not book_dir.is_dir() or book_dir.name in SKIP_DIRS:
            continue
        if only_book and book_dir.name != only_book:
            continue
        for shell_path in book_dir.rglob("*.json"):
            if shell_path.name.count(".") > 1:  # skip lang variants/sidecars
                continue
            shell = _load(shell_path)
            if not shell or shell.get("kind") != "verse_list":
                continue
            verse_paths = _chapter_verse_paths(shell)
            if not verse_paths:
                continue
            verses = []
            for vp in verse_paths:
                doc = _load(books_dir / _verse_path_to_relpath(vp))
                if doc and doc.get("kind") == "verse_detail":
                    data = doc.get("data") or {}
                    verse = data.get("verse") or {}
                    # gradings live at data-level (data.verse mirrors it, but
                    # not for every book) — surface it onto the verse so the
                    # grading-mix insight sees it.
                    if not verse.get("gradings") and data.get("gradings"):
                        verse = {**verse, "gradings": data["gradings"]}
                    verses.append(verse)
            if not verses:
                continue
            analysis = analyze_chapter(verses, profile)
            out = {
                "index": shell.get("index"),
                "kind": "narrator_analysis",
                "data": analysis,
            }
            out_path = shell_path.with_name(shell_path.stem + ".narrators.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
            written.append(out_path)
    return written


def build(data_root: Path) -> list[Path]:
    """Full two-pass build. Returns the list of sidecar paths written."""
    data_root = Path(data_root).resolve()
    if not (data_root / "books").is_dir():
        raise FileNotFoundError(f"ThaqalaynData/books not found under {data_root}")
    profile = build_profile(data_root)
    return build_chapter_sidecars(data_root, profile)
