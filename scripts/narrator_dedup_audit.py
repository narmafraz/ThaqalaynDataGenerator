#!/usr/bin/env python3
"""Audit canonical_narrators.json for duplicates, gaps, and quality issues.

Phase 0 of the narrator dedup effort. Read-only — no changes to data, registry,
or responses. Produces a single markdown report at
``ThaqalaynDataSources/ai-pipeline-data/narrator_dedup_audit.md``.

What the audit covers:

1. **Tier-1 duplicate candidates**: groups of registry entries whose
   ``canonical_lookup_key`` (post-normalize + honorific-strip + leading-verb-
   strip) is identical. These are extremely likely to be the same person and
   safe to auto-merge.

2. **Tier-2 duplicate candidates**: groups whose canonical-key isn't equal
   but where one is a substring of another (kunya vs full name) or share a
   distinctive ism token. Manual review needed.

3. **Incomplete entries**: empty ``canonical_name_en``, missing/unknown
   ``role``, or empty ``variants_ar`` — flagged for separate cleanup.

4. **Response coverage**: for each canonical_id, count how many AI response
   files reference it (via ``isnad_matn.narrators[].canonical_id``). Helps
   prioritize which duplicates to merge first by impact.

5. **Unresolved names from responses**: extract every narrator name where
   ``canonical_id`` is null after merging, group by canonical_lookup_key, and
   rank by frequency. Top of this list is the input to Phase C (registry gaps).

Usage:
    cd ThaqalaynDataGenerator
    AI_CONTENT_SUBDIR=corpus PYTHONPATH="$PWD:$PWD/app" \\
        SOURCE_DATA_DIR="../ThaqalaynDataSources/" \\
        .venv/Scripts/python.exe scripts/narrator_dedup_audit.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "app"))

os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from app.arabic_normalization import normalize_arabic  # noqa: E402
from app.narrator_registry import canonical_lookup_key  # noqa: E402


REGISTRY_PATH = PROJECT_ROOT / ".." / "ThaqalaynDataSources" / "ai-pipeline-data" / "canonical_narrators.json"
RESPONSES_DIR = PROJECT_ROOT / ".." / "ThaqalaynDataSources" / "ai-content" / "corpus" / "responses"
REPORT_PATH = PROJECT_ROOT / ".." / "ThaqalaynDataSources" / "ai-pipeline-data" / "narrator_dedup_audit.md"


def load_registry() -> Tuple[Dict[int, dict], int]:
    """Returns (narrators_by_id, last_id)."""
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    raw = data.get("narrators", {})
    narrators = {int(k): v for k, v in raw.items()}
    return narrators, int(data.get("last_id", 0))


def collect_response_canonical_ids() -> Counter:
    """Walk response files, count canonical_id occurrences across all narrators."""
    counter: Counter = Counter()
    if not RESPONSES_DIR.is_dir():
        return counter
    for entry in os.listdir(RESPONSES_DIR):
        if not entry.endswith(".json"):
            continue
        try:
            with open(RESPONSES_DIR / entry, "r", encoding="utf-8") as f:
                d = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        result = d.get("result", {})
        isnad = result.get("isnad_matn", {})
        if not isinstance(isnad, dict):
            continue
        for n in isnad.get("narrators", []) or []:
            if isinstance(n, dict):
                cid = n.get("canonical_id")
                if isinstance(cid, int):
                    counter[cid] += 1
    return counter


def collect_unresolved_names() -> Counter:
    """Walk response files, count narrator name_ar values where canonical_id is null.

    Group by canonical_lookup_key so spelling/honorific variants of the same
    name collapse to a single entry.
    """
    counter: Counter = Counter()
    samples: Dict[str, str] = {}  # canonical_key -> first observed surface form
    if not RESPONSES_DIR.is_dir():
        return counter
    for entry in os.listdir(RESPONSES_DIR):
        if not entry.endswith(".json"):
            continue
        try:
            with open(RESPONSES_DIR / entry, "r", encoding="utf-8") as f:
                d = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        result = d.get("result", {})
        isnad = result.get("isnad_matn", {})
        if not isinstance(isnad, dict):
            continue
        for n in isnad.get("narrators", []) or []:
            if isinstance(n, dict) and n.get("canonical_id") is None:
                name_ar = (n.get("name_ar") or "").strip()
                if not name_ar:
                    continue
                ckey = canonical_lookup_key(name_ar)
                if not ckey:
                    continue
                counter[ckey] += 1
                samples.setdefault(ckey, name_ar)
    # Stash samples on the counter object via a sneaky attribute
    counter._samples = samples  # type: ignore[attr-defined]
    return counter


def find_tier1_groups(narrators: Dict[int, dict]) -> Dict[str, List[int]]:
    """Tier 1: exact match after canonical_lookup_key on the canonical_name_ar.

    Excludes entries with non-empty disambiguation_context (registry author
    flagged these as ambiguous — never auto-merge).
    """
    groups: Dict[str, List[int]] = defaultdict(list)
    for nid, entry in narrators.items():
        if entry.get("disambiguation_context"):
            continue
        canonical_ar = entry.get("canonical_name_ar", "") or ""
        ckey = canonical_lookup_key(canonical_ar)
        if not ckey:
            continue
        groups[ckey].append(nid)
    # Only keep groups with >1 member
    return {k: sorted(v) for k, v in groups.items() if len(v) > 1}


def find_tier2_substring_groups(
    narrators: Dict[int, dict],
    tier1_keys: set,
) -> Dict[str, List[int]]:
    """Tier 2: kunya / partial-name overlap — one canonical_key is a substring
    of another (e.g. "ابن بكير" inside "عبد الله بن بكير").

    Skipped if either entry is already in a Tier-1 group (those will be merged
    first; we'll re-audit after).
    """
    # Build map of entry_id -> canonical_key
    keys: Dict[int, str] = {}
    for nid, entry in narrators.items():
        if entry.get("disambiguation_context"):
            continue
        if any(nid in v for v in [tier1_keys]):
            continue
        canonical_ar = entry.get("canonical_name_ar", "") or ""
        ckey = canonical_lookup_key(canonical_ar)
        if ckey and len(ckey) >= 4:  # too-short keys produce noise
            keys[nid] = ckey
    # Find substring relationships, but only between distinct keys
    groups: Dict[str, List[int]] = defaultdict(list)
    items = sorted(keys.items(), key=lambda kv: len(kv[1]))
    for i, (id_a, key_a) in enumerate(items):
        for id_b, key_b in items[i + 1:]:
            if key_a == key_b:
                continue  # already a Tier 1
            # key_a is shorter (sorted); check if it's a token-level substring of key_b
            if f" {key_a} " in f" {key_b} " or key_b.startswith(key_a + " ") or key_b.endswith(" " + key_a):
                # group under the shorter (likely-kunya) key
                if id_a not in groups[key_a]:
                    groups[key_a].append(id_a)
                if id_b not in groups[key_a]:
                    groups[key_a].append(id_b)
    return {k: sorted(v) for k, v in groups.items() if len(v) > 1}


def find_incomplete_entries(narrators: Dict[int, dict]) -> List[int]:
    """Flag entries missing English name, missing role, or with no variants."""
    incomplete = []
    for nid, entry in narrators.items():
        en = (entry.get("canonical_name_en") or "").strip()
        role = (entry.get("role") or "").strip()
        variants = entry.get("variants_ar") or []
        flags = []
        if not en:
            flags.append("no_en")
        if not role or role in ("unknown", "narrator"):
            # 'narrator' is the default — flag for manual review unless explicitly
            # marked another role
            if not role:
                flags.append("no_role")
        if not variants:
            flags.append("no_variants")
        if flags:
            incomplete.append((nid, flags, entry))
    return incomplete


def build_report(
    narrators: Dict[int, dict],
    tier1: Dict[str, List[int]],
    tier2: Dict[str, List[int]],
    incomplete: List[Tuple[int, List[str], dict]],
    response_counts: Counter,
    unresolved: Counter,
) -> str:
    out = []
    out.append("# Canonical Narrator Registry — Dedup & Quality Audit")
    out.append("")
    out.append(f"**Total entries**: {len(narrators)}")
    out.append(f"**Total response references**: {sum(response_counts.values()):,}")
    out.append(f"**Distinct IDs referenced**: {len(response_counts)}")
    out.append("")
    out.append("---")
    out.append("")

    # -------- Tier 1 --------
    out.append("## Tier 1 — High-confidence duplicate candidates")
    out.append("")
    out.append(f"Groups: **{len(tier1)}** | Total entries to consolidate: **{sum(len(v) - 1 for v in tier1.values())}**")
    out.append("")
    out.append("These have identical `canonical_lookup_key` after normalization. Same person almost certainly. Safe to auto-merge unless `disambiguation_context` says otherwise (already excluded above).")
    out.append("")
    out.append("| Canonical Key | IDs | Names | Response refs |")
    out.append("|---|---|---|---|")
    sorted_groups = sorted(
        tier1.items(),
        key=lambda kv: -sum(response_counts.get(i, 0) for i in kv[1]),
    )
    for ckey, ids in sorted_groups[:200]:
        ref_total = sum(response_counts.get(i, 0) for i in ids)
        names = " / ".join(
            f"`{narrators[i].get('canonical_name_ar', '')[:40]}` (id={i}, refs={response_counts.get(i, 0)})"
            for i in ids
        )
        out.append(f"| `{ckey}` | {','.join(str(i) for i in ids)} | {names} | {ref_total} |")
    if len(sorted_groups) > 200:
        out.append(f"| ... | | ({len(sorted_groups) - 200} more groups) | |")
    out.append("")

    # -------- Tier 2 --------
    out.append("## Tier 2 — Substring/kunya overlap candidates")
    out.append("")
    out.append(f"Groups: **{len(tier2)}**")
    out.append("")
    out.append("Token-level substring relationships. Could be same-person (kunya vs full name) or could be relatives. **Manual review required**.")
    out.append("")
    out.append("| Shared Key | IDs | Names | Response refs |")
    out.append("|---|---|---|---|")
    sorted_t2 = sorted(
        tier2.items(),
        key=lambda kv: -sum(response_counts.get(i, 0) for i in kv[1]),
    )
    for ckey, ids in sorted_t2[:100]:
        ref_total = sum(response_counts.get(i, 0) for i in ids)
        names = " / ".join(
            f"`{narrators[i].get('canonical_name_ar', '')[:40]}` (id={i}, refs={response_counts.get(i, 0)})"
            for i in ids
        )
        out.append(f"| `{ckey}` | {','.join(str(i) for i in ids)} | {names} | {ref_total} |")
    if len(sorted_t2) > 100:
        out.append(f"| ... | | ({len(sorted_t2) - 100} more groups) | |")
    out.append("")

    # -------- Incomplete entries --------
    out.append("## Incomplete entries")
    out.append("")
    out.append(f"Total: **{len(incomplete)}** entries have at least one quality flag.")
    out.append("")
    flag_counts = Counter()
    for _, flags, _ in incomplete:
        for f in flags:
            flag_counts[f] += 1
    for flag, count in flag_counts.most_common():
        out.append(f"- `{flag}`: {count}")
    out.append("")
    out.append("Top 50 by response count (these matter most to fix):")
    out.append("")
    out.append("| ID | Name AR | Name EN | Role | Flags | Response refs |")
    out.append("|---|---|---|---|---|---|")
    sorted_incomplete = sorted(
        incomplete,
        key=lambda triple: -response_counts.get(triple[0], 0),
    )
    for nid, flags, entry in sorted_incomplete[:50]:
        out.append(
            f"| {nid} | `{entry.get('canonical_name_ar', '')[:40]}` | "
            f"{entry.get('canonical_name_en', '') or '—'} | "
            f"{entry.get('role', '') or '—'} | "
            f"{', '.join(flags)} | "
            f"{response_counts.get(nid, 0)} |"
        )
    out.append("")

    # -------- Unresolved names --------
    out.append("## Unresolved narrator names (registry gap candidates)")
    out.append("")
    out.append(f"**Distinct unresolved canonical-keys**: {len(unresolved)}")
    out.append(f"**Total unresolved narrator instances**: {sum(unresolved.values()):,}")
    out.append("")
    out.append("These are narrator names extracted by the AI pipeline that could not be matched to any canonical entry — even after honorific/verb stripping. Top of this list is the high-priority list for Phase C (registry gap fill).")
    out.append("")
    out.append("Top 200 by frequency:")
    out.append("")
    out.append("| Frequency | Canonical Key | Sample surface form |")
    out.append("|---|---|---|")
    samples = getattr(unresolved, "_samples", {})
    for ckey, freq in unresolved.most_common(200):
        sample = samples.get(ckey, "?")
        out.append(f"| {freq} | `{ckey[:50]}` | `{sample[:50]}` |")
    if len(unresolved) > 200:
        out.append(f"| ... | ({len(unresolved) - 200} more keys) | |")
    out.append("")

    return "\n".join(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=REPORT_PATH,
                        help="Output markdown path (default: ai-pipeline-data/narrator_dedup_audit.md)")
    args = parser.parse_args()

    print("Loading registry...", flush=True)
    narrators, _ = load_registry()
    print(f"  {len(narrators)} entries", flush=True)

    print("Counting canonical_id references in responses...", flush=True)
    response_counts = collect_response_canonical_ids()
    print(f"  {sum(response_counts.values()):,} stamped references across {len(response_counts)} distinct IDs", flush=True)

    print("Collecting unresolved narrator names...", flush=True)
    unresolved = collect_unresolved_names()
    print(f"  {sum(unresolved.values()):,} unresolved instances across {len(unresolved)} distinct canonical-keys", flush=True)

    print("Finding Tier 1 duplicate candidates (exact canonical-key match)...", flush=True)
    tier1 = find_tier1_groups(narrators)
    tier1_ids = {i for ids in tier1.values() for i in ids}
    print(f"  {len(tier1)} groups, {sum(len(v) - 1 for v in tier1.values())} entries to consolidate", flush=True)

    print("Finding Tier 2 substring/kunya candidates...", flush=True)
    tier2 = find_tier2_substring_groups(narrators, tier1_ids)
    print(f"  {len(tier2)} groups", flush=True)

    print("Flagging incomplete entries...", flush=True)
    incomplete = find_incomplete_entries(narrators)
    print(f"  {len(incomplete)} entries with quality flags", flush=True)

    print("Writing report...", flush=True)
    report = build_report(narrators, tier1, tier2, incomplete, response_counts, unresolved)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"Report: {args.output}", flush=True)


if __name__ == "__main__":
    main()
