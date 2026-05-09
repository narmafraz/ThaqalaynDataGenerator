#!/usr/bin/env python3
"""Apply Tier-1 auto-merge: consolidate duplicate canonical_narrators entries.

Phase 1 of the narrator dedup effort. Algorithmic only, no LLM calls. Identifies
duplicate registry entries by exact ``canonical_lookup_key`` match (after
diacritic strip + honorific strip + leading-verb strip), picks a winner per
group, merges loser variants into the winner, and rewrites all canonical_id
references in AI response files so nothing points to a deleted ID.

**What this is safe for:**
- Entries with identical canonical-key after normalization (Tier 1 only)
- Entries WITHOUT non-empty `disambiguation_context` (those were flagged
  as ambiguous by the registry author and require manual review)

**What this is NOT safe for:**
- Tier 2 substring/kunya overlaps (kunya could refer to different people)
- Anything requiring rijal expertise

**Side effects:**
1. Updates `canonical_narrators.json` — drops loser entries, merges variants
2. Rewrites every AI response file in `responses/` whose
   `isnad_matn.narrators[*].canonical_id` referenced a loser ID
3. Writes an audit trail to `narrator_merges.log`

Run with `--dry-run` first (default). Add `--apply` to write changes.

Usage:
    cd ThaqalaynDataGenerator
    AI_CONTENT_SUBDIR=corpus PYTHONPATH="$PWD:$PWD/app" \\
        SOURCE_DATA_DIR="../ThaqalaynDataSources/" \\
        .venv/Scripts/python.exe scripts/narrator_dedup_apply.py            # dry run
    .venv/Scripts/python.exe scripts/narrator_dedup_apply.py --apply
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "app"))

os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from app.narrator_registry import canonical_lookup_key  # noqa: E402


REGISTRY_PATH = PROJECT_ROOT / ".." / "ThaqalaynDataSources" / "ai-pipeline-data" / "canonical_narrators.json"
RESPONSES_DIR = PROJECT_ROOT / ".." / "ThaqalaynDataSources" / "ai-content" / "corpus" / "responses"
MERGE_LOG_PATH = PROJECT_ROOT / ".." / "ThaqalaynDataSources" / "ai-pipeline-data" / "narrator_merges.log"


def load_registry() -> dict:
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def find_tier1_groups(narrators: Dict[str, dict]) -> Dict[str, List[int]]:
    """Same logic as the audit's Tier-1 — exact canonical-key match, skip
    entries with non-empty disambiguation_context."""
    groups: Dict[str, List[int]] = defaultdict(list)
    for nid_str, entry in narrators.items():
        if entry.get("disambiguation_context"):
            continue
        canonical_ar = entry.get("canonical_name_ar", "") or ""
        ckey = canonical_lookup_key(canonical_ar)
        if not ckey:
            continue
        groups[ckey].append(int(nid_str))
    return {k: sorted(v) for k, v in groups.items() if len(v) > 1}


def count_response_refs() -> Counter:
    """Count canonical_id occurrences across all AI response files."""
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


def pick_winner(group_ids: List[int], narrators: Dict[str, dict], refs: Counter) -> int:
    """Pick which entry survives a merge.

    Tiebreak chain:
    1. Most response references (most-used = most embedded in corpus)
    2. Most variants (richer entry, less work to rebuild)
    3. Longest canonical_name_ar (more specific form preferred)
    4. Lowest ID (deterministic stability)
    """
    def score(nid: int) -> tuple:
        entry = narrators.get(str(nid), {})
        return (
            refs.get(nid, 0),
            len(entry.get("variants_ar", []) or []),
            len((entry.get("canonical_name_ar") or "")),
            -nid,
        )
    return max(group_ids, key=score)


def merge_group(
    group_ids: List[int],
    narrators: Dict[str, dict],
    winner_id: int,
) -> dict:
    """Merge loser entries into winner. Returns the updated winner entry.

    - Loser variants_ar appended to winner.variants_ar (deduped)
    - Loser canonical_name_ar added to winner.variants_ar (so future lookups
      with the loser's surface form still resolve)
    - Loser IDs appended to winner.old_ids (audit trail; never reused)
    - If winner has empty name_en/role and a loser has it, copy from loser
    """
    winner = narrators[str(winner_id)]
    winner_variants = list(winner.get("variants_ar", []) or [])
    winner_old_ids = list(winner.get("old_ids", []) or [])

    for lid in group_ids:
        if lid == winner_id:
            continue
        loser = narrators.get(str(lid))
        if not loser:
            continue
        # Add loser's canonical name as a variant (so the surface form still resolves)
        loser_canon = (loser.get("canonical_name_ar") or "").strip()
        if loser_canon and loser_canon not in winner_variants:
            winner_variants.append(loser_canon)
        # Merge loser's variants
        for v in loser.get("variants_ar", []) or []:
            if v and v not in winner_variants:
                winner_variants.append(v)
        # Track ID merge in audit trail
        if lid not in winner_old_ids:
            winner_old_ids.append(lid)
        # Carry over loser's old_ids too (transitivity)
        for old in loser.get("old_ids", []) or []:
            if old not in winner_old_ids:
                winner_old_ids.append(old)
        # Backfill empty fields on winner from loser if loser has them
        if not (winner.get("canonical_name_en") or "").strip():
            loser_en = (loser.get("canonical_name_en") or "").strip()
            if loser_en:
                winner["canonical_name_en"] = loser_en
        if not (winner.get("role") or "").strip():
            loser_role = (loser.get("role") or "").strip()
            if loser_role:
                winner["role"] = loser_role
        if winner.get("known_identity") is None and loser.get("known_identity") is not None:
            winner["known_identity"] = loser["known_identity"]

    winner["variants_ar"] = winner_variants
    winner["old_ids"] = sorted(winner_old_ids)
    return winner


def rewrite_response_refs(
    id_remap: Dict[int, int],
    apply: bool,
) -> int:
    """Walk all AI response files; rewrite canonical_id where it points to a
    merged-away (loser) ID. Returns the number of files modified.

    `id_remap` maps loser_id -> winner_id. If a narrator's canonical_id
    matches a key, it gets replaced with the value.
    """
    if not RESPONSES_DIR.is_dir():
        return 0
    modified = 0
    for entry in sorted(os.listdir(RESPONSES_DIR)):
        if not entry.endswith(".json"):
            continue
        path = RESPONSES_DIR / entry
        try:
            with open(path, "r", encoding="utf-8") as f:
                d = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        result = d.get("result", {})
        isnad = result.get("isnad_matn", {})
        if not isinstance(isnad, dict):
            continue
        narrators = isnad.get("narrators", []) or []
        changed = False
        for n in narrators:
            if not isinstance(n, dict):
                continue
            cid = n.get("canonical_id")
            if isinstance(cid, int) and cid in id_remap:
                n["canonical_id"] = id_remap[cid]
                changed = True
        if changed:
            modified += 1
            if apply:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(d, f, ensure_ascii=False, indent=2)
    return modified


def append_merge_log(
    merges: List[tuple],  # (winner_id, loser_ids, ckey, ref_total)
    apply: bool,
):
    if not apply:
        return
    MERGE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(MERGE_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"\n# Merge run @ {timestamp}\n")
        for winner, losers, ckey, ref_total in merges:
            losers_str = ",".join(str(l) for l in losers)
            f.write(f"  winner={winner}  losers=[{losers_str}]  ckey={ckey!r}  refs={ref_total}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Write changes to disk (registry + responses + log). Default is dry-run.")
    args = parser.parse_args()

    print(f"Mode: {'APPLY' if args.apply else 'DRY RUN'}", flush=True)
    print()

    print("Loading registry...", flush=True)
    registry = load_registry()
    narrators: Dict[str, dict] = registry.get("narrators", {})
    print(f"  {len(narrators)} entries", flush=True)

    print("Counting response references...", flush=True)
    refs = count_response_refs()
    print(f"  {sum(refs.values()):,} references across {len(refs)} distinct IDs", flush=True)

    print("Identifying Tier-1 duplicate groups...", flush=True)
    groups = find_tier1_groups(narrators)
    print(f"  {len(groups)} groups, {sum(len(v) - 1 for v in groups.values())} losers to merge", flush=True)
    print()

    if not groups:
        print("No Tier-1 duplicates found — nothing to do.", flush=True)
        return

    id_remap: Dict[int, int] = {}
    merges: List[tuple] = []
    refs_consolidated = 0

    for ckey, ids in sorted(groups.items(), key=lambda kv: -sum(refs.get(i, 0) for i in kv[1])):
        winner = pick_winner(ids, narrators, refs)
        losers = [i for i in ids if i != winner]
        ref_total = sum(refs.get(i, 0) for i in ids)
        winner_refs = refs.get(winner, 0)
        loser_refs = sum(refs.get(l, 0) for l in losers)
        refs_consolidated += loser_refs

        # Build the merged winner entry
        merged = merge_group(ids, narrators, winner)

        print(f"  ckey={ckey!r}", flush=True)
        print(f"    winner={winner} (refs={winner_refs})", flush=True)
        for lid in losers:
            print(f"    loser={lid} (refs={refs.get(lid, 0)})  -> remap to {winner}", flush=True)
            id_remap[lid] = winner

        merges.append((winner, losers, ckey, ref_total))

        if args.apply:
            # Mutate registry: replace winner entry, drop losers
            narrators[str(winner)] = merged
            for lid in losers:
                narrators.pop(str(lid), None)

    print()
    print(f"Total losers to merge: {len(id_remap)}", flush=True)
    print(f"Refs to consolidate: {refs_consolidated:,}", flush=True)
    print()

    print("Rewriting canonical_id in response files...", flush=True)
    modified = rewrite_response_refs(id_remap, apply=args.apply)
    print(f"  {modified} files {'updated' if args.apply else 'would be updated'}", flush=True)

    if args.apply:
        # Save updated registry (preserve last_id; do NOT decrement — IDs are
        # never reused per the registry's invariant).
        registry["narrators"] = narrators
        with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
            json.dump(registry, f, ensure_ascii=False, indent=2)
        print(f"  Saved registry: {REGISTRY_PATH}", flush=True)
        append_merge_log(merges, apply=True)
        print(f"  Audit log: {MERGE_LOG_PATH}", flush=True)
    else:
        print()
        print("This was a dry run. Re-run with --apply to write changes.", flush=True)


if __name__ == "__main__":
    main()
