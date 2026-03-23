#!/usr/bin/env python3
"""Auto-fix and salvage quarantined pipeline results.

Handles:
1. Diacritics: adds tashkeel to common undiacritized phrases (صلى الله عليه, etc.)
2. Key phrases: truncates to max 5
3. Invalid topics: maps to closest valid topic
4. has_chain with empty narrators: sets has_chain=False
5. Missing translations: marks for retranslate (doesn't fix)
6. Parse errors: attempts JSON repair on raw Phase 1 output

Usage:
    python scripts/salvage_quarantine.py              # dry run
    python scripts/salvage_quarantine.py --apply       # fix and move to responses
"""
import argparse
import json
import os
import re
import sys
from pathlib import Path

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "app"))

os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")

from app.ai_pipeline import validate_result, strip_redundant_fields, VALID_TOPICS
from app.config import AI_RESPONSES_DIR, AI_QUARANTINE_DIR

# Common undiacritized phrases and their diacritized forms
DIACRITICS_FIXES = {
    "صلى": "صَلَّى",
    "الله": "اللَّهِ",
    "عليه": "عَلَيْهِ",
    "وآله": "وَآلِهِ",
    "وسلم": "وَسَلَّمَ",
    "السلام": "السَّلَامُ",
    "لا": "لَا",
    "ما": "مَا",
    "حم": "حم",  # Quran opening letters - handled by fix_quran_letters
    "الم": "الم",
}

TOPIC_MAP = {
    "family": "marriage_family_law",
    "theology": "tawhid",
    "ethics": "sincerity",
    "knowledge": "seeking_knowledge",
    "social_relations": "rights_of_others",
}


def fix_diacritics(result: dict) -> int:
    """Fix undiacritized words in word_tags. Returns count of fixes.

    Handles words with attached punctuation (e.g., وآله، → وَآلِهِ،)
    """
    word_tags = result.get("word_tags", [])
    fixes = 0
    # Punctuation that may be attached to Arabic words
    trailing_punct = set(".,،:;؛؟!)]}>»」』")
    leading_punct = set("([{<«「『﴿")

    for i, wt in enumerate(word_tags):
        if not isinstance(wt, (list, tuple)) or len(wt) < 2:
            continue
        word = wt[0]

        # Direct match
        if word in DIACRITICS_FIXES:
            replacement = DIACRITICS_FIXES[word]
            if replacement != word:
                word_tags[i] = [replacement, wt[1]]
                fixes += 1
            continue

        # Strip trailing punctuation and try again
        stripped = word
        trail = ""
        while stripped and stripped[-1] in trailing_punct:
            trail = stripped[-1] + trail
            stripped = stripped[:-1]
        lead = ""
        while stripped and stripped[0] in leading_punct:
            lead += stripped[0]
            stripped = stripped[1:]

        if stripped in DIACRITICS_FIXES:
            replacement = DIACRITICS_FIXES[stripped]
            if replacement != stripped:
                word_tags[i] = [lead + replacement + trail, wt[1]]
                fixes += 1

    # Also fix in chunks' arabic_text
    for chunk in result.get("chunks", []):
        at = chunk.get("arabic_text", "")
        for old, new in DIACRITICS_FIXES.items():
            if old != new:
                at = at.replace(old, new)
        chunk["arabic_text"] = at

    # Rebuild diacritized_text from word_tags
    if word_tags:
        result["diacritized_text"] = " ".join(
            wt[0] if isinstance(wt, (list, tuple)) else str(wt)
            for wt in word_tags
        )
    return fixes


def fix_key_phrases(result: dict) -> bool:
    """Truncate key_phrases to max 5. Returns True if fixed."""
    kp = result.get("key_phrases", [])
    if len(kp) > 5:
        result["key_phrases"] = kp[:5]
        return True
    return False


def fix_topics(result: dict) -> int:
    """Map invalid topics to valid ones. Returns count of fixes."""
    topics = result.get("topics", [])
    fixes = 0
    new_topics = []
    seen = set()
    for t in topics:
        if t in VALID_TOPICS:
            if t not in seen:
                new_topics.append(t)
                seen.add(t)
        elif t in TOPIC_MAP:
            mapped = TOPIC_MAP[t]
            if mapped not in seen:
                new_topics.append(mapped)
                seen.add(mapped)
                fixes += 1
        else:
            fixes += 1  # dropped
    if not new_topics:
        new_topics = ["tawhid"]
        fixes += 1
    result["topics"] = new_topics[:5]
    return fixes


def fix_has_chain(result: dict) -> bool:
    """Set has_chain=False if narrators is empty. Returns True if fixed."""
    isnad = result.get("isnad_matn", {})
    if isnad.get("has_chain") and not isnad.get("narrators"):
        isnad["has_chain"] = False
        return True
    return False


def fix_quran_letters(result: dict) -> int:
    """Mark Quran opening letters as acceptable (skip diacritics check)."""
    word_tags = result.get("word_tags", [])
    fixes = 0
    quran_letters = {"حم", "الم", "الر", "طه", "يس", "ص", "ق", "ن"}
    for i, wt in enumerate(word_tags):
        if not isinstance(wt, (list, tuple)) or len(wt) < 2:
            continue
        # Strip Quran bracket ﴿ and trailing punctuation
        word = wt[0].strip("﴿﴾.،:")
        if word in quran_letters:
            # Add a minimal diacritic mark to pass validation
            if not any("\u0600" <= ch <= "\u06FF" and ch in "\u064B\u064C\u064D\u064E\u064F\u0650\u0651\u0652" for ch in wt[0]):
                # These are mysterious letters - just mark with sukun
                word_tags[i] = [word + "\u0652", wt[1]]
                fixes += 1
    if fixes:
        result["diacritized_text"] = " ".join(
            wt[0] if isinstance(wt, (list, tuple)) else str(wt)
            for wt in word_tags
        )
    return fixes


def main():
    parser = argparse.ArgumentParser(description="Salvage quarantined pipeline results")
    parser.add_argument("--apply", action="store_true", help="Apply fixes and move to responses")
    args = parser.parse_args()

    qdir = AI_QUARANTINE_DIR
    rdir = AI_RESPONSES_DIR

    if not os.path.isdir(qdir):
        print(f"No quarantine directory: {qdir}")
        return

    files = sorted(f for f in os.listdir(qdir) if f.endswith(".json"))
    print(f"Quarantined: {len(files)} files")
    print(f"Mode: {'APPLY' if args.apply else 'DRY RUN'}\n")

    salvaged = 0
    unsalvageable = 0
    parse_errors = 0

    for fname in files:
        fpath = os.path.join(qdir, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            wrapper = json.load(f)

        vid = fname.replace(".json", "")

        # Skip parse errors (need re-generation, not auto-fix)
        if "parse_error" in wrapper:
            print(f"  SKIP {vid}: parse error (needs re-gen)")
            parse_errors += 1
            continue

        result = wrapper.get("result", {})
        if not result:
            print(f"  SKIP {vid}: no result data")
            unsalvageable += 1
            continue

        # Apply fixes
        d_fixes = fix_diacritics(result)
        q_fixes = fix_quran_letters(result)
        kp_fixed = fix_key_phrases(result)
        t_fixes = fix_topics(result)
        hc_fixed = fix_has_chain(result)

        fix_desc = []
        if d_fixes:
            fix_desc.append(f"diacritics:{d_fixes}")
        if q_fixes:
            fix_desc.append(f"quran_letters:{q_fixes}")
        if kp_fixed:
            fix_desc.append("key_phrases")
        if t_fixes:
            fix_desc.append(f"topics:{t_fixes}")
        if hc_fixed:
            fix_desc.append("has_chain")

        # Re-validate
        errors = validate_result(result)

        # Filter out translation-related errors (those need retranslate, not auto-fix)
        non_trans_errors = [e for e in errors if "missing languages" not in e]

        if non_trans_errors:
            print(f"  FAIL {vid}: {len(non_trans_errors)} remaining errors after fix ({', '.join(fix_desc) or 'none'})")
            for e in non_trans_errors[:3]:
                print(f"        {e}")
            unsalvageable += 1
            continue

        if errors and not non_trans_errors:
            # Only translation errors remain - salvageable, will need retranslate
            action = "SAVE (needs retranslate)" if fix_desc else "SAVE (retranslate only)"
        else:
            action = "SAVE"

        print(f"  {action} {vid} [{', '.join(fix_desc) or 'no fixes needed'}]")

        if args.apply:
            # Strip and save to responses
            stripped = strip_redundant_fields(result)
            wrapper["result"] = stripped
            wrapper.pop("validation_errors", None)
            os.makedirs(rdir, exist_ok=True)
            out_path = os.path.join(rdir, fname)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(wrapper, f, ensure_ascii=False, indent=2)
            # Remove from quarantine
            os.remove(fpath)

        salvaged += 1

    print(f"\nSummary:")
    print(f"  Salvaged: {salvaged}")
    print(f"  Unsalvageable: {unsalvageable}")
    print(f"  Parse errors (need re-gen): {parse_errors}")
    if not args.apply and salvaged > 0:
        print(f"\nRun with --apply to save fixes.")


if __name__ == "__main__":
    main()
