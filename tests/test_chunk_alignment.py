"""Tests for scraped-translation chunk alignment (align-scraped)."""

import json
import os

import pytest

from app.pipeline_cli.chunk_alignment_phase import (
    _alignment_schema,
    build_alignment_prompt,
    eligible_scraped_ids,
    is_eligible,
    prepared_chunks,
    scraped_text_for,
    validate_alignment,
)
from app.ai_content_merger import (
    _kept_parts,
    _sister_path,
    load_chunk_alignments,
    merge_alignment_into_file,
)


# ── validate_alignment ─────────────────────────────────────────────────────

def test_validate_accepts_clean_partition():
    scraped = "A number of our companions narrated. The Imam said: seek knowledge."
    parts = ["A number of our companions narrated.", "The Imam said: seek knowledge."]
    ok, reason = validate_alignment(parts, scraped)
    assert ok, reason


def test_validate_accepts_empty_isnad_part():
    # Translation that omits the chain: isnad part empty, matn carries all.
    scraped = "The Imam said: seek knowledge from cradle to grave."
    parts = ["", "The Imam said: seek knowledge from cradle to grave."]
    ok, reason = validate_alignment(parts, scraped)
    assert ok, reason


def test_validate_rejects_paraphrase():
    scraped = "A number of our companions narrated. The Imam said: seek knowledge."
    # Reworded (translated afresh) — low overlap with the original wording.
    parts = ["Several associates reported this.", "He instructed us to pursue learning."]
    ok, _ = validate_alignment(parts, scraped)
    assert not ok


def test_validate_rejects_dropped_text():
    scraped = "A number of our companions narrated. The Imam said: seek knowledge always."
    parts = ["A number of our companions narrated.", ""]  # matn dropped entirely
    ok, reason = validate_alignment(parts, scraped)
    assert not ok
    assert "not verbatim" in reason


def test_validate_rejects_added_commentary():
    scraped = "The Imam said: seek knowledge."
    parts = [
        "The Imam said: seek knowledge.",
        "This hadith is graded reliable and appears in several sources with commentary.",
    ]
    ok, reason = validate_alignment(parts, scraped)
    assert not ok
    assert "not verbatim" in reason


# ── schema / prompt ─────────────────────────────────────────────────────────

def test_alignment_schema_locks_part_count():
    schema = _alignment_schema(3)
    parts = schema["properties"]["parts"]
    assert parts["minItems"] == 3
    assert parts["maxItems"] == 3
    assert parts["items"]["required"] == ["text"]
    assert parts["items"]["additionalProperties"] is False


def test_build_prompt_lists_all_chunks_and_text():
    chunks = [
        {"chunk_type": "isnad", "arabic_text": "عدة من أصحابنا"},
        {"chunk_type": "body", "arabic_text": "قال الإمام"},
    ]
    system, user = build_alignment_prompt(chunks, "The chain. The saying.")
    assert "verbatim" in system.lower()
    assert "عدة من أصحابنا" in user
    assert "قال الإمام" in user
    assert "The chain. The saying." in user
    assert "2 items" in user


# ── eligibility ─────────────────────────────────────────────────────────────

def test_is_eligible_requires_two_arabic_chunks():
    two = [
        {"chunk_type": "isnad", "arabic_text": "a b"},
        {"chunk_type": "body", "arabic_text": "c d"},
    ]
    assert is_eligible(two)
    # single chunk → not eligible (block fallback renders identically)
    assert not is_eligible(two[:1])
    # two chunks but only one has arabic_text → not eligible
    assert not is_eligible([two[0], {"chunk_type": "body", "arabic_text": ""}])


def test_eligible_scraped_ids_filters_ai_empty_and_lang():
    verse = {
        "translations": {
            "en.qarai": ["hello"],
            "en.sarwar": ["hi"],
            "en.ai": ["ai text"],       # excluded: AI
            "ur.mulla": ["اردو"],        # excluded when langs={en}
            "en.empty": [],              # excluded: empty
        }
    }
    assert eligible_scraped_ids(verse, {"en"}) == ["en.qarai", "en.sarwar"]
    assert "ur.mulla" in eligible_scraped_ids(verse, None)


def test_prepared_chunks_reconstructs_arabic():
    # Built base file: chunks lack arabic_text; word_analysis carries the words.
    verse = {
        "ai": {
            "chunks": [
                {"chunk_type": "isnad", "word_start": 0, "word_end": 2},
                {"chunk_type": "body", "word_start": 2, "word_end": 4},
            ],
            "word_analysis": [
                {"word": "عدة"}, {"word": "أصحابنا"},
                {"word": "قال"}, {"word": "الإمام"},
            ],
        }
    }
    prepped = prepared_chunks(verse)
    assert [c["arabic_text"] for c in prepped] == ["عدة أصحابنا", "قال الإمام"]
    # Response-file arabic_text wins over word_analysis when present.
    resp = {"chunks": [{"arabic_text": "عِدَّةٌ مِنْ أَصْحَابِنَا"}, {"arabic_text": "قَالَ"}]}
    prepped2 = prepared_chunks(verse, resp)
    assert prepped2[0]["arabic_text"] == "عِدَّةٌ مِنْ أَصْحَابِنَا"


def test_scraped_text_joins_list_dropping_empties():
    verse = {"translations": {"en.hubeali": ["Sentence one.", "", "Sentence two."]}}
    assert scraped_text_for(verse, "en.hubeali") == "Sentence one.\nSentence two."


# ── merge ────────────────────────────────────────────────────────────────────

def _verse_with_chunks(n=2, path="/books/al-amali-mufid:1:1:1"):
    return {
        "path": path,
        "translations": {"en.qarai": ["The chain narrated. The saying followed."]},
        "ai": {"chunks": [{"chunk_type": "body", "arabic_text": f"c{i}"} for i in range(n)]},
    }


def test_kept_parts_valid():
    verse = _verse_with_chunks(2)
    keep = _kept_parts(verse, {"en.qarai": ["The chain narrated.", "The saying followed."]})
    assert keep == {"en.qarai": ["The chain narrated.", "The saying followed."]}


def test_kept_parts_skips_length_mismatch():
    verse = _verse_with_chunks(2)
    assert _kept_parts(verse, {"en.qarai": ["only one part"]}) == {}  # 1 != 2 chunks


def test_kept_parts_skips_all_empty():
    verse = _verse_with_chunks(2)
    assert _kept_parts(verse, {"en.qarai": ["", ""]}) == {}


def test_merge_into_file_writes_sister_and_strips_base(tmp_path):
    # Alignment-scoped sister model: parts go to {base}.en.json under
    # chunk_translations; the flat scraped text is removed from base.
    verse = _verse_with_chunks(2)
    doc = {
        "index": "al-amali-mufid:1:1:1",
        "kind": "verse_detail",
        "data": {"verse": verse, "verse_translations": ["en.qarai"]},
    }
    fp = tmp_path / "1.json"
    fp.write_text(json.dumps(doc), encoding="utf-8")
    parts = ["The chain narrated.", "The saying followed."]
    lookup = {verse["path"]: {"en.qarai": parts}}

    count = merge_alignment_into_file(str(fp), lookup)
    assert count == 1

    base = json.loads(fp.read_text(encoding="utf-8"))
    base_verse = base["data"]["verse"]
    # flat text dropped from base, but id still discoverable
    assert "en.qarai" not in base_verse.get("translations", {})
    assert "en.qarai" in base["data"]["verse_translations"]
    # no chunk_translations on base — it lives in the sister
    assert "chunk_translations" not in base_verse

    sister = json.loads((tmp_path / "1.en.json").read_text(encoding="utf-8"))
    assert sister["lang"] == "en"
    assert sister["chunk_translations"]["en.qarai"] == parts


def test_merge_into_file_preserves_existing_ai_sister(tmp_path):
    # A pre-existing AI sister (from merge_ai_content) must keep its ai block.
    verse = _verse_with_chunks(2)
    (tmp_path / "1.json").write_text(json.dumps({
        "kind": "verse_detail",
        "data": {"verse": verse, "verse_translations": ["en.qarai"]},
    }), encoding="utf-8")
    (tmp_path / "1.en.json").write_text(json.dumps({
        "ai": {"summary": "AI summary", "chunks": ["c0", "c1"]},
        "lang": "en", "path": verse["path"],
    }), encoding="utf-8")

    merge_alignment_into_file(str(tmp_path / "1.json"),
                              {verse["path"]: {"en.qarai": ["p0", "p1"]}})
    sister = json.loads((tmp_path / "1.en.json").read_text(encoding="utf-8"))
    assert sister["ai"]["summary"] == "AI summary"          # preserved
    assert sister["chunk_translations"]["en.qarai"] == ["p0", "p1"]  # added


def test_no_artifact_leaves_file_untouched(tmp_path):
    verse = _verse_with_chunks(2)
    doc = {"kind": "verse_detail", "data": {"verse": verse}}
    fp = tmp_path / "1.json"
    fp.write_text(json.dumps(doc), encoding="utf-8")
    assert merge_alignment_into_file(str(fp), {}) == 0
    assert not (tmp_path / "1.en.json").exists()


def test_load_chunk_alignments(tmp_path):
    art = {
        "verse_path": "/books/al-amali-mufid:1:1:1",
        "aligned": {"en.qarai": ["a", "b"]},
        "quarantined": {},
    }
    (tmp_path / "al-amali-mufid_1_1_1.json").write_text(
        json.dumps(art), encoding="utf-8")
    # malformed file is skipped, not fatal
    (tmp_path / "bad.json").write_text("{not json", encoding="utf-8")

    lookup = load_chunk_alignments(str(tmp_path))
    assert lookup == {"/books/al-amali-mufid:1:1:1": {"en.qarai": ["a", "b"]}}


# -- strict verbatim validation (replaces the old 0.90 token-overlap check) --

def test_validate_rejects_single_dropped_word():
    # The old 0.90 token-overlap check waved this through on longer texts; the
    # strict check must not - the sister parts are the sole copy of the text.
    words = [f"word{i}" for i in range(30)]
    scraped = " ".join(words)
    parts = [" ".join(words[:10]), " ".join(words[10:29])]  # word29 lost
    ok, reason = validate_alignment(parts, scraped)
    assert not ok
    assert "not verbatim" in reason


def test_validate_rejects_reordered_words():
    scraped = "seek knowledge from cradle to grave"
    parts = ["seek knowledge", "from grave to cradle"]
    ok, _ = validate_alignment(parts, scraped)
    assert not ok


def test_validate_rejects_case_change():
    # Extractive means character-exact: silent recapitalisation is a rewrite.
    scraped = "The Imam said: seek knowledge."
    parts = ["The Imam said:", "Seek knowledge."]
    ok, _ = validate_alignment(parts, scraped)
    assert not ok


def test_validate_accepts_whitespace_differences_only():
    # Cut points may trim/normalise whitespace (incl. the newline joining the
    # scraped array items) - that must still pass.
    scraped = "The chain narrated." + "\n" + "The saying   followed."
    parts = ["The chain narrated. ", " The saying followed."]
    ok, reason = validate_alignment(parts, scraped)
    assert ok, reason


def test_validate_preserves_markup_exactly():
    scraped = "Abu Abdullah<sup>asws</sup> said: seek knowledge."
    ok, _ = validate_alignment(["Abu Abdullah<sup>asws</sup> said:", "seek knowledge."], scraped)
    assert ok
    # Markup dropped -> rewrite -> reject.
    ok2, _ = validate_alignment(["Abu Abdullah said:", "seek knowledge."], scraped)
    assert not ok2


def test_validate_empty_original_accepts_only_empty_parts():
    assert validate_alignment(["", ""], "")[0]
    assert not validate_alignment(["stray text", ""], "")[0]


def test_validate_accepts_canonically_equivalent_arabic_marks():
    # shadda+kasra vs kasra+shadda are the same text under Unicode NFC -
    # models re-emit combining marks in canonical order (seen on HubeAli
    # inline Arabic in the July pilot). Must pass.
    dal, shadda, kasra = "د", "ّ", "ِ"
    scraped = f"the word {dal}{shadda}{kasra} appears"
    reordered_part = f"the word {dal}{kasra}{shadda}"
    ok, reason = validate_alignment([reordered_part, "appears"], scraped)
    assert ok, reason
