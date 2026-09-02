"""Tests for scraped-translation chunk alignment (align-scraped)."""

import json
import os

import pytest

from app.pipeline_cli.chunk_alignment_phase import (
    _alignment_schema,
    attach_prefix,
    best_effort_split,
    fix_single_dump,
    placement_suspect,
    reslice_from_original,
    split_leading_number,
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


# -- leading hadith-number prefix handling ----------------------------------

def test_split_leading_number_variants():
    assert split_leading_number("1. The grand Shaikh") == ("1. ", "The grand Shaikh")
    assert split_leading_number(" 5. A number of our people") == (" 5. ", "A number of our people")
    assert split_leading_number("10. He said") == ("10. ", "He said")
    assert split_leading_number("12) He said") == ("12) ", "He said")
    assert split_leading_number("3 - He said") == ("3 - ", "He said")


def test_split_leading_number_leaves_ordinary_text():
    # No separator marker after the digits -> not a hadith-number prefix.
    assert split_leading_number("40 rakats are prescribed") == ("", "40 rakats are prescribed")
    assert split_leading_number("He said: pray") == ("", "He said: pray")
    assert split_leading_number("") == ("", "")


def test_attach_prefix_first_nonempty_part():
    assert attach_prefix(["", "He said", "more"], "10. ") == ["", "10. He said", "more"]
    assert attach_prefix(["He said", "more"], "1. ") == ["1. He said", "more"]
    assert attach_prefix(["", ""], "5. ") == ["5. ", ""]
    assert attach_prefix(["He said"], "") == ["He said"]


def test_prefix_roundtrip_passes_strict_validation():
    # End-to-end shape of the fix: strip prefix, segment the body, re-attach,
    # validate against the FULL original -> must pass.
    scraped = "10. He said: Al-Sharif reported. Seek knowledge."
    prefix, body = split_leading_number(scraped)
    parts_from_model = ["He said: Al-Sharif reported.", "Seek knowledge."]
    parts = attach_prefix(parts_from_model, prefix)
    ok, reason = validate_alignment(parts, scraped)
    assert ok, reason
    # Without re-attachment the same output fails (the July-pilot failure).
    ok2, _ = validate_alignment(["He said: Al-Sharif reported.", "Seek knowledge."], scraped)
    assert not ok2


# -- cut-point salvage: reslice the original at model-implied boundaries ----
# Each case is modeled on a real pilot2 quarantine class (2026-08-20).

def test_reslice_restores_dropped_quote_marks():
    # 63/95 pilot2 quarantines: model drops the "..." around quoted speech.
    original = 'He said: "It is recorded in the book." Then he left the mosque.'
    model_parts = ["He said: It is recorded in the book.", "Then he left the mosque."]
    out = reslice_from_original(model_parts, original)
    assert out is not None
    assert "".join(out) == original          # literal slices - lossless
    ok, reason = validate_alignment(out, original)
    assert ok, reason
    assert out[0].startswith('He said: "It is recorded')
    assert out[1].strip() == "Then he left the mosque."


def test_reslice_fixes_curly_straight_quote_substitution():
    # 11/95: same length, curly quote re-typed as ASCII apostrophe.
    original = "saying: ’O, He, Whom one hearing does not distract’ and he wept."
    model_parts = ["saying: 'O, He, Whom one hearing does not distract'", "and he wept."]
    out = reslice_from_original(model_parts, original)
    assert out is not None
    assert "".join(out) == original
    assert validate_alignment(out, original)[0]
    assert "’" in out[0]                  # the original quote char survives


def test_reslice_fixes_boundary_duplication():
    # 18/95: model duplicates a few words across the cut.
    original = "Muhammad al-Katib al-Iskafi reported. From Abu Ali who said: pray at dawn."
    model_parts = ["Muhammad al-Katib al-Katib al-Iskafi reported.",
                   "From Abu Ali who said: pray at dawn."]
    out = reslice_from_original(model_parts, original)
    assert out is not None
    assert "".join(out) == original
    assert validate_alignment(out, original)[0]
    assert out[1].strip().startswith("From Abu Ali")


def test_reslice_preserves_empty_parts():
    original = "The Imam said: seek knowledge from the cradle."
    model_parts = ["", "The Imam said: seek knowledge from the cradle."]
    out = reslice_from_original(model_parts, original)
    assert out is not None
    assert out[0] == ""
    assert "".join(out) == original


def test_reslice_rejects_unrelated_text():
    original = "The Imam said: seek knowledge from the cradle to the grave."
    model_parts = ["Completely different opening sentence here.",
                   "And an equally unrelated second half of text."]
    assert reslice_from_original(model_parts, original) is None


# -- placement accuracy gate (modeled on real pilot2 accuracy findings) ------

ISNAD_REF = ("Muhammad ibn Yahya, from Ahmad ibn Muhammad ibn Isa, from Ali "
             "ibn Hadid, from Murazim, from Abu Abdillah peace be upon him")
MATN_REF = ("Indeed Allah revealed in the Quran a clarification of everything "
            "so nothing about lawful and unlawful matters was left without a rule")


def _chunks(*refs):
    return [{"chunk_type": "body", "arabic_text": "x", "en_ref": r} for r in refs]


def test_fix_single_dump_moves_abridged_text_to_matching_slot():
    # Sarwar omits the isnad; model dumped the matn text into the isnad slot.
    text = ("There is nothing about lawful and unlawful matters that has been "
            "left without a rule in the Quran which clarifies everything")
    parts = [text, ""]
    out = fix_single_dump(parts, _chunks(ISNAD_REF, MATN_REF))
    assert out == ["", text]


def test_fix_single_dump_keeps_correct_placement():
    text = "Muhammad ibn Yahya from Ahmad ibn Muhammad ibn Isa from Ali ibn Hadid"
    parts = [text, ""]
    out = fix_single_dump(parts, _chunks(ISNAD_REF, MATN_REF))
    assert out == [text, ""]


def test_fix_single_dump_noop_on_degenerate_refs():
    # quran_11_29 class: all chunk references identical - nothing to judge by.
    text = "And O my people I do not ask you for any wealth for it"
    parts = [text, "", ""]
    out = fix_single_dump(parts, _chunks(MATN_REF, MATN_REF, MATN_REF))
    assert out == parts


def test_fix_single_dump_noop_when_multiple_parts():
    parts = ["chain text here", "matn text here"]
    assert fix_single_dump(parts, _chunks(ISNAD_REF, MATN_REF)) == parts


def test_placement_suspect_flags_off_by_one():
    # al-amali-mufid 18:6 class: parts lag their segments by one slot.
    r0 = "Ali ibn Muhammad ibn Hubaish al-Katib reported from al-Hasan al-Zafarani"
    r1 = ("you are like the absentees despite your presence and the deaf "
          "despite your hearing I recite unto you words of wisdom")
    r2 = ("rise to fight the enemy before they overwhelm you your hands are "
          "weakened and you occupied your minds with unavailing things")
    parts = [r0,  # slot 0 correct
             r2,  # slot 1 actually holds slot 2's content
             r1]  # slot 2 actually holds slot 1's content
    reason = placement_suspect(parts, _chunks(r0, r1, r2))
    assert reason is not None
    assert "placement suspect" in reason


def test_placement_suspect_accepts_good_alignment():
    r0 = "Ali ibn Muhammad reported from al-Hasan who reported from Ibrahim"
    r1 = "seek knowledge from the cradle to the grave said the Imam clearly"
    parts = ["Ali ibn Muhammad reported to me from al-Hasan from Ibrahim",
             "the Imam said seek knowledge from the cradle unto the grave"]
    assert placement_suspect(parts, _chunks(r0, r1)) is None


def test_placement_suspect_none_on_missing_or_degenerate_refs():
    parts = ["some text here with many content words present", "other half"]
    assert placement_suspect(parts, _chunks("", "")) is None
    assert placement_suspect(parts, _chunks(MATN_REF, MATN_REF)) is None


def test_placement_suspect_flags_weak_single_dump():
    # Sarwar-abridgement class (al-kafi 1:2:20:1): one part, matches nothing.
    text = ("a completely different summary sentence discussing jurisprudence "
            "rulings and permissibility matters generally")
    reason = placement_suspect([text, ""], _chunks(ISNAD_REF, MATN_REF))
    assert reason is not None
    assert "abridged" in reason


def test_placement_suspect_allows_strong_single_dump():
    # Single part that clearly matches its own slot must NOT be flagged.
    text = "Muhammad ibn Yahya from Ahmad ibn Muhammad ibn Isa from Ali ibn Hadid from Murazim"
    assert placement_suspect([text, ""], _chunks(ISNAD_REF, MATN_REF)) is None


# -- best-effort split (deterministic DP over sentences x chunk references) --

def test_best_effort_split_empties_omitted_isnad():
    # Sarwar class: translation omits the chain entirely; one matn sentence.
    text = ("There is nothing about lawful and unlawful matters that has been "
            "left without a rule in the Quran which clarifies everything.")
    out = best_effort_split(text, _chunks(ISNAD_REF, MATN_REF))
    assert out is not None
    assert out[0] == ""                      # isnad stays empty
    assert out[1] == text
    assert "".join(out) == text


def test_best_effort_split_distributes_multiple_sentences():
    r_chain = "Ali ibn Muhammad reported from al-Hasan from Ibrahim ibn Muhammad"
    r_battle = ("rise to fight the enemy before they overwhelm you your hands "
                "weakened occupied minds unavailing things preparedness war")
    r_ashath = ("al-Ashath ibn Qais al-Kindi stood and asked why he did not do "
                "what Uthman ibn Affan did granting favours compromising")
    text = ("Ali ibn Muhammad reported to me from al-Hasan, from Ibrahim ibn "
            "Muhammad. Rise to fight the enemy before they overwhelm you, for "
            "your hands are weakened by unavailing things. Then al-Ashath ibn "
            "Qais al-Kindi stood up and asked why he did not do what Uthman "
            "ibn Affan did.")
    out = best_effort_split(text, _chunks(r_chain, r_battle, r_ashath))
    assert out is not None
    assert "".join(out) == text
    assert "Ali ibn Muhammad reported" in out[0]
    assert "Rise to fight" in out[1]
    assert "al-Ashath" in out[2]


def test_best_effort_split_none_without_signal():
    text = "Completely unrelated words about gardening and carpentry hobbies."
    assert best_effort_split(text, _chunks(ISNAD_REF, MATN_REF)) is None


def test_best_effort_split_none_on_degenerate_refs():
    text = "Some sentence here. Another sentence there."
    assert best_effort_split(text, _chunks(MATN_REF, MATN_REF)) is None


def test_best_effort_split_lossless_on_odd_whitespace():
    text = ("First sentence matching the chain: Ali ibn Muhammad reported."
            + "\n\n"
            + "  Second one: nothing lawful was left without a rule in the Quran. ")
    out = best_effort_split(text, _chunks(ISNAD_REF, MATN_REF))
    assert out is not None
    assert "".join(out) == text


def test_reslice_multi_part_with_whitespace_regression():
    # Regression: boundary monotonicity was checked across mixed coordinate
    # spaces (fold vs original), so any multi-boundary text with accumulated
    # whitespace falsely returned None (pilot cases al-amali-mufid 33:6,
    # al-kafi 1:1:1:12). Must reslice successfully with 4 parts.
    original = ('He said:  "The first ruling stands."   Then the people '
                'gathered  around him.  He recited: "Patience is the key."  '
                'And they departed  to their homes quietly.  The end came '
                'after  the third night watch.')
    model_parts = [  # model dropped the quote marks in two places
        'He said: The first ruling stands.',
        'Then the people gathered around him.',
        'He recited: Patience is the key. And they departed to their homes quietly.',
        'The end came after the third night watch.',
    ]
    out = reslice_from_original(model_parts, original)
    assert out is not None, "reslice must handle multi-part whitespace texts"
    assert "".join(out) == original
    assert validate_alignment(out, original)[0]
