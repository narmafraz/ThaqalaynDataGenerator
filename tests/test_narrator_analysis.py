"""Tests for per-chapter narrator-chain analysis (app/narrator_analysis.py)."""

from __future__ import annotations

import json

import pytest

import app.narrator_analysis as na


# --------------------------------------------------------------------------- #
# Helpers to build synthetic verse dicts
# --------------------------------------------------------------------------- #
def ai_verse(local_index, narrators, gradings=None):
    """narrators: list of (id, role, name_en, ambiguous?)."""
    return {
        "local_index": local_index,
        "gradings": gradings or {},
        "ai": {
            "isnad_matn": {
                "has_chain": True,
                "narrators": [
                    {
                        "canonical_id": nid,
                        "role": role,
                        "name_en": name,
                        "name_ar": name,
                        "identity_confidence": "ambiguous" if amb else "definite",
                        "ambiguity_note": "x" if amb else None,
                        "position": i + 1,
                    }
                    for i, (nid, role, name, amb) in enumerate(narrators)
                ],
            }
        },
    }


def plain_verse(local_index, ids):
    """Verse with only narrator_chain.parts (no AI)."""
    parts = []
    for nid in ids:
        parts.append({"kind": "narrator", "path": f"/people/narrators/{nid}",
                      "text": f"n{nid}"})
    return {"local_index": local_index, "narrator_chain": {"parts": parts}}


def make_profile(verses):
    p = na.NarratorProfile()
    for v in verses:
        for e in na._chain_from_verse(v):
            p.observe(e)
    return p


# --------------------------------------------------------------------------- #
# path conversion
# --------------------------------------------------------------------------- #
def test_verse_path_to_relpath():
    assert na._verse_path_to_relpath("/books/al-kafi:1:1:1:1") == "al-kafi/1/1/1/1.json"
    assert na._verse_path_to_relpath("books/quran:2:5") == "quran/2/5.json"


# --------------------------------------------------------------------------- #
# chain extraction + fallback
# --------------------------------------------------------------------------- #
def test_chain_prefers_ai_isnad():
    v = ai_verse(1, [(10, "narrator", "A", False), (20, "source", "Imam", False)])
    chain = na._chain_from_verse(v)
    assert [e["id"] for e in chain] == [10, 20]
    assert chain[1]["role"] == "source"


def test_chain_falls_back_to_parts():
    v = plain_verse(1, [10, 20, 30])
    chain = na._chain_from_verse(v)
    assert [e["id"] for e in chain] == [10, 20, 30]
    assert all(e["role"] == "" for e in chain)


def test_chain_empty_when_no_data():
    assert na._chain_from_verse({"local_index": 1}) == []


# --------------------------------------------------------------------------- #
# profile classification
# --------------------------------------------------------------------------- #
def test_source_detection_by_majority_role():
    # id 99 is labelled source in 2 of 2 verses -> source
    verses = [
        ai_verse(1, [(10, "narrator", "A", False), (99, "source", "Imam", False)]),
        ai_verse(2, [(11, "narrator", "B", False), (99, "source", "Imam", False)]),
    ]
    p = make_profile(verses)
    assert p.is_source(99)
    assert not p.is_source(10)


def test_placeholder_detection_collective_and_relative():
    v = ai_verse(1, [
        (6, "narrator", "a number of our companions", False),
        (2, "narrator", "his father", False),
        (10, "narrator", "Real Person", False),
    ])
    p = make_profile([v])
    assert 6 in p.placeholder_ids
    assert 2 in p.placeholder_ids
    assert 10 not in p.placeholder_ids


def test_excluded_covers_sources_and_placeholders_not_ambiguous():
    v = ai_verse(1, [
        (99, "source", "Imam", False),
        (6, "narrator", "a number of our companions", False),
        (10, "narrator", "Ambiguous Guy", True),
    ])
    p = make_profile([v])
    assert p.is_excluded(99)          # source
    assert p.is_excluded(6)           # placeholder
    assert not p.is_excluded(10)      # ambiguous is reported, not excluded


# --------------------------------------------------------------------------- #
# clustering
# --------------------------------------------------------------------------- #
def test_cluster_groups_shared_narrators():
    chains = {1: {10, 11}, 2: {11, 12}, 3: {99}}  # 1&2 share 11; 3 alone
    clusters = na._cluster(chains)
    assert clusters[0] == [1, 2]
    assert clusters[1] == [3]


def test_cluster_singletons_when_no_overlap():
    chains = {1: {10}, 2: {20}, 3: {30}}
    assert na._cluster(chains) == [[1], [2], [3]]


def test_independent_paths_excludes_shared_imam():
    # both hadith share only the source Imam (99); after exclusion they split
    verses = [
        ai_verse(1, [(10, "narrator", "A", False), (99, "source", "Imam", False)]),
        ai_verse(2, [(20, "narrator", "B", False), (99, "source", "Imam", False)]),
    ]
    p = make_profile(verses)
    res = na.analyze_chapter(verses, p)
    assert res["independent_paths"] == 2


def test_shared_real_transmitter_merges_paths():
    verses = [
        ai_verse(1, [(10, "narrator", "Shared", False), (99, "source", "Imam", False)]),
        ai_verse(2, [(10, "narrator", "Shared", False), (88, "source", "Imam2", False)]),
    ]
    p = make_profile(verses)
    res = na.analyze_chapter(verses, p)
    assert res["independent_paths"] == 1


# --------------------------------------------------------------------------- #
# aggregate insights
# --------------------------------------------------------------------------- #
def test_analyze_chapter_full_bundle():
    verses = [
        ai_verse(1, [(10, "narrator", "A", False), (11, "narrator", "B", False),
                     (99, "source", "Imam", False)], gradings={"majlisi": "sahih"}),
        ai_verse(2, [(10, "narrator", "A", False), (12, "narrator", "C", False),
                     (99, "source", "Imam", False)], gradings={"majlisi": "hasan"}),
        ai_verse(3, [(20, "narrator", "D", False), (88, "source", "Imam2", False)]),
    ]
    p = make_profile(verses)
    res = na.analyze_chapter(verses, p)

    assert res["hadith_count"] == 3
    assert res["analyzed_count"] == 3
    assert res["no_chain_count"] == 0
    # prolific excludes the source Imam; id 10 appears in 2 hadith -> top
    assert res["prolific"][0]["id"] == 10
    assert res["prolific"][0]["hadith"] == [1, 2]   # the two hadith it's in
    assert all(pr["id"] not in (99, 88) for pr in res["prolific"])
    # sources lists Imams, each with its citing hadith
    src_ids = {s["id"] for s in res["sources"]}
    assert src_ids == {99, 88}
    src99 = next(s for s in res["sources"] if s["id"] == 99)
    assert src99["hadith"] == [1, 2]
    # spine: id 10 in 2/3 chains >= 0.5
    spine10 = next((s for s in res["spine"] if s["id"] == 10), None)
    assert spine10 and spine10["hadith"] == [1, 2]
    # gradings carry the hadith indices per grade
    assert res["gradings"]["majlisi"] == {"sahih": [1], "hasan": [2]}
    # chain length stats present
    assert res["chain_lengths"]["min"] == 2
    assert res["chain_lengths"]["max"] == 3
    # graph nodes exclude sources
    assert all(n["id"] not in (99, 88) for n in res["graph"]["nodes"])
    # names live once in the lookup map, keyed by string id; refs carry no names
    assert res["narrators"]["10"] == ["A", "A"]
    assert "99" in res["narrators"] and "88" in res["narrators"]
    assert "name_en" not in res["prolific"][0]
    assert all(isinstance(x, int) for x in res["clusters"][0]["shared_ids"])


def test_classify_grade_arabic_and_english():
    assert na._classify_grade("صحيح") == "sahih"
    assert na._classify_grade("Sahih al-Kafi") == "sahih"
    assert na._classify_grade("ضعيف") == "da'if"
    assert na._classify_grade("مجهول") == "majhul"
    assert na._classify_grade("معتبر") == "mu'tabar"
    assert na._classify_grade("something else") == "other"


def test_gradings_array_format_parsed():
    # al-Kafi/Sarwar format: "Scholar: <span>term</span> - source"
    verse = {
        "local_index": 1,
        "gradings": [
            "Allamah Baqir al-Majlisi: <span> مجهول </span> - Mir'at (98/1)",
            "Shaykh Baqir al-Behbudi: <span>ضعيف</span> - Sahih al-Kafi",
        ],
    }
    out = na._gradings_of(verse)
    assert out["Allamah Baqir al-Majlisi"] == "majhul"
    assert out["Shaykh Baqir al-Behbudi"] == "da'if"


def test_gradings_array_format_in_chapter():
    verses = [
        {**ai_verse(1, [(10, "narrator", "A", False)]),
         "gradings": ["Allamah Baqir al-Majlisi: <span>صحيح</span> - x"]},
        {**ai_verse(2, [(11, "narrator", "B", False)]),
         "gradings": ["Allamah Baqir al-Majlisi: <span>حسن</span> - y"]},
    ]
    p = make_profile(verses)
    res = na.analyze_chapter(verses, p)
    assert res["gradings"]["Allamah Baqir al-Majlisi"] == {"sahih": [1], "hasan": [2]}


def test_gradings_from_data_level(tmp_path):
    # gradings only present at data.gradings (not data.verse.gradings)
    books = tmp_path / "books"
    _write(books / "demo" / "1.json", {
        "index": "demo:1", "kind": "verse_list",
        "data": {"verse_refs": [
            {"local_index": 1, "part_type": "Hadith", "path": "/books/demo:1:1"},
        ]},
    })
    _write(books / "demo" / "1" / "1.json", {
        "index": "demo:1:1", "kind": "verse_detail",
        "data": {
            "verse": ai_verse(1, [(10, "narrator", "A", False)]),
            "gradings": ["Majlisi: <span>صحيح</span> - x"],
        },
    })
    na.build(tmp_path)
    doc = json.loads((books / "demo" / "1.narrators.json").read_text(encoding="utf-8"))
    assert doc["data"]["gradings"]["Majlisi"] == {"sahih": [1]}


def test_no_chain_counted():
    verses = [ai_verse(1, [(10, "narrator", "A", False)]),
              {"local_index": 2}]  # no chain
    p = make_profile(verses)
    res = na.analyze_chapter(verses, p)
    assert res["no_chain_count"] == 1
    assert res["analyzed_count"] == 1


def test_ambiguity_reported():
    verses = [ai_verse(1, [(10, "narrator", "Amb", True),
                           (11, "narrator", "Clean", False)])]
    p = make_profile(verses)
    res = na.analyze_chapter(verses, p)
    assert res["ambiguity"]["chains_with_ambiguous"] == 1
    assert any(n["id"] == 10 for n in res["ambiguity"]["narrators"])


def test_corroboration_counts_independent_paths_to_source():
    # Imam 99 reached by two hadith with disjoint real transmitters -> 2 paths
    verses = [
        ai_verse(1, [(10, "narrator", "A", False), (99, "source", "Imam", False)]),
        ai_verse(2, [(20, "narrator", "B", False), (99, "source", "Imam", False)]),
    ]
    p = make_profile(verses)
    res = na.analyze_chapter(verses, p)
    corr = [c for c in res["corroboration"] if c["id"] == 99]
    assert corr and corr[0]["independent_paths"] == 2


# --------------------------------------------------------------------------- #
# sidecar build (filesystem)
# --------------------------------------------------------------------------- #
def _write(path, doc):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def test_build_chapter_sidecars(tmp_path):
    books = tmp_path / "books"
    # one chapter shell with 2 hadith
    _write(books / "demo" / "1.json", {
        "index": "demo:1", "kind": "verse_list",
        "data": {"verse_refs": [
            {"local_index": 1, "part_type": "Hadith", "path": "/books/demo:1:1"},
            {"local_index": 2, "part_type": "Hadith", "path": "/books/demo:1:2"},
        ]},
    })
    _write(books / "demo" / "1" / "1.json", {
        "index": "demo:1:1", "kind": "verse_detail",
        "data": {"verse": ai_verse(1, [(10, "narrator", "A", False),
                                        (99, "source", "Imam", False)])},
    })
    _write(books / "demo" / "1" / "2.json", {
        "index": "demo:1:2", "kind": "verse_detail",
        "data": {"verse": ai_verse(2, [(10, "narrator", "A", False),
                                        (88, "source", "Imam2", False)])},
    })

    written = na.build(tmp_path)
    assert len(written) == 1
    sidecar = books / "demo" / "1.narrators.json"
    assert sidecar in written
    doc = json.loads(sidecar.read_text(encoding="utf-8"))
    assert doc["kind"] == "narrator_analysis"
    assert doc["index"] == "demo:1"
    # shared transmitter 10 -> single path
    assert doc["data"]["independent_paths"] == 1
    assert doc["data"]["hadith_count"] == 2


def test_build_missing_books_dir_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        na.build(tmp_path)
