"""Unit tests for app.words.clitic_labels."""
from __future__ import annotations

from app.words.clitic_labels import (
    CLITIC_CODE_LABELS,
    PROCLITIC_ORDER,
    ENCLITIC_ORDER,
    render_clitics,
)


# Every CAMeL clitic code that appears in the corpus (audited 2026-05-14
# against all 102,003 surfaces in ThaqalaynWords/surfaces/*.json).
CORPUS_CLITIC_CODES = {
    # Proclitic codes
    "Al_det", "bi_prep", "li_prep", "ka_prep", "fiy_prep",
    "wa_conj", "wa_sub", "wa_part", "fa_conj",
    "sa_fut", "la_emph", "la_rc", "yA_voc", ">a_ques",
    # Enclitic codes — possessives
    "1s_poss", "1p_poss",
    "2ms_poss", "2fs_poss", "2d_poss", "2mp_poss", "2fp_poss",
    "3ms_poss", "3fs_poss", "3d_poss", "3mp_poss", "3fp_poss",
    # Enclitic codes — direct objects
    "1s_dobj", "1p_dobj",
    "2ms_dobj", "2fs_dobj", "2d_dobj", "2mp_dobj", "2fp_dobj",
    "3ms_dobj", "3fs_dobj", "3d_dobj", "3mp_dobj", "3fp_dobj",
    # Enclitic codes — pronouns
    "1s_pron", "1p_pron",
    "2ms_pron", "2fs_pron", "2d_pron", "2mp_pron", "2fp_pron",
    "3ms_pron", "3fs_pron", "3d_pron", "3mp_pron", "3fp_pron",
    # Negation / relative / interrogative
    "lA_neg", "mA_rel", "mA_sub", "ma_interrog", "man_rel",
}


def test_every_corpus_code_has_a_label() -> None:
    """All clitic codes observed in the actual corpus must be in the table."""
    missing = CORPUS_CLITIC_CODES - set(CLITIC_CODE_LABELS.keys())
    assert not missing, f"Missing labels for: {sorted(missing)}"


def test_every_label_is_a_form_plus_gloss_tuple() -> None:
    for code, value in CLITIC_CODE_LABELS.items():
        assert isinstance(value, tuple), f"{code}: not a tuple"
        assert len(value) == 2, f"{code}: tuple length != 2"
        form, label = value
        assert form and isinstance(form, str), f"{code}: bad form"
        assert label and isinstance(label, str), f"{code}: bad label"


def test_render_empty_clitics() -> None:
    assert render_clitics({}) == ""
    assert render_clitics(None) == ""  # type: ignore[arg-type]


def test_render_single_proclitic_definite_article() -> None:
    out = render_clitics({"prc0": "Al_det"})
    assert "proclitics:" in out
    assert "al-" in out
    assert "the" in out
    assert "enclitics:" not in out


def test_render_compound_proclitics_in_correct_order() -> None:
    """وَبِالْعَهْدِ has prc0=Al_det, prc1=bi_prep, prc2=wa_conj.
    Output order should be prc3→prc2→prc1→prc0 (left-to-right reading order).
    """
    out = render_clitics({"prc0": "Al_det", "prc1": "bi_prep", "prc2": "wa_conj"})
    assert out.index("wa-") < out.index("bi-") < out.index("al-")


def test_render_proclitic_plus_enclitic() -> None:
    """فَأَخَذَنِي has fa_conj + 1s_dobj."""
    out = render_clitics({"prc2": "fa_conj", "enc0": "1s_dobj"})
    assert "proclitics:" in out
    assert "enclitics:" in out
    assert "fa-" in out
    assert "-nī" in out
    # Proclitics rendered before enclitics
    assert out.index("proclitics:") < out.index("enclitics:")


def test_unknown_code_renders_verbatim_not_dropped() -> None:
    """Coverage gaps must be visible in the prompt, not silently masked."""
    out = render_clitics({"prc1": "totally_made_up_code"})
    assert "totally_made_up_code" in out
    assert "unknown clitic" in out


def test_proclitic_and_enclitic_order_constants() -> None:
    assert PROCLITIC_ORDER == ("prc3", "prc2", "prc1", "prc0")
    assert ENCLITIC_ORDER == ("enc0", "enc1")


def test_pronominal_3ms_consistent_across_pron_poss_dobj() -> None:
    """3ms pronoun/possessive/dobj all encode as -hu; labels distinguish role."""
    pron_form, pron_label = CLITIC_CODE_LABELS["3ms_pron"]
    poss_form, poss_label = CLITIC_CODE_LABELS["3ms_poss"]
    dobj_form, dobj_label = CLITIC_CODE_LABELS["3ms_dobj"]
    assert pron_form == poss_form == dobj_form == "-hu"
    # Labels must differ — the LLM uses them to pick role-appropriate translation
    assert pron_label != poss_label
    assert poss_label != dobj_label
