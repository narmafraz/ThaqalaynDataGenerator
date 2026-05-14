"""Map CAMeL Tools clitic codes (proclitic / enclitic) to prompt-readable labels.

`morphology.clitics` on a surface page is an object like
`{"prc0": "Al_det", "prc2": "wa_part"}` where each value is a CAMeL Tools
clitic code. Surface translation prompts need human-readable English
labels so the LLM can produce a coherent translation of the full surface
form. This table covers every code that appears in the corpus (audited
2026-05-14 against all 102,003 surfaces).

Each entry is a tuple `(form, label)`:
  - `form`  — the romanized Arabic form (e.g. "wa-", "-hu")
  - `label` — the English gloss (e.g. "and", "him/his/it")

Codes ending in `_pron` are pronominal enclitics functioning as the subject
of a nominal sentence (e.g. أَنَّهُ "that he"); `_poss` are possessive
suffixes (e.g. كِتَابُهُ "his book"); `_dobj` are direct object suffixes
(e.g. أَخَذَهُ "he took it").
"""
from __future__ import annotations

CLITIC_CODE_LABELS: dict[str, tuple[str, str]] = {
    # ──────────────── proclitics (prc0–prc3) ────────────────
    # Definite article
    "Al_det":      ("al-",   "the"),

    # Prepositions
    "bi_prep":     ("bi-",   "with/by"),
    "li_prep":     ("li-",   "to/for"),
    "ka_prep":     ("ka-",   "like/as"),
    "fiy_prep":    ("fī-",   "in"),

    # Conjunctions / coordination
    "wa_conj":     ("wa-",   "and (coordinating)"),
    "wa_sub":      ("wa-",   "and (subordinating)"),
    "wa_part":     ("wa-",   "and (particle / oath)"),
    "fa_conj":     ("fa-",   "so/then (coordinating)"),
    "fa_sub":      ("fa-",   "so/then (subordinating)"),

    # Future, emphatic, relative-clause markers
    "sa_fut":      ("sa-",   "will (future)"),
    "la_emph":     ("la-",   "indeed (emphatic)"),
    "la_rc":       ("la-",   "verily (introductory)"),

    # Vocative
    "yA_voc":      ("yā-",   "O (vocative)"),

    # Interrogative
    ">a_ques":     ("a-",    "is/does (interrogative)"),

    # ──────────────── enclitics (enc0) ────────────────
    # Possessive suffixes (attached to nouns)
    "1s_poss":     ("-ī",    "my"),
    "1p_poss":     ("-nā",   "our"),
    "2ms_poss":    ("-ka",   "your (m sg)"),
    "2fs_poss":    ("-ki",   "your (f sg)"),
    "2d_poss":     ("-kumā", "your (dual)"),
    "2mp_poss":    ("-kum",  "your (m pl)"),
    "2fp_poss":    ("-kunna","your (f pl)"),
    "3ms_poss":    ("-hu",   "his / its"),
    "3fs_poss":    ("-hā",   "her / its"),
    "3d_poss":     ("-humā", "their (dual)"),
    "3mp_poss":    ("-hum",  "their (m pl)"),
    "3fp_poss":    ("-hunna","their (f pl)"),

    # Direct-object suffixes (attached to verbs)
    "1s_dobj":     ("-nī",   "me"),
    "1p_dobj":     ("-nā",   "us"),
    "2ms_dobj":    ("-ka",   "you (m sg)"),
    "2fs_dobj":    ("-ki",   "you (f sg)"),
    "2d_dobj":     ("-kumā", "you two"),
    "2mp_dobj":    ("-kum",  "you (m pl)"),
    "2fp_dobj":    ("-kunna","you (f pl)"),
    "3ms_dobj":    ("-hu",   "him / it"),
    "3fs_dobj":    ("-hā",   "her / it"),
    "3d_dobj":     ("-humā", "them (dual)"),
    "3mp_dobj":    ("-hum",  "them (m pl)"),
    "3fp_dobj":    ("-hunna","them (f pl)"),

    # Pronominal copulas (e.g. attached to إِنَّ / أَنَّ — "that he", "that she")
    "1s_pron":     ("-ī",    "I"),
    "1p_pron":     ("-nā",   "we"),
    "2ms_pron":    ("-ka",   "you (m sg)"),
    "2fs_pron":    ("-ki",   "you (f sg)"),
    "2d_pron":     ("-kumā", "you two"),
    "2mp_pron":    ("-kum",  "you (m pl)"),
    "2fp_pron":    ("-kunna","you (f pl)"),
    "3ms_pron":    ("-hu",   "he / it"),
    "3fs_pron":    ("-hā",   "she / it"),
    "3d_pron":     ("-humā", "they (dual)"),
    "3mp_pron":    ("-hum",  "they (m pl)"),
    "3fp_pron":    ("-hunna","they (f pl)"),

    # Negation / relative / interrogative enclitics
    "lA_neg":      ("-lā",   "not (negation)"),
    "mA_rel":      ("-mā",   "what (relative)"),
    "mA_sub":      ("-mā",   "what / when (subordinating)"),
    "ma_interrog": ("-mā",   "what (interrogative)"),
    "man_rel":     ("-man",  "who/whom (relative)"),
}


# Position-ordering for prompt rendering.
# CAMeL slots roughly left-to-right by their numbered position; we render
# proclitics in increasing index order (prc3 → prc0 → stem) and enclitic in
# stem → enc0 order. Practical effect: "prc3, prc2, prc1, prc0, [stem], enc0".
PROCLITIC_ORDER = ("prc3", "prc2", "prc1", "prc0")
ENCLITIC_ORDER = ("enc0", "enc1")


def render_clitics(clitics: dict[str, str]) -> str:
    """Render a surface's clitics dict as a single prompt-ready line.

    Example:
      {"prc0": "Al_det", "prc1": "bi_prep", "prc2": "wa_conj"} →
      'proclitics: wa- "and (coordinating)" + bi- "with/by" + al- "the"'

    Returns an empty string for surfaces with no clitics. Unknown codes are
    rendered verbatim ('{code} (unknown clitic)') rather than dropped, so
    coverage gaps are visible in the prompt instead of silently masked.
    """
    if not clitics:
        return ""

    parts_pre: list[str] = []
    parts_post: list[str] = []

    for slot in PROCLITIC_ORDER:
        code = clitics.get(slot)
        if not code:
            continue
        form, label = CLITIC_CODE_LABELS.get(code, (code, "unknown clitic"))
        parts_pre.append(f'{form} "{label}"')

    for slot in ENCLITIC_ORDER:
        code = clitics.get(slot)
        if not code:
            continue
        form, label = CLITIC_CODE_LABELS.get(code, (code, "unknown clitic"))
        parts_post.append(f'{form} "{label}"')

    lines = []
    if parts_pre:
        lines.append("proclitics: " + " + ".join(parts_pre))
    if parts_post:
        lines.append("enclitics: " + " + ".join(parts_post))
    return "; ".join(lines)
