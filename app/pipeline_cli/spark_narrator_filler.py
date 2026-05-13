"""Spark-only Phase 2 narrator enrichment: fill missing name_en transliterations.

Phase 2's `programmatic_enrichment.enrich_narrators()` resolves narrators by
matching extracted AR names against `NarratorRegistry`. Names not in the
registry get left with `name_en = ""` and `identity_confidence = "ambiguous"`.

The UI's narrator hover cards / narrator profile pages render the empty
fields as blanks, which is a visible gap. This module asks Spark/Qwen to
produce clean EN transliterations for those unresolved AR names, in a
single batched call per verse.

Gated by `is_spark_model(model)` at the caller — no OpenAI cost path.

The batched call uses a positional `values` array (same workaround as
per-language key_terms) to dodge vLLM's Arabic-property-name corruption
bug in strict json_schema mode.
"""
from __future__ import annotations

import json
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = (
    "You are a transliterator of classical Arabic hadith narrator names. "
    "Produce a single clean English transliteration for each input name. "
    "Use the standard scholarly conventions (ibn = b., al- prefix preserved, "
    "diacritics dropped, common nisbas spelled consistently). Do NOT translate "
    "honorifics (radiyallah anhu, alaihis salam) — drop them from the output. "
    "Output valid JSON only."
)


def _build_user_message(names: List[str]) -> str:
    parts = [
        f"Transliterate each of the following Arabic narrator names into "
        f"English. Return a JSON object {{\"values\": [...]}} with exactly "
        f"{len(names)} strings, in the same order as the inputs.",
        "",
        "Conventions:",
        "  - Preserve 'al-' prefixes (e.g. al-Saduq, al-Kulayni)",
        "  - Use 'b.' for ibn between names (e.g. 'Muhammad b. Yahya')",
        "  - Drop honorifics (radiyallah anhu, alaihis salam, etc.)",
        "  - Use standard scholarly spelling (e.g. 'al-Hasan' not 'al-Hassan')",
        "",
        f"Inputs ({len(names)}):",
    ]
    for i, name in enumerate(names, 1):
        parts.append(f"  {i}. {name}")
    return "\n".join(parts)


def _build_schema(n: int) -> dict:
    return {
        "type": "object",
        "properties": {
            "values": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": n,
                "maxItems": n,
            }
        },
        "required": ["values"],
        "additionalProperties": False,
    }


async def fill_unresolved_narrators(
    result: dict,
    model: str = "qwen36-fast",
) -> dict:
    """Fill empty `name_en` fields for narrators the registry couldn't resolve.

    Modifies `result["isnad_matn"]["narrators"][i]["name_en"]` in place.
    Records metadata at `result["_phase2_spark_narrator_calls"]` (call count)
    and `result["_phase2_spark_narrator_filled"]` (count of names filled).

    No-op (returns result unchanged) when:
      - No narrators present
      - All narrators already have name_en filled by the registry
      - Spark call fails

    Args:
        result: Pipeline result dict with isnad_matn.narrators populated.
        model: Spark model alias (qwen36-fast etc.). Caller is responsible
               for ensuring this routes to Spark — typically by checking
               `is_spark_model(model)` before invoking this function.

    Returns:
        Same `result` dict, mutated in place.
    """
    narrators = (result.get("isnad_matn") or {}).get("narrators") or []
    if not narrators:
        return result

    # Pick out narrators that need filling: have an AR name but no EN name.
    targets = []
    for i, n in enumerate(narrators):
        name_ar = (n.get("name_ar") or "").strip()
        name_en = (n.get("name_en") or "").strip()
        if name_ar and not name_en:
            targets.append((i, name_ar))

    if not targets:
        return result

    from app.pipeline_cli.openai_backend import call_openai

    ar_names = [name for _, name in targets]
    user = _build_user_message(ar_names)
    schema = _build_schema(len(ar_names))

    cr = await call_openai(
        SYSTEM_PROMPT, user, model=model,
        max_output_tokens=200 + 60 * len(ar_names),  # ~60 tokens/name + slack
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "narrator_transliteration",
                "schema": schema,
                "strict": True,
            },
        },
    )

    result["_phase2_spark_narrator_calls"] = 1
    result["_phase2_spark_narrator_filled"] = 0

    if "error" in cr:
        logger.warning("Phase 2 (Spark) narrator filler failed: %s",
                       cr.get("error", "?")[:120])
        return result
    try:
        parsed = json.loads(cr.get("result", ""))
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("Phase 2 (Spark) narrator filler parse fail: %s", e)
        return result

    values = parsed.get("values", []) or []
    if len(values) != len(ar_names):
        logger.warning(
            "Phase 2 (Spark) narrator filler returned %d values, expected %d "
            "— leaving name_en empty for this verse",
            len(values), len(ar_names),
        )
        return result

    filled = 0
    for (idx, _ar), en in zip(targets, values):
        if isinstance(en, str) and en.strip():
            narrators[idx]["name_en"] = en.strip()
            # Bump confidence note so downstream tools know this came from
            # Spark, not the canonical registry. Keep canonical_id as-is
            # (still None — we didn't resolve to a registry entry, just
            # gave a clean transliteration).
            if not narrators[idx].get("ambiguity_note"):
                narrators[idx]["ambiguity_note"] = (
                    "name_en filled by Spark transliteration "
                    "(not matched against canonical registry)"
                )
            filled += 1

    result["_phase2_spark_narrator_filled"] = filled
    return result
