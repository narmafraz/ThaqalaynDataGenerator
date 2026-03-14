"""Phase 3: Scholarly enrichment via Claude CLI.

Optional phase that enriches the EN summary with scholarly context:
- Historical background
- Scholarly interpretations
- Additional thematic Quran refs the programmatic phase may have missed

Gated by --skip-scholarly flag. Uses claude -p (the expensive backend).
"""

import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def build_scholarly_prompt(
    arabic_text: str,
    en_summary: str,
    quran_refs: list,
    book_name: str = "",
    chapter_title: str = "",
) -> tuple:
    """Build system and user prompts for scholarly enrichment.

    Args:
        arabic_text: Original Arabic text
        en_summary: Current English summary from Phase 1
        quran_refs: Current related_quran refs (Phase 1 thematic + Phase 2 explicit)
        book_name: Source book name
        chapter_title: Chapter title

    Returns:
        (system_prompt, user_message) tuple
    """
    system = """You are a Shia Islamic scholar enriching hadith summaries with historical and scholarly context.

OUTPUT: A JSON object with exactly two fields:
{
  "enriched_summary": "2-3 sentences: the existing summary enhanced with historical context, scholarly significance, or practical implications",
  "additional_quran_refs": [{"ref": "surah:ayah", "relationship": "thematic"}]
}

RULES:
- Keep the enriched_summary to 2-3 sentences maximum
- Only add Quran refs that are clearly thematically connected
- Do not repeat refs already provided
- Be faithful to Shia scholarly tradition
- Output valid JSON only"""

    user_parts = [
        f"Arabic text: {arabic_text}",
        f"Book: {book_name}",
        f"Chapter: {chapter_title}",
        f"Current summary: {en_summary}",
    ]

    if quran_refs:
        refs_str = ", ".join(r.get("ref", "") for r in quran_refs)
        user_parts.append(f"Existing Quran references: {refs_str}")

    user_parts.append("")
    user_parts.append(
        "Enrich the summary with scholarly context and suggest any additional thematic Quran references."
    )

    return system, "\n".join(user_parts)


def _extract_json(raw: str) -> dict:
    """Extract a JSON object from potentially wrapped LLM output.

    Handles code fences, preamble text, and trailing content.

    Args:
        raw: Raw LLM response string

    Returns:
        Parsed dict

    Raises:
        ValueError: If no valid JSON object can be extracted
    """
    text = raw.strip()

    # Strip code fences (```json ... ``` or ``` ... ```)
    if text.startswith("```"):
        first_nl = text.find("\n")
        if first_nl == -1:
            first_nl = 3
        text = text[first_nl + 1 :]
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3].rstrip()

    # If text doesn't start with {, try to find a JSON object
    if not text.startswith("{"):
        brace_start = text.find("{")
        brace_end = text.rfind("}")
        if brace_start != -1 and brace_end > brace_start:
            text = text[brace_start : brace_end + 1]
        else:
            raise ValueError("No JSON object found in response")

    return json.loads(text)


async def enrich_scholarly(
    result: dict,
    arabic_text: str,
    book_name: str = "",
    chapter_title: str = "",
    backend: str = "claude",
    model: str = "sonnet",
) -> dict:
    """Run Phase 3 scholarly enrichment.

    Takes a pipeline result that already has Phase 1 (core AI) + Phase 2
    (programmatic) data and enriches the EN summary with scholarly context.

    Args:
        result: Pipeline result with Phase 1 + Phase 2 data
        arabic_text: Original Arabic text
        book_name: Source book
        chapter_title: Chapter title
        backend: LLM backend ("claude" or "openai")
        model: Model name

    Returns:
        Updated result dict with enriched summary and possibly more Quran refs.
        On failure, returns the original result unchanged.
    """
    en_trans = result.get("translations", {}).get("en", {})
    en_summary = en_trans.get("summary", "")
    quran_refs = result.get("related_quran", [])

    if not en_summary:
        logger.warning("Phase 3: No EN summary to enrich, skipping")
        return result

    system, user = build_scholarly_prompt(
        arabic_text, en_summary, quran_refs, book_name, chapter_title
    )

    # Call LLM via the appropriate backend
    if backend == "openai":
        from app.pipeline_cli.openai_backend import call_openai

        cr = await call_openai(system, user, model=model)
    else:
        from app.pipeline_cli.pipeline import call_claude

        cr = await call_claude(system, user, model=model)

    if "error" in cr:
        logger.error("Phase 3 scholarly enrichment failed: %s", cr["error"])
        return result

    # Parse response
    try:
        data = _extract_json(cr.get("result", ""))
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("Phase 3 JSON parse failed: %s", e)
        return result

    # Merge enriched summary
    enriched = data.get("enriched_summary", "")
    if enriched and isinstance(enriched, str):
        result["translations"]["en"]["summary"] = enriched

    # Merge additional Quran refs (dedup against existing)
    additional = data.get("additional_quran_refs", [])
    if additional and isinstance(additional, list):
        existing_refs = {r.get("ref") for r in result.get("related_quran", [])}
        for ref in additional:
            if (
                isinstance(ref, dict)
                and ref.get("ref")
                and ref["ref"] not in existing_refs
            ):
                result["related_quran"].append(ref)
                existing_refs.add(ref["ref"])

    # Attach cost metadata for pipeline accounting
    result["_phase3_cost"] = cr.get("cost", 0)
    result["_phase3_tokens"] = cr.get("output_tokens", 0)

    return result
