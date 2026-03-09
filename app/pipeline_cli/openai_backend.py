"""OpenAI API backend for the pipeline.

Provides call_openai() as a drop-in alternative to call_claude().
Returns the same dict format: {result, cost, output_tokens, elapsed, ...}

Requires:
    pip install openai
    OPENAI_API_KEY environment variable

Usage:
    from app.pipeline_cli.openai_backend import call_openai
    cr = await call_openai(system_prompt, user_message, model="gpt-4.1-mini")
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Batch API pricing is 50% of standard
BATCH_DISCOUNT = 0.5

# Pricing per 1M tokens (input, output) — updated 2026-03
# Source: https://openai.com/api/pricing/
OPENAI_PRICING = {
    "gpt-4.1": (2.00, 8.00),
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-4.1-nano": (0.10, 0.40),
    "gpt-4o": (2.50, 10.00),
    "gpt-4o-mini": (0.15, 0.60),
}


def compute_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Compute cost in USD from token counts and known pricing."""
    pricing = OPENAI_PRICING.get(model)
    if not pricing:
        # Unknown model — try prefix match (e.g. gpt-4.1-mini-2025-04-14)
        # Sort by longest key first to match most specific prefix
        for key, val in sorted(OPENAI_PRICING.items(), key=lambda x: -len(x[0])):
            if model.startswith(key):
                pricing = val
                break
    if not pricing:
        logger.warning("Unknown OpenAI model %r — cannot compute cost, using gpt-4.1-mini pricing", model)
        pricing = OPENAI_PRICING["gpt-4.1-mini"]

    input_cost = (input_tokens / 1_000_000) * pricing[0]
    output_cost = (output_tokens / 1_000_000) * pricing[1]
    return round(input_cost + output_cost, 6)


def _get_client():
    """Lazy-import and create OpenAI client."""
    try:
        from openai import AsyncOpenAI
    except ImportError:
        raise ImportError(
            "openai package not installed. Install with: pip install openai\n"
            "Or add to pyproject.toml [project.optional-dependencies] openai group."
        )

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY environment variable not set. "
            "Get your API key from https://platform.openai.com/api-keys"
        )

    return AsyncOpenAI(
        api_key=api_key,
        max_retries=3,
        timeout=300.0,  # 5 minute timeout per request
    )


async def call_openai(
    system_prompt: str,
    user_message: str,
    model: str = "gpt-4.1-mini",
    max_retries: int = 2,
    temperature: float = 0.0,
    max_output_tokens: int = 32768,
    json_mode: bool = False,
) -> dict:
    """Call OpenAI chat completion API. Returns dict matching call_claude() format.

    Returns:
        {
            "result": str,          # Model response text
            "cost": float,          # Computed cost in USD
            "output_tokens": int,   # Output token count
            "input_tokens": int,    # Input token count (OpenAI-specific bonus)
            "elapsed": float,       # Wall-clock seconds
            "model": str,           # Actual model used (may include date suffix)
            "stop_reason": str,     # "stop", "length", etc.
            "num_turns": 1,         # Always 1 (no multi-turn)
            "backend": "openai",    # Backend identifier
        }
    """
    try:
        client = _get_client()
    except (ImportError, ValueError) as e:
        return {"error": str(e), "elapsed": 0.0, "backend": "openai"}

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message},
    ]

    kwargs = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_output_tokens,
    }

    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    for attempt in range(max_retries + 1):
        start = time.time()
        try:
            response = await client.chat.completions.create(**kwargs)
            elapsed = round(time.time() - start, 2)

            choice = response.choices[0]
            result_text = choice.message.content or ""

            # Extract usage
            usage = response.usage
            input_tokens = usage.prompt_tokens if usage else 0
            output_tokens = usage.completion_tokens if usage else 0

            # Compute cost
            actual_model = response.model or model
            cost = compute_cost(actual_model, input_tokens, output_tokens)

            # Map finish_reason to our format
            stop_reason = choice.finish_reason  # "stop", "length", "content_filter"

            return {
                "result": result_text,
                "cost": cost,
                "output_tokens": output_tokens,
                "input_tokens": input_tokens,
                "elapsed": elapsed,
                "model": actual_model,
                "stop_reason": stop_reason,
                "num_turns": 1,
                "backend": "openai",
            }

        except Exception as e:
            elapsed = round(time.time() - start, 2)
            error_type = type(e).__name__
            error_msg = str(e)[:500]

            # Check if retryable
            retryable = False
            try:
                from openai import RateLimitError, APITimeoutError, APIConnectionError, InternalServerError
                if isinstance(e, (RateLimitError, APITimeoutError, APIConnectionError, InternalServerError)):
                    retryable = True
            except ImportError:
                pass

            if retryable and attempt < max_retries:
                wait = 5 * (2 ** attempt)
                logger.warning(
                    "OpenAI call failed (attempt %d/%d, %s): %s. Retrying in %ds...",
                    attempt + 1, max_retries + 1, error_type, error_msg[:80], wait,
                )
                await asyncio.sleep(wait)
                continue

            return {
                "error": f"{error_type}: {error_msg}",
                "elapsed": elapsed,
                "backend": "openai",
            }

    return {"error": "max retries exceeded", "elapsed": 0.0, "backend": "openai"}


def get_available_models() -> list:
    """Return list of supported OpenAI models with pricing info."""
    return [
        {
            "id": model_id,
            "input_per_mtok": pricing[0],
            "output_per_mtok": pricing[1],
        }
        for model_id, pricing in sorted(OPENAI_PRICING.items())
    ]
