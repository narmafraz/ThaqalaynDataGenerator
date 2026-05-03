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

# Per-model max output token cap. The default `max_output_tokens=40000` we
# pass exceeds the gpt-4.1 family's 32,768 ceiling on `max_tokens`, causing a
# 400 BadRequestError ("max_tokens is too large"). gpt-5 family uses
# `max_completion_tokens` and accepts higher values, so we never hit it there.
# Lookup falls back to 40000 (no clamp) for unknown models.
MODEL_MAX_OUTPUT_TOKENS = {
    "gpt-4.1": 32768,
    "gpt-4.1-mini": 32768,
    "gpt-4.1-nano": 32768,
    "gpt-4o": 16384,
    "gpt-4o-mini": 16384,
    # gpt-5 family + o-series accept up to 128K via max_completion_tokens
    # (no clamp needed for our 40K default).
}


def _cap_output_tokens(model: str, requested: int) -> int:
    """Clamp `requested` to the smallest known cap for this model.

    Tries exact key, then longest-prefix match (for dated suffixes like
    'gpt-4.1-mini-2025-04-14'). No clamp if model is unknown.
    """
    cap = MODEL_MAX_OUTPUT_TOKENS.get(model)
    if cap is None:
        for key in sorted(MODEL_MAX_OUTPUT_TOKENS.keys(), key=len, reverse=True):
            if model.startswith(key):
                cap = MODEL_MAX_OUTPUT_TOKENS[key]
                break
    return min(requested, cap) if cap else requested


# Pricing per 1M tokens (input, cached_input, output) — verified 2026-05-03
# Source: https://openai.com/api/pricing/
#
# cached_input is the discounted rate for tokens served from OpenAI's automatic
# prompt cache (prompt_tokens_details.cached_tokens in the response). For "pro"
# models OpenAI does not list a cached rate — we set cached_input = input so
# they fall back to full price (no benefit, no breakage).
OPENAI_PRICING = {
    # (input,  cached_input, output)
    "gpt-4.1":      (2.00,  0.50,   8.00),
    "gpt-4.1-mini": (0.40,  0.10,   1.60),
    "gpt-4.1-nano": (0.10,  0.025,  0.40),
    "gpt-4o":       (2.50,  1.25,  10.00),
    "gpt-4o-mini":  (0.15,  0.075,  0.60),
    "gpt-5":        (1.25,  0.125, 10.00),
    "gpt-5-mini":   (0.25,  0.025,  2.00),
    "gpt-5-nano":   (0.05,  0.005,  0.40),
    "gpt-5-pro":    (15.00, 15.00, 120.00),    # no cached rate listed
    "gpt-5.1":      (1.25,  0.125, 10.00),
    "gpt-5.2":      (1.75,  0.175, 14.00),
    "gpt-5.2-pro":  (21.00, 21.00, 168.00),    # no cached rate listed
    # gpt-5.3 / gpt-5.3-codex are not on openai.com/api/pricing as of 2026-05-03
    # — likely deprecated or codex-channel-only. Pricing kept here for legacy
    # callers; verify against current API before relying on these values.
    "gpt-5.3":       (1.75, 0.175, 14.00),
    "gpt-5.3-codex": (1.75, 0.175, 14.00),
    "gpt-5.4":      (2.50,  0.25,  15.00),
    "gpt-5.4-mini": (0.75,  0.075,  4.50),
    "gpt-5.4-nano": (0.20,  0.02,   1.25),
    "gpt-5.4-pro":  (30.00, 30.00, 180.00),    # no cached rate listed
    # Released 2026-04-24
    "gpt-5.5":      (5.00,  0.50,  30.00),
    "gpt-5.5-pro":  (30.00, 30.00, 180.00),    # no cached rate listed
}


def compute_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int = 0,
) -> float:
    """Compute cost in USD from token counts and known pricing.

    `input_tokens` is the API's `prompt_tokens` field — which in the OpenAI
    response is the SUPERSET of cached + non-cached. `cached_tokens` (from
    `prompt_tokens_details.cached_tokens`) is billed at the discounted rate;
    the remainder pays the full input rate.

    See https://github.com/BerriAI/litellm/issues/6215 for the recurring
    "double-charge cached tokens" bug we are explicitly avoiding here.
    """
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

    input_rate, cached_rate, output_rate = pricing

    # Clamp cached_tokens to [0, input_tokens] to handle reporting anomalies.
    # (See LiteLLM #14874 — providers occasionally return cached > prompt.)
    cached = max(0, min(cached_tokens, input_tokens))
    non_cached = input_tokens - cached

    input_cost = (non_cached / 1_000_000) * input_rate
    cached_cost = (cached / 1_000_000) * cached_rate
    output_cost = (output_tokens / 1_000_000) * output_rate
    return round(input_cost + cached_cost + output_cost, 6)


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
        timeout=3600.0,  # 1 hour timeout (long chunked verses with reasoning models)
    )


async def call_openai(
    system_prompt: str,
    user_message: str,
    model: str = "gpt-4.1-mini",
    max_retries: int = 0,  # SDK already retries 3x; no need to retry again
    temperature: float = 0.0,
    max_output_tokens: int = 40000,
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

    # GPT-5 family and o-series API differences:
    #
    # 1. max_completion_tokens vs max_tokens:
    #    ALL gpt-5*, o3, o4 models require max_completion_tokens.
    #    gpt-4.1* and gpt-4o* use max_tokens.
    #
    # 2. Reasoning models (developer role, no temperature):
    #    gpt-5, gpt-5.1, gpt-5.2, gpt-5.3-codex, gpt-5.4, gpt-5.4-pro, o3, o4
    #    These use 'developer' role and don't support temperature.
    #
    # 3. Standard gpt-5 models (system role, temperature OK):
    #    gpt-5-mini, gpt-5-nano, gpt-5.4-mini, gpt-5.4-nano
    #    These use the new token param but still support system role + temperature.
    uses_new_token_param = model.startswith(("gpt-5", "o3", "o4"))

    _STANDARD_GPT5 = ("gpt-5-mini", "gpt-5-nano", "gpt-5.4-mini", "gpt-5.4-nano")
    is_reasoning = uses_new_token_param and not any(
        model.startswith(prefix) for prefix in _STANDARD_GPT5
    )

    if is_reasoning:
        messages = [
            {"role": "developer", "content": system_prompt},
            {"role": "user", "content": user_message},
        ]
    else:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ]

    kwargs = {
        "model": model,
        "messages": messages,
    }

    capped_output_tokens = _cap_output_tokens(model, max_output_tokens)
    if uses_new_token_param:
        kwargs["max_completion_tokens"] = capped_output_tokens
    else:
        kwargs["max_tokens"] = capped_output_tokens

    if not is_reasoning:
        kwargs["temperature"] = temperature

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
            # Defensively read cached_tokens — older models, mocked responses,
            # or providers in OpenAI-compatible mode may not populate this
            # nested field (see LiteLLM #1896 / cline issue).
            cached_tokens = 0
            if usage is not None:
                details = getattr(usage, "prompt_tokens_details", None)
                if details is not None:
                    cached_tokens = getattr(details, "cached_tokens", 0) or 0

            # Compute cost (subtracts cached from input internally)
            actual_model = response.model or model
            cost = compute_cost(actual_model, input_tokens, output_tokens, cached_tokens)

            # Map finish_reason to our format
            stop_reason = choice.finish_reason  # "stop", "length", "content_filter"

            return {
                "result": result_text,
                "cost": cost,
                "output_tokens": output_tokens,
                "input_tokens": input_tokens,
                "cached_tokens": cached_tokens,
                # Alias matching the Anthropic field name pipeline.py already
                # aggregates (cache_read_input_tokens) so the existing stats
                # plumbing picks up OpenAI cache hits with no further changes.
                "cache_read_tokens": cached_tokens,
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
            is_timeout = False
            try:
                from openai import RateLimitError, APITimeoutError, APIConnectionError, InternalServerError
                if isinstance(e, (RateLimitError, APITimeoutError, APIConnectionError, InternalServerError)):
                    retryable = True
                is_timeout = isinstance(e, APITimeoutError)
            except ImportError:
                pass

            # Estimate cost for timeouts — model may have processed tokens we'll be charged for
            timeout_cost = 0.0
            if is_timeout and elapsed > 30:
                # Estimate input tokens (~4 chars per token) and assume partial output
                est_input_tokens = (len(system_prompt) + len(user_message)) // 4
                timeout_cost = compute_cost(model, est_input_tokens, 0)
                logger.warning(
                    "TIMEOUT after %.0fs (attempt %d/%d) — estimated input cost: $%.4f (output cost unknown, may be charged by OpenAI)",
                    elapsed, attempt + 1, max_retries + 1, timeout_cost,
                )

            if retryable and attempt < max_retries:
                wait = 5 * (2 ** attempt)
                logger.warning(
                    "OpenAI call failed (attempt %d/%d, %s): %s. Retrying in %ds...",
                    attempt + 1, max_retries + 1, error_type, error_msg[:80], wait,
                )
                await asyncio.sleep(wait)
                continue

            result = {
                "error": f"{error_type}: {error_msg}",
                "elapsed": elapsed,
                "backend": "openai",
            }
            if timeout_cost > 0:
                result["timeout_cost_estimate"] = timeout_cost
            return result

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
