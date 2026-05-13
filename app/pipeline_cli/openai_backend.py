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
    # Qwen models served from the DGX Spark via vLLM — zero $ (electricity is
    # the user's, not OpenAI's). See PHASE4_OPENWEIGHT_BENCHMARK.md for
    # quality/throughput data and the integration approach.
    "qwen36-fast":      (0.0, 0.0, 0.0),
    "qwen36-deep":      (0.0, 0.0, 0.0),
    "qwen36-27b":       (0.0, 0.0, 0.0),
    "qwen36-35b-heretic": (0.0, 0.0, 0.0),
}


# Models served from Spark (OpenAI-compatible base_url, no API key required)
SPARK_MODEL_PREFIX = "qwen36"


def is_spark_model(model: str) -> bool:
    """Return True if the given model is served via the Spark vLLM endpoint."""
    return model.startswith(SPARK_MODEL_PREFIX)


def archive_raw_response(
    raw_archive_dir: Optional[str],
    verse_id: Optional[str],
    suffix: str,
    raw_text: str,
) -> None:
    """Persist a raw API response to disk so it can be salvaged offline.

    Used on parse failure paths to preserve the (paid-for) LLM output that
    would otherwise be lost when the parser raises. Silently no-ops if either
    `raw_archive_dir` or `verse_id` is None — call sites can pass through
    optional config without conditional wrapping.

    Filename: {raw_archive_dir}/{verse_id}.{suffix}.raw.txt
    """
    if not raw_archive_dir or not verse_id:
        return
    if not raw_text:
        return
    try:
        os.makedirs(raw_archive_dir, exist_ok=True)
        path = os.path.join(raw_archive_dir, f"{verse_id}.{suffix}.raw.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(raw_text)
        logger.info("Archived raw response: %s", path)
    except OSError as e:
        # Never let archiving failure cascade — the caller is already on an
        # error path and we don't want to mask the original failure.
        logger.warning("Failed to archive raw response for %s.%s: %s",
                       verse_id, suffix, e)


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


def _get_client(base_url: Optional[str] = None, timeout: float = 600.0):
    """Lazy-import and create OpenAI client.

    Args:
        base_url: Custom base URL (for vLLM-compatible endpoints e.g. Spark).
                  When set, OPENAI_API_KEY is not required.
        timeout: Per-request timeout in seconds.
    """
    try:
        from openai import AsyncOpenAI
    except ImportError:
        raise ImportError(
            "openai package not installed. Install with: pip install openai\n"
            "Or add to pyproject.toml [project.optional-dependencies] openai group."
        )

    if base_url:
        # Custom endpoint (Spark vLLM). API key is unused but the SDK still
        # requires a non-empty value.
        return AsyncOpenAI(
            api_key=os.environ.get("OPENAI_API_KEY", "not-needed"),
            base_url=base_url,
            max_retries=3,
            timeout=timeout,
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
        # 10-min per-request timeout. Was 3600s but that meant a dead TCP
        # connection (e.g. machine slept then woke) sat for an hour before
        # the SDK gave up — losing 1-3 hours per stuck call across retries.
        # 600s still leaves ample headroom for legitimate long-reasoning
        # calls (typical Phase 1 finishes 10-30s, worst-case under 5 min).
        timeout=timeout,
    )


async def call_openai(
    system_prompt: str,
    user_message: str,
    model: str = "gpt-4.1-mini",
    max_retries: int = 0,  # SDK already retries 3x; no need to retry again
    temperature: float = 0.0,
    max_output_tokens: int = 40000,
    json_mode: bool = False,
    base_url: Optional[str] = None,
    response_format: Optional[dict] = None,
    extra_body: Optional[dict] = None,
    timeout: Optional[float] = None,
) -> dict:
    """Call an OpenAI-compatible chat completion API.

    Returns dict matching call_claude() format. Backend identifier is "openai"
    for the standard path or "spark" when `base_url` points at a vLLM endpoint.

    Args:
        base_url: Custom OpenAI-compatible endpoint (e.g. Spark vLLM at
                  http://192.168.0.66:8000/v1). When set, OPENAI_API_KEY is
                  optional. Also auto-detected from qwen36-* model names.
        response_format: dict for OpenAI's structured-output mode. For Spark
                  Qwen with strict schema enforcement use:
                  {"type": "json_schema",
                   "json_schema": {"name": ..., "schema": {...}, "strict": True}}
        extra_body: Passed through to the SDK. Required for Qwen on vLLM:
                  {"chat_template_kwargs": {"enable_thinking": False}}
        timeout: Per-request timeout. For long Spark calls bump to 1800s.

    Returns:
        {
            "result": str,          # Model response text
            "cost": float,          # Computed cost in USD ($0 for Spark)
            "output_tokens": int,
            "input_tokens": int,
            "elapsed": float,
            "model": str,
            "stop_reason": str,
            "num_turns": 1,
            "backend": "openai" | "spark",
        }
    """
    # Auto-detect Spark from model name if base_url not given.
    if base_url is None and is_spark_model(model):
        base_url = os.environ.get("SPARK_BASE_URL", "http://192.168.0.66:8000/v1")

    is_spark = base_url is not None
    backend_id = "spark" if is_spark else "openai"
    effective_timeout = timeout if timeout is not None else (1800.0 if is_spark else 600.0)

    try:
        client = _get_client(base_url=base_url, timeout=effective_timeout)
    except (ImportError, ValueError) as e:
        return {"error": str(e), "elapsed": 0.0, "backend": backend_id}

    # GPT-5 family and o-series API differences (Spark/Qwen uses neither path
    # — Qwen on vLLM uses system role + temperature + max_tokens like gpt-4.1):
    #
    # 1. max_completion_tokens vs max_tokens:
    #    ALL gpt-5*, o3, o4 models require max_completion_tokens.
    #    gpt-4.1* / gpt-4o* / qwen36* use max_tokens.
    #
    # 2. Reasoning models (developer role, no temperature):
    #    gpt-5, gpt-5.1, gpt-5.2, gpt-5.3-codex, gpt-5.4, gpt-5.4-pro, o3, o4
    #    These use 'developer' role and don't support temperature.
    #
    # 3. Standard gpt-5 models (system role, temperature OK):
    #    gpt-5-mini, gpt-5-nano, gpt-5.4-mini, gpt-5.4-nano
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

    # response_format precedence: explicit param > json_mode flag
    if response_format is not None:
        kwargs["response_format"] = response_format
    elif json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    # extra_body for Spark/Qwen thinking-disable flag (and any other vLLM
    # passthroughs). Auto-inject the thinking-disable when calling Spark
    # without an explicit extra_body — that's the safe default for structured
    # output (PHASE4_OPENWEIGHT_BENCHMARK.md).
    if extra_body is not None:
        kwargs["extra_body"] = extra_body
    elif is_spark:
        kwargs["extra_body"] = {"chat_template_kwargs": {"enable_thinking": False}}

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
                "backend": backend_id,
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
                "backend": backend_id,
            }
            if timeout_cost > 0:
                result["timeout_cost_estimate"] = timeout_cost
            return result

    return {"error": "max retries exceeded", "elapsed": 0.0, "backend": backend_id}


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
