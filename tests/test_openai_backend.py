"""Tests for the OpenAI API backend module."""

import asyncio
import json
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestComputeCost:
    """Tests for cost computation from token counts."""

    def test_gpt_4_1_mini_cost(self):
        from app.pipeline_cli.openai_backend import compute_cost
        # 1000 input tokens, 2000 output tokens
        # gpt-4.1-mini: $0.40/MTok input, $1.60/MTok output
        cost = compute_cost("gpt-4.1-mini", 1000, 2000)
        expected = (1000 / 1_000_000) * 0.40 + (2000 / 1_000_000) * 1.60
        assert abs(cost - expected) < 0.000001

    def test_gpt_4_1_cost(self):
        from app.pipeline_cli.openai_backend import compute_cost
        cost = compute_cost("gpt-4.1", 10000, 5000)
        expected = (10000 / 1_000_000) * 2.00 + (5000 / 1_000_000) * 8.00
        assert abs(cost - expected) < 0.000001

    def test_gpt_4_1_nano_cost(self):
        from app.pipeline_cli.openai_backend import compute_cost
        cost = compute_cost("gpt-4.1-nano", 50000, 30000)
        expected = (50000 / 1_000_000) * 0.10 + (30000 / 1_000_000) * 0.40
        assert abs(cost - expected) < 0.000001

    def test_gpt_4o_cost(self):
        from app.pipeline_cli.openai_backend import compute_cost
        cost = compute_cost("gpt-4o", 10000, 5000)
        expected = (10000 / 1_000_000) * 2.50 + (5000 / 1_000_000) * 10.00
        assert abs(cost - expected) < 0.000001

    def test_gpt_4o_mini_cost(self):
        from app.pipeline_cli.openai_backend import compute_cost
        cost = compute_cost("gpt-4o-mini", 10000, 5000)
        expected = (10000 / 1_000_000) * 0.15 + (5000 / 1_000_000) * 0.60
        assert abs(cost - expected) < 0.000001

    def test_dated_model_prefix_match(self):
        from app.pipeline_cli.openai_backend import compute_cost
        # Model IDs sometimes have date suffixes
        cost = compute_cost("gpt-4.1-mini-2025-04-14", 1000, 1000)
        expected_cost = compute_cost("gpt-4.1-mini", 1000, 1000)
        assert cost == expected_cost

    def test_unknown_model_falls_back_to_gpt_4_1_mini(self):
        from app.pipeline_cli.openai_backend import compute_cost
        cost = compute_cost("some-unknown-model", 1000, 1000)
        expected = compute_cost("gpt-4.1-mini", 1000, 1000)
        assert cost == expected

    def test_zero_tokens(self):
        from app.pipeline_cli.openai_backend import compute_cost
        cost = compute_cost("gpt-4.1-mini", 0, 0)
        assert cost == 0.0

    def test_large_token_count(self):
        from app.pipeline_cli.openai_backend import compute_cost
        # 100K input, 32K output (max for gpt-4.1)
        cost = compute_cost("gpt-4.1-mini", 100000, 32000)
        expected = (100000 / 1_000_000) * 0.40 + (32000 / 1_000_000) * 1.60
        assert abs(cost - expected) < 0.000001
        # Sanity check: this should be a few cents
        assert 0.01 < cost < 1.0

    def test_corpus_cost_estimate(self):
        """Estimate cost for full 58K hadith corpus with gpt-4.1-mini."""
        from app.pipeline_cli.openai_backend import compute_cost
        # Avg hadith: ~15K input tokens (system prompt + verse), ~33K output tokens (v4)
        per_hadith = compute_cost("gpt-4.1-mini", 15000, 33000)
        total_58k = per_hadith * 58000
        # Should be in the hundreds of dollars range, not thousands
        assert total_58k < 5000, f"58K corpus would cost ${total_58k:.2f} — too expensive"
        assert total_58k > 100, f"58K corpus would cost ${total_58k:.2f} — suspiciously cheap"


class TestGetAvailableModels:
    """Tests for model listing."""

    def test_returns_list(self):
        from app.pipeline_cli.openai_backend import get_available_models
        models = get_available_models()
        assert isinstance(models, list)
        assert len(models) >= 5

    def test_model_has_pricing(self):
        from app.pipeline_cli.openai_backend import get_available_models
        models = get_available_models()
        for m in models:
            assert "id" in m
            assert "input_per_mtok" in m
            assert "output_per_mtok" in m
            assert m["input_per_mtok"] > 0
            assert m["output_per_mtok"] > 0

    def test_gpt_4_1_mini_in_list(self):
        from app.pipeline_cli.openai_backend import get_available_models
        models = get_available_models()
        ids = [m["id"] for m in models]
        assert "gpt-4.1-mini" in ids


class TestCallOpenAI:
    """Tests for call_openai with mocked API."""

    def test_missing_api_key(self):
        from app.pipeline_cli.openai_backend import call_openai
        import app.pipeline_cli.openai_backend as mod
        # Mock _get_client to raise ValueError (missing API key)
        with patch.object(mod, "_get_client", side_effect=ValueError("OPENAI_API_KEY environment variable not set.")):
            result = asyncio.run(call_openai("system", "user"))
            assert "error" in result
            assert "OPENAI_API_KEY" in result["error"]
            assert result["backend"] == "openai"

    def test_missing_openai_package(self):
        from app.pipeline_cli.openai_backend import call_openai
        import app.pipeline_cli.openai_backend as mod
        # Mock _get_client to raise ImportError
        with patch.object(mod, "_get_client", side_effect=ImportError("No module named 'openai'")):
            result = asyncio.run(call_openai("system", "user"))
            assert "error" in result
            assert "openai" in result["error"].lower()

    def test_successful_call(self):
        """Test successful OpenAI API call with mocked response."""
        from app.pipeline_cli.openai_backend import call_openai

        # Build mock response
        mock_usage = MagicMock()
        mock_usage.prompt_tokens = 5000
        mock_usage.completion_tokens = 10000
        mock_usage.prompt_tokens_details = None  # no cache hit info

        mock_choice = MagicMock()
        mock_choice.message.content = '{"content_type": "hadith"}'
        mock_choice.finish_reason = "stop"

        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        mock_response.usage = mock_usage
        mock_response.model = "gpt-4.1-mini"

        mock_client = AsyncMock()
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("app.pipeline_cli.openai_backend._get_client", return_value=mock_client):
            result = asyncio.run(call_openai("system prompt", "user message", model="gpt-4.1-mini"))

        assert "error" not in result
        assert result["result"] == '{"content_type": "hadith"}'
        assert result["output_tokens"] == 10000
        assert result["input_tokens"] == 5000
        assert result["cost"] > 0
        assert result["stop_reason"] == "stop"
        assert result["num_turns"] == 1
        assert result["backend"] == "openai"
        assert result["model"] == "gpt-4.1-mini"

    def test_cost_computation_in_response(self):
        """Verify cost is correctly computed from token counts in API response."""
        from app.pipeline_cli.openai_backend import call_openai, compute_cost

        mock_usage = MagicMock()
        mock_usage.prompt_tokens = 15000
        mock_usage.completion_tokens = 33000
        mock_usage.prompt_tokens_details = None

        mock_choice = MagicMock()
        mock_choice.message.content = '{"test": true}'
        mock_choice.finish_reason = "stop"

        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        mock_response.usage = mock_usage
        mock_response.model = "gpt-4.1-mini"

        mock_client = AsyncMock()
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("app.pipeline_cli.openai_backend._get_client", return_value=mock_client):
            result = asyncio.run(call_openai("sys", "usr", model="gpt-4.1-mini"))

        expected_cost = compute_cost("gpt-4.1-mini", 15000, 33000)
        assert result["cost"] == expected_cost

    def test_empty_response_content(self):
        """Test handling of empty/None content from API."""
        from app.pipeline_cli.openai_backend import call_openai

        mock_usage = MagicMock()
        mock_usage.prompt_tokens = 100
        mock_usage.completion_tokens = 0
        mock_usage.prompt_tokens_details = None

        mock_choice = MagicMock()
        mock_choice.message.content = None
        mock_choice.finish_reason = "stop"

        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        mock_response.usage = mock_usage
        mock_response.model = "gpt-4.1-mini"

        mock_client = AsyncMock()
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("app.pipeline_cli.openai_backend._get_client", return_value=mock_client):
            result = asyncio.run(call_openai("sys", "usr"))

        assert result["result"] == ""
        assert result["output_tokens"] == 0

    def test_extracts_cached_tokens_from_prompt_tokens_details(self):
        """Verify call_openai reads usage.prompt_tokens_details.cached_tokens
        and exposes it as both `cached_tokens` and `cache_read_tokens`."""
        from app.pipeline_cli.openai_backend import call_openai, compute_cost

        mock_details = MagicMock()
        mock_details.cached_tokens = 800

        mock_usage = MagicMock()
        mock_usage.prompt_tokens = 1000
        mock_usage.completion_tokens = 500
        mock_usage.prompt_tokens_details = mock_details

        mock_choice = MagicMock()
        mock_choice.message.content = '{"ok": true}'
        mock_choice.finish_reason = "stop"

        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        mock_response.usage = mock_usage
        mock_response.model = "gpt-5.4-mini"

        mock_client = AsyncMock()
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("app.pipeline_cli.openai_backend._get_client", return_value=mock_client):
            result = asyncio.run(call_openai("sys", "usr", model="gpt-5.4-mini"))

        assert result["cached_tokens"] == 800
        assert result["cache_read_tokens"] == 800  # alias for pipeline.py
        # Cost should reflect the cache discount, not full input rate
        expected = compute_cost("gpt-5.4-mini", 1000, 500, cached_tokens=800)
        assert result["cost"] == expected
        no_cache_cost = compute_cost("gpt-5.4-mini", 1000, 500, cached_tokens=0)
        assert result["cost"] < no_cache_cost


class TestCallLLMDispatcher:
    """Tests for the call_llm dispatcher in pipeline.py."""

    def test_dispatches_to_claude_by_default(self):
        from app.pipeline_cli.pipeline import call_llm

        with patch("app.pipeline_cli.pipeline.call_claude", new_callable=AsyncMock) as mock_claude:
            mock_claude.return_value = {"result": "test", "cost": 0.01}
            result = asyncio.run(call_llm("sys", "usr", model="sonnet", backend="claude"))
            mock_claude.assert_called_once()

    def test_dispatches_to_openai(self):
        from app.pipeline_cli.pipeline import call_llm

        with patch("app.pipeline_cli.openai_backend.call_openai", new_callable=AsyncMock) as mock_openai:
            mock_openai.return_value = {"result": "test", "cost": 0.001, "backend": "openai"}
            result = asyncio.run(call_llm("sys", "usr", model="gpt-4.1-mini", backend="openai"))
            mock_openai.assert_called_once()


class TestPipelineConfigDefaults:
    """Verify that default pipeline config uses claude backend."""

    def test_default_backend_is_claude(self):
        from app.pipeline_cli.pipeline import PipelineConfig
        config = PipelineConfig()
        assert config.backend == "claude"
        assert config.model == "sonnet"

    def test_openai_backend_config(self):
        from app.pipeline_cli.pipeline import PipelineConfig
        config = PipelineConfig(backend="openai", model="gpt-4.1-mini")
        assert config.backend == "openai"
        assert config.model == "gpt-4.1-mini"


class TestVersePlanBackend:
    """Verify VersePlan carries backend info."""

    def test_default_backend(self):
        from app.pipeline_cli.verse_processor import VersePlan
        from app.ai_pipeline import PipelineRequest
        plan = VersePlan(
            verse_path="/books/test:1",
            verse_id="test_1",
            mode="single",
            request=PipelineRequest(
                verse_path="/books/test:1",
                book_name="Test",
                chapter_title="Test Ch",
                arabic_text="بسم الله",
                english_text="In the name of God",
            ),
            system_prompt="sys",
            user_message="usr",
            work_dir="/tmp/test",
        )
        assert plan.backend == "claude"
        assert plan.model == ""

    def test_openai_backend(self):
        from app.pipeline_cli.verse_processor import VersePlan
        from app.ai_pipeline import PipelineRequest
        plan = VersePlan(
            verse_path="/books/test:1",
            verse_id="test_1",
            mode="single",
            request=PipelineRequest(
                verse_path="/books/test:1",
                book_name="Test",
                chapter_title="Test Ch",
                arabic_text="بسم الله",
                english_text="In the name of God",
            ),
            system_prompt="sys",
            user_message="usr",
            work_dir="/tmp/test",
            backend="openai",
            model="gpt-4.1-mini",
        )
        assert plan.backend == "openai"
        assert plan.model == "gpt-4.1-mini"


class TestOpenAIPricing:
    """Verify pricing table is complete and reasonable."""

    def test_all_models_have_positive_prices(self):
        from app.pipeline_cli.openai_backend import OPENAI_PRICING
        for model_id, (input_price, cached_price, output_price) in OPENAI_PRICING.items():
            assert input_price > 0, f"{model_id} has zero input price"
            assert cached_price > 0, f"{model_id} has zero cached_input price"
            assert output_price > 0, f"{model_id} has zero output price"
            # Cached must never exceed input (otherwise caching makes things more expensive)
            assert cached_price <= input_price, (
                f"{model_id}: cached ({cached_price}) > input ({input_price})"
            )
            # Output should be >= input for all models
            assert output_price >= input_price, f"{model_id}: output ({output_price}) < input ({input_price})"

    def test_nano_cheapest(self):
        from app.pipeline_cli.openai_backend import OPENAI_PRICING
        nano_input = OPENAI_PRICING["gpt-5-nano"][0]
        for model_id, (input_price, _, _) in OPENAI_PRICING.items():
            assert input_price >= nano_input, f"{model_id} cheaper than gpt-5-nano"

    def test_relative_pricing(self):
        """gpt-4.1-mini should be cheaper than gpt-4.1."""
        from app.pipeline_cli.openai_backend import OPENAI_PRICING
        mini_input = OPENAI_PRICING["gpt-4.1-mini"][0]
        full_input = OPENAI_PRICING["gpt-4.1"][0]
        assert mini_input < full_input


class TestComputeCostCachedTokens:
    """Cost arithmetic with prompt_tokens_details.cached_tokens."""

    def test_cached_zero_matches_legacy_formula(self):
        """cached_tokens=0 (the default) reproduces the old (input,output) cost."""
        from app.pipeline_cli.openai_backend import compute_cost, OPENAI_PRICING
        i, _, o = OPENAI_PRICING["gpt-5.4-mini"]
        expected = (1000 * i + 2000 * o) / 1_000_000
        cost = compute_cost("gpt-5.4-mini", 1000, 2000, cached_tokens=0)
        assert abs(cost - expected) < 1e-9

    def test_cached_subtracts_from_input_no_double_charge(self):
        """The cached portion is billed at the cached rate, NOT both rates.

        This is the LiteLLM #6215 / #19680 bug we explicitly avoid.
        """
        from app.pipeline_cli.openai_backend import compute_cost, OPENAI_PRICING
        i, c, o = OPENAI_PRICING["gpt-5.4-mini"]
        # 1000 prompt = 800 cached + 200 fresh; 500 output
        cost = compute_cost("gpt-5.4-mini", 1000, 500, cached_tokens=800)
        expected = (200 * i + 800 * c + 500 * o) / 1_000_000
        assert abs(cost - expected) < 1e-9
        # And cost must be lower than billing all 1000 at full input rate
        no_cache = compute_cost("gpt-5.4-mini", 1000, 500, cached_tokens=0)
        assert cost < no_cache

    def test_cached_equals_input_full_cache_hit(self):
        """100% cache hit: pay only cached + output rates."""
        from app.pipeline_cli.openai_backend import compute_cost, OPENAI_PRICING
        i, c, o = OPENAI_PRICING["gpt-5.4"]
        cost = compute_cost("gpt-5.4", 5000, 1000, cached_tokens=5000)
        expected = (5000 * c + 1000 * o) / 1_000_000
        assert abs(cost - expected) < 1e-9

    def test_cached_exceeds_input_clamps(self):
        """Reporting anomaly (cached > prompt): clamp to prompt, no negative cost."""
        from app.pipeline_cli.openai_backend import compute_cost, OPENAI_PRICING
        _, c, o = OPENAI_PRICING["gpt-5.4-mini"]
        cost = compute_cost("gpt-5.4-mini", 1000, 500, cached_tokens=99999)
        # Should bill as if cached=1000, no negative non-cached portion
        expected = (1000 * c + 500 * o) / 1_000_000
        assert abs(cost - expected) < 1e-9

    def test_pro_model_no_cache_discount(self):
        """Pro models have cached_input == input — caching gives no benefit."""
        from app.pipeline_cli.openai_backend import compute_cost
        # gpt-5.4-pro is configured with cached==input
        with_cache = compute_cost("gpt-5.4-pro", 1000, 500, cached_tokens=800)
        without_cache = compute_cost("gpt-5.4-pro", 1000, 500, cached_tokens=0)
        assert abs(with_cache - without_cache) < 1e-9

    def test_savings_match_published_discount(self):
        """gpt-5.4-mini cached discount is documented as 90% off ($0.075/$0.75)."""
        from app.pipeline_cli.openai_backend import compute_cost, OPENAI_PRICING
        i, c, _ = OPENAI_PRICING["gpt-5.4-mini"]
        # Sanity: ratio should be 10% (cached is 1/10 of input)
        assert abs(c / i - 0.10) < 0.001
        # Full cache hit on input-only (output=0): cost should be 10% of uncached
        full_input = compute_cost("gpt-5.4-mini", 1_000_000, 0, cached_tokens=0)
        full_cached = compute_cost("gpt-5.4-mini", 1_000_000, 0, cached_tokens=1_000_000)
        assert abs(full_cached / full_input - 0.10) < 0.001


class TestValidationErrorToField:
    """Verify _validation_error_to_field maps error messages to correct result keys.

    This is critical for the fix pass: build_fix_prompt() uses the field name to
    look up current values in the result dict. If the field is "validation" (the
    old default), flagged_fields stays empty and the fix model has no context.
    """

    def _field(self, msg):
        from app.pipeline_cli.verse_processor import _validation_error_to_field
        return _validation_error_to_field(msg)

    def test_word_tags_diacritics(self):
        """Diacritics error on word_tags should map to 'word_tags'."""
        msg = "word_tags[5] word 'بسم' has no diacritics (must be fully diacritized)"
        assert self._field(msg) == "word_tags"

    def test_word_analysis_diacritics(self):
        """Diacritics error on word_analysis (v3) should map to 'word_analysis'."""
        msg = "word_analysis[3] word 'بسم' has no diacritics (must be fully diacritized)"
        assert self._field(msg) == "word_analysis"

    def test_invalid_topic(self):
        assert self._field("invalid topic: quran_commentary") == "topics"

    def test_invalid_tag(self):
        assert self._field("invalid tag: bad_tag") == "tags"

    def test_invalid_content_type(self):
        assert self._field("invalid content_type: foo") == "content_type"

    def test_missing_ambiguity_note(self):
        assert self._field("missing ambiguity_note for narrator X") == "isnad_matn"

    def test_invalid_narrator_role(self):
        assert self._field("invalid narrator role: scribe") == "isnad_matn"

    def test_invalid_identity_confidence(self):
        assert self._field("invalid identity_confidence: uncertain") == "isnad_matn"

    def test_invalid_chunk_type(self):
        assert self._field("invalid chunk_type: header") == "chunks"

    def test_invalid_diacritics_status(self):
        assert self._field("invalid diacritics_status: partial") == "diacritics_status"

    def test_invalid_quran_relationship(self):
        assert self._field("invalid quran relationship: implicit") == "related_quran"

    def test_key_terms_key(self):
        assert self._field("key_terms key 'en' is not an Arabic term") == "translations"

    def test_invalid_pos_generic(self):
        """Generic 'invalid pos:' without explicit field name defaults to word_tags."""
        assert self._field("invalid pos: VERB for word قَالَ") == "word_tags"

    def test_unknown_error_fallback(self):
        """Unknown error messages should fall back to 'validation'."""
        assert self._field("some unknown validation error") == "validation"
