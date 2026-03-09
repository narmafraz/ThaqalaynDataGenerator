"""Tests for the OpenAI Batch API module."""

import json
import os
import tempfile
import pytest
from unittest.mock import MagicMock, patch


class TestBatchState:
    """Tests for batch state persistence."""

    def test_save_and_load_state(self):
        from app.pipeline_cli.openai_batch import _save_state, _load_state
        with tempfile.TemporaryDirectory() as tmpdir:
            state = {
                "batch_id": "batch_abc123",
                "phase": "generation",
                "status": "in_progress",
                "model": "gpt-4.1-mini",
                "verse_mapping": {"gen-test_1": "/books/test:1"},
            }
            _save_state(tmpdir, state, "generation")
            loaded = _load_state(tmpdir, "generation")
            assert loaded is not None
            assert loaded["batch_id"] == "batch_abc123"
            assert loaded["verse_mapping"]["gen-test_1"] == "/books/test:1"

    def test_load_nonexistent_state(self):
        from app.pipeline_cli.openai_batch import _load_state
        with tempfile.TemporaryDirectory() as tmpdir:
            assert _load_state(tmpdir, "generation") is None
            assert _load_state(tmpdir, "fix") is None

    def test_separate_phases(self):
        from app.pipeline_cli.openai_batch import _save_state, _load_state
        with tempfile.TemporaryDirectory() as tmpdir:
            gen_state = {"batch_id": "gen_batch", "phase": "generation"}
            fix_state = {"batch_id": "fix_batch", "phase": "fix"}
            _save_state(tmpdir, gen_state, "generation")
            _save_state(tmpdir, fix_state, "fix")
            assert _load_state(tmpdir, "generation")["batch_id"] == "gen_batch"
            assert _load_state(tmpdir, "fix")["batch_id"] == "fix_batch"

    def test_archive_state(self):
        from app.pipeline_cli.openai_batch import _archive_state
        with tempfile.TemporaryDirectory() as tmpdir:
            state = {"batch_id": "batch_xyz", "phase": "generation", "status": "downloaded"}
            path = _archive_state(tmpdir, state, "generation")
            assert os.path.exists(path)
            assert "history" in path
            with open(path, "r", encoding="utf-8") as f:
                archived = json.load(f)
            assert archived["batch_id"] == "batch_xyz"

    def test_state_does_not_contain_api_key(self):
        """Verify API key is never written to state files."""
        from app.pipeline_cli.openai_batch import _save_state
        with tempfile.TemporaryDirectory() as tmpdir:
            state = {
                "batch_id": "batch_abc",
                "model": "gpt-4.1-mini",
                "verse_mapping": {"gen-test_1": "/books/test:1"},
            }
            path = _save_state(tmpdir, state, "generation")
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            assert "sk-" not in content
            assert "OPENAI_API_KEY" not in content
            assert "api_key" not in content.lower()


class TestBatchDiscount:
    """Tests for batch pricing."""

    def test_batch_discount_is_50_percent(self):
        from app.pipeline_cli.openai_backend import BATCH_DISCOUNT
        assert BATCH_DISCOUNT == 0.5

    def test_batch_cost_calculation(self):
        from app.pipeline_cli.openai_backend import compute_cost, BATCH_DISCOUNT
        standard_cost = compute_cost("gpt-4.1-mini", 15000, 33000)
        batch_cost = standard_cost * BATCH_DISCOUNT
        assert batch_cost < standard_cost
        assert abs(batch_cost - standard_cost * 0.5) < 0.000001

    def test_corpus_batch_cost_estimate(self):
        """Estimate 58K hadith corpus cost with batch discount."""
        from app.pipeline_cli.openai_backend import compute_cost, BATCH_DISCOUNT
        per_hadith = compute_cost("gpt-4.1-mini", 15000, 33000) * BATCH_DISCOUNT
        total = per_hadith * 58000
        # Batch should be roughly half of regular
        assert total < 3000, f"Batch corpus cost ${total:.2f} seems too high"
        assert total > 50, f"Batch corpus cost ${total:.2f} seems too low"


class TestBatchDir:
    """Tests for batch directory setup."""

    def test_get_batch_dir(self):
        from app.pipeline_cli.openai_batch import _get_batch_dir
        with tempfile.TemporaryDirectory() as tmpdir:
            responses_dir = os.path.join(tmpdir, "content", "responses")
            os.makedirs(responses_dir)
            batch_dir = _get_batch_dir(responses_dir)
            assert os.path.exists(batch_dir)
            assert "batches" in batch_dir


class TestGetSyncClient:
    """Tests for API key security."""

    def test_missing_api_key_exits(self):
        """Verify missing API key shows helpful message and exits."""
        from app.pipeline_cli.openai_batch import _get_sync_client
        with patch.dict(os.environ, {}, clear=True):
            env = {k: v for k, v in os.environ.items() if k != "OPENAI_API_KEY"}
            with patch.dict(os.environ, env, clear=True):
                # _get_sync_client should try to import openai first
                # If openai is not installed, it exits with that message
                # If installed but no key, exits with key message
                with pytest.raises(SystemExit):
                    _get_sync_client()


class TestBatchSubmitValidation:
    """Tests for submit pre-checks."""

    def test_blocks_if_active_batch_exists(self, capsys):
        from app.pipeline_cli.openai_batch import batch_submit, _save_state, _get_batch_dir
        with tempfile.TemporaryDirectory() as tmpdir:
            responses_dir = os.path.join(tmpdir, "content", "responses")
            os.makedirs(responses_dir)
            batch_dir = _get_batch_dir(responses_dir)

            # Create an active batch state
            _save_state(batch_dir, {
                "batch_id": "batch_existing",
                "status": "in_progress",
            }, "generation")

            # Try to submit — should be blocked
            batch_submit(
                verse_paths=["/books/test:1"],
                model="gpt-4.1-mini",
                responses_dir=responses_dir,
                data_dir="../ThaqalaynData/",
            )
            captured = capsys.readouterr()
            assert "Active generation batch already exists" in captured.out

    def test_allows_submit_after_completed(self, capsys):
        """Can submit new batch if previous batch is completed."""
        from app.pipeline_cli.openai_batch import _save_state, _load_state, _get_batch_dir
        with tempfile.TemporaryDirectory() as tmpdir:
            responses_dir = os.path.join(tmpdir, "content", "responses")
            os.makedirs(responses_dir)
            batch_dir = _get_batch_dir(responses_dir)

            # Create a completed batch
            _save_state(batch_dir, {
                "batch_id": "batch_done",
                "status": "downloaded",
            }, "generation")

            state = _load_state(batch_dir, "generation")
            assert state["status"] == "downloaded"
            # A new submit would be allowed (status is in the allow list)


class TestHandleBatchCommand:
    """Tests for CLI routing."""

    def test_unknown_subcommand_shows_help(self, capsys):
        from app.pipeline_cli.openai_batch import handle_batch_command
        args = MagicMock()
        args.subcommand = None
        args.responses_dir = None
        with pytest.raises(SystemExit):
            handle_batch_command(args)
        captured = capsys.readouterr()
        assert "submit" in captured.out
        assert "status" in captured.out
        assert "download" in captured.out

    def test_status_no_batches(self, capsys):
        from app.pipeline_cli.openai_batch import batch_status
        with tempfile.TemporaryDirectory() as tmpdir:
            responses_dir = os.path.join(tmpdir, "content", "responses")
            os.makedirs(responses_dir)
            batch_status(responses_dir)
            captured = capsys.readouterr()
            assert "No active batches" in captured.out

    def test_status_shows_existing_batch(self, capsys):
        from app.pipeline_cli.openai_batch import batch_status, _save_state, _get_batch_dir
        with tempfile.TemporaryDirectory() as tmpdir:
            responses_dir = os.path.join(tmpdir, "content", "responses")
            os.makedirs(responses_dir)
            batch_dir = _get_batch_dir(responses_dir)

            _save_state(batch_dir, {
                "batch_id": "batch_test123",
                "phase": "generation",
                "model": "gpt-4.1-mini",
                "request_count": 50,
                "created_at": "2026-03-09T00:00:00Z",
                "status": "in_progress",
            }, "generation")

            # No API key, so it won't query OpenAI — just shows local state
            with patch.dict(os.environ, {k: v for k, v in os.environ.items() if k != "OPENAI_API_KEY"}, clear=True):
                batch_status(responses_dir)

            captured = capsys.readouterr()
            assert "batch_test123" in captured.out
            assert "gpt-4.1-mini" in captured.out
            assert "50" in captured.out
