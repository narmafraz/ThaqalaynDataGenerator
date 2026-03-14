"""Tests for verse_processor narrator canonical_id resolution."""

import json
import os
import unittest
from unittest.mock import MagicMock

from app.pipeline_cli.verse_processor import override_narrators, _is_ambiguous_name


class TestOverrideNarratorsCanonicalId(unittest.TestCase):
    """Test canonical_id resolution in override_narrators()."""

    def _make_result(self, narrators):
        """Build a minimal result dict with given narrators."""
        return {
            "isnad_matn": {
                "narrators": narrators,
            }
        }

    def test_canonical_id_from_template(self):
        """Fast path: canonical_id comes from template when present."""
        result = self._make_result([
            {"name_ar": "عَلِيُّ بْنُ إِبْرَاهِيمَ", "name_en": "Ali", "position": 1},
        ])
        templates = {
            "narrators": {
                "عَلِيُّ بْنُ إِبْرَاهِيمَ": {
                    "name_en": "Ali ibn Ibrahim",
                    "canonical_id": 42,
                },
            }
        }
        result, overrides = override_narrators(result, templates)
        narrator = result["isnad_matn"]["narrators"][0]
        assert narrator["canonical_id"] == 42
        assert narrator["name_en"] == "Ali ibn Ibrahim"

    def test_canonical_id_from_registry_when_template_lacks_it(self):
        """Registry resolves canonical_id when template has no canonical_id."""
        result = self._make_result([
            {"name_ar": "محمد", "name_en": "Muhammad", "position": 1},
        ])
        templates = {
            "narrators": {
                "محمد": {"name_en": "Muhammad"},
            }
        }
        # Track calls with copies of preceding_names (list is mutable)
        resolve_calls = []

        def track_resolve(name_ar, preceding_names=None):
            resolve_calls.append((name_ar, list(preceding_names or [])))
            return 99

        registry = MagicMock()
        registry.resolve.side_effect = track_resolve

        result, overrides = override_narrators(result, templates, registry=registry)
        narrator = result["isnad_matn"]["narrators"][0]
        assert narrator["canonical_id"] == 99
        assert len(resolve_calls) == 1
        assert resolve_calls[0] == ("محمد", [])

    def test_canonical_id_none_when_not_in_registry_or_templates(self):
        """canonical_id not set when narrator unknown to both template and registry."""
        result = self._make_result([
            {"name_ar": "مجهول", "name_en": "Unknown", "position": 1},
        ])
        templates = {"narrators": {}}
        registry = MagicMock()
        registry.resolve.return_value = None

        result, overrides = override_narrators(result, templates, registry=registry)
        narrator = result["isnad_matn"]["narrators"][0]
        assert "canonical_id" not in narrator

    def test_backward_compat_registry_none(self):
        """When registry is None, only template path is used (backward compat)."""
        result = self._make_result([
            {"name_ar": "عَلِيّ", "name_en": "Ali", "position": 1},
        ])
        templates = {
            "narrators": {
                "عَلِيّ": {"name_en": "Ali", "canonical_id": 10},
            }
        }
        result, overrides = override_narrators(result, templates, registry=None)
        narrator = result["isnad_matn"]["narrators"][0]
        assert narrator["canonical_id"] == 10

    def test_preceding_names_passed_for_disambiguation(self):
        """Registry receives preceding_names for chain-context disambiguation."""
        result = self._make_result([
            {"name_ar": "عَلِيّ", "name_en": "Ali", "position": 1},
            {"name_ar": "أَبِيهِ", "name_en": "his father", "position": 2},
        ])
        templates = {"narrators": {}}

        # Track calls with copies of preceding_names (list is mutable)
        resolve_calls = []

        def track_resolve(name_ar, preceding_names=None):
            resolve_calls.append((name_ar, list(preceding_names or [])))
            return {0: 100, 1: 200}[len(resolve_calls) - 1]  # won't work, use counter

        call_count = [0]

        def track_resolve(name_ar, preceding_names=None):
            resolve_calls.append((name_ar, list(preceding_names or [])))
            call_count[0] += 1
            return [100, 200][call_count[0] - 1]

        registry = MagicMock()
        registry.resolve.side_effect = track_resolve

        result, overrides = override_narrators(result, templates, registry=registry)

        # First call: no preceding names
        assert resolve_calls[0] == ("عَلِيّ", [])
        # Second call: first narrator as preceding name
        assert resolve_calls[1] == ("أَبِيهِ", ["عَلِيّ"])

        assert result["isnad_matn"]["narrators"][0]["canonical_id"] == 100
        assert result["isnad_matn"]["narrators"][1]["canonical_id"] == 200

    def test_ambiguous_name_uses_registry_over_template(self):
        """Ambiguous names re-resolve via registry even if template has canonical_id."""
        result = self._make_result([
            {"name_ar": "أَبِيهِ", "name_en": "his father", "position": 1},
        ])
        templates = {
            "narrators": {
                "أَبِيهِ": {
                    "name_en": "his father",
                    "canonical_id": 50,
                    "disambiguation_context": "When preceded by عَلِيّ",
                },
            }
        }
        registry = MagicMock()
        registry.resolve.return_value = 77

        result, overrides = override_narrators(result, templates, registry=registry)
        # Registry result takes precedence for ambiguous names
        assert result["isnad_matn"]["narrators"][0]["canonical_id"] == 77

    def test_no_templates_no_registry(self):
        """No-op when neither templates nor registry are provided."""
        result = self._make_result([
            {"name_ar": "عَلِيّ", "name_en": "Ali", "position": 1},
        ])
        result, overrides = override_narrators(result, None, registry=None)
        assert overrides == []
        assert "canonical_id" not in result["isnad_matn"]["narrators"][0]

    def test_empty_narrators_list(self):
        """Empty narrators list doesn't crash."""
        result = self._make_result([])
        templates = {"narrators": {"عَلِيّ": {"name_en": "Ali", "canonical_id": 1}}}
        result, overrides = override_narrators(result, templates)
        assert overrides == []

    def test_name_en_override_still_works(self):
        """name_en overrides still function alongside canonical_id resolution."""
        result = self._make_result([
            {"name_ar": "عَلِيّ", "name_en": "Wrong Name", "position": 1},
        ])
        templates = {
            "narrators": {
                "عَلِيّ": {"name_en": "Correct Name", "canonical_id": 5},
            }
        }
        result, overrides = override_narrators(result, templates)
        narrator = result["isnad_matn"]["narrators"][0]
        assert narrator["name_en"] == "Correct Name"
        assert narrator["canonical_id"] == 5
        # Should have overrides for both name_en and canonical_id
        fields = {o["field"] for o in overrides}
        assert "name_en" in fields
        assert "canonical_id" in fields


class TestIsAmbiguousName(unittest.TestCase):
    """Test _is_ambiguous_name helper."""

    def test_name_not_in_templates(self):
        assert not _is_ambiguous_name("مجهول", {})

    def test_name_with_disambiguation_context(self):
        templates = {
            "أَبِيهِ": {
                "name_en": "his father",
                "disambiguation_context": "When preceded by عَلِيّ",
            }
        }
        assert _is_ambiguous_name("أَبِيهِ", templates)

    def test_name_with_ambiguous_confidence(self):
        templates = {
            "محمد": {
                "name_en": "Muhammad",
                "identity_confidence": "ambiguous",
            }
        }
        assert _is_ambiguous_name("محمد", templates)

    def test_name_without_ambiguity_markers(self):
        templates = {
            "عَلِيّ": {
                "name_en": "Ali",
                "canonical_id": 1,
            }
        }
        assert not _is_ambiguous_name("عَلِيّ", templates)


if __name__ == "__main__":
    unittest.main()
