"""Tests for gliner_detector.py -- helper functions with mocked model."""

import pytest
from dbxredact.gliner_detector import (
    _merge_adjacent_names,
    _build_offset_map,
    _chunk_and_predict,
    _map_label,
)
from dbxredact.config import (
    GLINER_MODEL_PRESETS,
    GLINER_LABEL_MAP,
    get_gliner_preset,
    DEFAULT_GLINER_MODEL,
)


class TestMergeAdjacentNames:

    def test_merge_first_last(self):
        entities = [
            {"text": "John", "label": "first_name", "start": 0, "end": 4, "score": 0.9},
            {"text": "Smith", "label": "last_name", "start": 5, "end": 10, "score": 0.8},
        ]
        merged = _merge_adjacent_names(entities)
        assert len(merged) == 1
        assert merged[0]["text"] == "John Smith"
        assert merged[0]["start"] == 0
        assert merged[0]["end"] == 10
        assert merged[0]["score"] == 0.8

    def test_no_merge_same_label(self):
        entities = [
            {"text": "John", "label": "first_name", "start": 0, "end": 4, "score": 0.9},
            {"text": "James", "label": "first_name", "start": 5, "end": 10, "score": 0.8},
        ]
        merged = _merge_adjacent_names(entities)
        assert len(merged) == 2

    def test_no_merge_too_far_apart(self):
        entities = [
            {"text": "John", "label": "first_name", "start": 0, "end": 4, "score": 0.9},
            {"text": "Smith", "label": "last_name", "start": 20, "end": 25, "score": 0.8},
        ]
        merged = _merge_adjacent_names(entities)
        assert len(merged) == 2

    def test_empty_list(self):
        assert _merge_adjacent_names([]) == []

    def test_single_entity(self):
        entities = [{"text": "John", "label": "first_name", "start": 0, "end": 4, "score": 0.9}]
        assert len(_merge_adjacent_names(entities)) == 1

    def test_name_entity_subsumes_parts(self):
        """Step 2: a 'name' span containing a 'first_name' should drop the part."""
        entities = [
            {"text": "John Smith", "label": "name", "start": 0, "end": 10, "score": 0.85},
            {"text": "John", "label": "first_name", "start": 0, "end": 4, "score": 0.9},
        ]
        merged = _merge_adjacent_names(entities)
        assert len(merged) == 1
        assert merged[0]["label"] == "name"
        assert merged[0]["text"] == "John Smith"

    def test_merge_last_first_reversed(self):
        """last_name + first_name (alphabetical order after sort) should merge."""
        entities = [
            {"text": "Smith", "label": "last_name", "start": 0, "end": 5, "score": 0.8},
            {"text": "John", "label": "first_name", "start": 6, "end": 10, "score": 0.9},
        ]
        merged = _merge_adjacent_names(entities)
        assert len(merged) == 1
        assert merged[0]["label"] == "name"
        assert merged[0]["text"] == "Smith John"

    def test_non_name_entities_pass_through(self):
        """Only adjacent first/last name pairs merge; other entity types are untouched."""
        entities = [
            {"text": "test@x.com", "label": "email", "start": 0, "end": 10, "score": 0.9},
            {"text": "John", "label": "first_name", "start": 15, "end": 19, "score": 0.8},
            {"text": "Smith", "label": "last_name", "start": 20, "end": 25, "score": 0.7},
            {"text": "555-1234", "label": "phone_number", "start": 30, "end": 38, "score": 0.9},
        ]
        merged = _merge_adjacent_names(entities)
        labels = [e["label"] for e in merged]
        assert labels == ["email", "name", "phone_number"]

    def test_three_consecutive_name_parts(self):
        """first + last + first: only the first pair merges."""
        entities = [
            {"text": "John", "label": "first_name", "start": 0, "end": 4, "score": 0.9},
            {"text": "Smith", "label": "last_name", "start": 5, "end": 10, "score": 0.8},
            {"text": "Jane", "label": "first_name", "start": 11, "end": 15, "score": 0.7},
        ]
        merged = _merge_adjacent_names(entities)
        assert len(merged) == 2
        assert merged[0]["label"] == "name"
        assert merged[0]["text"] == "John Smith"
        assert merged[1]["label"] == "first_name"
        assert merged[1]["text"] == "Jane"


class TestBuildOffsetMap:

    def test_simple_text(self):
        mapping = _build_offset_map("hello world")
        assert len(mapping) == 11
        assert mapping[0] == 0   # 'h'
        assert mapping[5] == 5   # ' '
        assert mapping[6] == 6   # 'w'

    def test_leading_trailing_whitespace(self):
        # "  hello  " normalizes to "hello" (len 5)
        mapping = _build_offset_map("  hello  ")
        assert len(mapping) == 5
        assert mapping[0] == 2   # 'h' at original pos 2
        assert mapping[4] == 6   # 'o' at original pos 6

    def test_collapsed_interior_whitespace(self):
        # "a  b" normalizes to "a b" (len 3)
        mapping = _build_offset_map("a  b")
        assert len(mapping) == 3
        assert mapping[0] == 0   # 'a' at original pos 0
        assert mapping[1] == 1   # ' ' maps to first space at original pos 1
        assert mapping[2] == 3   # 'b' at original pos 3

    def test_empty_string(self):
        assert _build_offset_map("") == []

    def test_tabs_and_newlines(self):
        # "a\t\nb" normalizes to "a b" (len 3)
        mapping = _build_offset_map("a\t\nb")
        assert len(mapping) == 3
        assert mapping[0] == 0   # 'a'
        assert mapping[2] == 3   # 'b'


class TestChunkAndPredict:

    def test_short_text_no_chunking(self):
        class MockModel:
            def predict_entities(self, text, labels, threshold=0.5):
                return [{"text": "John", "label": "person", "start": 0, "end": 4, "score": 0.9}]

        result = _chunk_and_predict(MockModel(), "John went home", ["person"], 0.5)
        assert len(result) == 1
        assert result[0]["text"] == "John"

    def test_deduplication(self):
        call_count = 0

        class MockModel:
            def predict_entities(self, text, labels, threshold=0.5):
                nonlocal call_count
                call_count += 1
                return [{"text": "John", "label": "person", "start": 0, "end": 4, "score": 0.9}]

        result = _chunk_and_predict(MockModel(), "John", ["person"], 0.5)
        assert len(result) == 1

    def test_long_text_triggers_chunking(self):
        """Verify chunking is triggered when text exceeds MAX_WORDS."""
        words = ["word"] * 600
        text = " ".join(words)
        call_count = 0

        class MockModel:
            def predict_entities(self, text, labels, threshold=0.5):
                nonlocal call_count
                call_count += 1
                return []

        _chunk_and_predict(MockModel(), text, ["person"], 0.5)
        assert call_count > 1

    def test_chunk_offset_adjustment(self):
        """Entities from later chunks should have adjusted offsets."""
        words = ["word"] * 600
        text = " ".join(words)

        class MockModel:
            def predict_entities(self, text_input, labels, threshold=0.5):
                if text_input.startswith("word word"):
                    return [{"text": "word", "label": "person", "start": 0, "end": 4, "score": 0.9}]
                return []

        result = _chunk_and_predict(MockModel(), text, ["person"], 0.5)
        assert len(result) >= 2
        starts = sorted(e["start"] for e in result)
        assert starts[0] == 0
        assert starts[-1] > 0, "Later chunks should produce offset-adjusted entities"
        for e in result:
            assert e["end"] - e["start"] == 4

    def test_overlap_dedup_keeps_highest_score(self):
        """When overlapping chunks produce the same entity, the higher score wins."""
        # OVERLAP_WORDS=50, so max_words must exceed 50. Use 60 words with max_words=55
        # so step = 55 - 50 = 5. Chunk 1: words 0-54, chunk 2: words 5-59.
        # "target" at word 10 appears in both chunks' overlap zone.
        filler = ["filler"] * 60
        filler[10] = "target"
        text = " ".join(filler)
        call_count = 0

        class MockModel:
            def predict_entities(self, text_input, labels, threshold=0.5):
                nonlocal call_count
                call_count += 1
                idx = text_input.find("target")
                if idx >= 0:
                    return [{"text": "target", "label": "person",
                             "start": idx, "end": idx + 6,
                             "score": 0.6 if call_count == 1 else 0.95}]
                return []

        result = _chunk_and_predict(MockModel(), text, ["person"], 0.5, max_words=55)
        assert call_count >= 2
        targets = [e for e in result if e["text"] == "target"]
        assert len(targets) == 1
        assert targets[0]["score"] == 0.95


class TestMapLabel:

    def test_known_nvidia_label(self):
        assert _map_label("first_name", GLINER_LABEL_MAP) == "PERSON"
        assert _map_label("email", GLINER_LABEL_MAP) == "EMAIL_ADDRESS"
        assert _map_label("ssn", GLINER_LABEL_MAP) == "US_SSN"

    def test_hospital_maps_to_hospital_name(self):
        assert _map_label("hospital_or_medical_facility", GLINER_LABEL_MAP) == "HOSPITAL_NAME"

    def test_unknown_label_uppercased(self):
        assert _map_label("custom type", GLINER_LABEL_MAP) == "CUSTOM_TYPE"

    def test_urchade_labels(self):
        urchade = get_gliner_preset("urchade/gliner_multi_pii-v1")
        lm = urchade["label_map"]
        assert _map_label("person", lm) == "PERSON"
        assert _map_label("email address", lm) == "EMAIL_ADDRESS"
        assert _map_label("social security number", lm) == "US_SSN"
        assert _map_label("credit card number", lm) == "CREDIT_CARD"
        assert _map_label("medical condition", lm) == "MEDICAL_INFO"
        assert _map_label("username", lm) == "ONLINE_IDENTIFIER"

    def test_unknown_urchade_style_label_fallback(self):
        """Unknown multi-word labels should uppercase and replace spaces with underscores."""
        lm = get_gliner_preset("urchade/gliner_multi_pii-v1")["label_map"]
        assert _map_label("my custom thing", lm) == "MY_CUSTOM_THING"
        assert _map_label("another label", {}) == "ANOTHER_LABEL"


class TestOffsetMapRoundTrip:

    def test_remap_preserves_text(self):
        original = "John  Smith\nfrom  NYC"
        mapping = _build_offset_map(original)
        import re
        normalized = re.sub(r"\s+", " ", original).strip()
        for ni, oi in enumerate(mapping):
            if ni < len(normalized):
                nc, oc = normalized[ni], original[oi]
                if nc == " ":
                    assert oc in " \t\n\r", f"pos {ni}: expected whitespace, got {oc!r}"
                else:
                    assert nc == oc, f"pos {ni}: {nc!r} != {oc!r}"


class TestGlinerModelPresets:

    def test_all_presets_have_required_keys(self):
        for model_name, preset in GLINER_MODEL_PRESETS.items():
            assert "labels" in preset, f"{model_name} missing labels"
            assert "label_map" in preset, f"{model_name} missing label_map"
            assert "thresholds" in preset, f"{model_name} missing thresholds"

    def test_labels_covered_by_label_map(self):
        for model_name, preset in GLINER_MODEL_PRESETS.items():
            for label in preset["labels"]:
                assert label in preset["label_map"], (
                    f"{model_name}: label '{label}' not in label_map"
                )

    def test_labels_covered_by_thresholds(self):
        for model_name, preset in GLINER_MODEL_PRESETS.items():
            for label in preset["labels"]:
                assert label in preset["thresholds"], (
                    f"{model_name}: label '{label}' not in thresholds"
                )

    def test_get_preset_known_model(self):
        nvidia = get_gliner_preset("nvidia/gliner-PII")
        assert nvidia["labels"] is not None
        assert len(nvidia["labels"]) > 0

        urchade = get_gliner_preset("urchade/gliner_multi_pii-v1")
        assert urchade["labels"] is not None
        assert len(urchade["labels"]) > 0
        assert urchade["labels"] != nvidia["labels"]

    def test_get_preset_unknown_model_falls_back(self):
        fallback = get_gliner_preset("some/unknown-model")
        nvidia = get_gliner_preset(DEFAULT_GLINER_MODEL)
        assert fallback["labels"] == nvidia["labels"]
        assert fallback["label_map"] == nvidia["label_map"]

    def test_urchade_and_nvidia_map_to_overlapping_entity_types(self):
        """Both presets should produce the same core entity types."""
        nvidia_types = set(GLINER_MODEL_PRESETS["nvidia/gliner-PII"]["label_map"].values())
        urchade_types = set(GLINER_MODEL_PRESETS["urchade/gliner_multi_pii-v1"]["label_map"].values())
        common = {"PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "LOCATION", "US_SSN",
                  "ID_NUMBER", "CREDIT_CARD", "IP_ADDRESS", "ORGANIZATION"}
        assert common <= nvidia_types
        assert common <= urchade_types

    def test_thresholds_in_valid_range(self):
        for model_name, preset in GLINER_MODEL_PRESETS.items():
            for label, thresh in preset["thresholds"].items():
                assert 0.0 < thresh <= 1.0, (
                    f"{model_name}: threshold for '{label}' is {thresh}, expected (0, 1]"
                )

    def test_no_duplicate_labels(self):
        for model_name, preset in GLINER_MODEL_PRESETS.items():
            labels = preset["labels"]
            assert len(labels) == len(set(labels)), (
                f"{model_name}: duplicate labels found"
            )

    def test_label_map_no_orphaned_keys(self):
        """Every key in label_map should be a label the model actually uses."""
        for model_name, preset in GLINER_MODEL_PRESETS.items():
            labels_set = set(preset["labels"])
            for key in preset["label_map"]:
                assert key in labels_set, (
                    f"{model_name}: label_map key '{key}' not in labels list"
                )
