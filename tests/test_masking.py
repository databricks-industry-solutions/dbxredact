"""Tests for masking.py -- structured column masking."""

from unittest.mock import MagicMock, patch

import pytest

from dbxredact.masking import (
    MaskingStrategy,
    MASKING_RULES,
    apply_structured_masking,
)

# All masking tests need pyspark SQL functions patched since there's no SparkContext
_PATCHES = [
    "dbxredact.masking.col",
    "dbxredact.masking.when",
    "dbxredact.masking.lit",
    "dbxredact.masking.regexp_replace",
    "dbxredact.masking.sha2",
    "dbxredact.masking.aes_encrypt",
]


def _apply_patches(fn):
    """Decorator stacking all necessary pyspark function patches."""
    for p in reversed(_PATCHES):
        fn = patch(p)(fn)
    return fn


class TestMaskingRules:

    def test_all_expected_types_present(self):
        expected = {"ssn", "phone", "email", "dob", "name", "address", "mrn", "ip_address", "zip"}
        assert expected == set(MASKING_RULES.keys())

    def test_rules_are_callable(self):
        for pii_type, rule in MASKING_RULES.items():
            assert callable(rule), f"Rule for {pii_type} is not callable"


class TestApplyStructuredMasking:

    def test_rejects_encrypt_without_key(self):
        df = MagicMock()
        with pytest.raises(ValueError, match="encryption_key is required"):
            apply_structured_masking(df, {"col": "ssn"}, strategy="encrypt")

    def test_rejects_unknown_strategy(self):
        df = MagicMock()
        with pytest.raises(ValueError, match="Unknown masking strategy"):
            apply_structured_masking(df, {"col": "ssn"}, strategy="unknown")

    @_apply_patches
    def test_mask_returns_dataframe(self, *_mocks):
        df = MagicMock()
        df.withColumn = MagicMock(return_value=df)
        result = apply_structured_masking(df, {"col_a": "ssn"}, strategy="mask")
        assert result is df
        df.withColumn.assert_called_once()

    @_apply_patches
    def test_hash_returns_dataframe(self, *_mocks):
        df = MagicMock()
        df.withColumn = MagicMock(return_value=df)
        result = apply_structured_masking(df, {"col_a": "ssn"}, strategy="hash")
        assert result is df
        df.withColumn.assert_called_once()

    @_apply_patches
    def test_encrypt_returns_dataframe(self, *_mocks):
        df = MagicMock()
        df.withColumn = MagicMock(return_value=df)
        result = apply_structured_masking(
            df, {"col_a": "ssn"}, strategy="encrypt", encryption_key="test_key_16bytes!"
        )
        assert result is df
        df.withColumn.assert_called_once()

    @_apply_patches
    def test_multiple_columns(self, *_mocks):
        df = MagicMock()
        df.withColumn = MagicMock(return_value=df)
        result = apply_structured_masking(
            df, {"ssn_col": "ssn", "phone_col": "phone", "email_col": "email"}, strategy="mask"
        )
        assert result is df
        assert df.withColumn.call_count == 3

    @_apply_patches
    def test_generic_redaction_strategy(self, *_mocks):
        df = MagicMock()
        df.withColumn = MagicMock(return_value=df)
        result = apply_structured_masking(
            df, {"col": "ssn"}, strategy="mask", redaction_strategy="generic"
        )
        assert result is df

    @_apply_patches
    def test_unknown_pii_type_falls_back(self, *_mocks):
        df = MagicMock()
        df.withColumn = MagicMock(return_value=df)
        result = apply_structured_masking(
            df, {"col": "unknown_type"}, strategy="mask"
        )
        assert result is df

    def test_empty_column_map_is_noop(self):
        df = MagicMock()
        result = apply_structured_masking(df, {}, strategy="mask")
        assert result is df
        df.withColumn.assert_not_called()


class TestMaskingStrategyType:

    def test_literal_values(self):
        valid: list[MaskingStrategy] = ["mask", "hash", "encrypt"]
        for v in valid:
            assert v in ("mask", "hash", "encrypt")
