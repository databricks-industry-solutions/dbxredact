"""Tests for masking.py -- rule-based structured column masking."""

import base64
import re

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from dbxredact.masking import (
    MASKING_RULES,
    apply_structured_masking,
)


# ---------------------------------------------------------------------------
# TestMaskStrategy
# ---------------------------------------------------------------------------

class TestMaskStrategy:

    def test_ssn_replacement(self, spark):
        df = spark.createDataFrame([("123-45-6789",)], ["ssn_col"])
        out = apply_structured_masking(df, {"ssn_col": "ssn"})
        assert out.first().ssn_col == "[SSN]"

    def test_ssn_no_dashes(self, spark):
        df = spark.createDataFrame([("123456789",)], ["ssn_col"])
        out = apply_structured_masking(df, {"ssn_col": "ssn"})
        assert out.first().ssn_col == "[SSN]"

    def test_phone_replacement(self, spark):
        df = spark.createDataFrame([("(555) 867-5309",)], ["phone_col"])
        out = apply_structured_masking(df, {"phone_col": "phone"})
        assert out.first().phone_col == "[PHONE]"

    def test_email_replacement(self, spark):
        df = spark.createDataFrame([("user@example.com",)], ["email_col"])
        out = apply_structured_masking(df, {"email_col": "email"})
        assert out.first().email_col == "[EMAIL]"

    def test_dob_full_replacement(self, spark):
        df = spark.createDataFrame([("1990-01-15",)], ["dob_col"])
        out = apply_structured_masking(df, {"dob_col": "dob"})
        assert out.first().dob_col == "[DATE_OF_BIRTH]"

    def test_name_full_replacement(self, spark):
        df = spark.createDataFrame([("Jane Doe",)], ["name_col"])
        out = apply_structured_masking(df, {"name_col": "name"})
        assert out.first().name_col == "[PERSON]"

    def test_unknown_pii_type_typed(self, spark):
        df = spark.createDataFrame([("ABC123",)], ["custom_col"])
        out = apply_structured_masking(df, {"custom_col": "custom_id"})
        assert out.first().custom_col == "[CUSTOM_ID]"

    def test_unknown_pii_type_generic(self, spark):
        df = spark.createDataFrame([("ABC123",)], ["custom_col"])
        out = apply_structured_masking(
            df, {"custom_col": "custom_id"}, label_mode="generic"
        )
        assert out.first().custom_col == "[REDACTED]"

    def test_non_pii_columns_passthrough(self, spark):
        df = spark.createDataFrame(
            [("123-45-6789", "safe_value")], ["ssn_col", "notes"]
        )
        out = apply_structured_masking(df, {"ssn_col": "ssn"})
        row = out.first()
        assert row.ssn_col == "[SSN]"
        assert row.notes == "safe_value"

    def test_multiple_columns_single_pass(self, spark):
        df = spark.createDataFrame(
            [("123-45-6789", "user@x.com", "(555) 123-4567", "Jane", "1990-01-01", "keep")],
            ["c_ssn", "c_email", "c_phone", "c_name", "c_dob", "c_safe"],
        )
        col_map = {
            "c_ssn": "ssn",
            "c_email": "email",
            "c_phone": "phone",
            "c_name": "name",
            "c_dob": "dob",
        }
        row = apply_structured_masking(df, col_map).first()
        assert row.c_ssn == "[SSN]"
        assert row.c_email == "[EMAIL]"
        assert row.c_phone == "[PHONE]"
        assert row.c_name == "[PERSON]"
        assert row.c_dob == "[DATE_OF_BIRTH]"
        assert row.c_safe == "keep"

    def test_address_replacement(self, spark):
        df = spark.createDataFrame([("123 Main St",)], ["addr"])
        out = apply_structured_masking(df, {"addr": "address"})
        assert out.first().addr == "[ADDRESS]"

    def test_mrn_replacement(self, spark):
        df = spark.createDataFrame([("MRN: 1234567",)], ["mrn_col"])
        out = apply_structured_masking(df, {"mrn_col": "mrn"})
        assert "[MRN]" in out.first().mrn_col

    def test_ip_replacement(self, spark):
        df = spark.createDataFrame([("Visit from 192.168.1.1 today",)], ["log"])
        out = apply_structured_masking(df, {"log": "ip_address"})
        row = out.first()
        assert "[IP]" in row.log
        assert "192.168.1.1" not in row.log

    def test_zip_replacement(self, spark):
        df = spark.createDataFrame([("ZIP: 90210",)], ["zip_col"])
        out = apply_structured_masking(df, {"zip_col": "zip"})
        assert "[ZIP]" in out.first().zip_col


# ---------------------------------------------------------------------------
# TestRegexPrecision
# ---------------------------------------------------------------------------

class TestRegexPrecision:
    """Verify hardened patterns reject false-positive inputs."""

    def test_phone_rejects_plain_digits(self, spark):
        df = spark.createDataFrame([("1234567",)], ["val"])
        out = apply_structured_masking(df, {"val": "phone"})
        assert out.first().val == "1234567"

    def test_phone_accepts_dashed(self, spark):
        df = spark.createDataFrame([("555-867-5309",)], ["val"])
        out = apply_structured_masking(df, {"val": "phone"})
        assert out.first().val == "[PHONE]"

    def test_phone_accepts_dotted(self, spark):
        df = spark.createDataFrame([("555.867.5309",)], ["val"])
        out = apply_structured_masking(df, {"val": "phone"})
        assert out.first().val == "[PHONE]"

    def test_phone_accepts_intl_prefix(self, spark):
        df = spark.createDataFrame([("+1 555-867-5309",)], ["val"])
        out = apply_structured_masking(df, {"val": "phone"})
        assert "[PHONE]" in out.first().val

    def test_ssn_no_match_in_longer_number(self, spark):
        df = spark.createDataFrame([("12345678901",)], ["val"])
        out = apply_structured_masking(df, {"val": "ssn"})
        assert out.first().val == "12345678901"

    def test_mrn_digit_boundaries(self, spark):
        """A 7-digit MRN embedded in a longer number should not match."""
        df = spark.createDataFrame([("ID:12345678901",)], ["val"])
        out = apply_structured_masking(df, {"val": "mrn"})
        assert out.first().val == "ID:12345678901"

    def test_ip_rejects_invalid_octets(self, spark):
        df = spark.createDataFrame([("999.999.999.999",)], ["val"])
        out = apply_structured_masking(df, {"val": "ip_address"})
        assert out.first().val == "999.999.999.999"

    def test_ip_accepts_valid(self, spark):
        df = spark.createDataFrame([("10.0.0.1",)], ["val"])
        out = apply_structured_masking(df, {"val": "ip_address"})
        assert out.first().val == "[IP]"


# ---------------------------------------------------------------------------
# TestMultiMatch
# ---------------------------------------------------------------------------

class TestMultiMatch:
    """Verify regex replaces ALL occurrences in a single cell."""

    def test_two_ssns_in_one_cell(self, spark):
        df = spark.createDataFrame([("SSN1: 123-45-6789 SSN2: 987-65-4321",)], ["val"])
        row = apply_structured_masking(df, {"val": "ssn"}).first()
        assert "123-45-6789" not in row.val
        assert "987-65-4321" not in row.val
        assert row.val.count("[SSN]") == 2

    def test_mixed_ssn_and_text(self, spark):
        df = spark.createDataFrame([("Patient SSN is 123-45-6789, admitted today.",)], ["val"])
        row = apply_structured_masking(df, {"val": "ssn"}).first()
        assert "[SSN]" in row.val
        assert "admitted today" in row.val

    def test_multiple_emails_in_one_cell(self, spark):
        df = spark.createDataFrame([("a@b.com and c@d.org",)], ["val"])
        row = apply_structured_masking(df, {"val": "email"}).first()
        assert row.val.count("[EMAIL]") == 2


# ---------------------------------------------------------------------------
# TestDeepNesting
# ---------------------------------------------------------------------------

class TestDeepNesting:
    """Verify masking works at 3+ struct depth."""

    @pytest.fixture
    def deep_df(self, spark):
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("level1", StructType([
                StructField("level2", StructType([
                    StructField("ssn", StringType()),
                    StructField("other", StringType()),
                ])),
                StructField("sibling", StringType()),
            ])),
        ])
        return spark.createDataFrame(
            [Row(id=1, level1=Row(
                level2=Row(ssn="123-45-6789", other="keep"),
                sibling="also_keep",
            ))],
            schema=schema,
        )

    def test_three_level_nested_struct(self, deep_df):
        out = apply_structured_masking(
            deep_df, {"level1.level2.ssn": "ssn"}
        ).first()
        assert out.level1.level2.ssn == "[SSN]"

    def test_three_level_siblings_preserved(self, deep_df):
        out = apply_structured_masking(
            deep_df, {"level1.level2.ssn": "ssn"}
        ).first()
        assert out.level1.level2.other == "keep"
        assert out.level1.sibling == "also_keep"
        assert out.id == 1


# ---------------------------------------------------------------------------
# TestHashStrategy
# ---------------------------------------------------------------------------

class TestHashStrategy:

    def test_hash_produces_hex_string(self, spark):
        df = spark.createDataFrame([("secret",)], ["val"])
        row = apply_structured_masking(df, {"val": "ssn"}, strategy="hash").first()
        assert re.fullmatch(r"[0-9a-f]{64}", row.val)

    def test_hash_deterministic(self, spark):
        df = spark.createDataFrame([("abc",), ("abc",)], ["val"])
        rows = apply_structured_masking(df, {"val": "ssn"}, strategy="hash").collect()
        assert rows[0].val == rows[1].val

    def test_hash_different_values(self, spark):
        df = spark.createDataFrame([("aaa",), ("bbb",)], ["val"])
        rows = apply_structured_masking(df, {"val": "ssn"}, strategy="hash").collect()
        assert rows[0].val != rows[1].val

    def test_hash_multiple_columns(self, spark):
        df = spark.createDataFrame(
            [("name_val", "ssn_val", "safe")], ["c_name", "c_ssn", "c_safe"]
        )
        out = apply_structured_masking(
            df, {"c_name": "name", "c_ssn": "ssn"}, strategy="hash"
        ).first()
        assert re.fullmatch(r"[0-9a-f]{64}", out.c_name)
        assert re.fullmatch(r"[0-9a-f]{64}", out.c_ssn)
        assert out.c_safe == "safe"


# ---------------------------------------------------------------------------
# TestEncryptStrategy
# ---------------------------------------------------------------------------

class TestEncryptStrategy:

    _KEY = "0123456789abcdef"  # 16-byte AES key

    def test_encrypt_produces_base64(self, spark):
        df = spark.createDataFrame([("secret",)], ["val"])
        row = apply_structured_masking(
            df, {"val": "ssn"}, strategy="encrypt", encryption_key=self._KEY
        ).first()
        base64.b64decode(row.val)  # should not raise

    def test_encrypt_requires_key(self, spark):
        df = spark.createDataFrame([("x",)], ["val"])
        with pytest.raises(ValueError, match="encryption_key"):
            apply_structured_masking(df, {"val": "ssn"}, strategy="encrypt")

    def test_encrypt_roundtrip(self, spark):
        df = spark.createDataFrame([("hello world",)], ["val"])
        encrypted_df = apply_structured_masking(
            df, {"val": "ssn"}, strategy="encrypt", encryption_key=self._KEY
        )
        decrypted = encrypted_df.selectExpr(
            f"cast(aes_decrypt(unbase64(val), '{self._KEY}') as string) as val"
        ).first()
        assert decrypted.val == "hello world"


# ---------------------------------------------------------------------------
# TestNestedStructMasking
# ---------------------------------------------------------------------------

class TestNestedStructMasking:

    @pytest.fixture
    def nested_df(self, spark):
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("personal_info", StructType([
                StructField("ssn", StringType()),
                StructField("first_name", StringType()),
            ])),
        ])
        return spark.createDataFrame(
            [Row(id=1, personal_info=Row(ssn="123-45-6789", first_name="Alice"))],
            schema=schema,
        )

    def test_nested_ssn_masked(self, nested_df):
        out = apply_structured_masking(
            nested_df, {"personal_info.ssn": "ssn"}
        ).first()
        assert out.personal_info.ssn == "[SSN]"

    def test_sibling_fields_preserved(self, nested_df):
        out = apply_structured_masking(
            nested_df, {"personal_info.ssn": "ssn"}
        ).first()
        assert out.personal_info.first_name == "Alice"
        assert out.id == 1


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_empty_column_type_map(self, spark):
        df = spark.createDataFrame([("val",)], ["col1"])
        out = apply_structured_masking(df, {})
        assert out.first().col1 == "val"

    def test_null_values_preserved(self, spark):
        df = spark.createDataFrame([(None,)], schema="ssn_col string")
        out = apply_structured_masking(df, {"ssn_col": "ssn"})
        assert out.first().ssn_col is None

    def test_empty_strings(self, spark):
        df = spark.createDataFrame([("",)], ["ssn_col"])
        out = apply_structured_masking(df, {"ssn_col": "ssn"})
        assert out.first().ssn_col == ""

    def test_mixed_types_hash(self, spark):
        df = spark.createDataFrame([(42, "2024-01-01")], ["int_col", "date_col"])
        out = apply_structured_masking(
            df, {"int_col": "mrn", "date_col": "dob"}, strategy="hash"
        ).first()
        assert re.fullmatch(r"[0-9a-f]{64}", out.int_col)
        assert re.fullmatch(r"[0-9a-f]{64}", out.date_col)
