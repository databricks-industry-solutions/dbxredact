"""Structured column masking using Spark SQL expressions.

Provides rule-based masking (label replacement), hashing (SHA-256), and
AES encryption for columns identified by ``pii_type`` tags.  All operations
are pure Spark SQL -- no UDFs or model inference.
"""

import logging
from typing import Callable, Dict, Literal, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    aes_encrypt,
    col,
    lit,
    regexp_replace,
    sha2,
    struct,
    when,
)

logger = logging.getLogger(__name__)

MaskingStrategy = Literal["mask", "hash", "encrypt"]

MASKING_RULES: Dict[str, Callable] = {
    "ssn": lambda c: regexp_replace(c, r"\d{3}-?\d{2}-?\d{4}", "[SSN]"),
    "phone": lambda c: regexp_replace(c, r"[\d\s\-\(\)\.]{7,}", "[PHONE]"),
    "email": lambda c: regexp_replace(c, r"[^\s@]+@[^\s@]+\.[^\s@]+", "[EMAIL]"),
    "dob": lambda c: lit("[DATE_OF_BIRTH]"),
    "name": lambda c: lit("[PERSON]"),
    "address": lambda c: lit("[ADDRESS]"),
    "mrn": lambda c: lit("[MRN]"),
    "ip_address": lambda c: regexp_replace(c, r"\d{1,3}(\.\d{1,3}){3}", "[IP]"),
    "zip": lambda c: regexp_replace(c, r"\d{5}(-\d{4})?", "[ZIP]"),
}


def _update_nested_field(df: DataFrame, col_path: str, transform_fn: Callable) -> DataFrame:
    """Apply *transform_fn* to a nested struct field specified by dot notation.

    E.g. ``col_path="personal_info.ssn"`` transforms the ``ssn`` leaf inside
    the ``personal_info`` struct while preserving all sibling fields.
    """
    parts = col_path.split(".")
    if len(parts) == 1:
        return df.withColumn(col_path, transform_fn(col(col_path)))

    root = parts[0]
    schema = df.schema[root].dataType

    def _rebuild(current_schema, depth):
        fields = []
        for field in current_schema.fields:
            target_name = parts[depth + 1] if depth + 1 < len(parts) else None
            if field.name == target_name:
                if depth + 2 == len(parts):
                    fields.append(transform_fn(col(f"{root}.{'.' .join(parts[1:])}")). alias(field.name))
                else:
                    inner = _rebuild(field.dataType, depth + 1)
                    fields.append(struct(*inner).alias(field.name))
            else:
                ref = f"{'.'.join(parts[:depth+1])}.{field.name}"
                fields.append(col(ref).alias(field.name))
        return fields

    rebuilt_fields = _rebuild(schema, 0)
    return df.withColumn(root, struct(*rebuilt_fields))


def _hash_column(c, _algorithm: str = "sha2"):
    """SHA-256 hash, casting non-string types first."""
    return sha2(c.cast("string"), 256)


def _encrypt_column(c, key_expr):
    """AES encrypt, casting non-string types first."""
    return aes_encrypt(c.cast("string"), key_expr)


def apply_structured_masking(
    df: DataFrame,
    column_type_map: Dict[str, str],
    strategy: MaskingStrategy = "mask",
    redaction_strategy: str = "typed",
    encryption_key: Optional[str] = None,
    hash_algorithm: str = "sha2",
) -> DataFrame:
    """Apply rule-based masking to structured columns.

    Args:
        df: Input DataFrame.
        column_type_map: Mapping of column name (dot-notation for nested) to
            ``pii_type`` string (e.g. ``{"patient_ssn": "ssn", "phone": "phone"}``).
        strategy: ``"mask"`` (label replacement), ``"hash"`` (SHA-256), or
            ``"encrypt"`` (AES).
        redaction_strategy: ``"typed"`` uses type-specific labels like ``[SSN]``;
            ``"generic"`` replaces everything with ``[REDACTED]``.
        encryption_key: Required when ``strategy="encrypt"``.
        hash_algorithm: Hash algorithm for ``strategy="hash"`` (only ``sha2`` supported).

    Returns:
        DataFrame with specified columns masked/hashed/encrypted in place.
    """
    if strategy == "encrypt" and not encryption_key:
        raise ValueError("encryption_key is required when strategy='encrypt'")

    for col_name, pii_type in column_type_map.items():
        is_nested = "." in col_name

        if strategy == "mask":
            if redaction_strategy == "generic":
                transform = lambda c: lit("[REDACTED]")
            else:
                rule = MASKING_RULES.get(pii_type)
                if rule is None:
                    logger.warning("No masking rule for pii_type=%r on column %r; using [REDACTED]", pii_type, col_name)
                    transform = lambda c: lit("[REDACTED]")
                else:
                    transform = rule

        elif strategy == "hash":
            transform = lambda c: _hash_column(c, hash_algorithm)

        elif strategy == "encrypt":
            key_lit = lit(encryption_key)
            transform = lambda c, _k=key_lit: _encrypt_column(c, _k)

        else:
            raise ValueError(f"Unknown masking strategy: {strategy!r}")

        if is_nested:
            df = _update_nested_field(df, col_name, transform)
        else:
            null_safe = lambda c, _t=transform: when(c.isNotNull(), _t(c))
            df = df.withColumn(col_name, null_safe(col(col_name)))

        logger.info("Masked column %s (pii_type=%s, strategy=%s)", col_name, pii_type, strategy)

    return df
