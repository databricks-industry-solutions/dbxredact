"""Rule-based structured column masking -- mask, hash, or encrypt."""

import logging
import uuid
from typing import Callable, Dict, Literal, Optional

from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    base64,
    col,
    concat,
    lit,
    regexp_replace,
    sha2,
    when,
)

logger = logging.getLogger(__name__)

MaskingStrategy = Literal["mask", "hash", "encrypt"]

MASKING_RULES: Dict[str, Callable[[Column], Column]] = {
    "ssn": lambda c: regexp_replace(
        c, r"(?<!\d)(\d{3}-\d{2}-\d{4}|\d{9})(?!\d)", "[SSN]"
    ),
    "phone": lambda c: regexp_replace(
        c,
        r"(\+?\d{1,3}[\s.\-]?)?\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}",
        "[PHONE]",
    ),
    "email": lambda c: regexp_replace(c, r"[^\s@]+@[^\s@]+\.[^\s@]+", "[EMAIL]"),
    "dob": lambda c: lit("[DATE_OF_BIRTH]"),
    "name": lambda c: lit("[PERSON]"),
    "address": lambda c: lit("[ADDRESS]"),
    "mrn": lambda c: regexp_replace(c, r"(?<!\d)\d{5,10}(?!\d)", "[MRN]"),
    "ip_address": lambda c: regexp_replace(
        c,
        r"(?<!\d)(25[0-5]|2[0-4]\d|[01]?\d\d?)(\.(25[0-5]|2[0-4]\d|[01]?\d\d?)){3}(?!\d)",
        "[IP]",
    ),
    "zip": lambda c: regexp_replace(c, r"(?<!\d)\d{5}(-\d{4})?(?!\d)", "[ZIP]"),
}


def _build_mask_expr(
    c: Column,
    pii_type: str,
    label_mode: str,
) -> Column:
    """Return a mask expression for a single column, preserving NULLs."""
    rule = MASKING_RULES.get(pii_type)
    if rule:
        masked = rule(c)
    elif label_mode == "generic":
        masked = lit("[REDACTED]")
    else:
        masked = lit(f"[{pii_type.upper()}]")
    return when(c.isNull(), lit(None)).otherwise(masked)


def _build_hash_expr(c: Column, salt: str = "") -> Column:
    value = concat(c.cast("string"), lit(salt)) if salt else c.cast("string")
    return when(c.isNull(), lit(None)).otherwise(sha2(value, 256))


def _resolve_encryption_key(key: str) -> str:
    """Resolve a key that may be a Databricks secret reference (secret://scope/key)."""
    if key.startswith("secret://"):
        parts = key[len("secret://"):].split("/", 1)
        scope, secret_key = parts[0], parts[1]
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        return spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils().secrets().get(scope, secret_key)
    return key


def _build_encrypt_expr(c: Column, key: str) -> Column:
    from pyspark.sql.functions import aes_encrypt
    resolved = _resolve_encryption_key(key)
    return when(c.isNull(), lit(None)).otherwise(
        base64(aes_encrypt(c.cast("string"), lit(resolved)))
    )


def _mask_nested_field(
    df: DataFrame,
    dotted_path: str,
    transform_fn: Callable[[Column], Column],
) -> DataFrame:
    """Apply *transform_fn* to a nested struct field specified by dot notation.

    Handles arbitrary nesting depth by chaining ``withField`` calls from the
    leaf up to the top-level column, then applying a single ``withColumn`` on
    the root struct.
    """
    parts = dotted_path.split(".")
    top_col = parts[0]
    inner_parts = parts[1:]

    expr = transform_fn(col(dotted_path))
    for depth in range(len(inner_parts) - 1, 0, -1):
        parent_path = ".".join([top_col] + inner_parts[:depth])
        expr = col(parent_path).withField(inner_parts[depth], expr)
    return df.withColumn(top_col, col(top_col).withField(inner_parts[0], expr))


def apply_structured_masking(
    df: DataFrame,
    column_type_map: Dict[str, str],
    strategy: MaskingStrategy = "mask",
    label_mode: str = "typed",
    encryption_key: Optional[str] = None,
    hash_algorithm: str = "sha2",
    hash_salt: Optional[str] = None,
) -> DataFrame:
    """Apply rule-based masking to structured PII columns in a single pass.

    All top-level columns are handled in one ``df.select()``.  Nested struct
    fields (dot-notation keys) are applied afterward via ``withColumn``.

    Args:
        df: Source DataFrame.
        column_type_map: Mapping of column name to pii_type
            (e.g. ``{"patient_ssn": "ssn", "phone": "phone"}``).
            Dot-notation is supported for nested structs
            (e.g. ``{"info.ssn": "ssn"}``).
        strategy: ``"mask"`` (regex/label), ``"hash"`` (SHA-256),
            or ``"encrypt"`` (AES + base64).
        label_mode: ``"typed"`` (fallback label is ``[PII_TYPE]``) or
            ``"generic"`` (fallback label is ``[REDACTED]``).  Only used
            when *strategy* is ``"mask"`` and the pii_type has no rule.
        encryption_key: Required when *strategy* is ``"encrypt"``.
        hash_algorithm: Reserved for future use (currently always SHA-256).
        hash_salt: Salt prepended before hashing when *strategy* is
            ``"hash"``. If ``None``, a random UUID is generated per call.
            Without a salt, low-cardinality fields (SSNs, zip codes) are
            vulnerable to rainbow-table reversal.

    Returns:
        DataFrame with PII columns masked/hashed/encrypted.
    """
    if not column_type_map:
        return df

    if strategy == "encrypt" and not encryption_key:
        raise ValueError("encryption_key is required when strategy='encrypt'")

    if strategy == "hash" and hash_salt is None:
        hash_salt = uuid.uuid4().hex
        logger.info("Generated random hash salt for this masking run (pass hash_salt= for deterministic joins)")

    salt = hash_salt or ""

    top_level: Dict[str, str] = {}
    nested: Dict[str, str] = {}
    for path, pii_type in column_type_map.items():
        if "." in path:
            nested[path] = pii_type
        else:
            top_level[path] = pii_type

    if top_level:
        select_exprs = []
        for field in df.schema:
            if field.name in top_level:
                pii_type = top_level[field.name]
                c = col(f"`{field.name}`")
                if strategy == "mask":
                    select_exprs.append(
                        _build_mask_expr(c, pii_type, label_mode).alias(field.name)
                    )
                elif strategy == "hash":
                    select_exprs.append(_build_hash_expr(c, salt).alias(field.name))
                else:
                    select_exprs.append(
                        _build_encrypt_expr(c, encryption_key).alias(field.name)
                    )
            else:
                select_exprs.append(col(f"`{field.name}`"))
        df = df.select(*select_exprs)

    for dotted_path, pii_type in nested.items():
        if strategy == "mask":
            fn = lambda c, pt=pii_type: _build_mask_expr(c, pt, label_mode)
        elif strategy == "hash":
            fn = lambda c, s=salt: _build_hash_expr(c, s)
        else:
            fn = lambda c, k=encryption_key: _build_encrypt_expr(c, k)
        df = _mask_nested_field(df, dotted_path, fn)

    logger.info(
        "Structured masking applied: strategy=%s, columns=%d (top=%d, nested=%d)",
        strategy, len(column_type_map), len(top_level), len(nested),
    )
    return df
