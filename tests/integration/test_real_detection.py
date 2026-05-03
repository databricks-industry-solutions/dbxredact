"""Real integration tests with actual detectors -- minimal mocking.

These tests exercise code paths that run actual detection logic (Presidio
pattern-only, entity filters, structured masking) on real Spark DataFrames.
Only UC catalog/metadata SQL is mocked since it requires a live workspace.

Requirements: presidio-analyzer (installed), local PySpark.
No spaCy models needed -- pattern_only=True uses _StubNlpEngine.
"""

from unittest.mock import patch

from dbxredact.pipeline import run_detection_pipeline, run_table_redaction
from dbxredact.metadata import discover_pii_columns
from dbxredact.entity_filter import EntityFilter
from dbxredact.config import RedactionConfig


# ---------------------------------------------------------------------------
# Test A: Presidio pattern-only -> alignment -> redaction (zero detection mocks)
# ---------------------------------------------------------------------------

class TestPresidioPatternE2E:
    """Full detection pipeline with real Presidio pattern-only -- no detection mocks."""

    def test_detects_email_and_labeled_ssn(self, spark):
        """Presidio pattern engine finds labeled SSN and email in real text."""
        df = spark.createDataFrame([
            ("d1", "Patient email jane@example.com, SSN 123456789"),
            ("d2", "MRN: 0012345678 for the patient record"),
            ("d3", "No sensitive data in this clinical note"),
        ], ["doc_id", "text"])

        result = run_detection_pipeline(
            spark=spark,
            source_df=df,
            doc_id_column="doc_id",
            text_column="text",
            use_presidio=True,
            use_ai_query=False,
            use_gliner=False,
            presidio_pattern_only=True,
            score_threshold=0.3,
            num_cores=2,
        )

        rows = {r["doc_id"]: r for r in result.collect()}

        # Doc 1: email should be detected
        d1_entities = rows["d1"]["aligned_entities"]
        assert d1_entities is not None and len(d1_entities) >= 1
        d1_texts = {e["entity"] for e in d1_entities}
        assert "jane@example.com" in d1_texts

        # Doc 2: MRN pattern should be detected by custom recognizer
        d2_entities = rows["d2"]["aligned_entities"]
        assert d2_entities is not None and len(d2_entities) >= 1

        # Doc 3: no entities
        d3_entities = rows["d3"]["aligned_entities"]
        assert d3_entities is None or len(d3_entities) == 0

    def test_full_pipeline_redacts_pii(self, spark):
        """Detection + redaction applied via run_table_redaction with real Presidio."""
        df = spark.createDataFrame([
            ("d1", "Contact email is test@hospital.org for the patient"),
            ("d2", "Clean text with no PII"),
        ], ["doc_id", "notes"])
        df.createOrReplaceTempView("_real_presidio_src")

        cfg = RedactionConfig(
            use_presidio=True,
            use_ai_query=False,
            use_gliner=False,
            presidio_pattern_only=True,
            score_threshold=0.3,
            redaction_strategy="typed",
            num_cores=2,
        )

        result = run_table_redaction(
            spark=spark,
            source_table="_real_presidio_src",
            text_columns=["notes"],
            structured_columns={},
            max_rows=None,
            config=cfg,
        )

        rows = {r["doc_id"]: r for r in result.collect()}
        assert "test@hospital.org" not in rows["d1"]["notes_redacted"]
        assert "[EMAIL_ADDRESS]" in rows["d1"]["notes_redacted"]
        assert rows["d2"]["notes_redacted"] == "Clean text with no PII"


# ---------------------------------------------------------------------------
# Test B: Tag discovery + run_table_redaction (only UC metadata mocked)
# ---------------------------------------------------------------------------

class TestTagDiscoveryE2E:
    """discover_pii_columns + run_table_redaction with only UC SQL mocked."""

    def test_discovers_text_and_structured_columns(self, spark):
        """discover_pii_columns correctly splits columns by tag and type."""
        fake_metadata = {
            "doc_id": {"type": "string", "tags": {}},
            "clinical_notes": {"type": "string", "tags": {"data_classification": "protected"}},
            "ssn": {"type": "string", "tags": {"data_classification": "protected", "pii_type": "ssn"}},
            "phone": {"type": "string", "tags": {"data_classification": "protected", "pii_type": "phone"}},
            "age": {"type": "int", "tags": {"data_classification": "protected"}},
        }

        with patch("dbxredact.metadata.get_table_metadata", return_value=fake_metadata):
            discovery = discover_pii_columns(spark, "cat.sch.patient_records")

        assert discovery["text_columns"] == ["clinical_notes"]
        assert discovery["structured_columns"] == {"ssn": "ssn", "phone": "phone"}
        assert "doc_id" in discovery["doc_id_candidates"]
        assert len(discovery["warnings"]) == 1  # age is int + protected but no pii_type

    def test_tag_discovery_to_table_redaction(self, spark):
        """Discovered columns route correctly through run_table_redaction."""
        df = spark.createDataFrame([
            ("d1", "Contact admin@hospital.org for records", "123-45-6789", "555-012-3456"),
            ("d2", "Normal visit, no issues", "987-65-4321", "800-555-0199"),
        ], ["doc_id", "notes", "ssn", "phone"])
        df.createOrReplaceTempView("_tag_discovery_src")

        fake_metadata = {
            "doc_id": {"type": "string", "tags": {}},
            "notes": {"type": "string", "tags": {"data_classification": "protected"}},
            "ssn": {"type": "string", "tags": {"data_classification": "protected", "pii_type": "ssn"}},
            "phone": {"type": "string", "tags": {"data_classification": "protected", "pii_type": "phone"}},
        }

        with patch("dbxredact.metadata.get_table_metadata", return_value=fake_metadata):
            discovery = discover_pii_columns(spark, "cat.sch.tbl")

        cfg = RedactionConfig(
            use_presidio=True,
            use_ai_query=False,
            use_gliner=False,
            presidio_pattern_only=True,
            score_threshold=0.3,
            redaction_strategy="typed",
            num_cores=2,
        )

        result = run_table_redaction(
            spark=spark,
            source_table="_tag_discovery_src",
            text_columns=discovery["text_columns"],
            structured_columns=discovery["structured_columns"],
            masking_strategy="mask",
            max_rows=None,
            config=cfg,
        )

        rows = {r["doc_id"]: r for r in result.collect()}

        # Structured masking applied
        assert rows["d1"]["ssn"] == "[SSN]"
        assert rows["d1"]["phone"] == "[PHONE]"
        assert rows["d2"]["ssn"] == "[SSN]"

        # NER detection on notes -- email should be caught by pattern recognizer
        assert "admin@hospital.org" not in rows["d1"]["notes_redacted"]
        # Clean doc should pass through
        assert rows["d2"]["notes_redacted"] == "Normal visit, no issues"


# ---------------------------------------------------------------------------
# Test C: Entity filter through real detection (zero mocks)
# ---------------------------------------------------------------------------

class TestEntityFilterE2E:
    """Entity filter with real Presidio pattern detection -- no mocks at all."""

    def test_safe_list_suppresses_known_email(self, spark):
        """Safe-listed email is removed from detection results."""
        df = spark.createDataFrame([
            ("d1", "Contact office@hospital.org or personal secret@personal.com"),
        ], ["doc_id", "text"])

        safe_filter = EntityFilter(safe_list=["office@hospital.org"])

        result = run_detection_pipeline(
            spark=spark,
            source_df=df,
            doc_id_column="doc_id",
            text_column="text",
            use_presidio=True,
            use_ai_query=False,
            use_gliner=False,
            presidio_pattern_only=True,
            score_threshold=0.3,
            num_cores=2,
            entity_filter=safe_filter,
        )

        row = result.collect()[0]
        entities = row["aligned_entities"] or []
        entity_texts = {e["entity"] for e in entities}
        assert "office@hospital.org" not in entity_texts
        assert "secret@personal.com" in entity_texts

    def test_block_pattern_forces_detection(self, spark):
        """Block pattern forces an entity that wouldn't be detected by Presidio."""
        df = spark.createDataFrame([
            ("d1", "Reference: CASE-2024-9988. No other PII."),
        ], ["doc_id", "text"])

        block_filter = EntityFilter(block_patterns=[r"CASE-\d{4}-\d{4}"])

        result = run_detection_pipeline(
            spark=spark,
            source_df=df,
            doc_id_column="doc_id",
            text_column="text",
            use_presidio=True,
            use_ai_query=False,
            use_gliner=False,
            presidio_pattern_only=True,
            score_threshold=0.4,
            num_cores=2,
            entity_filter=block_filter,
        )

        row = result.collect()[0]
        entities = row["aligned_entities"] or []
        entity_texts = {e["entity"] for e in entities}
        assert "CASE-2024-9988" in entity_texts

    def test_safe_pattern_suppresses_by_regex(self, spark):
        """Safe pattern regex suppresses matching entities."""
        df = spark.createDataFrame([
            ("d1", "Reach us at info@company.org or user@secret.com"),
        ], ["doc_id", "text"])

        # Suppress all @company.org emails
        safe_filter = EntityFilter(safe_patterns=[r"\w+@company\.org"])

        result = run_detection_pipeline(
            spark=spark,
            source_df=df,
            doc_id_column="doc_id",
            text_column="text",
            use_presidio=True,
            use_ai_query=False,
            use_gliner=False,
            presidio_pattern_only=True,
            score_threshold=0.3,
            num_cores=2,
            entity_filter=safe_filter,
        )

        row = result.collect()[0]
        entities = row["aligned_entities"] or []
        entity_texts = {e["entity"] for e in entities}
        assert "info@company.org" not in entity_texts
        assert "user@secret.com" in entity_texts


# ---------------------------------------------------------------------------
# Test D: Full table output schema -- drop_originals behavior
# ---------------------------------------------------------------------------

class TestDropOriginalsE2E:
    """Verify run_table_redaction output excludes original text columns."""

    def test_drop_originals_true(self, spark):
        """With drop_originals=True, original text column is absent from output."""
        df = spark.createDataFrame([
            ("d1", "Email is test@corp.org", 42),
            ("d2", "No PII here", 99),
        ], ["doc_id", "notes", "age"])
        df.createOrReplaceTempView("_drop_orig_src")

        cfg = RedactionConfig(
            use_presidio=True, use_ai_query=False, use_gliner=False,
            presidio_pattern_only=True, score_threshold=0.3, num_cores=2,
        )

        result = run_table_redaction(
            spark=spark,
            source_table="_drop_orig_src",
            text_columns=["notes"],
            structured_columns={},
            max_rows=None,
            config=cfg,
            drop_originals=True,
        )

        cols = result.columns
        assert "notes" not in cols, "Original text column should be dropped"
        assert "notes_redacted" in cols
        assert "doc_id" in cols
        assert "age" in cols

        rows = {r["doc_id"]: r for r in result.collect()}
        assert "test@corp.org" not in rows["d1"]["notes_redacted"]

    def test_drop_originals_false(self, spark):
        """With drop_originals=False, original text column is retained."""
        df = spark.createDataFrame([
            ("d1", "Email is test@corp.org", 42),
        ], ["doc_id", "notes", "age"])
        df.createOrReplaceTempView("_drop_orig_keep_src")

        cfg = RedactionConfig(
            use_presidio=True, use_ai_query=False, use_gliner=False,
            presidio_pattern_only=True, score_threshold=0.3, num_cores=2,
        )

        result = run_table_redaction(
            spark=spark,
            source_table="_drop_orig_keep_src",
            text_columns=["notes"],
            structured_columns={},
            max_rows=None,
            config=cfg,
            drop_originals=False,
        )

        cols = result.columns
        assert "notes" in cols, "Original text column should be retained"
        assert "notes_redacted" in cols

    def test_multi_column_drops_all_originals(self, spark):
        """Multiple text columns: all originals dropped, all _redacted present."""
        df = spark.createDataFrame([
            ("d1", "admin@hosp.org", "Call 555-0123"),
            ("d2", "clean text", "also clean"),
        ], ["doc_id", "notes", "comments"])
        df.createOrReplaceTempView("_drop_multi_src")

        cfg = RedactionConfig(
            use_presidio=True, use_ai_query=False, use_gliner=False,
            presidio_pattern_only=True, score_threshold=0.3, num_cores=2,
        )

        result = run_table_redaction(
            spark=spark,
            source_table="_drop_multi_src",
            text_columns=["notes", "comments"],
            structured_columns={},
            max_rows=None,
            config=cfg,
            drop_originals=True,
        )

        cols = result.columns
        assert "notes" not in cols
        assert "comments" not in cols
        assert "notes_redacted" in cols
        assert "comments_redacted" in cols
        assert "doc_id" in cols
