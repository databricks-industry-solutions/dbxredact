# Databricks notebook source
"""
Integration Test Utilities for dbxredact

Provides helper functions for creating test data with known PII,
running assertions, and cleaning up test artifacts.
"""

import json
from pyspark.sql import SparkSession


class TestFailure(Exception):
    pass


class IntegrationTestUtils:
    def __init__(self, spark, test_catalog, test_schema):
        self.spark = spark
        self.test_catalog = test_catalog
        self.test_schema = test_schema
        self.test_artifacts = []

    def setup_test_environment(self):
        print(f"Setting up test environment: {self.test_catalog}.{self.test_schema}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.test_catalog}.{self.test_schema}")
        print(f"  Schema {self.test_schema} ready")
        try:
            self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {self.test_catalog}.{self.test_schema}.test_volume")
            print(f"  Volume test_volume ready")
        except Exception as e:
            print(f"  Note: Could not create volume: {e}")

    def _fqn(self, table_name):
        return f"{self.test_catalog}.{self.test_schema}.{table_name}"

    def _track(self, full_name):
        self.test_artifacts.append(("table", full_name))

    # ---- Table factories ----

    def create_pii_test_table(self, table_name="test_pii_data"):
        """Create a table with known PII for deterministic validation."""
        fqn = self._fqn(table_name)
        self.spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        self.spark.sql(f"""
            CREATE TABLE {fqn} (doc_id STRING, text STRING) USING DELTA
        """)
        self.spark.sql(f"""
            INSERT INTO {fqn} VALUES
            ('d1', 'Patient John Smith (SSN 123-45-6789) visited on 2024-01-15'),
            ('d2', 'Contact jane.doe@example.com or call 555-867-5309'),
            ('d3', 'Prescription for Alice Johnson, DOB 1985-03-22, MRN 98765'),
            ('d4', 'No personal information in this clinical note about hypertension treatment'),
            ('d5', 'Dr. Robert Chen prescribed metformin for patient ID 44221'),
            ('d6', 'Send records to 742 Evergreen Terrace, Springfield IL 62704'),
            ('d7', 'Credit card ending in 4242 was charged on 2024-02-28'),
            ('d8', 'Admit Bob Williams DOB 07/14/1990 to ICU bed 4A'),
            ('d9', 'Lab results for Mary Garcia SSN 987-65-4321 are pending'),
            ('d10', 'Follow-up scheduled for next Tuesday regarding blood pressure medication')
        """)
        self._track(fqn)
        print(f"  Created PII test table: {fqn} (10 rows)")
        return fqn

    def create_non_string_test_table(self, table_name="test_non_string"):
        """Create a table with an integer text column to test the TypeError fix."""
        fqn = self._fqn(table_name)
        self.spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        self.spark.sql(f"""
            CREATE TABLE {fqn} (doc_id STRING, text_col INT) USING DELTA
        """)
        self.spark.sql(f"""
            INSERT INTO {fqn} VALUES ('d1', 12345), ('d2', 67890), ('d3', 11111)
        """)
        self._track(fqn)
        print(f"  Created non-string test table: {fqn}")
        return fqn

    def create_tagged_test_table(self, table_name="test_tagged",
                                  tag_name="data_classification", tag_value="protected"):
        """Create a table and set a UC column tag on the text column."""
        fqn = self._fqn(table_name)
        self.spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        self.spark.sql(f"""
            CREATE TABLE {fqn} (doc_id STRING, text STRING, notes STRING) USING DELTA
        """)
        self.spark.sql(f"""
            INSERT INTO {fqn} VALUES
            ('d1', 'Patient John Smith SSN 123-45-6789', 'routine visit'),
            ('d2', 'Contact jane.doe@example.com', 'follow-up needed'),
            ('d3', 'No PII here', 'clean record')
        """)
        self.spark.sql(f"ALTER TABLE {fqn} ALTER COLUMN text SET TAGS ('{tag_name}' = '{tag_value}')")
        self._track(fqn)
        print(f"  Created tagged test table: {fqn} (tag {tag_name}={tag_value} on 'text')")
        return fqn

    def create_large_test_table(self, table_name="test_large", num_rows=100):
        """Create a table with many rows for max_rows testing."""
        fqn = self._fqn(table_name)
        self.spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        rows = [(f"d{i}", f"Patient Person_{i} SSN {100+i}-00-{1000+i}") for i in range(num_rows)]
        df = self.spark.createDataFrame(rows, ["doc_id", "text"])
        df.write.format("delta").saveAsTable(fqn)
        self._track(fqn)
        print(f"  Created large test table: {fqn} ({num_rows} rows)")
        return fqn

    def create_filter_table(self, table_name="test_safe_list"):
        """Create an entity filter safe-list table."""
        fqn = self._fqn(table_name)
        self.spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        self.spark.sql(f"""
            CREATE TABLE {fqn} (entity_text STRING, entity_type STRING, list_type STRING) USING DELTA
        """)
        self.spark.sql(f"""
            INSERT INTO {fqn} VALUES
            ('Acme Corp', 'ORGANIZATION', 'safe'),
            ('Springfield', 'LOCATION', 'safe')
        """)
        self._track(fqn)
        print(f"  Created filter table: {fqn}")
        return fqn

    # ---- Cleanup ----

    def cleanup_test_artifacts(self):
        print("\nCleaning up test artifacts...")
        for artifact_type, name in self.test_artifacts:
            try:
                if artifact_type == "table":
                    self.spark.sql(f"DROP TABLE IF EXISTS {name}")
                    print(f"  Dropped: {name}")
            except Exception as e:
                print(f"  Could not cleanup {name}: {e}")
        self.test_artifacts = []

    def track_table(self, full_name):
        self.test_artifacts.append(("table", full_name))

    # ---- Assertions ----

    def assert_true(self, condition, message):
        if not condition:
            raise TestFailure(f"ASSERTION FAILED: {message}")
        print(f"  PASS: {message}")

    def assert_false(self, condition, message):
        if condition:
            raise TestFailure(f"ASSERTION FAILED: {message}")
        print(f"  PASS: {message}")

    def assert_equals(self, actual, expected, message):
        if actual != expected:
            raise TestFailure(f"ASSERTION FAILED: {message}\n  Expected: {expected}\n  Actual: {actual}")
        print(f"  PASS: {message}")

    def assert_not_none(self, value, message):
        if value is None:
            raise TestFailure(f"ASSERTION FAILED: {message} (value is None)")
        print(f"  PASS: {message}")

    def assert_contains(self, text, substring, message):
        if substring not in str(text):
            raise TestFailure(f"ASSERTION FAILED: {message}\n  Expected '{substring}' in: {text}")
        print(f"  PASS: {message}")

    def assert_greater(self, actual, threshold, message):
        if actual <= threshold:
            raise TestFailure(f"ASSERTION FAILED: {message}\n  Expected > {threshold}, got {actual}")
        print(f"  PASS: {message}")

    def assert_less_equal(self, actual, threshold, message):
        if actual > threshold:
            raise TestFailure(f"ASSERTION FAILED: {message}\n  Expected <= {threshold}, got {actual}")
        print(f"  PASS: {message}")


# ---- Verification helpers ----

def verify_output_table(spark, output_table, expected_columns=None):
    """Verify output table exists and has expected columns."""
    df = spark.table(output_table)
    count = df.count()
    if count == 0:
        raise TestFailure(f"Output table {output_table} is empty")
    print(f"  Output table {output_table}: {count} rows")
    if expected_columns:
        actual_cols = set(df.columns)
        missing = set(expected_columns) - actual_cols
        if missing:
            raise TestFailure(f"Missing columns in {output_table}: {missing}\n  Actual: {actual_cols}")
        print(f"  PASS: All expected columns present: {expected_columns}")
    return df


def verify_no_pii_in_redacted(df, redacted_col, known_pii_strings):
    """Spot-check that known PII strings are absent from the redacted column."""
    rows = df.select(redacted_col).collect()
    for row in rows:
        text = row[0] or ""
        for pii in known_pii_strings:
            if pii in text:
                raise TestFailure(f"PII '{pii}' found in redacted output: {text[:200]}")
    print(f"  PASS: No known PII found in '{redacted_col}' ({len(known_pii_strings)} patterns checked)")


def verify_entities_detected(df, entities_col, min_total=1):
    """Verify at least min_total entities were detected across all rows."""
    from pyspark.sql.functions import size, sum as spark_sum
    total = df.select(spark_sum(size(entities_col))).collect()[0][0] or 0
    if total < min_total:
        raise TestFailure(f"Expected >= {min_total} entities in '{entities_col}', got {total}")
    print(f"  PASS: {total} entities detected in '{entities_col}'")
    return total


def verify_redaction_markers(df, redacted_col, markers):
    """Verify that at least one expected marker appears in the redacted output."""
    rows = df.select(redacted_col).collect()
    all_text = " ".join(row[0] or "" for row in rows)
    found = [m for m in markers if m in all_text]
    if not found:
        raise TestFailure(f"No redaction markers {markers} found in '{redacted_col}'")
    print(f"  PASS: Found redaction markers: {found}")


# ---- Display helpers ----

def print_test_header(test_name):
    print("\n" + "=" * 80)
    print(f"  {test_name}")
    print("=" * 80)


def print_test_result(test_name, passed, error=None):
    print("\n" + "=" * 80)
    if passed:
        print(f"  TEST PASSED: {test_name}")
    else:
        print(f"  TEST FAILED: {test_name}")
        if error:
            print(f"  Error: {error}")
    print("=" * 80)
