# Databricks notebook source
# MAGIC %md
# MAGIC # Load ai4privacy/pii-masking-300k Benchmark Data
# MAGIC
# MAGIC Downloads the ai4privacy/pii-masking-300k dataset from HuggingFace,
# MAGIC filters by language (English / Spanish), samples documents at 5/50/500 sizes,
# MAGIC and writes 6 benchmark tables to Unity Catalog in the dbxredact ground-truth format.

# COMMAND ----------

# MAGIC %pip install datasets
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "your_catalog", "Catalog")
dbutils.widgets.text("schema", "your_schema", "Schema")
dbutils.widgets.text("table_prefix", "pii_masking_300k", "Table Prefix")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_prefix = dbutils.widgets.get("table_prefix")

# COMMAND ----------

from datasets import load_dataset

ds = load_dataset("ai4privacy/pii-masking-300k", split="train")
print(f"Loaded {len(ds)} rows")

# COMMAND ----------

# Aligned with OpenAI Privacy Filter model card Section 7.2.1 category mapping.
# Categories without a Privacy Filter equivalent are mapped to dbxredact standard types.
LABEL_MAP = {
    # private_person
    "USERNAME": "PERSON", "FIRSTNAME": "PERSON",
    "GIVENNAME1": "PERSON", "GIVENNAME2": "PERSON",
    "LASTNAME1": "PERSON", "LASTNAME2": "PERSON", "LASTNAME3": "PERSON",
    "TITLE": "PERSON",
    # account_number (financial/ID identifiers)
    "ACCOUNT": "ACCOUNT_NUMBER", "BANKNUM": "ACCOUNT_NUMBER",
    "BIC": "ACCOUNT_NUMBER", "CREDITCARD": "ACCOUNT_NUMBER",
    "CRYPTOADDRESS": "ACCOUNT_NUMBER", "DOCNUM": "ACCOUNT_NUMBER",
    "IBAN": "ACCOUNT_NUMBER", "TAXNUM": "ACCOUNT_NUMBER",
    "SOCIALNUMBER": "ACCOUNT_NUMBER",
    "IDCARD": "ACCOUNT_NUMBER", "DRIVERLICENSE": "ACCOUNT_NUMBER",
    "PASSPORT": "ACCOUNT_NUMBER",
    # private_address
    "STREET": "ADDRESS", "CITY": "ADDRESS", "STATE": "ADDRESS",
    "POSTCODE": "ADDRESS", "COUNTRY": "ADDRESS", "BUILDING": "ADDRESS",
    "SECADDRESS": "ADDRESS", "GEOCOORD": "ADDRESS",
    "BANKMUNICIP": "ADDRESS", "BANKPOSTCODE": "ADDRESS", "BANKSTREET": "ADDRESS",
    # private_date
    "DATE": "DATE", "BOD": "DATE", "CARDEXPIRY": "DATE", "TIME": "DATE",
    # private_email
    "EMAIL": "EMAIL_ADDRESS",
    # private_phone
    "TEL": "PHONE_NUMBER",
    # private_url (OpenAI maps IP here too)
    "IP": "URL",
    # secret
    "PASS": "PASSWORD", "OTP": "PASSWORD", "PIN": "PASSWORD",
    # no Privacy Filter equivalent -- keep as-is for analysis
    "SEX": "DEMOGRAPHIC",
}

SAMPLE_SIZES = [5, 50, 500]
LANGUAGES = {"English": "en", "Spanish": "es"}

# COMMAND ----------

import random, json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

bench_schema = StructType([
    StructField("doc_id", StringType()),
    StructField("text", StringType()),
    StructField("begin", IntegerType()),
    StructField("end", IntegerType()),
    StructField("chunk", StringType()),
    StructField("chunk_label", StringType()),
])

random.seed(42)

for lang_name, lang_code in LANGUAGES.items():
    lang_rows = [r for r in ds if r.get("language") == lang_name]
    doc_ids = list({r["id"] for r in lang_rows})
    random.shuffle(doc_ids)
    print(f"{lang_name}: {len(doc_ids)} unique documents")

    docs_by_id = {}
    for r in lang_rows:
        docs_by_id.setdefault(r["id"], r)

    for n in SAMPLE_SIZES:
        sampled_ids = set(doc_ids[:n])
        rows = []
        for did in sampled_ids:
            doc = docs_by_id[did]
            text = doc["source_text"]
            masks = doc.get("privacy_mask") or []
            if isinstance(masks, str):
                masks = json.loads(masks)
            if not masks:
                rows.append((str(did), text, None, None, None, None))
                continue
            for m in masks:
                label = LABEL_MAP.get(m.get("label", ""), m.get("label", "OTHER"))
                rows.append((
                    str(did),
                    text,
                    int(m["start"]),
                    int(m["end"]),
                    m.get("value", text[int(m["start"]):int(m["end"])]),
                    label,
                ))
        table_name = f"{catalog}.{schema}.{table_prefix}_{lang_code}_{n}"
        df = spark.createDataFrame(rows, schema=bench_schema)
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        doc_count = df.select("doc_id").distinct().count()
        entity_count = df.filter("chunk IS NOT NULL").count()
        print(f"  Wrote {table_name}: {doc_count} docs, {entity_count} entities")

# COMMAND ----------

print("Done. Tables created:")
for lang_name, lang_code in LANGUAGES.items():
    for n in SAMPLE_SIZES:
        print(f"  {catalog}.{schema}.{table_prefix}_{lang_code}_{n}")
