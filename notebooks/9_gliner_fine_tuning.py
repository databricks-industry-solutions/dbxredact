# Databricks notebook source
# MAGIC %md
# MAGIC # GLiNER Fine-Tuning
# MAGIC
# MAGIC Fine-tune a GLiNER model on your organization's ground truth annotations.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Ground truth table with columns: `doc_id`, `text`, `entities` (JSON array of `{entity, entity_type, start, end}`)
# MAGIC - GPU cluster (single node, e.g. g5.xlarge / A10)
# MAGIC
# MAGIC **Outputs:**
# MAGIC - Fine-tuned model saved to a Unity Catalog Volume
# MAGIC - Evaluation metrics comparing base vs fine-tuned

# COMMAND ----------

# MAGIC %pip install gliner[train] --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("ground_truth_table", "", "Ground Truth Table (catalog.schema.table)")
dbutils.widgets.text("text_column", "text", "Text Column")
dbutils.widgets.text("entities_column", "entities", "Entities Column (JSON)")
dbutils.widgets.text("doc_id_column", "doc_id", "Document ID Column")
dbutils.widgets.text("base_model", "nvidia/gliner-PII", "Base GLiNER Model")
dbutils.widgets.text("output_volume", "", "Output Volume (/Volumes/catalog/schema/vol)")
dbutils.widgets.text("num_epochs", "3", "Training Epochs")
dbutils.widgets.text("learning_rate", "1e-5", "Learning Rate")
dbutils.widgets.text("train_split", "0.8", "Train Split Ratio")

# COMMAND ----------

import json
import os
import random
from pathlib import Path

ground_truth_table = dbutils.widgets.get("ground_truth_table")
text_column = dbutils.widgets.get("text_column")
entities_column = dbutils.widgets.get("entities_column")
doc_id_column = dbutils.widgets.get("doc_id_column")
base_model = dbutils.widgets.get("base_model")
output_volume = dbutils.widgets.get("output_volume")
num_epochs = int(dbutils.widgets.get("num_epochs"))
learning_rate = float(dbutils.widgets.get("learning_rate"))
train_split = float(dbutils.widgets.get("train_split"))

assert ground_truth_table, "ground_truth_table is required"
assert output_volume, "output_volume is required"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load and Prepare Data

# COMMAND ----------

df = spark.table(ground_truth_table).select(doc_id_column, text_column, entities_column).toPandas()
print(f"Loaded {len(df)} documents from {ground_truth_table}")

# COMMAND ----------

def row_to_gliner_sample(row):
    """Convert a ground truth row to GLiNER training format."""
    text = row[text_column]
    entities_raw = row[entities_column]
    if isinstance(entities_raw, str):
        entities_raw = json.loads(entities_raw)

    ner = []
    for ent in entities_raw:
        ner.append([ent["start"], ent["end"], ent["entity_type"]])

    tokenized = text.split()
    return {"tokenized_text": tokenized, "ner": ner, "text": text}


samples = [row_to_gliner_sample(row) for _, row in df.iterrows()]

random.seed(42)
random.shuffle(samples)
split_idx = int(len(samples) * train_split)
train_samples = samples[:split_idx]
val_samples = samples[split_idx:]
print(f"Train: {len(train_samples)}, Val: {len(val_samples)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fine-Tune Model

# COMMAND ----------

from gliner import GLiNER

model = GLiNER.from_pretrained(base_model)

entity_types = sorted({
    ent["entity_type"]
    for _, row in df.iterrows()
    for ent in (json.loads(row[entities_column]) if isinstance(row[entities_column], str) else row[entities_column])
})
print(f"Entity types: {entity_types}")

# COMMAND ----------

from gliner.training import TrainingArguments, Trainer

output_dir = "/tmp/gliner_finetune"

training_args = TrainingArguments(
    output_dir=output_dir,
    num_train_epochs=num_epochs,
    learning_rate=learning_rate,
    per_device_train_batch_size=4,
    per_device_eval_batch_size=4,
    evaluation_strategy="epoch",
    save_strategy="epoch",
    load_best_model_at_end=True,
    report_to="none",
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_samples,
    eval_dataset=val_samples,
)

trainer.train()
print("Training complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Evaluate Base vs Fine-Tuned

# COMMAND ----------

def evaluate_model(gliner_model, samples, labels, threshold=0.2):
    """Compute entity-level precision, recall, F1."""
    tp, fp, fn = 0, 0, 0
    for sample in samples:
        text = sample["text"]
        gold = {(s, e, t) for s, e, t in sample["ner"]}
        preds_raw = gliner_model.predict_entities(text, labels, threshold=threshold)
        preds = {(p["start"], p["end"], p["label"]) for p in preds_raw}
        tp += len(gold & preds)
        fp += len(preds - gold)
        fn += len(gold - preds)

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1, "tp": tp, "fp": fp, "fn": fn}


base_model_obj = GLiNER.from_pretrained(base_model)
base_metrics = evaluate_model(base_model_obj, val_samples, entity_types)
finetuned_metrics = evaluate_model(model, val_samples, entity_types)

print(f"Base model:       P={base_metrics['precision']:.3f}  R={base_metrics['recall']:.3f}  F1={base_metrics['f1']:.3f}")
print(f"Fine-tuned model: P={finetuned_metrics['precision']:.3f}  R={finetuned_metrics['recall']:.3f}  F1={finetuned_metrics['f1']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Save to Unity Catalog Volume

# COMMAND ----------

save_path = os.path.join(output_volume, "gliner_finetuned")
model.save_pretrained(save_path)
print(f"Model saved to {save_path}")

metrics_path = os.path.join(output_volume, "gliner_finetune_metrics.json")
with open(metrics_path, "w") as f:
    json.dump({"base": base_metrics, "finetuned": finetuned_metrics}, f, indent=2)
print(f"Metrics saved to {metrics_path}")
