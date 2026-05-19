# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table VACUUM Candidate Analyzer
# MAGIC
# MAGIC Scans every Delta table in a given catalog and ranks them by how much they
# MAGIC would benefit from `VACUUM`.
# MAGIC
# MAGIC ## How it works
# MAGIC - **Discovery**: lists schemas in the input catalog via
# MAGIC   `information_schema.schemata` (or just the schemas passed via widget), then
# MAGIC   every base table per schema. **Only EXTERNAL Delta tables are analyzed** —
# MAGIC   managed Delta tables are skipped because Unity Catalog Predictive
# MAGIC   Optimization handles VACUUM for them automatically. Views and non-Delta
# MAGIC   tables are also skipped.
# MAGIC - **History scan**: reads `DESCRIBE HISTORY` and finds the most recent
# MAGIC   `VACUUM END` row. Operations *before* that timestamp are ignored — those
# MAGIC   tombstones have already been physically deleted. `VACUUM START` / `VACUUM
# MAGIC   END` rows themselves are always excluded so we never double-count the
# MAGIC   files those operations cleaned up.
# MAGIC - **Removable-file estimation** (hybrid):
# MAGIC   - Tables ≤ `size_threshold_gb` → `VACUUM ... DRY RUN` (exact file list).
# MAGIC   - Tables > threshold → sum `numRemovedFiles` / `numTargetFilesRemoved`
# MAGIC     from `operationMetrics` of DELETE / UPDATE / MERGE / OPTIMIZE / WRITE /
# MAGIC     REORG / TRUNCATE / RESTORE rows after the last VACUUM. If a transaction
# MAGIC     removes 1000 files and rewrites 800, all 1000 old files are tombstoned
# MAGIC     and will be physically deleted by the next VACUUM once they age past
# MAGIC     retention — that's why this count is the right opportunity signal.
# MAGIC - **Output**: one row per table written to a Delta table, ranked by a
# MAGIC   `candidate_score` based on removable files and age since last VACUUM.
# MAGIC   Bytes are intentionally **not** reported — neither DRY RUN nor history
# MAGIC   reliably tells us how many bytes the next VACUUM will actually free.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog to analyze")
dbutils.widgets.text("schemas", "ALL", "Schema filter: ALL, single name, or comma-separated list")
dbutils.widgets.text("output_table", "", "Output Delta table (blank → <catalog>.vacuum_analysis.candidates)")
dbutils.widgets.text("size_threshold_gb", "100", "DRY RUN size cutoff (GB)")
dbutils.widgets.text("retention_hours", "168", "VACUUM retention hours for DRY RUN (default 168 = 7d)")

catalog = dbutils.widgets.get("catalog").strip()
if not catalog:
    raise ValueError("`catalog` widget is required")

schemas_raw = dbutils.widgets.get("schemas").strip()
if not schemas_raw or schemas_raw.upper() == "ALL":
    schema_filter = None  # means: scan every schema in the catalog
else:
    schema_filter = [s.strip() for s in schemas_raw.split(",") if s.strip()]

output_table = dbutils.widgets.get("output_table").strip() or f"{catalog}.vacuum_analysis.candidates"
size_threshold_gb = float(dbutils.widgets.get("size_threshold_gb"))
retention_hours = int(dbutils.widgets.get("retention_hours"))
size_threshold_bytes = int(size_threshold_gb * (1024 ** 3))

print(f"Catalog:         {catalog}")
print(f"Schemas:         {'ALL' if schema_filter is None else schema_filter}")
print(f"Output table:    {output_table}")
print(f"Size threshold:  {size_threshold_gb} GB  ({size_threshold_bytes:,} bytes)")
print(f"Retention hours: {retention_hours}")
print("Scope:           EXTERNAL Delta tables only (MANAGED handled by Predictive Optimization)")

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    BooleanType, TimestampType, IntegerType,
)

# operationMetrics keys that report tombstoned (removable) files.
# VACUUM rows are excluded entirely below — those numbers reflect files that
# have already been physically deleted, so summing them would double-count.
FILE_METRIC_KEYS = ("numRemovedFiles", "numTargetFilesRemoved")

def _bt(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"

def fq(catalog: str, schema: str, table: str) -> str:
    return f"{_bt(catalog)}.{_bt(schema)}.{_bt(table)}"

# COMMAND ----------

def list_schemas(catalog: str):
    df = spark.sql(
        f"SELECT schema_name FROM {_bt(catalog)}.information_schema.schemata"
    )
    return [
        r["schema_name"]
        for r in df.collect()
        if r["schema_name"].lower() != "information_schema"
    ]

def list_external_delta_tables(catalog: str, schema: str):
    # Only EXTERNAL Delta tables. Managed tables are skipped because UC
    # Predictive Optimization runs VACUUM (and OPTIMIZE) on them automatically.
    return spark.sql(f"""
        SELECT table_name, table_type, data_source_format
        FROM {_bt(catalog)}.information_schema.tables
        WHERE table_schema = '{schema}'
          AND table_type = 'EXTERNAL'
          AND UPPER(data_source_format) = 'DELTA'
    """).collect()

def describe_detail(full_name: str):
    return spark.sql(f"DESCRIBE DETAIL {full_name}").first().asDict()

def describe_history(full_name: str):
    return spark.sql(f"DESCRIBE HISTORY {full_name}").collect()

def last_vacuum_end_ts(history_rows):
    """Most recent successful VACUUM END timestamp, or None."""
    for r in history_rows:  # DESCRIBE HISTORY returns newest-first
        if r["operation"] == "VACUUM END":
            return r["timestamp"]
    return None

def sum_removable_from_history(history_rows, since_ts):
    """
    Aggregate tombstoned files from non-VACUUM ops *after* the last VACUUM.
    Skipping VACUUM rows is the key step that prevents double-counting.
    """
    files = 0
    op_count = 0
    for r in history_rows:
        if since_ts is not None and r["timestamp"] <= since_ts:
            continue
        op = r["operation"] or ""
        if op.startswith("VACUUM"):
            continue
        metrics = r["operationMetrics"] or {}
        counted = False
        for k in FILE_METRIC_KEYS:
            if k in metrics:
                try:
                    files += int(metrics[k]); counted = True
                except (TypeError, ValueError):
                    pass
        if counted:
            op_count += 1
    return files, op_count

def vacuum_dry_run_count(full_name: str, retention_hours: int) -> int:
    return spark.sql(
        f"VACUUM {full_name} RETAIN {retention_hours} HOURS DRY RUN"
    ).count()

# COMMAND ----------

result_schema = StructType([
    StructField("catalog", StringType()),
    StructField("schema", StringType()),
    StructField("table", StringType()),
    StructField("full_name", StringType()),
    StructField("is_delta", BooleanType()),
    StructField("size_bytes", LongType()),
    StructField("num_files", LongType()),
    StructField("last_vacuum_ts", TimestampType()),
    StructField("days_since_last_vacuum", DoubleType()),
    StructField("ever_vacuumed", BooleanType()),
    StructField("ops_since_last_vacuum", IntegerType()),
    StructField("est_files_to_clean", LongType()),
    StructField("estimation_method", StringType()),
    StructField("candidate_score", DoubleType()),
    StructField("reason", StringType()),
    StructField("error", StringType()),
    StructField("analyzed_at", TimestampType()),
])

# COMMAND ----------

now = datetime.now(timezone.utc).replace(tzinfo=None)
results = []
if schema_filter is None:
    schemas = list_schemas(catalog)
    print(f"Discovered {len(schemas)} schema(s) in `{catalog}`")
else:
    all_schemas = set(list_schemas(catalog))
    schemas = [s for s in schema_filter if s in all_schemas]
    missing = [s for s in schema_filter if s not in all_schemas]
    if missing:
        print(f"WARNING: requested schemas not found: {missing}")
    print(f"Scanning {len(schemas)} requested schema(s) in `{catalog}`")

for schema in schemas:
    try:
        tables = list_external_delta_tables(catalog, schema)
    except Exception as e:
        print(f"  schema `{schema}` skipped: {type(e).__name__}: {e}")
        continue
    print(f"  `{schema}`: {len(tables)} external Delta table(s)")
    for t in tables:
        table_name = t["table_name"]
        full_name = fq(catalog, schema, table_name)
        row = {
            "catalog": catalog,
            "schema": schema,
            "table": table_name,
            "full_name": f"{catalog}.{schema}.{table_name}",
            "is_delta": True,
            "size_bytes": None,
            "num_files": None,
            "last_vacuum_ts": None,
            "days_since_last_vacuum": None,
            "ever_vacuumed": None,
            "ops_since_last_vacuum": None,
            "est_files_to_clean": None,
            "estimation_method": None,
            "candidate_score": None,
            "reason": None,
            "error": None,
            "analyzed_at": now,
        }

        try:
            detail = describe_detail(full_name)
            size_bytes = int(detail.get("sizeInBytes") or 0)
            num_files = int(detail.get("numFiles") or 0)
            row["size_bytes"] = size_bytes
            row["num_files"] = num_files

            history = describe_history(full_name)
            lv = last_vacuum_end_ts(history)
            row["last_vacuum_ts"] = lv
            row["ever_vacuumed"] = lv is not None
            if lv is not None:
                row["days_since_last_vacuum"] = (now - lv).total_seconds() / 86400.0

            files_h, op_count = sum_removable_from_history(history, lv)
            row["ops_since_last_vacuum"] = op_count

            if size_bytes <= size_threshold_bytes:
                try:
                    dr_files = vacuum_dry_run_count(full_name, retention_hours)
                    row["est_files_to_clean"] = int(dr_files)
                    row["estimation_method"] = "dry_run"
                except Exception as e:
                    row["est_files_to_clean"] = files_h
                    row["estimation_method"] = f"history_fallback ({type(e).__name__})"
            else:
                row["est_files_to_clean"] = files_h
                row["estimation_method"] = "history"

            files_to_clean = row["est_files_to_clean"] or 0
            # Age multiplier: if never vacuumed, fall back to 30d so a freshly
            # created table with many removable files still scores reasonably.
            age_days = row["days_since_last_vacuum"]
            if age_days is None:
                age_days = 30.0
            age_mult = 1.0 + min(age_days, 365.0) / 30.0
            row["candidate_score"] = float(files_to_clean) * age_mult

            reasons = []
            if not row["ever_vacuumed"]:
                reasons.append("never vacuumed")
            else:
                reasons.append(f"last VACUUM {age_days:.1f}d ago")
            if files_to_clean > 0:
                reasons.append(f"~{files_to_clean:,} removable files")
            if op_count > 0:
                reasons.append(
                    f"{op_count} mutating op(s) since last VACUUM "
                    f"(VACUUM ops excluded to prevent double-counting)"
                )
            row["reason"] = "; ".join(reasons)
        except Exception as e:
            row["error"] = f"{type(e).__name__}: {e}"
        results.append(row)

print(f"Analyzed {len(results)} table(s)")

# COMMAND ----------

out_df = (
    spark.createDataFrame(results, schema=result_schema)
         .orderBy(F.col("candidate_score").desc_nulls_last())
)

out_catalog, out_schema, _ = output_table.split(".")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_bt(out_catalog)}.{_bt(out_schema)}")

(out_df.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(output_table))

print(f"Wrote {out_df.count()} rows to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top candidates

# COMMAND ----------

display(
    spark.table(output_table)
         .where("is_delta AND error IS NULL")
         .orderBy(F.col("candidate_score").desc_nulls_last())
         .limit(50)
)
