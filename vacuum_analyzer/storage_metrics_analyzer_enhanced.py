# Databricks notebook source
# MAGIC %md
# MAGIC # Storage Metrics Analyzer (Enhanced)
# MAGIC
# MAGIC A comprehensive storage analysis notebook that collects **storage metrics**, **table metadata**, **maintenance history**, and **optimization readiness** for all tables in a given catalog and schema.
# MAGIC
# MAGIC ### Metrics Collected
# MAGIC | Category | What It Tells You |
# MAGIC |---|---|
# MAGIC | **Storage Metrics** | Total, active, vacuumable, and time-travel bytes (with MB/GB/TB conversions) |
# MAGIC | **Table Detail** | Format, location, file count, avg file size, partitioning, clustering, table features, protocol versions |
# MAGIC | **Table Properties** | Retention settings, deletion vectors, autoOptimize, autoCompaction |
# MAGIC | **Maintenance History** | Last OPTIMIZE, last VACUUM, days since each, total VACUUM operations |
# MAGIC | **Health Indicators** | Small file ratio, bloat ratio, optimization recommendations |
# MAGIC
# MAGIC ### Usage
# MAGIC 1. Enter your **Catalog** and **Schema** in the widgets
# MAGIC 2. Click **Run All**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Widgets for Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "1. Catalog Name")
dbutils.widgets.text("schema_name", "", "2. Schema Name")

CATALOG_NAME = dbutils.widgets.get("catalog_name").strip()
SCHEMA_NAME = dbutils.widgets.get("schema_name").strip()

assert CATALOG_NAME, "Please provide a Catalog Name in the widget above."
assert SCHEMA_NAME, "Please provide a Schema Name in the widget above."

print(f"Analyzing tables in: {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Discover All Tables

# COMMAND ----------

tables_df = spark.sql(f"""
    SELECT table_name, table_type
    FROM `{CATALOG_NAME}`.`information_schema`.`tables`
    WHERE table_schema = '{SCHEMA_NAME}'
      AND table_type IN ('MANAGED', 'EXTERNAL')
    ORDER BY table_name
""")

table_list = [row.table_name for row in tables_df.collect()]
table_types = {row.table_name: row.table_type for row in tables_df.collect()}
print(f"Found {len(table_list)} tables")
display(tables_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Collect All Metrics
# MAGIC
# MAGIC This cell collects four categories of metrics per table:
# MAGIC 1. **Storage Metrics** — `ANALYZE TABLE ... COMPUTE STORAGE METRICS`
# MAGIC 2. **Table Detail** — `DESCRIBE DETAIL`
# MAGIC 3. **Table Properties** — `SHOW TBLPROPERTIES`
# MAGIC 4. **Maintenance History** — `DESCRIBE HISTORY`

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import (
    col, lit, round as spark_round, first, when, sum as spark_sum,
    current_timestamp, datediff, current_date, max as spark_max,
    count as spark_count, coalesce
)
from datetime import datetime

# ---- Collectors ----
storage_results = []
detail_results = []
properties_results = []
history_results = []
failed_tables = []

for idx, table_name in enumerate(table_list, 1):
    fqn = f"`{CATALOG_NAME}`.`{SCHEMA_NAME}`.`{table_name}`"
    print(f"  [{idx}/{len(table_list)}] Processing: {table_name}")

    # --- 3a. Storage Metrics ---
    try:
        metrics_df = spark.sql(f"ANALYZE TABLE {fqn} COMPUTE STORAGE METRICS")
        for row in metrics_df.collect():
            storage_results.append({
                "table_name": table_name,
                "metric_name": row["metric_name"],
                "metric_value": float(row["metric_value"]) if row["metric_value"] is not None else 0.0,
            })
    except Exception as e:
        failed_tables.append({"table_name": table_name, "step": "STORAGE_METRICS", "error": str(e)})

    # --- 3b. Table Detail ---
    try:
        detail_df = spark.sql(f"DESCRIBE DETAIL {fqn}")
        row = detail_df.collect()[0]
        detail_results.append({
            "table_name": table_name,
            "table_type": table_types.get(table_name, "UNKNOWN"),
            "format": row["format"] if "format" in row.asDict() else None,
            "location": row["location"] if "location" in row.asDict() else None,
            "created_at": row["createdAt"] if "createdAt" in row.asDict() else None,
            "last_modified": row["lastModified"] if "lastModified" in row.asDict() else None,
            "num_files": row["numFiles"] if "numFiles" in row.asDict() else None,
            "size_in_bytes": row["sizeInBytes"] if "sizeInBytes" in row.asDict() else None,
            "partition_columns": ",".join(row["partitionColumns"]) if "partitionColumns" in row.asDict() and row["partitionColumns"] else "",
            "num_partitions": len(row["partitionColumns"]) if "partitionColumns" in row.asDict() and row["partitionColumns"] else 0,
            "clustering_columns": ",".join(row["clusteringColumns"]) if "clusteringColumns" in row.asDict() and row["clusteringColumns"] else "",
            "min_reader_version": row["minReaderVersion"] if "minReaderVersion" in row.asDict() else None,
            "min_writer_version": row["minWriterVersion"] if "minWriterVersion" in row.asDict() else None,
            "table_features": ",".join(row["tableFeatures"]) if "tableFeatures" in row.asDict() and row["tableFeatures"] else "",
        })
    except Exception as e:
        failed_tables.append({"table_name": table_name, "step": "DESCRIBE_DETAIL", "error": str(e)})

    # --- 3c. Table Properties ---
    try:
        props_df = spark.sql(f"SHOW TBLPROPERTIES {fqn}")
        props = {row["key"]: row["value"] for row in props_df.collect()}
        properties_results.append({
            "table_name": table_name,
            "delta_deletion_vectors": props.get("delta.enableDeletionVectors", "not set"),
            "delta_auto_optimize": props.get("delta.autoOptimize.optimizeWrite", "not set"),
            "delta_auto_compact": props.get("delta.autoOptimize.autoCompact", "not set"),
            "delta_log_retention": props.get("delta.logRetentionDuration", "not set"),
            "delta_deleted_file_retention": props.get("delta.deletedFileRetentionDuration", "not set"),
            "delta_checkpoint_interval": props.get("delta.checkpointInterval", "not set"),
            "delta_data_skipping_cols": props.get("delta.dataSkippingNumIndexedCols", "not set"),
            "delta_append_only": props.get("delta.appendOnly", "not set"),
            "delta_target_file_size": props.get("delta.targetFileSize", "not set"),
            "delta_tune_file_sizes": props.get("delta.tuneFileSizesForRewrites", "not set"),
        })
    except Exception as e:
        failed_tables.append({"table_name": table_name, "step": "TBLPROPERTIES", "error": str(e)})

    # --- 3d. Maintenance History ---
    try:
        hist_df = spark.sql(f"DESCRIBE HISTORY {fqn}")
        hist_rows = hist_df.select("operation", "timestamp", "operationMetrics").collect()

        last_optimize = None
        last_vacuum = None
        optimize_count = 0
        vacuum_count = 0
        last_write = None
        total_versions = len(hist_rows)

        for hrow in hist_rows:
            op = hrow["operation"]
            ts = hrow["timestamp"]
            if op == "OPTIMIZE":
                optimize_count += 1
                if last_optimize is None or ts > last_optimize:
                    last_optimize = ts
            elif op in ("VACUUM START", "VACUUM END"):
                if op == "VACUUM END":
                    vacuum_count += 1
                if last_vacuum is None or ts > last_vacuum:
                    last_vacuum = ts
            elif op in ("WRITE", "MERGE", "DELETE", "UPDATE", "CREATE TABLE", "CREATE TABLE AS SELECT"):
                if last_write is None or ts > last_write:
                    last_write = ts

        history_results.append({
            "table_name": table_name,
            "last_optimize": last_optimize,
            "last_vacuum": last_vacuum,
            "last_write_operation": last_write,
            "optimize_count_in_history": optimize_count,
            "vacuum_count_in_history": vacuum_count,
            "total_versions_in_history": total_versions,
        })
    except Exception as e:
        failed_tables.append({"table_name": table_name, "step": "HISTORY", "error": str(e)})

print(f"\nCollection complete. Failures: {len(failed_tables)}")
if failed_tables:
    for ft in failed_tables:
        print(f"  - {ft['table_name']} ({ft['step']}): {ft['error']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Build Pivoted Storage Metrics with Conversions

# COMMAND ----------

if not storage_results:
    dbutils.notebook.exit("No storage metrics collected.")

storage_schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("metric_name", StringType(), True),
    StructField("metric_value", DoubleType(), True),
])

storage_df = spark.createDataFrame(storage_results, schema=storage_schema)

# Pivot storage metrics
storage_pivoted = (
    storage_df
    .groupBy("table_name")
    .pivot("metric_name")
    .agg(first("metric_value"))
)

# Byte conversions
BYTES_TO_MB = 1024 * 1024
BYTES_TO_GB = 1024 * 1024 * 1024
BYTES_TO_TB = 1024 * 1024 * 1024 * 1024

byte_columns = ["total_bytes", "active_bytes", "vacuumable_bytes", "time_travel_bytes"]

for byte_col in byte_columns:
    if byte_col in storage_pivoted.columns:
        base = byte_col.replace("_bytes", "")
        storage_pivoted = (
            storage_pivoted
            .withColumn(f"{base}_mb", spark_round(col(byte_col) / BYTES_TO_MB, 2))
            .withColumn(f"{base}_gb", spark_round(col(byte_col) / BYTES_TO_GB, 4))
            .withColumn(f"{base}_tb", spark_round(col(byte_col) / BYTES_TO_TB, 6))
        )

# Space savings
if "vacuumable_bytes" in storage_pivoted.columns:
    storage_pivoted = (
        storage_pivoted
        .withColumn("space_saving_mb", spark_round(col("vacuumable_bytes") / BYTES_TO_MB, 2))
        .withColumn("space_saving_gb", spark_round(col("vacuumable_bytes") / BYTES_TO_GB, 4))
        .withColumn("space_saving_tb", spark_round(col("vacuumable_bytes") / BYTES_TO_TB, 6))
        .withColumn("saving_pct",
                    spark_round(
                        when(col("total_bytes") > 0,
                             (col("vacuumable_bytes") / col("total_bytes")) * 100
                        ).otherwise(0), 2))
        .withColumn("bloat_ratio",
                    spark_round(
                        when(col("active_bytes") > 0,
                             col("total_bytes") / col("active_bytes")
                        ).otherwise(0), 2))
    )

print("Storage metrics pivoted and converted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Build Detail, Properties, and History DataFrames

# COMMAND ----------

# Detail DataFrame
if detail_results:
    detail_df = spark.createDataFrame(detail_results)
    detail_df = (
        detail_df
        .withColumn("avg_file_size_mb",
                    spark_round(
                        when(col("num_files") > 0, col("size_in_bytes") / col("num_files") / BYTES_TO_MB)
                        .otherwise(0), 2))
        .withColumn("is_partitioned", when(col("num_partitions") > 0, lit("Yes")).otherwise(lit("No")))
        .withColumn("has_liquid_clustering",
                    when(col("clustering_columns") != "", lit("Yes")).otherwise(lit("No")))
        .withColumn("days_since_last_modified",
                    datediff(current_date(), col("last_modified")))
    )
else:
    detail_df = None

# Properties DataFrame
if properties_results:
    props_df = spark.createDataFrame(properties_results)
else:
    props_df = None

# History DataFrame
if history_results:
    history_df = spark.createDataFrame(history_results)
    history_df = (
        history_df
        .withColumn("days_since_last_optimize",
                    when(col("last_optimize").isNotNull(),
                         datediff(current_date(), col("last_optimize")))
                    .otherwise(lit(-1)))
        .withColumn("days_since_last_vacuum",
                    when(col("last_vacuum").isNotNull(),
                         datediff(current_date(), col("last_vacuum")))
                    .otherwise(lit(-1)))
        .withColumn("days_since_last_write",
                    when(col("last_write_operation").isNotNull(),
                         datediff(current_date(), col("last_write_operation")))
                    .otherwise(lit(-1)))
        .withColumn("vacuum_ever_run",
                    when(col("vacuum_count_in_history") > 0, lit("Yes")).otherwise(lit("No")))
        .withColumn("optimize_ever_run",
                    when(col("optimize_count_in_history") > 0, lit("Yes")).otherwise(lit("No")))
    )
else:
    history_df = None

print("All DataFrames built.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Join Everything into a Single Report

# COMMAND ----------

report_df = storage_pivoted

if detail_df is not None:
    report_df = report_df.join(detail_df, on="table_name", how="left")

if props_df is not None:
    report_df = report_df.join(props_df, on="table_name", how="left")

if history_df is not None:
    report_df = report_df.join(history_df, on="table_name", how="left")

# Add health recommendations
report_df = report_df.withColumn("recommendation",
    when((col("vacuum_ever_run") == "No") & (col("saving_pct") > 20),
         lit("CRITICAL: VACUUM never run, high reclaimable space"))
    .when((col("vacuum_ever_run") == "No"),
         lit("WARNING: VACUUM has never been run"))
    .when(col("days_since_last_vacuum") > 30,
         lit("WARNING: VACUUM not run in 30+ days"))
    .when((col("avg_file_size_mb") < 8) & (col("num_files") > 100),
         lit("WARNING: Small file problem detected - run OPTIMIZE"))
    .when(col("bloat_ratio") > 3,
         lit("WARNING: High bloat ratio - run VACUUM"))
    .when(col("saving_pct") > 50,
         lit("INFO: >50% storage is reclaimable"))
    .otherwise(lit("OK"))
)

# Add catalog and schema columns
report_df = (
    report_df
    .withColumn("catalog", lit(CATALOG_NAME))
    .withColumn("schema", lit(SCHEMA_NAME))
)

# Move catalog/schema to front
front_cols = ["catalog", "schema", "table_name", "table_type", "format", "recommendation"]
remaining = [c for c in report_df.columns if c not in front_cols]
report_df = report_df.select(front_cols + remaining)

print(f"Final report: {report_df.count()} tables, {len(report_df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Report — All Tables

# COMMAND ----------

display(report_df.orderBy("table_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storage Summary — Totals Across All Tables

# COMMAND ----------

summary = report_df.agg(
    spark_round(spark_sum(col("total_bytes")) / BYTES_TO_GB, 2).alias("total_storage_gb"),
    spark_round(spark_sum(col("active_bytes")) / BYTES_TO_GB, 2).alias("active_storage_gb"),
    spark_round(spark_sum(col("vacuumable_bytes")) / BYTES_TO_GB, 2).alias("reclaimable_by_vacuum_gb"),
    spark_round(spark_sum(col("time_travel_bytes")) / BYTES_TO_GB, 2).alias("time_travel_gb"),
    spark_round(
        when(spark_sum(col("total_bytes")) > 0,
             (spark_sum(col("vacuumable_bytes")) / spark_sum(col("total_bytes"))) * 100
        ).otherwise(0), 2
    ).alias("overall_saving_pct"),
    spark_sum(col("num_files")).cast("long").alias("total_files"),
    spark_count("table_name").alias("total_tables"),
)
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 20 Tables by Reclaimable Space

# COMMAND ----------

top_reclaimable = (
    report_df
    .select("table_name", "table_type", "total_gb", "active_gb",
            "space_saving_gb", "saving_pct", "bloat_ratio",
            "vacuum_ever_run", "days_since_last_vacuum", "recommendation")
    .orderBy(col("space_saving_gb").desc())
    .limit(20)
)
display(top_reclaimable)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maintenance Health — Tables Needing Attention

# COMMAND ----------

needs_attention = (
    report_df
    .filter(col("recommendation") != "OK")
    .select("table_name", "table_type", "recommendation",
            "total_gb", "space_saving_gb", "saving_pct",
            "vacuum_ever_run", "days_since_last_vacuum",
            "optimize_ever_run", "days_since_last_optimize",
            "avg_file_size_mb", "num_files")
    .orderBy(col("space_saving_gb").desc())
)
print(f"Tables needing attention: {needs_attention.count()}")
display(needs_attention)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Features and Optimization Readiness

# COMMAND ----------

features_df = (
    report_df
    .select("table_name", "table_type", "format",
            "is_partitioned", "partition_columns",
            "has_liquid_clustering", "clustering_columns",
            "table_features",
            "delta_deletion_vectors", "delta_auto_optimize", "delta_auto_compact",
            "delta_log_retention", "delta_deleted_file_retention",
            "min_reader_version", "min_writer_version")
    .orderBy("table_name")
)
display(features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Staleness — Days Since Last Activity

# COMMAND ----------

staleness_df = (
    report_df
    .select("table_name", "table_type",
            "last_modified", "days_since_last_modified",
            "last_write_operation", "days_since_last_write",
            "last_optimize", "days_since_last_optimize",
            "last_vacuum", "days_since_last_vacuum",
            "total_versions_in_history",
            "optimize_count_in_history", "vacuum_count_in_history")
    .orderBy(col("days_since_last_modified").desc())
)
display(staleness_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Small File Analysis

# COMMAND ----------

small_files_df = (
    report_df
    .select("table_name", "num_files", "size_in_bytes",
            "avg_file_size_mb", "total_gb",
            "is_partitioned", "has_liquid_clustering",
            "optimize_ever_run", "days_since_last_optimize")
    .withColumn("file_size_assessment",
                when(col("avg_file_size_mb") < 8, lit("Too Small (< 8 MB)"))
                .when(col("avg_file_size_mb") > 1024, lit("Too Large (> 1 GB)"))
                .otherwise(lit("Healthy")))
    .orderBy("avg_file_size_mb")
)
display(small_files_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Save Full Report to Delta Table

# COMMAND ----------

# Uncomment to save:
# report_df_with_ts = report_df.withColumn("analyzed_at", current_timestamp())
# report_df_with_ts.write.mode("append").saveAsTable(
#     f"`{CATALOG_NAME}`.`{SCHEMA_NAME}`.`_storage_metrics_history`"
# )
# print(f"Saved to {CATALOG_NAME}.{SCHEMA_NAME}._storage_metrics_history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup Widgets (Optional)

# COMMAND ----------

# dbutils.widgets.removeAll()
