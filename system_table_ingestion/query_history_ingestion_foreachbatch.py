# Databricks notebook source
# MAGIC %md
# MAGIC # FIXED streaming ingestion: ignoreChanges + foreachBatch MERGE
# MAGIC
# MAGIC The buggy version uses `skipChangeCommits=true`, which drops any commit
# MAGIC that contains a `remove` action — including the UPDATEs that
# MAGIC `system.query.history` emits as it finalizes/corrects rows. The result
# MAGIC is rows that are missing entirely or stuck in a stale state.
# MAGIC
# MAGIC This version:
# MAGIC   * uses `ignoreChanges=true` so updates re-emit affected files
# MAGIC   * inside `foreachBatch`, MERGEs into the sink keyed on `statement_id`,
# MAGIC     keeping the row with the largest `update_time`
# MAGIC
# MAGIC Trade-off: re-emitted rows duplicate within a batch, which the MERGE
# MAGIC dedupes. Throughput cost is small relative to losing data.

# COMMAND ----------

SINK_TABLE = "users.mohdkashif_inamdar.stream_repro_sink_fixed"
DBFS_CHECKPOINT = "dbfs:/tmp/mohdkashif_inamdar/stream_repro_chk/sink_fixed"
dbutils.fs.mkdirs(DBFS_CHECKPOINT)

# COMMAND ----------

# Create the sink table with the same schema as the source up front so MERGE
# can target it even on the first run.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SINK_TABLE}
USING DELTA
AS SELECT * FROM system.query.history WHERE 1=0
""")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

def merge_batch(batch_df, batch_id):
    # Dedupe within the batch: same statement_id may appear multiple times
    # when ignoreChanges re-emits a file; keep the row with the latest update_time
    ranked = (batch_df
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("statement_id").orderBy(F.col("update_time").desc_nulls_last())))
        .filter("_rn = 1").drop("_rn"))

    sink = DeltaTable.forName(spark, SINK_TABLE)
    (sink.alias("t")
        .merge(ranked.alias("s"), "t.statement_id = s.statement_id")
        .whenMatchedUpdateAll(condition="s.update_time > t.update_time OR t.update_time IS NULL")
        .whenNotMatchedInsertAll()
        .execute())

(spark.readStream
    .option("ignoreChanges", "true")          # <-- key change vs. buggy version
    .table("system.query.history")
    .writeStream
    .option("checkpointLocation", DBFS_CHECKPOINT)
    .trigger(availableNow=True)
    .foreachBatch(merge_batch)
    .start()
    .awaitTermination()
)

# COMMAND ----------

display(spark.sql(f"""
SELECT
   (SELECT COUNT(*) FROM {SINK_TABLE}) AS rows_in_sink_fixed,
   (SELECT COUNT(*) FROM users.mohdkashif_inamdar.stream_repro_sink) AS rows_in_sink_buggy
"""))

