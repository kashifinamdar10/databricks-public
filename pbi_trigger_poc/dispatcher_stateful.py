# Databricks notebook source
# MAGIC %md
# MAGIC # Dispatcher (stateful, multi-table dependency + gate variant)
# MAGIC Refreshes a report only when **all** (ALL/AND) or **any** (ANY/OR) of its
# MAGIC dependency tables were updated **since that report last refreshed**.
# MAGIC
# MAGIC Durable Delta watermarks let dependencies arrive in **separate trigger runs**
# MAGIC minutes apart and still combine. Emits per-report task values:
# MAGIC   - `refresh_<id>`   = "true"/"false"  (consumed by condition gate)
# MAGIC   - `candidate_ts_<id>` = ISO ts       (consumed by commit_<id> AFTER refresh)
# MAGIC
# MAGIC COMMIT MODEL = at-least-once: the dispatcher does NOT advance report_state.
# MAGIC A downstream commit_<id> task advances it only after pbi_refresh_<id> succeeds.

# COMMAND ----------
import json
from pyspark.sql import functions as F

CAT, SCH = "mohdkashif_inamdar", "pbi_trigger_poc"
T_CONFIG = f"{CAT}.{SCH}.report_config"
T_DEPS   = f"{CAT}.{SCH}.report_dependencies"
T_TSTATE = f"{CAT}.{SCH}.table_state"
T_RSTATE = f"{CAT}.{SCH}.report_state"

dbutils.widgets.text("updated_tables", "")   # {{job.trigger.table_update.updated_tables}}
dbutils.widgets.text("run_id", "")           # {{job.run_id}}
dbutils.widgets.text("force_all", "false")   # manual full-refresh escape hatch

run_id = dbutils.widgets.get("run_id") or "manual"
force_all = dbutils.widgets.get("force_all").strip().lower() == "true"
raw = dbutils.widgets.get("updated_tables")

updated = []
try:
    if raw and raw.strip().startswith("["):
        updated = sorted({t.strip().lower() for t in json.loads(raw) if t and t.strip()})
except Exception as e:  # noqa
    print(f"WARN: could not parse updated_tables={raw!r}: {e}")
print(f"Trigger payload: {updated or '<empty / manual run>'}  force_all={force_all}  run_id={run_id}")

# Single server-side observation time for every table named in THIS run.
NOW = spark.sql("SELECT current_timestamp() AS t").collect()[0]["t"]

# COMMAND ----------
# 1) Upsert table watermarks for tables named in this trigger (durable state).
if updated:
    spark.createDataFrame(
        [(t, NOW, run_id) for t in updated],
        "source_table string, last_updated_ts timestamp, last_run_id string",
    ).createOrReplaceTempView("_updates")
    spark.sql(f"""
        MERGE INTO {T_TSTATE} AS s
        USING _updates AS u ON lower(s.source_table) = u.source_table
        WHEN MATCHED THEN UPDATE SET
          s.last_updated_ts = u.last_updated_ts, s.last_run_id = u.last_run_id
        WHEN NOT MATCHED THEN INSERT (source_table, last_updated_ts, last_run_id)
          VALUES (u.source_table, u.last_updated_ts, u.last_run_id)
    """)

# COMMAND ----------
# 2) Load config, dependency graph, watermarks.
cfg = {r["report_id"]: r.asDict() for r in
       spark.table(T_CONFIG).filter("enabled = true").collect()}
deps = {}
for r in spark.table(T_DEPS).collect():
    deps.setdefault(r["report_id"], []).append(r["source_table"].lower())
tstate = {r["source_table"].lower(): r["last_updated_ts"] for r in spark.table(T_TSTATE).collect()}
rstate = {r["report_id"]: r["last_refresh_ts"] for r in spark.table(T_RSTATE).collect()}

# COMMAND ----------
# 3) Evaluate dueness per report.
due, decisions = [], []
for rid, c in cfg.items():
    d = deps.get(rid, [])
    if not d:
        decisions.append((rid, False, "no dependencies configured")); continue
    r_wm = rstate.get(rid)  # None => never refreshed => any update counts
    fresh = {t: tstate[t] for t in d
             if tstate.get(t) is not None and (r_wm is None or tstate[t] > r_wm)}
    mode = (c.get("refresh_mode") or "ALL").upper()
    if force_all:
        is_due, why = True, "force_all"
    elif mode == "ANY":
        is_due, why = len(fresh) >= 1, f"ANY {len(fresh)}/{len(d)}"
    else:
        is_due, why = len(fresh) == len(d), f"ALL {len(fresh)}/{len(d)}"
    win = c.get("dependency_window_minutes")
    if is_due and not force_all and win and len(fresh) > 1:
        span = (max(fresh.values()) - min(fresh.values())).total_seconds() / 60.0
        if span > win:
            is_due, why = False, f"{why}; window {span:.1f}m>{win}m"
    decisions.append((rid, is_due, why))
    if is_due: due.append(rid)

for rid, is_due, why in decisions:
    print(f"  {rid:10s} due={str(is_due):5s} ({why})")

# COMMAND ----------
# 4) Emit gate + candidate-timestamp task values for EVERY report.
#    candidate_ts is what commit_<id> will write to report_state on success.
NOW_ISO = NOW.isoformat()
for rid in cfg:
    is_due = rid in due
    dbutils.jobs.taskValues.set(key=f"refresh_{rid}", value=("true" if is_due else "false"))
    dbutils.jobs.taskValues.set(key=f"candidate_ts_{rid}", value=(NOW_ISO if is_due else ""))
print(f"Reports to refresh ({len(due)}): {due or '<none>'}  candidate_ts={NOW_ISO}")

