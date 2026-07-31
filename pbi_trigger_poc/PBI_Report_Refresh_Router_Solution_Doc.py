# Databricks notebook source
# DBTITLE 1,Solution Overview
# MAGIC %md
# MAGIC # Power BI Report Refresh Router — Stateful Multi-Table Dependencies
# MAGIC
# MAGIC ## Solution Overview
# MAGIC
# MAGIC This solution implements an **event-driven, stateful architecture** that automatically triggers Power BI semantic model refreshes when Unity Catalog tables are updated in Databricks. It solves the key challenge of **multi-table dependencies** — a report that depends on tables A *and* B should only refresh when *both* have been updated since the last refresh, even if those updates arrive in separate trigger runs minutes apart.
# MAGIC
# MAGIC ### Key Benefits
# MAGIC * **Event-driven**: Reports refresh only when their source data changes (no polling, no fixed schedule)
# MAGIC * **Stateful watermarks**: Durable Delta tables track per-table update timestamps and per-report refresh timestamps, allowing dependencies to accumulate across multiple trigger runs
# MAGIC * **Multi-table dependency logic**: Supports `ALL` (every dependency must be updated) and `ANY` (at least one dependency updated) modes per report
# MAGIC * **At-least-once commit model**: Report watermarks advance only *after* a successful PBI refresh — failed refreshes automatically retry on the next trigger
# MAGIC * **Config-driven**: Adding new reports requires config table rows + job DAG tasks — no code changes
# MAGIC * **Native PBI tasks**: Uses the Lakeflow Jobs Power BI task type — no custom REST API code
# MAGIC * **Force-refresh escape hatch**: A `force_all` parameter allows manual full-refresh of all reports
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                     Databricks Lakeflow Job                                   │
# MAGIC │  Trigger: TABLE_UPDATE on [table_A, table_B, table_C]                         │
# MAGIC │  Params:  updated_tables, force_all                                           │
# MAGIC ├─────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                               │
# MAGIC │   ┌────────────────────┐      ┌────────────────────────────────────────┐       │
# MAGIC │   │  dispatcher_stateful │      │  Delta State Tables:                       │       │
# MAGIC │   │  (notebook)          │────▶│  • table_state   (per-table watermarks)   │       │
# MAGIC │   │                      │────◀│  • report_state  (per-report watermarks)  │       │
# MAGIC │   │  1. Upsert table_state│      │  • report_config (settings)              │       │
# MAGIC │   │  2. Evaluate dueness  │      │  • report_dependencies (graph)           │       │
# MAGIC │   │  3. Emit task values  │      └────────────────────────────────────────┘       │
# MAGIC │   └──────────┬─────────┘                                                      │
# MAGIC │              │                                                               │
# MAGIC │    ┌─────────┼───────────────────┐                                             │
# MAGIC │    ▼                             ▼                                             │
# MAGIC │ ┌─────────────┐          ┌─────────────┐                                       │
# MAGIC │ │ gate_report1 │          │ gate_report2 │    ← Condition Tasks (IF true)      │
# MAGIC │ └──────┬──────┘          └──────┬──────┘                                       │
# MAGIC │        ▼                         ▼                                             │
# MAGIC │ ┌───────────────┐     ┌───────────────┐                                      │
# MAGIC │ │refresh_report1│     │refresh_report2│    ← Native Power BI Tasks             │
# MAGIC │ │ (PBI task)     │     │ (PBI task)     │      (Import + Refresh after update) │
# MAGIC │ └───────┬───────┘     └───────┬───────┘                                      │
# MAGIC │         ▼                       ▼                                             │
# MAGIC │ ┌───────────────┐     ┌───────────────┐                                      │
# MAGIC │ │commit_report1 │     │commit_report2 │    ← Advance report_state watermark   │
# MAGIC │ │ (notebook)     │     │ (notebook)     │      only on SUCCESS                │
# MAGIC │ └───────────────┘     └───────────────┘                                      │
# MAGIC │                                                                               │
# MAGIC └─────────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,How It Works — Stateful Multi-Table Dependencies
# MAGIC %md
# MAGIC ## How It Works — Stateful Multi-Table Dependencies
# MAGIC
# MAGIC ### The Core Problem This Solves
# MAGIC
# MAGIC Consider a Power BI report that depends on **two** tables (e.g. `sales_orders` AND `finance_ledger`). These tables may be updated by different upstream pipelines that finish minutes apart. A simple "refresh when any table updates" approach would:
# MAGIC * Refresh the report after `sales_orders` lands (but `finance_ledger` is stale) — **wasted refresh**
# MAGIC * Refresh again after `finance_ledger` lands — correct data, but double the PBI API calls
# MAGIC
# MAGIC This solution uses **durable Delta watermarks** to track which tables have been updated since each report’s last refresh, and only triggers when the complete dependency set is satisfied.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 1: Table-Update Trigger (Event Source)
# MAGIC The job is configured with a **Table Update Trigger**:
# MAGIC
# MAGIC | Setting | Value | Purpose |
# MAGIC |---------|-------|---------|
# MAGIC | `table_names` | All source tables across all reports | Tables to monitor |
# MAGIC | `condition` | `ANY_UPDATED` | Fire when *any* monitored table changes |
# MAGIC | `min_time_between_triggers_seconds` | `60` | Debounce to avoid rapid-fire runs |
# MAGIC | `wait_after_last_change_seconds` | `90` | Let burst writes settle before triggering |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 2: Dispatcher — Stateful Watermark Logic
# MAGIC
# MAGIC The `dispatcher_stateful` notebook runs as the first task and performs three operations:
# MAGIC
# MAGIC #### 2a. Upsert `table_state` (durable per-table watermarks)
# MAGIC For each table in the trigger payload, MERGE into `table_state` with the current timestamp. This persists the knowledge that "table X was updated at time T" **across job runs**.
# MAGIC
# MAGIC #### 2b. Evaluate dueness per report
# MAGIC For each enabled report in `report_config`:
# MAGIC 1. Look up its dependencies from `report_dependencies`
# MAGIC 2. Compare each dependency’s `table_state.last_updated_ts` against the report’s `report_state.last_refresh_ts`
# MAGIC 3. Apply the report’s `refresh_mode`:
# MAGIC    - **`ALL`** (default): Refresh only when **every** dependency has been updated since last refresh
# MAGIC    - **`ANY`**: Refresh when **at least one** dependency has been updated
# MAGIC 4. Optionally check `dependency_window_minutes` — if the freshest and oldest dependency updates are more than N minutes apart, skip (avoids refreshing with time-skewed data)
# MAGIC
# MAGIC #### 2c. Emit task values
# MAGIC For each report, set:
# MAGIC * `refresh_<report_id>` = `"true"` / `"false"` — consumed by condition gate
# MAGIC * `candidate_ts_<report_id>` = ISO timestamp — consumed by commit task
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 3: Condition Gates (Selective Execution)
# MAGIC For each report, a **Condition Task** checks the dispatcher output:
# MAGIC ```
# MAGIC Condition: {{tasks.dispatcher.values.refresh_<report_id>}} EQUAL_TO "true"
# MAGIC ```
# MAGIC * `true` → downstream PBI refresh task runs
# MAGIC * `false` → skipped (zero compute)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 4: Power BI Tasks (Native Refresh)
# MAGIC Each refresh task uses the **native Power BI task type** in Lakeflow Jobs:
# MAGIC * Connects via a **Unity Catalog Power BI Connection**
# MAGIC * Targets a specific **workspace** and **semantic model**
# MAGIC * Uses **Import** mode with **"Refresh after update"** ✅ to trigger the PBI dataset refresh
# MAGIC * No custom REST API code or secret management needed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 5: Commit State (At-Least-Once Guarantee)
# MAGIC A `commit_<report_id>` task runs **only after** the PBI refresh succeeds. It:
# MAGIC 1. Receives the `candidate_ts` from the dispatcher (the observation timestamp)
# MAGIC 2. MERGEs into `report_state`, advancing `last_refresh_ts` to the candidate timestamp
# MAGIC
# MAGIC **Why this matters:**
# MAGIC * If the PBI refresh **fails**, the commit task never runs, so `report_state` stays at the old watermark
# MAGIC * On the next trigger, the dispatcher re-evaluates and finds the report is still "due" — **automatic retry**
# MAGIC * This is the **at-least-once** guarantee: a report may be refreshed more than once in edge cases, but never silently missed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Worked Example
# MAGIC
# MAGIC ```
# MAGIC Report1 depends on: table_A (ALL mode) AND table_B
# MAGIC Report2 depends on: table_B (ALL mode) AND table_C
# MAGIC
# MAGIC Trigger Run #1: table_A updated
# MAGIC   → table_state: A=T1
# MAGIC   → Report1: A=fresh, B=stale → 1/2 ALL → NOT DUE
# MAGIC   → Report2: B=stale, C=stale → 0/2 ALL → NOT DUE
# MAGIC   → No refreshes
# MAGIC
# MAGIC Trigger Run #2: table_B updated
# MAGIC   → table_state: A=T1, B=T2
# MAGIC   → Report1: A=fresh, B=fresh → 2/2 ALL → DUE ✅
# MAGIC   → Report2: B=fresh, C=stale → 1/2 ALL → NOT DUE
# MAGIC   → Report1 refreshes, commit advances Report1 watermark
# MAGIC
# MAGIC Trigger Run #3: table_C updated
# MAGIC   → table_state: A=T1, B=T2, C=T3
# MAGIC   → Report1: A=stale(below new wm), B=stale → 0/2 ALL → NOT DUE
# MAGIC   → Report2: B=fresh, C=fresh → 2/2 ALL → DUE ✅
# MAGIC   → Report2 refreshes
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Implementation Guide — Tables & Notebooks
# MAGIC %md
# MAGIC ## Implementation Guide
# MAGIC
# MAGIC ### Prerequisites
# MAGIC * Databricks workspace with **Unity Catalog** enabled
# MAGIC * Source tables registered in Unity Catalog (managed or external Delta tables)
# MAGIC * A **SQL Warehouse** (required by the Power BI task for Import-mode refreshes)
# MAGIC * **Azure AD / Entra ID App Registration** with Power BI API permissions
# MAGIC * A **Power BI Connection** in Unity Catalog
# MAGIC * **Service principal** or user (job Run-As identity) with:
# MAGIC   - `USE CONNECTION` on the PBI connection
# MAGIC   - `SELECT` on source UC tables and config tables
# MAGIC   - `INSERT`/`UPDATE` on state tables (`table_state`, `report_state`)
# MAGIC   - Access to the SQL Warehouse
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 1: Create the Configuration & State Tables
# MAGIC
# MAGIC #### 1a. `report_config` — Report settings
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS <catalog>.<schema>.report_config (
# MAGIC   report_id                  STRING   COMMENT 'Unique report identifier (used in task keys)',
# MAGIC   report_name                STRING   COMMENT 'Human-readable name for logging',
# MAGIC   powerbi_workspace          STRING   COMMENT 'Power BI workspace name',
# MAGIC   powerbi_model              STRING   COMMENT 'Power BI semantic model name',
# MAGIC   refresh_mode               STRING   COMMENT 'ALL = all deps must update; ANY = at least one',
# MAGIC   dependency_window_minutes  INT      COMMENT 'Optional: max time spread between dep updates (NULL = unlimited)',
# MAGIC   enabled                    BOOLEAN  COMMENT 'Set false to pause refreshes'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC #### 1b. `report_dependencies` — Dependency graph (many-to-many)
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS <catalog>.<schema>.report_dependencies (
# MAGIC   report_id     STRING   COMMENT 'FK to report_config.report_id',
# MAGIC   source_table  STRING   COMMENT 'Fully-qualified UC table name this report depends on'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC #### 1c. `table_state` — Durable per-table watermarks
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS <catalog>.<schema>.table_state (
# MAGIC   source_table     STRING     COMMENT 'Fully-qualified UC table name',
# MAGIC   last_updated_ts  TIMESTAMP  COMMENT 'When this table was last seen in a trigger payload',
# MAGIC   last_run_id      STRING     COMMENT 'Job run ID that recorded this update'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC #### 1d. `report_state` — Durable per-report watermarks
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS <catalog>.<schema>.report_state (
# MAGIC   report_id            STRING     COMMENT 'FK to report_config.report_id',
# MAGIC   last_refresh_ts      TIMESTAMP  COMMENT 'Timestamp of the last successful refresh',
# MAGIC   last_refresh_run_id  STRING     COMMENT 'Job run ID that committed this refresh'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC #### Example data
# MAGIC ```sql
# MAGIC -- Report 1 depends on BOTH sales_orders AND finance_ledger (ALL mode)
# MAGIC INSERT INTO <catalog>.<schema>.report_config VALUES
# MAGIC   ('report1', 'Report 1 (A&B)', 'R1 WS', 'Report 1 Model', 'ALL', NULL, true),
# MAGIC   ('report2', 'Report 2 (B&C)', 'R2 WS', 'Report 2 Model', 'ALL', NULL, true);
# MAGIC
# MAGIC INSERT INTO <catalog>.<schema>.report_dependencies VALUES
# MAGIC   ('report1', '<catalog>.<schema>.sales_orders'),
# MAGIC   ('report1', '<catalog>.<schema>.finance_ledger'),
# MAGIC   ('report2', '<catalog>.<schema>.finance_ledger'),
# MAGIC   ('report2', '<catalog>.<schema>.ops_events');
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 2: Create the Stateful Dispatcher Notebook
# MAGIC
# MAGIC Create `dispatcher_stateful.py`:
# MAGIC
# MAGIC ```python
# MAGIC import json
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC CAT, SCH = "<catalog>", "<schema>"
# MAGIC T_CONFIG = f"{CAT}.{SCH}.report_config"
# MAGIC T_DEPS   = f"{CAT}.{SCH}.report_dependencies"
# MAGIC T_TSTATE = f"{CAT}.{SCH}.table_state"
# MAGIC T_RSTATE = f"{CAT}.{SCH}.report_state"
# MAGIC
# MAGIC dbutils.widgets.text("updated_tables", "")  # {{job.trigger.table_update.updated_tables}}
# MAGIC dbutils.widgets.text("run_id", "")          # {{job.run_id}}
# MAGIC dbutils.widgets.text("force_all", "false")  # manual full-refresh escape hatch
# MAGIC
# MAGIC run_id = dbutils.widgets.get("run_id") or "manual"
# MAGIC force_all = dbutils.widgets.get("force_all").strip().lower() == "true"
# MAGIC raw = dbutils.widgets.get("updated_tables")
# MAGIC
# MAGIC updated = []
# MAGIC try:
# MAGIC     if raw and raw.strip().startswith("["):
# MAGIC         updated = sorted({t.strip().lower() for t in json.loads(raw) if t and t.strip()})
# MAGIC except Exception as e:
# MAGIC     print(f"WARN: could not parse updated_tables={raw!r}: {e}")
# MAGIC
# MAGIC NOW = spark.sql("SELECT current_timestamp() AS t").collect()[0]["t"]
# MAGIC
# MAGIC # ---- 1) Persist table watermarks (durable across runs) ----
# MAGIC if updated:
# MAGIC     spark.createDataFrame(
# MAGIC         [(t, NOW, run_id) for t in updated],
# MAGIC         "source_table string, last_updated_ts timestamp, last_run_id string",
# MAGIC     ).createOrReplaceTempView("_updates")
# MAGIC     spark.sql(f"""
# MAGIC         MERGE INTO {T_TSTATE} AS s
# MAGIC         USING _updates AS u ON lower(s.source_table) = u.source_table
# MAGIC         WHEN MATCHED THEN UPDATE SET
# MAGIC           s.last_updated_ts = u.last_updated_ts, s.last_run_id = u.last_run_id
# MAGIC         WHEN NOT MATCHED THEN INSERT (source_table, last_updated_ts, last_run_id)
# MAGIC           VALUES (u.source_table, u.last_updated_ts, u.last_run_id)
# MAGIC     """)
# MAGIC
# MAGIC # ---- 2) Load config + evaluate dueness ----
# MAGIC cfg = {r["report_id"]: r.asDict() for r in
# MAGIC        spark.table(T_CONFIG).filter("enabled = true").collect()}
# MAGIC deps = {}
# MAGIC for r in spark.table(T_DEPS).collect():
# MAGIC     deps.setdefault(r["report_id"], []).append(r["source_table"].lower())
# MAGIC tstate = {r["source_table"].lower(): r["last_updated_ts"] for r in spark.table(T_TSTATE).collect()}
# MAGIC rstate = {r["report_id"]: r["last_refresh_ts"] for r in spark.table(T_RSTATE).collect()}
# MAGIC
# MAGIC due = []
# MAGIC for rid, c in cfg.items():
# MAGIC     d = deps.get(rid, [])
# MAGIC     if not d: continue
# MAGIC     r_wm = rstate.get(rid)  # None => never refreshed
# MAGIC     fresh = {t: tstate[t] for t in d
# MAGIC              if tstate.get(t) is not None and (r_wm is None or tstate[t] > r_wm)}
# MAGIC     mode = (c.get("refresh_mode") or "ALL").upper()
# MAGIC     if force_all:
# MAGIC         is_due = True
# MAGIC     elif mode == "ANY":
# MAGIC         is_due = len(fresh) >= 1
# MAGIC     else:  # ALL
# MAGIC         is_due = len(fresh) == len(d)
# MAGIC     # Optional window check
# MAGIC     win = c.get("dependency_window_minutes")
# MAGIC     if is_due and not force_all and win and len(fresh) > 1:
# MAGIC         span = (max(fresh.values()) - min(fresh.values())).total_seconds() / 60.0
# MAGIC         if span > win:
# MAGIC             is_due = False
# MAGIC     if is_due: due.append(rid)
# MAGIC
# MAGIC # ---- 3) Emit task values ----
# MAGIC NOW_ISO = NOW.isoformat()
# MAGIC for rid in cfg:
# MAGIC     is_due = rid in due
# MAGIC     dbutils.jobs.taskValues.set(key=f"refresh_{rid}", value=("true" if is_due else "false"))
# MAGIC     dbutils.jobs.taskValues.set(key=f"candidate_ts_{rid}", value=(NOW_ISO if is_due else ""))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 3: Create the Commit State Notebook
# MAGIC
# MAGIC Create `commit_state.py` — runs only after a successful PBI refresh:
# MAGIC
# MAGIC ```python
# MAGIC CAT, SCH = "<catalog>", "<schema>"
# MAGIC T_RSTATE = f"{CAT}.{SCH}.report_state"
# MAGIC
# MAGIC dbutils.widgets.text("report_id", "")
# MAGIC dbutils.widgets.text("candidate_ts", "")  # {{tasks.dispatcher.values.candidate_ts_<id>}}
# MAGIC dbutils.widgets.text("run_id", "")
# MAGIC
# MAGIC report_id = dbutils.widgets.get("report_id").strip()
# MAGIC candidate_ts = dbutils.widgets.get("candidate_ts").strip()
# MAGIC run_id = dbutils.widgets.get("run_id") or "manual"
# MAGIC
# MAGIC assert report_id, "report_id required"
# MAGIC assert candidate_ts, "candidate_ts empty — nothing to commit"
# MAGIC
# MAGIC spark.createDataFrame(
# MAGIC     [(report_id, candidate_ts, run_id)],
# MAGIC     "report_id string, ts_str string, last_refresh_run_id string",
# MAGIC ).createOrReplaceTempView("_commit")
# MAGIC
# MAGIC spark.sql(f"""
# MAGIC     MERGE INTO {T_RSTATE} AS s
# MAGIC     USING (SELECT report_id, to_timestamp(ts_str) AS last_refresh_ts, 
# MAGIC                   last_refresh_run_id FROM _commit) AS u
# MAGIC       ON s.report_id = u.report_id
# MAGIC     WHEN MATCHED THEN UPDATE SET
# MAGIC       s.last_refresh_ts = u.last_refresh_ts, 
# MAGIC       s.last_refresh_run_id = u.last_refresh_run_id
# MAGIC     WHEN NOT MATCHED THEN INSERT (report_id, last_refresh_ts, last_refresh_run_id)
# MAGIC       VALUES (u.report_id, u.last_refresh_ts, u.last_refresh_run_id)
# MAGIC """)
# MAGIC print(f"Committed {report_id} watermark -> {candidate_ts}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 4: Create a Power BI Connection in Unity Catalog
# MAGIC
# MAGIC 1. **Catalog Explorer** → Create → Create a connection
# MAGIC 2. Connection type: `Power BI`
# MAGIC 3. Auth type: `Service credential` (Client ID + Secret from Azure AD App Registration)
# MAGIC 4. Click **Create connection**
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CONNECTION ON CONNECTION pbi_refresh_connection 
# MAGIC   TO `<service-principal-or-user>`;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Job Configuration
# MAGIC %md
# MAGIC ## Step 5: Configure the Databricks Job
# MAGIC
# MAGIC ### 5a. Create the Job with Table-Update Trigger
# MAGIC
# MAGIC 1. **Create a new Job** (e.g. "PBI Refresh Router - Stateful Multi-Table Deps")
# MAGIC 2. **Set the Trigger** to `Table Update` with:
# MAGIC    - **Tables to monitor**: Add ALL source tables across ALL reports (union of `report_dependencies`)
# MAGIC    - **Condition**: `ANY_UPDATED`
# MAGIC    - **Min time between triggers**: `60` seconds
# MAGIC    - **Wait after last change**: `90` seconds
# MAGIC 3. **Set Max Concurrent Runs**: `1` (prevents race conditions on state tables)
# MAGIC
# MAGIC #### Supported Table Types for Trigger Monitoring
# MAGIC | Supported | Not Supported |
# MAGIC |-----------|---------------|
# MAGIC | UC Delta managed tables | Hive metastore tables |
# MAGIC | UC Iceberg managed tables | Views using `read_files` |
# MAGIC | UC external tables (Delta-backed) | Views depending on non-UC tables |
# MAGIC | Materialized views | Views depending on federated tables |
# MAGIC | Streaming tables | |
# MAGIC | UC views / metric views (deps must be supported types) | |
# MAGIC | OpenSharing tables & views (Beta) | |
# MAGIC | System tables (Beta) | |
# MAGIC
# MAGIC > **Limit**: Max 10 tables per trigger. For UC views, the underlying source tables count toward this limit (e.g. a view over 6 tables uses 6 of the 10 slots).
# MAGIC
# MAGIC ### 5b. Define Job Parameters
# MAGIC
# MAGIC | Parameter Name | Default Value |
# MAGIC |---|---|
# MAGIC | `updated_tables` | `{{job.trigger.table_update.updated_tables}}` |
# MAGIC | `force_all` | `false` |
# MAGIC
# MAGIC The `force_all` parameter is an escape hatch to manually trigger all reports regardless of dependency state.
# MAGIC
# MAGIC ### 5c. Build the Task DAG
# MAGIC
# MAGIC For each report, create **four** tasks (one more than the simple pattern):
# MAGIC
# MAGIC #### Task 1: `dispatcher` (runs first)
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | Type | Notebook |
# MAGIC | Notebook path | `<path>/dispatcher_stateful` |
# MAGIC | Base parameters | `updated_tables` = `{{job.parameters.updated_tables}}`<br>`run_id` = `{{job.run_id}}`<br>`force_all` = `{{job.parameters.force_all}}` |
# MAGIC | Compute | Serverless (recommended) |
# MAGIC
# MAGIC #### Task 2: `gate_<report_id>` (condition gate)
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | Type | If/else condition |
# MAGIC | Depends on | `dispatcher` |
# MAGIC | Condition | `{{tasks.dispatcher.values.refresh_<report_id>}}` EQUAL_TO `"true"` |
# MAGIC
# MAGIC #### Task 3: `refresh_<report_id>` (native Power BI task)
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | Type | **Power BI** |
# MAGIC | Depends on | `gate_<report_id>` (outcome = `true`) |
# MAGIC | SQL Warehouse | A SQL warehouse for model refresh queries |
# MAGIC | Power BI connection | `pbi_refresh_connection` |
# MAGIC | Power BI workspace | Target PBI workspace |
# MAGIC | Power BI semantic model | The semantic model to update |
# MAGIC | Tables to update | Source UC tables for this model |
# MAGIC | Power BI query mode | `Import` |
# MAGIC | Refresh after update | **✅ Checked** |
# MAGIC | Authentication method | `PAT` (recommended) or `OAuth` |
# MAGIC
# MAGIC #### Task 4: `commit_<report_id>` (advance watermark on success)
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | Type | Notebook |
# MAGIC | Depends on | `refresh_<report_id>` |
# MAGIC | Run if | `ALL_SUCCESS` (critical — only runs if PBI refresh succeeded) |
# MAGIC | Notebook path | `<path>/commit_state` |
# MAGIC | Base parameters | `report_id` = `<report_id>`<br>`candidate_ts` = `{{tasks.dispatcher.values.candidate_ts_<report_id>}}`<br>`run_id` = `{{job.run_id}}` |
# MAGIC | Compute | Serverless |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Visual DAG Structure
# MAGIC
# MAGIC ```
# MAGIC dispatcher
# MAGIC     ├─── gate_report1 ─ [if true] ─── refresh_report1 (PBI) ─── commit_report1
# MAGIC     └─── gate_report2 ─ [if true] ─── refresh_report2 (PBI) ─── commit_report2
# MAGIC ```
# MAGIC
# MAGIC * Gates run in **parallel** after the dispatcher
# MAGIC * If a PBI refresh **fails**, its commit task is skipped → report stays "due" for next trigger
# MAGIC * If a PBI refresh **succeeds**, the commit advances the watermark → report won’t re-trigger until new data arrives
# MAGIC
# MAGIC > **Important**: Set `max_concurrent_runs = 1` to prevent concurrent runs from racing on the state tables. The debounce settings (60s min + 90s wait) handle overlapping triggers.

# COMMAND ----------

# DBTITLE 1,PBI Connection & Permissions
# MAGIC %md
# MAGIC ## Step 6: Power BI Connection & Permissions Setup
# MAGIC
# MAGIC ### Prerequisites in Azure / Power BI
# MAGIC
# MAGIC 1. **Azure AD / Entra ID App Registration** (for Service Credential auth):
# MAGIC    - Create an app registration (e.g. `databricks-pbi-refresh`)
# MAGIC    - Note the **Application (client) ID** and **Directory (tenant) ID**
# MAGIC    - Create a **Client Secret**
# MAGIC    - Grant API permission: `Power BI Service` → `Dataset.ReadWrite.All` + admin consent
# MAGIC
# MAGIC 2. **Power BI Admin Portal**:
# MAGIC    - Tenant settings → Developer settings → Enable "Service principals can use Fabric APIs"
# MAGIC    - Add the service principal to a security group with access to target workspaces
# MAGIC    - Ensure the service principal has at least **Member** role on target PBI workspaces
# MAGIC
# MAGIC ### Unity Catalog Connection (replaces Secret Scope)
# MAGIC
# MAGIC Instead of managing secrets manually, create a **Power BI Connection** in Unity Catalog:
# MAGIC
# MAGIC 1. **Catalog Explorer** → Create → Create a connection
# MAGIC 2. Connection type: `Power BI`
# MAGIC 3. Auth type: `Service credential`
# MAGIC 4. Enter: Tenant ID, Client ID, Client Secret
# MAGIC 5. Click **Create connection**
# MAGIC
# MAGIC ### Required Permissions
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant the job's Run-As identity access to the connection
# MAGIC GRANT USE CONNECTION ON CONNECTION pbi_refresh_connection 
# MAGIC   TO `<service-principal-application-id>`;
# MAGIC
# MAGIC -- The same identity also needs:
# MAGIC --   SELECT on source UC tables
# MAGIC --   USE SCHEMA / USE CATALOG on relevant schemas
# MAGIC --   Access to the SQL Warehouse used by the PBI task
# MAGIC ```
# MAGIC
# MAGIC ### Why Native PBI Tasks + UC Connection?
# MAGIC
# MAGIC | Aspect | Custom REST API Approach | Native PBI Task + UC Connection |
# MAGIC |--------|------------------------|----------------------------------|
# MAGIC | Credential storage | Manual (Secrets CLI) | Managed by Unity Catalog |
# MAGIC | Token refresh | Custom code needed | Handled automatically |
# MAGIC | Access control | Scope ACLs | Standard UC GRANT/REVOKE |
# MAGIC | Audit trail | Custom audit table | Job run history + UC lineage |
# MAGIC | Code maintenance | Refresh notebook to maintain | Zero code — pure configuration |
# MAGIC | Metadata sync | Manual | Automatic (schema/column/PK-FK) |
# MAGIC | Retry on failure | Custom logic | Built-in task retry policies |

# COMMAND ----------

# DBTITLE 1,Adding New Reports & Operations
# MAGIC %md
# MAGIC ## Step 7: Adding New Reports (Day-2 Operations)
# MAGIC
# MAGIC To onboard a new Power BI report:
# MAGIC
# MAGIC ### 1. Insert config + dependency rows
# MAGIC ```sql
# MAGIC -- Add report config
# MAGIC INSERT INTO <catalog>.<schema>.report_config VALUES (
# MAGIC   'inventory', 'Inventory Dashboard', 'Supply Chain WS', 
# MAGIC   'Inventory Model', 'ALL', NULL, true
# MAGIC );
# MAGIC
# MAGIC -- Define its dependencies (all tables it needs)
# MAGIC INSERT INTO <catalog>.<schema>.report_dependencies VALUES
# MAGIC   ('inventory', 'my_catalog.my_schema.inventory_facts'),
# MAGIC   ('inventory', 'my_catalog.my_schema.supplier_dim');
# MAGIC ```
# MAGIC
# MAGIC ### 2. Add source tables to the job trigger
# MAGIC Edit the job → Trigger settings → add any NEW tables to the monitored list (tables already monitored for other reports don’t need re-adding).
# MAGIC
# MAGIC ### 3. Add gate + PBI + commit tasks to the DAG
# MAGIC * `gate_inventory`: condition checks `{{tasks.dispatcher.values.refresh_inventory}}` = `"true"`
# MAGIC * `refresh_inventory`: Power BI task targeting the `Inventory Model` in `Supply Chain WS`
# MAGIC * `commit_inventory`: notebook task running `commit_state` with `report_id=inventory`, `candidate_ts={{tasks.dispatcher.values.candidate_ts_inventory}}`
# MAGIC
# MAGIC > **No code changes needed** — the dispatcher dynamically reads `report_config` and `report_dependencies`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Disabling / Pausing a Report
# MAGIC
# MAGIC ```sql
# MAGIC UPDATE <catalog>.<schema>.report_config 
# MAGIC SET enabled = false 
# MAGIC WHERE report_id = 'inventory';
# MAGIC ```
# MAGIC
# MAGIC The dispatcher will skip disabled reports entirely.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Force-Refresh All Reports (Manual Override)
# MAGIC
# MAGIC Run the job manually with parameter:
# MAGIC ```
# MAGIC force_all = true
# MAGIC ```
# MAGIC
# MAGIC This bypasses all dependency logic and marks every enabled report as "due", useful for:
# MAGIC * Initial deployment (bootstrap all reports)
# MAGIC * Recovery after a failed state reset
# MAGIC * On-demand full refresh before a presentation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Changing Dependency Mode
# MAGIC
# MAGIC ```sql
# MAGIC -- Switch report2 to refresh when ANY dependency updates (not all)
# MAGIC UPDATE <catalog>.<schema>.report_config 
# MAGIC SET refresh_mode = 'ANY' 
# MAGIC WHERE report_id = 'report2';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Manual Testing
# MAGIC
# MAGIC Override `updated_tables` with a JSON array to simulate a trigger:
# MAGIC ```json
# MAGIC ["my_catalog.my_schema.sales_orders"]
# MAGIC ```
# MAGIC
# MAGIC This records the table update in `table_state` and evaluates all reports. If a report’s other dependencies haven’t arrived yet, it correctly remains "not due".
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Key Design Decisions & Best Practices
# MAGIC %md
# MAGIC ## Key Design Decisions & Best Practices
# MAGIC
# MAGIC ### Why Stateful Watermarks?
# MAGIC | Approach | Behavior | Limitation |
# MAGIC |----------|----------|------------|
# MAGIC | Simple dispatcher (v1) | Refresh if source table is in *this* trigger payload | Forgets across runs; multi-dep reports over-trigger or miss |
# MAGIC | **Stateful dispatcher (v2)** | Persist per-table update times; compare against per-report last-refresh time | Handles deps arriving in separate runs; automatic retry on failure |
# MAGIC
# MAGIC ### Why `max_concurrent_runs = 1`?
# MAGIC The state tables (`table_state`, `report_state`) are shared mutable state. Concurrent runs could:
# MAGIC * Both see a report as "due" and double-refresh
# MAGIC * Race on MERGE operations
# MAGIC
# MAGIC With `max_concurrent_runs = 1` + queue enabled, runs process sequentially while the debounce settings prevent a backlog from building up.
# MAGIC
# MAGIC ### Why Table-Update Trigger (not scheduled)?
# MAGIC | Approach | Pros | Cons |
# MAGIC |----------|------|------|
# MAGIC | Fixed schedule (e.g. hourly) | Simple | Refreshes even when nothing changed; latency up to 1 hour |
# MAGIC | File arrival trigger | Works for batch drops | Doesn’t cover streaming or MERGE workloads |
# MAGIC | **Table-update trigger** | Fires only on actual data change; near-real-time | Requires Unity Catalog tables |
# MAGIC
# MAGIC ### Why condition tasks instead of `if` in Python?
# MAGIC * The DAG visually shows which reports were refreshed vs. skipped
# MAGIC * Skipped tasks consume **zero compute** (not even a cluster start)
# MAGIC * Easier to monitor and troubleshoot in the Jobs UI
# MAGIC
# MAGIC ### Concurrency
# MAGIC * `max_concurrent_runs = 1` ensures serial processing of state tables (no race conditions)
# MAGIC * The debounce settings (`min_time_between_triggers`, `wait_after_last_change`) prevent queuing buildup during bulk loads
# MAGIC * Jobs queue enabled: overlapping triggers wait in line rather than being dropped
# MAGIC
# MAGIC ### Error Handling & Automatic Retry
# MAGIC * If a PBI refresh **fails**, the `commit_<id>` task is skipped → `report_state` stays unchanged
# MAGIC * On the **next trigger**, the dispatcher re-evaluates and finds the report still "due" → **automatic retry without manual intervention**
# MAGIC * Add **retry policies** on PBI tasks for transient failures (Power BI throttles at 8 refreshes/day for Pro, 48+ for Premium)
# MAGIC * Add **email/webhook notifications** on task failures for alerting
# MAGIC * Query `report_state` to detect stale reports: `SELECT * FROM report_state WHERE last_refresh_ts < current_timestamp() - INTERVAL 24 HOURS`
# MAGIC
# MAGIC ### Security
# MAGIC * PBI credentials are managed via a **Unity Catalog Connection** (no secrets in code)
# MAGIC * Use a **Service Principal** as the job’s Run-As identity with least-privilege permissions
# MAGIC * Grant `USE CONNECTION` only to identities that need it
# MAGIC * Run the job as a service principal (not a personal user) for production
# MAGIC * The SQL Warehouse used by PBI tasks controls data access — standard UC permissions apply
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Limitations & Considerations
# MAGIC
# MAGIC * **Power BI refresh limits**: Pro licenses allow 8 refreshes/day per dataset; Premium/Fabric allows 48+. The stateful pattern naturally reduces refresh frequency by waiting for all deps.
# MAGIC * **Table-update triggers require Unity Catalog**: Supported sources include UC Delta/Iceberg managed tables, UC external tables backed by Delta Lake, materialized views, streaming tables, and UC views/metric views (with limitations). **Not supported**: Hive metastore tables, views using `read_files`, views depending on non-UC or federated tables. Source tables underlying a view count toward the 10-table-per-trigger limit.
# MAGIC * **Max concurrent runs = 1**: Required to avoid race conditions on state tables. If trigger events stack up, they queue (the debounce settings minimise queuing).
# MAGIC * **Job DAG is static**: Adding a new report requires editing the job (adding gate + PBI + commit tasks). Use the **Job Generator notebook** (`job_generator.py`) to programmatically create/update jobs from the config tables at scale (supports 100s of reports).
# MAGIC * **State table maintenance**: Over time, `table_state` accumulates rows for every monitored table. This is small and self-managing (MERGE upserts, no unbounded growth).
# MAGIC * **Clock skew**: The `dependency_window_minutes` feature assumes cluster clocks are reasonably synchronized (which serverless guarantees).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Components in This Solution
# MAGIC
# MAGIC | Component | Purpose |
# MAGIC |-----------|---------|
# MAGIC | `dispatcher_stateful.py` | Upserts table watermarks, evaluates multi-dep dueness, emits task values |
# MAGIC | `commit_state.py` | Advances report watermark after successful PBI refresh |
# MAGIC | `report_config` table | Report settings (workspace, model, refresh mode, window) |
# MAGIC | `report_dependencies` table | Many-to-many: report → source table dependency graph |
# MAGIC | `table_state` table | Durable per-table watermarks (when last updated) |
# MAGIC | `report_state` table | Durable per-report watermarks (when last refreshed) |
# MAGIC | Power BI Connection (UC) | Managed credential for PBI API access |
# MAGIC | Power BI tasks (in Job) | Native tasks that refresh semantic models — no custom code |
# MAGIC | SQL Warehouse | Used by PBI tasks for Import-mode refreshes |
# MAGIC | `job_generator.py` | Programmatically creates Lakeflow Jobs from config tables (for 100+ reports at scale) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Scaling to 100s of Reports: Job Generator
# MAGIC
# MAGIC For large deployments, manually creating jobs is impractical. The **`job_generator.py`** notebook:
# MAGIC 1. Reads `report_config` + `report_dependencies`
# MAGIC 2. Groups reports into jobs respecting the 10-table trigger limit (greedy bin-packing)
# MAGIC 3. Builds the full DAG per job (dispatcher → gate → PBI task → commit per report)
# MAGIC 4. Creates/updates jobs via the Databricks REST API
# MAGIC 5. Supports dry-run mode for safe preview before creation
# MAGIC
# MAGIC Run it whenever reports are added or removed from the config tables.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Reference Documentation
# MAGIC
# MAGIC * [Power BI task for Lakeflow Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/tasks/powerbi/) — full configuration reference
# MAGIC * [Create a Power BI connection in Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi/uc-connect/) — connection setup guide
# MAGIC * [Publish to Power BI Online from Databricks](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi/) — prerequisites and requirements
# MAGIC * [Table-Update Triggers for Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/triggers#table-update) — trigger configuration
