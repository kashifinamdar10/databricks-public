# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # PBI Refresh Router — Lakeflow Job Generator
# MAGIC
# MAGIC This notebook **programmatically creates Lakeflow Jobs** from the `report_config` and
# MAGIC `report_dependencies` tables. It is designed for customers with 100s of reports who
# MAGIC cannot manually build each job.
# MAGIC
# MAGIC ### What it does:
# MAGIC 1. Reads `report_config` + `report_dependencies` to discover all reports and their source tables
# MAGIC 2. **Groups reports into jobs** respecting the 10-table trigger limit per job
# MAGIC 3. For each job, builds the full DAG:
# MAGIC    - Table-Update Trigger (with debounce settings)
# MAGIC    - `dispatcher_stateful` task
# MAGIC    - Per-report: `gate_<id>` → `refresh_<id>` (Power BI task) → `commit_<id>`
# MAGIC 4. Creates (or updates) the jobs via the Databricks SDK
# MAGIC
# MAGIC ### Parameters (widgets):
# MAGIC | Parameter | Description |
# MAGIC |-----------|-------------|
# MAGIC | `catalog` | UC catalog containing config tables |
# MAGIC | `schema` | UC schema containing config tables |
# MAGIC | `dispatcher_notebook_path` | Workspace path to `dispatcher_stateful` notebook |
# MAGIC | `commit_notebook_path` | Workspace path to `commit_state` notebook |
# MAGIC | `pbi_connection_name` | UC Power BI connection name |
# MAGIC | `sql_warehouse_id` | SQL Warehouse ID for PBI tasks |
# MAGIC | `max_tables_per_trigger` | Max monitored tables per job (default 10) |
# MAGIC | `job_name_prefix` | Prefix for generated job names |
# MAGIC | `dry_run` | If `true`, prints the job JSON without creating |
# MAGIC | `run_as_service_principal` | Optional SP application ID for Run-As identity |

# COMMAND ----------

# DBTITLE 1,Configuration Widgets
# ============================================================
# Configuration Widgets
# ============================================================

dbutils.widgets.text("catalog", "", "UC Catalog")
dbutils.widgets.text("schema", "", "UC Schema")
dbutils.widgets.text("dispatcher_notebook_path", "", "Dispatcher Notebook Path")
dbutils.widgets.text("commit_notebook_path", "", "Commit State Notebook Path")
dbutils.widgets.text("pbi_connection_name", "", "PBI Connection Name")
dbutils.widgets.text("sql_warehouse_id", "", "SQL Warehouse ID")
dbutils.widgets.text("max_tables_per_trigger", "10", "Max Tables Per Trigger")
dbutils.widgets.text("job_name_prefix", "PBI Refresh Router", "Job Name Prefix")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run")
dbutils.widgets.text("run_as_service_principal", "", "Run-As SP (optional)")
dbutils.widgets.text("min_time_between_triggers_seconds", "60", "Min Time Between Triggers (s)")
dbutils.widgets.text("wait_after_last_change_seconds", "90", "Wait After Last Change (s)")

# COMMAND ----------

# DBTITLE 1,Read Parameters
# ============================================================
# Read Parameters
# ============================================================

CAT = dbutils.widgets.get("catalog").strip()
SCH = dbutils.widgets.get("schema").strip()
DISPATCHER_PATH = dbutils.widgets.get("dispatcher_notebook_path").strip()
COMMIT_PATH = dbutils.widgets.get("commit_notebook_path").strip()
PBI_CONNECTION = dbutils.widgets.get("pbi_connection_name").strip()
SQL_WAREHOUSE_ID = dbutils.widgets.get("sql_warehouse_id").strip()
MAX_TABLES = int(dbutils.widgets.get("max_tables_per_trigger"))
JOB_PREFIX = dbutils.widgets.get("job_name_prefix").strip()
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
RUN_AS_SP = dbutils.widgets.get("run_as_service_principal").strip() or None
MIN_TRIGGER_INTERVAL = int(dbutils.widgets.get("min_time_between_triggers_seconds"))
WAIT_AFTER_CHANGE = int(dbutils.widgets.get("wait_after_last_change_seconds"))

assert CAT, "catalog is required"
assert SCH, "schema is required"
assert DISPATCHER_PATH, "dispatcher_notebook_path is required"
assert COMMIT_PATH, "commit_notebook_path is required"
assert PBI_CONNECTION, "pbi_connection_name is required"
assert SQL_WAREHOUSE_ID, "sql_warehouse_id is required"

print(f"Config: {CAT}.{SCH}")
print(f"Dispatcher: {DISPATCHER_PATH}")
print(f"Commit: {COMMIT_PATH}")
print(f"PBI Connection: {PBI_CONNECTION}")
print(f"SQL Warehouse: {SQL_WAREHOUSE_ID}")
print(f"Max tables/trigger: {MAX_TABLES}")
print(f"Dry run: {DRY_RUN}")

# COMMAND ----------

# DBTITLE 1,Load Config Tables & Build Report Graph
# ============================================================
# Load Config Tables & Build Report Graph
# ============================================================
from collections import defaultdict

T_CONFIG = f"{CAT}.{SCH}.report_config"
T_DEPS = f"{CAT}.{SCH}.report_dependencies"

# Load enabled reports
reports = {
    r["report_id"]: r.asDict()
    for r in spark.table(T_CONFIG).filter("enabled = true").collect()
}
print(f"Enabled reports: {len(reports)}")

# Load dependency graph: report_id -> [source_table, ...]
deps = defaultdict(list)
for r in spark.table(T_DEPS).collect():
    if r["report_id"] in reports:
        deps[r["report_id"]].append(r["source_table"].lower())

# Build reverse map: source_table -> [report_id, ...]
table_to_reports = defaultdict(list)
for rid, tables in deps.items():
    for t in tables:
        table_to_reports[t].append(rid)

# All unique source tables
all_tables = sorted(table_to_reports.keys())
print(f"Total source tables: {len(all_tables)}")
print(f"Reports with dependencies: {len(deps)}")

# Validate: reports without dependencies
no_deps = [rid for rid in reports if rid not in deps or not deps[rid]]
if no_deps:
    print(f"\nWARNING: {len(no_deps)} reports have no dependencies (will be skipped): {no_deps}")

# COMMAND ----------

# DBTITLE 1,Group Reports into Jobs (Respecting Table Limit)
# ============================================================
# Group Reports into Jobs (Respecting Table Limit)
# ============================================================
# Strategy: Greedy bin-packing. Each job has a set of monitored tables.
# Add reports to the current job if their tables fit within the limit.
# If adding a report would exceed the limit, start a new job.
# ============================================================

def group_reports_into_jobs(reports_deps: dict, max_tables: int) -> list:
    """
    Groups reports into job buckets where each bucket's union of
    source tables does not exceed max_tables.
    
    Returns: list of dicts, each with:
      - 'reports': list of report_ids
      - 'tables': set of source tables for this job's trigger
    """
    # Sort reports by number of dependencies (descending) for better packing
    sorted_reports = sorted(reports_deps.items(), key=lambda x: len(x[1]), reverse=True)
    
    jobs = []  # [{"reports": [...], "tables": set(...)}]
    
    for rid, tables in sorted_reports:
        table_set = set(tables)
        placed = False
        
        # Try to fit into an existing job
        for job in jobs:
            combined = job["tables"] | table_set
            if len(combined) <= max_tables:
                job["reports"].append(rid)
                job["tables"] = combined
                placed = True
                break
        
        # If no existing job can accommodate, start a new one
        if not placed:
            if len(table_set) > max_tables:
                print(f"WARNING: Report '{rid}' has {len(table_set)} dependencies "
                      f"(exceeds max_tables={max_tables}). It will be placed alone.")
            jobs.append({"reports": [rid], "tables": table_set})
    
    return jobs


# Group reports into jobs
job_groups = group_reports_into_jobs(dict(deps), MAX_TABLES)

print(f"\nJob grouping summary:")
print(f"  Total jobs to create: {len(job_groups)}")
print(f"  Max tables per trigger: {MAX_TABLES}")
print(f"{'='*60}")
for i, jg in enumerate(job_groups, 1):
    print(f"  Job {i}: {len(jg['reports'])} reports, {len(jg['tables'])} tables")
    for rid in jg['reports']:
        print(f"    - {rid} ({reports[rid]['report_name']})")
    print(f"    Tables: {sorted(jg['tables'])}")
    print()

# COMMAND ----------

# DBTITLE 1,Build Job JSON Payloads
# ============================================================
# Build Job JSON Payloads
# ============================================================
import json


def build_job_payload(job_index: int, job_group: dict) -> dict:
    """
    Builds the Databricks Jobs API payload for a single job.
    """
    report_ids = job_group["reports"]
    trigger_tables = sorted(job_group["tables"])
    
    job_name = f"{JOB_PREFIX} - Group {job_index}"
    if len(job_groups) == 1:
        job_name = JOB_PREFIX  # No suffix needed for a single job
    
    # ---- Build tasks ----
    tasks = []
    
    # Task 1: Dispatcher
    dispatcher_task = {
        "task_key": "dispatcher",
        "notebook_task": {
            "notebook_path": DISPATCHER_PATH,
            "base_parameters": {
                "updated_tables": "{{job.parameters.updated_tables}}",
                "run_id": "{{job.run_id}}",
                "force_all": "{{job.parameters.force_all}}",
            },
            "source": "WORKSPACE",
        },
        "timeout_seconds": 0,
    }
    tasks.append(dispatcher_task)
    
    # Per-report tasks: gate -> refresh (PBI) -> commit
    for rid in report_ids:
        rcfg = reports[rid]
        report_tables = deps[rid]
        
        # Gate task (condition)
        gate_task = {
            "task_key": f"gate_{rid}",
            "depends_on": [{"task_key": "dispatcher"}],
            "condition_task": {
                "op": "EQUAL_TO",
                "left": f"{{{{tasks.dispatcher.values.refresh_{rid}}}}}",
                "right": "true",
            },
            "timeout_seconds": 0,
        }
        tasks.append(gate_task)
        
        # Refresh task (Power BI native task)
        refresh_task = {
            "task_key": f"refresh_{rid}",
            "depends_on": [{"task_key": f"gate_{rid}", "outcome": "true"}],
            "power_bi_task": {
                "connection_resource_name": PBI_CONNECTION,
                "warehouse_id": SQL_WAREHOUSE_ID,
                "power_bi_model": {
                    "workspace_name": rcfg.get("powerbi_workspace", ""),
                    "model_name": rcfg.get("powerbi_model", ""),
                },
                "tables": [
                    {"table_name": t} for t in report_tables
                ],
                "refresh_after_update": True,
            },
            "timeout_seconds": 0,
        }
        tasks.append(refresh_task)
        
        # Commit task (advance watermark)
        commit_task = {
            "task_key": f"commit_{rid}",
            "depends_on": [{"task_key": f"refresh_{rid}"}],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": COMMIT_PATH,
                "base_parameters": {
                    "report_id": rid,
                    "candidate_ts": f"{{{{tasks.dispatcher.values.candidate_ts_{rid}}}}}",
                    "run_id": "{{job.run_id}}",
                },
                "source": "WORKSPACE",
            },
            "timeout_seconds": 0,
        }
        tasks.append(commit_task)
    
    # ---- Build full job payload ----
    payload = {
        "name": job_name,
        "tasks": tasks,
        "trigger": {
            "pause_status": "PAUSED",  # Created paused; user enables when ready
            "table_update": {
                "table_names": trigger_tables,
                "min_time_between_triggers_seconds": MIN_TRIGGER_INTERVAL,
                "wait_after_last_change_seconds": WAIT_AFTER_CHANGE,
                "condition": "ANY_UPDATED",
            },
        },
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "parameters": [
            {"name": "updated_tables", "default": "{{job.trigger.table_update.updated_tables}}"},
            {"name": "force_all", "default": "false"},
        ],
        "tags": {
            "generated_by": "pbi_job_generator",
            "config_source": f"{CAT}.{SCH}",
        },
        "format": "MULTI_TASK",
    }
    
    # Optional: Run-As service principal
    if RUN_AS_SP:
        payload["run_as"] = {"service_principal_name": RUN_AS_SP}
    
    return payload


# Build all job payloads
job_payloads = []
for i, jg in enumerate(job_groups, 1):
    payload = build_job_payload(i, jg)
    job_payloads.append(payload)

print(f"Built {len(job_payloads)} job payload(s)")
print(f"Total tasks across all jobs: {sum(len(p['tasks']) for p in job_payloads)}")

# COMMAND ----------

# DBTITLE 1,Dry Run Preview
# ============================================================
# Dry Run Preview — Print Job Definitions
# ============================================================

if DRY_RUN:
    print("="*70)
    print("DRY RUN MODE — No jobs will be created. Review the payloads below.")
    print("="*70)
    for i, payload in enumerate(job_payloads, 1):
        print(f"\n{'─'*70}")
        print(f"JOB {i}: {payload['name']}")
        print(f"{'─'*70}")
        print(f"  Trigger tables ({len(payload['trigger']['table_update']['table_names'])}):")
        for t in payload['trigger']['table_update']['table_names']:
            print(f"    • {t}")
        print(f"  Tasks ({len(payload['tasks'])}):")
        for task in payload['tasks']:
            tk = task['task_key']
            if 'condition_task' in task:
                print(f"    • {tk} [IF/ELSE condition]")
            elif 'power_bi_task' in task:
                pbi = task['power_bi_task']['power_bi_model']
                print(f"    • {tk} [Power BI → {pbi['workspace_name']}/{pbi['model_name']}]")
            elif 'notebook_task' in task:
                nb = task['notebook_task']['notebook_path'].split('/')[-1]
                print(f"    • {tk} [Notebook: {nb}]")
        print(f"  Parameters: {[p['name'] for p in payload['parameters']]}")
        print(f"  Max concurrent runs: {payload['max_concurrent_runs']}")
        print(f"  Trigger paused: {payload['trigger']['pause_status']}")
    
    print(f"\n{'='*70}")
    print("Full JSON (first job):")
    print(json.dumps(job_payloads[0], indent=2, default=str))
else:
    print("Dry run is OFF — proceeding to create jobs...")

# COMMAND ----------

# DBTITLE 1,Create Jobs via Databricks SDK
# ============================================================
# Create Jobs via Databricks SDK
# ============================================================

if not DRY_RUN:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import (
        Task, NotebookTask, ConditionTask, ConditionTaskOp,
        TableUpdateTriggerConfiguration, TriggerSettings,
        JobParameter, QueueSettings, TaskDependency,
        RunIf, Source,
    )
    import requests

    w = WorkspaceClient()
    
    created_jobs = []
    
    for i, payload in enumerate(job_payloads, 1):
        print(f"\nCreating job {i}/{len(job_payloads)}: '{payload['name']}'...")
        
        # Use the REST API directly for full control over the payload
        # (the SDK's create_job may not support power_bi_task natively yet)
        host = w.config.host
        token = w.config.authenticate()
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        
        response = requests.post(
            f"{host}/api/2.1/jobs/create",
            headers=headers,
            json={"settings": payload},
        )
        
        if response.status_code == 200:
            job_id = response.json()["job_id"]
            created_jobs.append({"job_id": job_id, "name": payload["name"]})
            print(f"  ✅ Created job_id={job_id}")
        else:
            print(f"  ❌ FAILED ({response.status_code}): {response.text}")
    
    # Summary
    print(f"\n{'='*70}")
    print(f"SUMMARY: Created {len(created_jobs)}/{len(job_payloads)} jobs")
    print(f"{'='*70}")
    for j in created_jobs:
        print(f"  • {j['name']} (job_id: {j['job_id']})")
    
    if created_jobs:
        print(f"\n⚠️  All jobs created in PAUSED state. Enable triggers via:")
        print(f"   Jobs UI → Edit job → Trigger → Unpause")
        print(f"   Or use: w.jobs.update(job_id=<id>, new_settings=dict(trigger=dict(pause_status='UNPAUSED')))")
else:
    print("\n⚠️  DRY RUN: No jobs created. Set dry_run=false to create.")

# COMMAND ----------

# DBTITLE 1,Update Existing Jobs (Idempotent Re-Run)
# ============================================================
# (Optional) Update Existing Jobs — Idempotent Re-Run
# ============================================================
# If jobs with the same name already exist, this cell resets them
# to match the current config. Useful for re-running the generator
# after adding new reports to the config tables.
# ============================================================

def find_existing_jobs(prefix: str) -> dict:
    """Find existing jobs matching our naming pattern."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    existing = {}
    for job in w.jobs.list(name=prefix):
        existing[job.settings.name] = job.job_id
    return existing


def update_or_create_jobs(payloads: list, dry_run: bool = True):
    """
    Idempotent: updates existing jobs or creates new ones.
    """
    from databricks.sdk import WorkspaceClient
    import requests
    
    if dry_run:
        print("DRY RUN: Would update/create the following jobs:")
        for p in payloads:
            print(f"  • {p['name']}")
        return
    
    w = WorkspaceClient()
    existing = find_existing_jobs(JOB_PREFIX)
    host = w.config.host
    token = w.config.authenticate()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    results = []
    for payload in payloads:
        name = payload["name"]
        if name in existing:
            # Update existing job (reset)
            job_id = existing[name]
            resp = requests.post(
                f"{host}/api/2.1/jobs/reset",
                headers=headers,
                json={"job_id": job_id, "new_settings": payload},
            )
            if resp.status_code == 200:
                results.append(("UPDATED", job_id, name))
            else:
                results.append(("FAILED", job_id, f"{name}: {resp.text}"))
        else:
            # Create new job
            resp = requests.post(
                f"{host}/api/2.1/jobs/create",
                headers=headers,
                json={"settings": payload},
            )
            if resp.status_code == 200:
                job_id = resp.json()["job_id"]
                results.append(("CREATED", job_id, name))
            else:
                results.append(("FAILED", None, f"{name}: {resp.text}"))
    
    print(f"\nResults:")
    for action, jid, name in results:
        print(f"  {action}: {name} (job_id={jid})")


# Uncomment to run idempotent update:
# update_or_create_jobs(job_payloads, dry_run=DRY_RUN)

# COMMAND ----------

# DBTITLE 1,Usage Guide
# MAGIC %md
# MAGIC ## Usage Guide
# MAGIC
# MAGIC ### First-time setup
# MAGIC 1. Populate `report_config` and `report_dependencies` tables with your reports
# MAGIC 2. Fill in the widget parameters at the top of this notebook
# MAGIC 3. Run with `dry_run = true` to review the generated job structure
# MAGIC 4. Set `dry_run = false` and run again to create the jobs
# MAGIC 5. Verify jobs in the Jobs UI, then **unpause** the triggers
# MAGIC
# MAGIC ### Adding new reports
# MAGIC 1. Insert rows into `report_config` and `report_dependencies`
# MAGIC 2. Re-run this notebook — it will create new jobs or update existing ones
# MAGIC 3. Use the `update_or_create_jobs()` function (last cell) for idempotent updates
# MAGIC
# MAGIC ### Scaling considerations
# MAGIC | Reports | Jobs Generated | Notes |
# MAGIC |---------|---------------|-------|
# MAGIC | 1–10 | 1 job | All tables fit in a single trigger |
# MAGIC | 10–50 | 2–5 jobs | Reports grouped by shared tables |
# MAGIC | 50–200+ | 5–20+ jobs | Greedy bin-packing minimises job count |
# MAGIC
# MAGIC ### Customization points
# MAGIC * **`max_tables_per_trigger`**: Increase if your workspace supports more (default 10)
# MAGIC * **`min_time_between_triggers_seconds`**: Increase for less frequent checks
# MAGIC * **`wait_after_last_change_seconds`**: Increase if upstream writes are bursty
# MAGIC * **`run_as_service_principal`**: Set for production deployments
# MAGIC * **Job naming**: Modify `JOB_PREFIX` to match your org conventions
# MAGIC
# MAGIC ### Power BI Task Note
# MAGIC The `power_bi_task` configuration in the generated payload uses:
# MAGIC * `connection_resource_name`: Your UC Power BI connection
# MAGIC * `warehouse_id`: SQL Warehouse for Import-mode refresh queries
# MAGIC * `power_bi_model.workspace_name` + `model_name`: From `report_config`
# MAGIC * `tables`: Source UC tables from `report_dependencies`
# MAGIC * `refresh_after_update: true`: Triggers the actual PBI dataset refresh
# MAGIC
# MAGIC If the `power_bi_task` API field names change in future SDK versions, update
# MAGIC the `build_job_payload()` function accordingly.
