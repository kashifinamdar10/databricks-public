# Self-Healing Pipelines — Job Failure Taxonomy + Error Enrichment

A portable toolkit that quantifies production **Databricks Jobs** failures from Unity Catalog system tables and classifies each one into a root-cause category — separating failures already **solved by platform best practices** from the genuinely novel long tail that a triage / "self-healing" agent should own.

It answers a specific question that comes up when customers ask about a "self-healing pipeline agent": *how much of your failure volume actually needs an agent, versus how much is tech debt that best practices already eliminate?*

---

## Files in this package

| File | Purpose |
|---|---|
| `self_healing_job_failures.sql` | Three standalone SQL queries (Q1–Q3). Run in any SQL warehouse. |
| `self_healing_job_failures.py` | Full Databricks notebook (Python). Runs Q1–Q3 **plus** Q4 (Jobs API error enrichment with child drill-down) and Q5 (code-failure reason classification + recommendations). |
| `README.md` | This file. |

---

## Which file should I use?

- **Start with the SQL** if you just want the taxonomy / executive summary (no Jobs API access needed).
- **Use the Python notebook** for the full picture: actual error messages, stack traces, child-pipeline root causes, and per-failure recommendations mapped to Databricks capabilities.

---

## Prerequisites

1. **System tables enabled** on the workspace's metastore. The notebook reads:
   - `system.lakeflow.job_run_timeline`
   - `system.lakeflow.jobs` (Q3B, for job names)
   - `system.access.workspaces_latest` (workspace name resolution)
2. **Unity Catalog** — the notebook must run on a cluster (or serverless) with UC enabled.
3. **Permissions** — `USE CATALOG system`, `SELECT` on `system.lakeflow.*` and `system.access.*`.
4. **Jobs API access** — the running user (or service principal) must have `Can View` on the jobs being enriched (Q4). The Jobs API only sees jobs in the workspace the notebook runs in.
5. **DLT / pipeline event log access** (optional) — needed only if you want to drill into `pipeline_task` child failures (Q4 child drill-down).

---

## Running the Python notebook

### 1. Import into your workspace

**Option A — Git folder (recommended)**
1. In your workspace, go to **Repos** → **Add Repo**.
2. Clone this repository: `https://github.com/kashifinamdar10/databricks-public`
3. Open `self_healing_pipelines/self_healing_job_failures.py`.

**Option B — Manual upload**
1. Download `self_healing_job_failures.py`.
2. In your workspace, go to **Workspace** → **Import** → upload the `.py` file.

### 2. Attach a cluster

Attach to any cluster or serverless compute with Unity Catalog enabled. A **Serverless** cluster works and is the fastest option.

### 3. Configure the widgets

When you run the first cell the four parameter widgets appear at the top of the notebook:

| Widget | Default | Description |
|---|---|---|
| `lookback_days` | `30` | How many days of job history to analyse |
| `enrich_limit` | `50` | Max failed runs to fetch error text for via the Jobs API (Q4) |
| `enrich_scope` | `sporadic_triage` | Which failures to enrich: `sporadic_triage` (1–4× failures only), `all_exec_errors`, or `all_failures` |
| `enrich_child_depth` | `2` | How many child-pipeline levels to drill through when the parent error is a generic wrapper |
| `focus_workspace` | `current` | `current` = scope to the workspace this notebook runs in; `ALL` = every workspace in the metastore |
| `workspace_id` | *(blank)* | Optional explicit workspace ID override (takes precedence over `focus_workspace`) |

> **Named workspace shortcut**: to add a named entry to the `focus_workspace` dropdown, add a mapping to the `FOCUS_WS_NAME` dict in the Parameters cell, e.g. `{"MY-PROD": "my-prod-workspace-name"}`, and add `"MY-PROD"` to the dropdown list.

### 4. Run all cells

`Run All` — the notebook executes in five stages:

| Stage | Output |
|---|---|
| **Q1** | Daily failure matrix — workspace × day × category × resolution lever |
| **Q2** | Executive rollup — % solved by best practices vs. agent long-tail |
| **Q3** | Long-tail decomposition — chronic repeat-offenders vs. sporadic (the real agent surface) |
| **Q4** | Error enrichment — actual `error` + `error_trace` per failed task, with child drill-down through `run_job_task`, `pipeline_task` (DLT/Lakeflow), and `dbutils.notebook.run()` children |
| **Q5** | Code-failure reason classification + Databricks recommendation per error type |

A machine-readable JSON summary is printed and returned via `dbutils.notebook.exit()` at the end.

### 5. Optional: persist the enriched corpus

Uncomment the last cell and set `TARGET_TABLE` to write Q4 results to a Delta table for use by a downstream RCA / self-healing agent.

---

## The three SQL queries (Q1–Q3)

### Q1 — Daily failure matrix (`workspace_id × day × category`)
The raw deliverable: failed runs grouped by workspace, day, root-cause category, the Databricks lever that addresses it, and a coarse disposition (`Solved by best practices` vs. `Agent candidate`).

### Q2 — Executive rollup
Collapses Q1 into the headline: **% of failures solved by best practices today vs. the `RUN_EXECUTION_ERROR` long tail**.

### Q3 — Decompose the long tail
Bands each job by how many times it failed: `chronic (20+)` / `recurring (5–19)` / `sporadic (1–4)`. Only the sporadic band is a genuine triage-agent surface.

---

## Categorization logic

| Category | Termination codes | Disposition | Self-healing lever |
|---|---|---|---|
| **1. Transient / Infra** | `INTERNAL_ERROR`, `CLOUD_FAILURE`, `CLUSTER_ERROR`, `DRIVER_ERROR` | Solved by best practices | Job retries + serverless |
| **2. Config / Definition** | `INVALID_RUN_CONFIGURATION`, `INVALID_CLUSTER_REQUEST`, `RESOURCE_NOT_FOUND`, `REPOSITORY_CHECKOUT_FAILED`, `LIBRARY_INSTALLATION_ERROR`, `FEATURE_DISABLED` | Solved by best practices | DABs + CI/CD + serverless |
| **3. Permissions / Governance** | `UNAUTHORIZED_ERROR`, `STORAGE_ACCESS_ERROR` | Solved by best practices | Unity Catalog + service principals |
| **4. Concurrency / Limits** | `MAX_CONCURRENT_RUNS_EXCEEDED`, `MAX_JOB_QUEUE_SIZE_EXCEEDED`, `WORKSPACE_RUN_LIMIT_EXCEEDED`, `CLUSTER_REQUEST_LIMIT_EXCEEDED`, `MAX_SPARK_CONTEXTS_EXCEEDED` | Solved by best practices | Orchestration design + serverless elasticity |
| **5. Code / Data logic** | `RUN_EXECUTION_ERROR` | Agent candidate (long tail) | DLT expectations + Auto Loader; triage agent for residual |
| **6. Other / Uncategorized** | anything else | Review | Investigate |

---

## Child drill-down (Q4)

Many parent errors are opaque wrappers (`childPipelineFailed`, `WorkflowException`, etc.). Q4 follows all three ways a parent job launches a child to find the real root cause:

| Child type | Resolution method |
|---|---|
| `run_job_task` | Child `run_id` from `run_job_output` → recurse into child run |
| `pipeline_task` (DLT / Lakeflow Declarative Pipelines) | Read `event_log('<pipeline_id>')` for the most recent ERROR |
| `dbutils.notebook.run()` notebook child | Parse child `run_id` from the URL embedded in the wrapper error / trace → recurse |

The `resolved_error` / `resolved_via` / `depth` columns in Q4 output show the deepest real cause reached.

---

## Important caveats

- **`job_run_timeline` is period-based.** A `run_id` can appear multiple times; every query dedupes to the terminal period via `QUALIFY ROW_NUMBER() ... ORDER BY period_end_time DESC = 1`.
- **`termination_code` populated from late Aug 2024 onward.** Older rows fall into "Other / Uncategorized."
- **Jobs API scope.** The Jobs API (Q4) only sees jobs in the workspace the notebook runs in. To enrich a different workspace, run the notebook from inside that workspace.
- **`run_type = 'JOB_RUN'`** filters out sub-task / workflow rows so counts reflect job runs, not task fan-out.
