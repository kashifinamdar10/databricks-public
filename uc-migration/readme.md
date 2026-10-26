# HMS ➜ Unity Catalog table migration (Azure Databricks)

`hms_to_uc_migration_fixed.py` is a Databricks notebook that migrates tables and views from one or more
`hive_metastore` schemas into a Unity Catalog catalog, logs every object, and marks the source HMS tables
as deprecated.

## How each object is migrated

| Source object | Method |
|---|---|
| Table on DBFS root (managed or external, incl. `dbfs:/mnt/<unmounted>`) – Delta | `DEEP CLONE` into a UC managed table |
| Table on DBFS root – non-Delta (parquet, Hive SerDe, ...) | `CTAS` into a UC managed table, keeping partitioning, table and column comments |
| Table on a `/mnt` mount | Re-registered as a UC external table on the underlying `abfss://` path (keeps schema, partitioning and OPTIONS) |
| Table on a cloud path (`abfss://`) | `SYNC TABLE` (managed tables `AS EXTERNAL`) |
| View | Re-created in UC with `hive_metastore.` / `spark_catalog.` and 2-part references rewritten to the target catalog (honours `SCHEMA_MAP`); view-on-view dependencies are resolved automatically |

Ownership is set to `TABLE_OWNER`, each migrated HMS table gets a "deprecated – use ..." comment, and results
are appended to `LOG_TABLE`.

## Prerequisites

- Unity Catalog–enabled **Dedicated (single user)** cluster (tested on DBR 16.4 LTS).
- The running user can read the HMS tables and has `CREATE SCHEMA` / `CREATE TABLE` on `TARGET_CATALOG`.
- For mounted / cloud-path tables: a UC **external location** (and storage credential) covering the `abfss://` paths.

## Usage

1. Import the file as a notebook and edit the `CONFIG` block:

   | Setting | Meaning |
   |---|---|
   | `SOURCE_SCHEMAS` | `hive_metastore` schemas to migrate |
   | `TARGET_CATALOG` | UC catalog to migrate into |
   | `SCHEMA_MAP` | Optional schema renames, e.g. `{"sales": "sales_uc"}` |
   | `TABLE_OWNER` | User/group that will own migrated objects |
   | `DRY_RUN` | `True` prints the SQL and validates views without changing anything |
   | `REFRESH_EXISTING` | `True` re-copies DBFS-root tables that already exist in UC (otherwise they are `SKIPPED`) |
   | `LOG_TABLE` | Where the run log is appended |

2. Run with `DRY_RUN = True` and review every `FAILED` row.
3. Set `DRY_RUN = False` and run again. The script is safe to re-run.

## Result statuses

`OK`, `OK_WITH_WARNINGS` (e.g. `MSCK REPAIR` failed – run it manually), `SKIPPED` (target already exists),
`FAILED` (message explains why).

## Limitations

- HMS grants / table ACLs are **not** migrated – only ownership is set.
- Mount sources using `wasbs://` or ADLS Gen1 (`adl://`) are not supported by UC; the script reports the
  equivalent `abfss://` path (ADLS Gen2) to use instead.
- Hive SerDe tables on mounts can't be registered in UC and are reported as `FAILED` – convert them with CTAS.
- In view text, 2-part references are only rewritten after `FROM` / `JOIN`. Old-style comma joins that point at
  a renamed schema are reported as `FAILED` with the fully-qualified name to use.
- Views referencing HMS schemas that are not part of `SOURCE_SCHEMAS` will fail until those schemas are migrated.
- DBFS-root copies are point-in-time; use `REFRESH_EXISTING = True` to re-sync before cut-over.

## Testing status

Validated on Azure Databricks (DBR 16.4 LTS) for: DEEP CLONE / CTAS paths (Delta, parquet, Hive SerDe,
partitioned tables, external tables on DBFS root), all view-rewrite cases, re-runs and refresh.
The `SYNC` path and mount re-registration are covered by unit tests only – dry-run them on your data first.
