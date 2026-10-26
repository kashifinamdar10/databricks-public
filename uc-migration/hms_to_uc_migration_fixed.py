# Databricks notebook – HMS ➜ Unity Catalog table migration (Azure Databricks)
# Run on a UC-enabled Dedicated (single user) cluster as a user who can read the HMS tables
# and has CREATE SCHEMA / CREATE TABLE on the target catalog.

import re
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# ---------------- CONFIG ----------------
SOURCE_SCHEMAS   = ["sales", "finance"]        # hive_metastore schemas to migrate
TARGET_CATALOG   = "main"                      # UC catalog
SCHEMA_MAP       = {}                          # optional rename: {"sales": "sales_uc"}
TABLE_OWNER      = "data_engineers"            # group to own migrated tables
DRY_RUN          = True                        # True = only report what would happen
REFRESH_EXISTING = False                       # True = re-copy DBFS-root tables that already exist in UC
LOG_TABLE        = f"{TARGET_CATALOG}.migration_admin.hms_migration_log"
# ----------------------------------------

# Formats UC can register directly as external tables
UC_FILE_FORMATS = {"delta", "parquet", "csv", "json", "orc", "avro", "text"}
HMS_CATALOGS    = {"hive_metastore", "spark_catalog"}

# Only user mounts under /mnt; "/" (DatabricksRoot), /databricks-datasets etc. are not cloud paths UC can use
mounts = {m.mountPoint.rstrip("/"): m.source.rstrip("/") for m in dbutils.fs.mounts()
          if m.mountPoint.startswith("/mnt/")}

def resolve_mount(loc):
    """dbfs:/mnt/... -> underlying cloud URI, or None when no mount covers the path."""
    for mp, src in sorted(mounts.items(), key=lambda x: -len(x[0])):
        if loc == f"dbfs:{mp}" or loc.startswith(f"dbfs:{mp}/"):
            return src + loc[len(f"dbfs:{mp}"):]
    return None

def is_dbfs_root(loc):
    # Any dbfs:/ path not backed by a mount (including dbfs:/mnt/<unmounted>/...) is DBFS root storage
    return loc.startswith("dbfs:/") and not loc.startswith("dbfs:/Volumes/") and resolve_mount(loc) is None

def to_uc_uri(uri):
    """UC on Azure only accepts abfss:// locations."""
    scheme, _, rest = uri.partition("://")
    scheme = scheme.lower()
    if scheme == "abfs":
        return f"abfss://{rest}"
    if scheme in ("wasb", "wasbs"):
        hint = re.sub(r"^([^@]+)@([^.]+)\.blob\.", r"abfss://\1@\2.dfs.", rest)
        raise Exception(f"Mount source {uri} uses {scheme}://, which UC does not support. If the storage account "
                        f"is ADLS Gen2 (hierarchical namespace), register it as {hint} instead.")
    if scheme == "adl":
        raise Exception(f"Mount source {uri} is ADLS Gen1, which UC does not support; migrate the data to ADLS Gen2.")
    return uri

def describe(fqn):
    """DESCRIBE TABLE EXTENDED split into sections so column names never collide with detail keys."""
    cols, part_cols, detail, section = [], [], {}, "cols"
    for r in spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").collect():
        name = (r.col_name or "").strip()
        if name == "# Partition Information":
            section = "parts"
        elif name == "# Detailed Table Information":
            section = "detail"
        elif name and not name.startswith("#"):
            if section == "cols":
                cols.append((name, r.comment))
            elif section == "parts":
                part_cols.append(name)
            else:
                detail[name] = (r.data_type or "").strip()
    return cols, part_cols, detail

def tgt_schema_of(src_schema):
    return SCHEMA_MAP.get(src_schema.lower(), src_schema)

def q(ident):
    return "`" + ident.replace("`", "``") + "`"

def unq(ident):
    return ident[1:-1].replace("``", "`") if ident.startswith("`") else ident

def sql_str(s):
    return s.replace("\\", "\\\\").replace("'", "\\'")

LOG_SCHEMA = StructType([StructField(c, TimestampType() if c == "ts" else StringType())
                         for c in ("ts", "source", "target", "object_type", "method", "status", "message")])
results = []
def log(src, tgt, kind, method, status, msg=""):
    results.append(Row(ts=datetime.now(), source=src, target=tgt, object_type=kind,
                       method=method, status=status, message=msg[:2000]))

def run(sql):
    if DRY_RUN:
        print("[DRY RUN]", sql)
    else:
        spark.sql(sql)

# ---- view text rewriting (string literals and comments are never touched) ----
IDENT = r"(?:`(?:[^`]|``)+`|[A-Za-z_]\w*)"
TOKEN = re.compile(r"(?P<ident>`(?:[^`]|``)*`)|'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|--[^\n]*|/\*.*?\*/", re.S)
HMS_3PART = re.compile(rf"(?<![\w`.])`?(?:hive_metastore|spark_catalog)`?\s*\.\s*({IDENT})\s*\.", re.I)
SRC_2PART = re.compile(rf"(\b(?:FROM|JOIN)\s+)({IDENT})\s*\.\s*(?={IDENT}(?!\s*\.))", re.I)

def map_code(text, fn):
    """Apply fn to SQL code only, leaving string literals and comments unchanged."""
    out, buf, pos = [], [], 0
    for m in TOKEN.finditer(text):
        buf.append(text[pos:m.start()])
        if m.group("ident"):
            buf.append(m.group(0))
        else:
            out += [fn("".join(buf)), m.group(0)]
            buf = []
        pos = m.end()
    buf.append(text[pos:])
    out.append(fn("".join(buf)))
    return "".join(out)

def _rewrite_code(code):
    code = HMS_3PART.sub(lambda m: f"{TARGET_CATALOG}.{q(tgt_schema_of(unq(m.group(1))))}.", code)
    def two_part(m):
        schema = unq(m.group(2))
        if schema.lower() not in {s.lower() for s in SOURCE_SCHEMAS}:
            return m.group(0)
        return f"{m.group(1)}{TARGET_CATALOG}.{q(tgt_schema_of(schema))}."
    return SRC_2PART.sub(two_part, code)

def rewrite_view_text(text):
    return map_code(text, _rewrite_code)

def relations(query):
    """Table references of a query, from Spark's parser (so literals/aliases can't confuse it)."""
    plan = spark._jsparkSession.sessionState().sqlParser().parsePlan(query).treeString()
    return [[unq(p.strip()) for p in m.group(1).split(",")]
            for m in re.finditer(r"UnresolvedRelation \[([^\]]*)\]", plan)]

def check_view(body):
    renamed = {k.lower() for k, v in SCHEMA_MAP.items() if k.lower() != v.lower()}
    for parts in relations(body):
        p = [x.lower() for x in parts]
        if p[0] in HMS_CATALOGS:
            raise Exception(f"View still references {'.'.join(parts)} after rewrite")
        if len(p) == 2 and p[0] in renamed:
            raise Exception(f"Unqualified reference {'.'.join(parts)} points at renamed schema "
                            f"'{parts[0]}'; qualify it as {TARGET_CATALOG}.{tgt_schema_of(parts[0])}.{parts[1]}")

# ---- DDL helpers ----
NAME = rf"{IDENT}(?:\s*\.\s*{IDENT})*"
def rewrite_ddl(ddl, tgt, cloud_path):
    ddl, n1 = re.subn(rf"^\s*CREATE\s+(?:EXTERNAL\s+)?TABLE\s+{NAME}", f"CREATE TABLE IF NOT EXISTS {tgt}",
                      ddl, count=1, flags=re.I)
    ddl, n2 = re.subn(r"(?im)^LOCATION\s+'(?:[^'\\]|\\.)*'", f"LOCATION '{sql_str(cloud_path)}'", ddl, count=1)
    if (n1, n2) != (1, 1):
        raise Exception(f"Could not rewrite SHOW CREATE TABLE output:\n{ddl}")
    return ddl

NOTE_RE = re.compile(r"This table is deprecated\. Please use \S+ instead of \S+\.\s*")
def deprecate(src, new_name, old_name, existing_comment):
    note = f"This table is deprecated. Please use {new_name} instead of {old_name}."
    if note in existing_comment:
        return
    spark.sql(f"COMMENT ON TABLE {src} IS '{sql_str((note + ' ' + existing_comment).strip())}'")

def repr_or_null(s):
    return f"'{sql_str(s)}'" if s else "NULL"

def copy_table(src, tgt, fmt, cols, part_cols, comment, exists):
    """Copy a DBFS-root table into a UC managed table, keeping partitioning and comments."""
    create = "CREATE OR REPLACE TABLE" if exists else "CREATE TABLE"
    if fmt == "delta":
        run(f"{create} {tgt} DEEP CLONE {src}")
        method = "DEEP CLONE"
    else:
        parts = f" PARTITIONED BY ({', '.join(q(c) for c in part_cols)})" if part_cols else ""
        run(f"{create} {tgt}{parts} AS SELECT * FROM {src}")
        for c, cm in cols:
            if cm:
                run(f"ALTER TABLE {tgt} ALTER COLUMN {q(c)} COMMENT '{sql_str(cm)}'")
        method = "CTAS"
    # don't carry the HMS deprecation note (added on a previous run) into the UC table
    run(f"COMMENT ON TABLE {tgt} IS {repr_or_null(NOTE_RE.sub('', comment).strip())}")
    return method

# ---------------- tables ----------------
views = []   # collected across ALL schemas so cross-schema view dependencies resolve
for src_schema in SOURCE_SCHEMAS:
    tgt_schema = tgt_schema_of(src_schema)
    run(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{q(tgt_schema)}")
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN hive_metastore.{q(src_schema)}").collect()
              if not r.isTemporary]

    for t in tables:
        src = f"hive_metastore.{q(src_schema)}.{q(t)}"
        tgt = f"{TARGET_CATALOG}.{q(tgt_schema)}.{q(t)}"
        method, warnings = "-", []
        try:
            cols, part_cols, d = describe(src)
            ttype, fmt, loc = d.get("Type", "").upper(), d.get("Provider", "").lower(), d.get("Location", "")
            comment = d.get("Comment", "")
            if ttype == "VIEW":
                views.append((src_schema, src, tgt, d.get("View Text", "")))
                continue

            if is_dbfs_root(loc):
                # DBFS root data (managed OR external) can't be registered in UC → copy it
                exists = spark.catalog.tableExists(tgt)
                if exists and not REFRESH_EXISTING:
                    log(src, tgt, "TABLE", "-", "SKIPPED",
                        "Target already exists; set REFRESH_EXISTING = True to re-copy it")
                    continue
                method = copy_table(src, tgt, fmt, cols, part_cols, comment, exists)
            elif loc.startswith("dbfs:/mnt/"):
                # UC can't use mount paths → re-register on the underlying cloud path
                cloud_path = to_uc_uri(resolve_mount(loc))
                if fmt not in UC_FILE_FORMATS:
                    raise Exception(f"Provider '{fmt}' (Hive SerDe) can't be registered in UC; "
                                    "convert it (e.g. CTAS into a managed table) instead.")
                method = "CREATE ... LOCATION (mount resolved)"
                if fmt == "delta":
                    run(f"CREATE TABLE IF NOT EXISTS {tgt} LOCATION '{sql_str(cloud_path)}'")
                else:
                    # keep schema, partitioning and OPTIONS (csv header, delimiter, ...) from the source DDL
                    run(rewrite_ddl(spark.sql(f"SHOW CREATE TABLE {src}").first()[0], tgt, cloud_path))
                    if d.get("Partition Provider") == "Catalog":
                        try:
                            run(f"MSCK REPAIR TABLE {tgt}")
                        except Exception as e:
                            warnings.append(f"MSCK REPAIR failed, run it manually: {e}")
            else:
                method = "SYNC"
                as_ext = " AS EXTERNAL" if ttype == "MANAGED" else ""
                if DRY_RUN and not spark.catalog.databaseExists(f"{TARGET_CATALOG}.{tgt_schema}"):
                    # SYNC ... DRY RUN needs the target schema; CREATE SCHEMA above was only printed
                    print(f"[DRY RUN] SYNC TABLE {tgt}{as_ext} FROM {src}  (not validated: target schema missing)")
                else:
                    out = spark.sql(f"SYNC TABLE {tgt}{as_ext} FROM {src} SET OWNER {q(TABLE_OWNER)}"
                                    + (" DRY RUN" if DRY_RUN else "")).collect()
                    if out and out[0].status_code not in ("SUCCESS", "DRY_RUN_SUCCESS"):
                        raise Exception(f"{out[0].status_code}: {out[0].description}")

            if method != "SYNC":
                run(f"ALTER TABLE {tgt} OWNER TO {q(TABLE_OWNER)}")
            if not DRY_RUN:
                deprecate(src, f"{TARGET_CATALOG}.{tgt_schema}.{t}", f"hive_metastore.{src_schema}.{t}", comment)
            log(src, tgt, "TABLE", method, "OK_WITH_WARNINGS" if warnings else "OK", "\n".join(warnings))
        except Exception as e:
            log(src, tgt, "TABLE", method, "FAILED", str(e))

# ---------------- views ----------------
# Retry in passes so view-on-view dependencies resolve in any order
pending, errors = list(views), {}
while pending:
    still_pending = []
    for src_schema, src, tgt, text in pending:
        try:
            body = rewrite_view_text(text)
            check_view(body)
            run(f"USE CATALOG {TARGET_CATALOG}")
            run(f"USE SCHEMA {q(tgt_schema_of(src_schema))}")
            run(f"CREATE OR REPLACE VIEW {tgt} AS {body}")
            run(f"ALTER VIEW {tgt} OWNER TO {q(TABLE_OWNER)}")
            log(src, tgt, "VIEW", "CREATE VIEW", "OK")
        except Exception as e:
            errors[src] = str(e)
            still_pending.append((src_schema, src, tgt, text))
    if len(still_pending) == len(pending):   # no progress → remaining views genuinely fail
        for _, src, tgt, _ in still_pending:
            log(src, tgt, "VIEW", "CREATE VIEW", "FAILED", errors[src])
        break
    pending = still_pending

df = spark.createDataFrame(results, schema=LOG_SCHEMA)
display(df)
if not DRY_RUN:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LOG_TABLE.rsplit('.', 1)[0]}")
    df.write.mode("append").saveAsTable(LOG_TABLE)
