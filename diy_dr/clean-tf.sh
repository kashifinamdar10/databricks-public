#!/bin/bash
#
# clean-tf.sh — post-process Databricks Terraform exporter output for DIY DR.
#
# Does two things:
#   1. From uc-tables.tf, keep ONLY external tables — drop MANAGED tables,
#      VIEWs, and anything with a view_definition — writing uc-external-tables.tf.
#   2. Strip "dangling" depends_on references to databricks_grants that the
#      exporter emitted but never actually declared (the inherited-permission
#      bug: https://github.com/databricks/terraform-provider-databricks/issues/4025).
#      Only references whose grant is NOT declared in the grants file are removed;
#      valid references are preserved. The same cleanup is applied to
#      uc-volumes.tf, which is affected by the same bug.
#
# The proper long-term fix is to re-export with provider >= v1.75.0 (PR #4661),
# which no longer emits these dangling references. This script is a stopgap for
# output produced by older provider versions.
#
# Usage:
#   ./clean-tf.sh [GRANTS_FILE] [TABLES_FILE] [VOLUMES_FILE]
# Defaults: uc-grants.tf  uc-tables.tf  uc-volumes.tf

set -euo pipefail

GRANTS_FILE="${1:-uc-grants.tf}"
TABLES_FILE="${2:-uc-tables.tf}"
VOLUMES_FILE="${3:-uc-volumes.tf}"

if [[ ! -f "$GRANTS_FILE" ]]; then
    echo "ERROR: grants file '$GRANTS_FILE' not found — needed to know which grants are declared." >&2
    exit 1
fi

# --- shared awk that removes dangling depends_on grant refs -----------------
# Pass 1 (FNR==NR): read GRANTS_FILE, record every declared databricks_grants name.
# Pass 2: for each line, rewrite/drop depends_on entries that point at grants
#         which were never declared. Non-grant deps and valid grant deps are kept.
read -r -d '' DEPENDS_ON_FILTER <<'AWK' || true
FNR==NR {
    if (match($0, /resource "databricks_grants" "[^"]+"/)) {
        s = substr($0, RSTART, RLENGTH)
        sub(/resource "databricks_grants" "/, "", s)
        sub(/"$/, "", s)
        declared[s] = 1
    }
    next
}
function clean_depends(line,   prefix, inner, n, arr, i, tok, name, keptn, out) {
    if (line !~ /depends_on[[:space:]]*=[[:space:]]*\[/) return line
    prefix = line; sub(/\[.*/, "", prefix)      # keep "  depends_on = "
    inner  = line; sub(/^[^[]*\[/, "", inner); sub(/\].*/, "", inner)
    n = split(inner, arr, ",")
    keptn = 0
    for (i = 1; i <= n; i++) {
        tok = arr[i]; gsub(/^[[:space:]]+|[[:space:]]+$/, "", tok)
        if (tok == "") continue
        if (tok ~ /^databricks_grants\./) {
            name = tok; sub(/^databricks_grants\./, "", name)
            if (name in declared) kept[++keptn] = tok    # keep valid grant dep
            # else: drop dangling reference
        } else {
            kept[++keptn] = tok                          # keep non-grant dep
        }
    }
    if (keptn == 0) return "\x01"     # sentinel: drop the whole depends_on line
    out = prefix "["
    for (i = 1; i <= keptn; i++) { out = out kept[i]; if (i < keptn) out = out ", " }
    return out "]"
}
AWK

# --- 1. tables: filter to external + clean depends_on -----------------------
awk "$DEPENDS_ON_FILTER"'
/^resource "databricks_sql_table" ".*" \{/ {
    in_block = 1; nlines = 0; blockstr = ""
    lines[++nlines] = $0; blockstr = $0; next
}
in_block == 1 {
    lines[++nlines] = $0; blockstr = blockstr "\n" $0
    if ($0 ~ /^[[:space:]]*}$/) {
        if (blockstr ~ /table_type[[:space:]]*=[[:space:]]*"(MANAGED|VIEW)"/ || blockstr ~ /view_definition/) {
            # skip managed tables / views entirely
        } else {
            for (i = 1; i <= nlines; i++) {
                cl = clean_depends(lines[i])
                if (cl != "\x01") print cl      # sentinel means drop the line
            }
        }
        in_block = 0; blockstr = ""; nlines = 0; next
    }
    next
}
!in_block { print $0 }
' "$GRANTS_FILE" "$TABLES_FILE" > uc-external-tables.tf
echo "Wrote uc-external-tables.tf (external tables only, dangling grant depends_on removed)"

# --- 2. volumes: clean depends_on only (no table-type filtering) ------------
if [[ -f "$VOLUMES_FILE" ]]; then
    awk "$DEPENDS_ON_FILTER"'
    {   # pass 2 (grants file already consumed by the FNR==NR block above)
        line = clean_depends($0)
        if (line != "\x01") print line
    }
    ' "$GRANTS_FILE" "$VOLUMES_FILE" > "${VOLUMES_FILE%.tf}.cleaned.tf"
    echo "Wrote ${VOLUMES_FILE%.tf}.cleaned.tf (dangling grant depends_on removed)"
else
    echo "NOTE: '$VOLUMES_FILE' not found — skipping volumes cleanup."
fi
