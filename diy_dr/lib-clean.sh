#!/bin/bash
#
# lib-clean.sh — shared helpers for post-processing Databricks Terraform
# exporter output in the DIY DR workflow. Source this from the per-object
# clean-*.sh scripts; it is not meant to be run directly.
#
# Background
# ----------
# The exporter (provider < v1.75.0) emits
#     depends_on = [databricks_grants.schema_<name>]
# on tables, volumes, external locations, etc. — but does NOT generate the
# databricks_grants resource for objects whose permissions are only INHERITED
# from the catalog level. `terraform validate` then fails with:
#     Error: Reference to undeclared resource
#     A managed resource "databricks_grants" "schema_<name>" has not been
#     declared in the root module
# Upstream: https://github.com/databricks/terraform-provider-databricks/issues/4025
# Fixed in provider v1.75.0 (PR #4661). These scripts are a stopgap for output
# produced by older provider versions.
#
# The helpers below remove ONLY the dangling grant references (those whose
# grant is not declared in the grants file). Valid grant refs and all
# non-grant dependencies are preserved; a depends_on left empty is dropped
# entirely so no stray "[]" remains.

# awk program shared by every cleaner.
# Pass 1 (FNR==NR): read the grants file, record every declared
#   databricks_grants "<name>" so we know which references are valid.
# clean_depends(): rewrite one line, dropping dangling grant refs. Returns the
#   sentinel \x01 when the whole depends_on line should be removed.
read -r -d '' _DEPENDS_ON_FILTER <<'AWK' || true
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
    prefix = line; sub(/\[.*/, "", prefix)      # keep e.g. "  depends_on = "
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

# require_file <path> <human description>
require_file() {
    if [[ ! -f "$1" ]]; then
        echo "ERROR: $2 '$1' not found." >&2
        exit 1
    fi
}

# strip_dangling_grants <grants_file> <in_file> <out_file>
# Line-by-line cleanup of dangling grant depends_on. Used by objects that need
# no other filtering (volumes, external locations, ...).
strip_dangling_grants() {
    local grants="$1" infile="$2" outfile="$3"
    require_file "$grants"  "grants file"
    require_file "$infile"  "input file"
    awk "$_DEPENDS_ON_FILTER"'
    { line = clean_depends($0); if (line != "\x01") print line }
    ' "$grants" "$infile" > "$outfile"
    echo "Wrote $outfile (dangling grant depends_on removed)"
}

# filter_external_tables <grants_file> <in_file> <out_file>
# Keep only EXTERNAL databricks_sql_table blocks (drop MANAGED / VIEW /
# view_definition) AND strip dangling grant depends_on from the survivors.
filter_external_tables() {
    local grants="$1" infile="$2" outfile="$3"
    require_file "$grants" "grants file"
    require_file "$infile" "tables file"
    awk "$_DEPENDS_ON_FILTER"'
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
    ' "$grants" "$infile" > "$outfile"
    echo "Wrote $outfile (external tables only, dangling grant depends_on removed)"
}
