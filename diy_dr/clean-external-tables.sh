#!/bin/bash
#
# clean-external-tables.sh — from the exporter's uc-tables.tf, keep only
# EXTERNAL tables (drop MANAGED / VIEW / view_definition) and strip dangling
# databricks_grants depends_on references. See lib-clean.sh for background.
#
# Usage:
#   ./clean-external-tables.sh [GRANTS_FILE] [TABLES_FILE] [OUT_FILE]
# Defaults: uc-grants.tf  uc-tables.tf  uc-external-tables.tf

set -euo pipefail
source "$(dirname "$0")/lib-clean.sh"

GRANTS_FILE="${1:-uc-grants.tf}"
TABLES_FILE="${2:-uc-tables.tf}"
OUT_FILE="${3:-uc-external-tables.tf}"

filter_external_tables "$GRANTS_FILE" "$TABLES_FILE" "$OUT_FILE"
