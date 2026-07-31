#!/bin/bash
#
# clean-external-locations.sh — strip dangling databricks_grants depends_on
# references from the exporter's uc-external-locations.tf. See lib-clean.sh
# for background.
#
# Usage:
#   ./clean-external-locations.sh [GRANTS_FILE] [LOCATIONS_FILE] [OUT_FILE]
# Defaults: uc-grants.tf  uc-external-locations.tf  uc-external-locations.cleaned.tf

set -euo pipefail
source "$(dirname "$0")/lib-clean.sh"

GRANTS_FILE="${1:-uc-grants.tf}"
LOCATIONS_FILE="${2:-uc-external-locations.tf}"
OUT_FILE="${3:-uc-external-locations.cleaned.tf}"

strip_dangling_grants "$GRANTS_FILE" "$LOCATIONS_FILE" "$OUT_FILE"
