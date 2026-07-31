#!/bin/bash
#
# clean-volumes.sh — strip dangling databricks_grants depends_on references
# from the exporter's uc-volumes.tf. See lib-clean.sh for background.
#
# Usage:
#   ./clean-volumes.sh [GRANTS_FILE] [VOLUMES_FILE] [OUT_FILE]
# Defaults: uc-grants.tf  uc-volumes.tf  uc-volumes.cleaned.tf

set -euo pipefail
source "$(dirname "$0")/lib-clean.sh"

GRANTS_FILE="${1:-uc-grants.tf}"
VOLUMES_FILE="${2:-uc-volumes.tf}"
OUT_FILE="${3:-uc-volumes.cleaned.tf}"

strip_dangling_grants "$GRANTS_FILE" "$VOLUMES_FILE" "$OUT_FILE"
