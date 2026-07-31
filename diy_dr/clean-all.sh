#!/bin/bash
#
# clean-all.sh — run every DIY DR exporter cleanup that applies. Skips any
# object whose input file is absent, so it is safe to run against a partial
# export. See lib-clean.sh for background on what is cleaned and why.
#
# Usage:
#   ./clean-all.sh [GRANTS_FILE]
# Default GRANTS_FILE: uc-grants.tf
# Object input files are the per-script defaults (uc-tables.tf, uc-volumes.tf,
# uc-external-locations.tf). Run a per-object script directly to override.

set -euo pipefail
DIR="$(dirname "$0")"
GRANTS_FILE="${1:-uc-grants.tf}"

if [[ ! -f "$GRANTS_FILE" ]]; then
    echo "ERROR: grants file '$GRANTS_FILE' not found — required by all cleaners." >&2
    exit 1
fi

run_if_present() {  # <script> <input_file>
    if [[ -f "$2" ]]; then
        "$DIR/$1" "$GRANTS_FILE" "$2"
    else
        echo "SKIP: '$2' not found — skipping $1"
    fi
}

run_if_present clean-external-tables.sh    uc-tables.tf
run_if_present clean-volumes.sh            uc-volumes.tf
run_if_present clean-external-locations.sh uc-external-locations.tf

echo "Done."
