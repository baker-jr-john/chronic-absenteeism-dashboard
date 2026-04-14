#!/usr/bin/env bash
set -euo pipefail

# Extract BEDS Day Enrollment table from a NYSED Enrollment Database .mdb into a CSV.
# Usage: ./extract_enrollment.sh <path-to-enrollment_YYYY.mdb> <output-dir>

MDB_PATH="${1:?usage: extract_enrollment.sh <mdb> <out-dir>}"
OUT_DIR="${2:?usage: extract_enrollment.sh <mdb> <out-dir>}"

mkdir -p "$OUT_DIR"

TABLE="BEDS Day Enrollment"
OUT_FILE="${OUT_DIR}/beds_day_enrollment.csv"

echo "Extracting '${TABLE}' -> ${OUT_FILE}"
mdb-export "$MDB_PATH" "$TABLE" > "$OUT_FILE"

echo "Done:"
ls -lh "$OUT_FILE"
