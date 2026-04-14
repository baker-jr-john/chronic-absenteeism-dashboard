#!/usr/bin/env bash
set -euo pipefail

# Extract the tables we need from a NYSED SRC .mdb file into CSVs.
# Usage: ./extract_tables.sh <path-to-SRCYYYY_Group5.mdb> <output-dir>

MDB_PATH="${1:?usage: extract_tables.sh <mdb> <out-dir>}"
OUT_DIR="${2:?usage: extract_tables.sh <mdb> <out-dir>}"

mkdir -p "$OUT_DIR"

TABLES=(
    "Annual EM ELA"
    "ACC EM Chronic Absenteeism"
    "BOCES and N/RC"
    "Annual Regents Exams"
    "ACC HS Chronic Absenteeism"
)

for table in "${TABLES[@]}"; do
    slug=$(echo "$table" | tr '[:upper:] /' '[:lower:]__' | tr -s '_')
    out_file="${OUT_DIR}/${slug}.csv"
    echo "Extracting '${table}' -> ${out_file}"
    mdb-export "$MDB_PATH" "$table" > "$out_file"
done

echo "Done. Files:"
ls -lh "$OUT_DIR"/*.csv
