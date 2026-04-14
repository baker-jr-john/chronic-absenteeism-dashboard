"""
Extract the three needed tables from the NYSED SRC Access database to CSV.

Uses access_parser (pure Python) so we don't depend on mdbtools.

Usage:
    python scripts/extract_tables.py path/to/SRC2024_Group5.accdb
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

from access_parser import AccessParser

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

TABLES = {
    "Annual EM ELA": "annual_em_ela.csv",
    "ACC EM Chronic Absenteeism": "acc_em_chronic_absenteeism.csv",
    "BOCES and N/RC": "boces_and_n_rc.csv",
}


def export_table(db: AccessParser, table_name: str, out_path: Path) -> int:
    table = db.parse_table(table_name)
    columns = list(table.keys())
    n_rows = len(next(iter(table.values()))) if columns else 0
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for i in range(n_rows):
            writer.writerow(["" if table[c][i] is None else table[c][i] for c in columns])
    return n_rows


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: extract_tables.py <path-to-accdb>")
    accdb_path = Path(sys.argv[1])
    if not accdb_path.exists():
        sys.exit(f"not found: {accdb_path}")

    print(f"Opening {accdb_path.name} ...")
    db = AccessParser(str(accdb_path))

    for src_name, out_name in TABLES.items():
        out_path = RAW_DIR / out_name
        print(f"  {src_name} -> {out_path.name}", end=" ", flush=True)
        n = export_table(db, src_name, out_path)
        print(f"({n:,} rows)")


if __name__ == "__main__":
    main()
