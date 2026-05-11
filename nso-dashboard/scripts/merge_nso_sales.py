"""
merge_nso_sales.py — Merge incremental nso_sales_data into the existing full file.

Usage:
  python scripts/merge_nso_sales.py --existing nso_sales_data.json --new nso_sales_new.json

Logic:
  For each studio, removes all rows whose date falls within the date range of
  the new file, then appends the new rows. Recalculates totals after merging.
  This handles late-arriving Snowflake data: re-fetching the last 2-3 days
  and merging ensures corrections are reflected without losing history.
"""

import argparse
import json
from datetime import datetime
from pathlib import Path


def merge(existing_path: str, new_path: str, output_path: str) -> None:
    with open(existing_path, encoding="utf-8") as f:
        existing = json.load(f)
    with open(new_path, encoding="utf-8") as f:
        new = json.load(f)

    new_studios = new.get("studios", {})

    for sid, new_studio in new_studios.items():
        if sid not in existing["studios"]:
            existing["studios"][sid] = new_studio
            continue

        new_rows = new_studio.get("daily", [])
        if not new_rows:
            continue

        # Date range covered by the new fetch
        new_dates = {r["date"] for r in new_rows}

        existing_rows = existing["studios"][sid].get("daily", [])
        kept = [r for r in existing_rows if r["date"] not in new_dates]
        merged = kept + new_rows
        merged.sort(key=lambda r: (r["date"], r.get("source", "")))

        existing["studios"][sid]["daily"] = merged

        # Recalculate totals
        presales      = sum(r.get("presales", 0)      for r in merged)
        cancellations = sum(r.get("cancellations", 0) for r in merged)
        gross         = sum(r.get("gross_revenue", 0) for r in merged)
        existing["studios"][sid]["totals"] = {
            "presales":      presales,
            "cancellations": cancellations,
            "net_presales":  presales - cancellations,
            "gross_revenue": round(gross, 2),
        }

    existing["generated_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False, default=str)

    total_rows = sum(
        len(s.get("daily", [])) for s in existing["studios"].values()
    )
    print(f"  merge_nso_sales: {total_rows} total daily rows → {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--existing", required=True)
    parser.add_argument("--new",      required=True)
    parser.add_argument("--output",   default=None,
                        help="Output path (defaults to --existing, i.e. in-place)")
    args = parser.parse_args()

    output = args.output or args.existing
    merge(args.existing, args.new, output)


if __name__ == "__main__":
    main()
