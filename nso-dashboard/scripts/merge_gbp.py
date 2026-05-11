"""
merge_gbp.py — Merge incremental gbp_data into the existing full file.

Usage:
  python scripts/merge_gbp.py --existing gbp_data.json --new gbp_data_new.json

Logic:
  gbp_data.json structure:
    {studios: [{name, code, location_id, daily: [{date, total_views, ...}], reviews}]}

  For each studio (matched by location_id), removes rows whose date falls
  within the new file's date range, then appends the new rows.
"""

import argparse
import json
from datetime import datetime


def merge(existing_path: str, new_path: str, output_path: str) -> None:
    with open(existing_path, encoding="utf-8") as f:
        existing = json.load(f)
    with open(new_path, encoding="utf-8") as f:
        new = json.load(f)

    # Index existing studios by location_id for fast lookup
    existing_by_id = {s["location_id"]: s for s in existing.get("studios", [])}

    for new_studio in new.get("studios", []):
        loc_id = new_studio.get("location_id")
        new_rows = new_studio.get("daily", [])
        if not new_rows:
            continue

        if loc_id not in existing_by_id:
            existing["studios"].append(new_studio)
            continue

        new_dates = {r["date"] for r in new_rows}
        kept = [r for r in existing_by_id[loc_id].get("daily", [])
                if r["date"] not in new_dates]
        merged = kept + new_rows
        merged.sort(key=lambda r: r["date"])

        existing_by_id[loc_id]["daily"] = merged

        if new_studio.get("reviews") is not None:
            existing_by_id[loc_id]["reviews"] = new_studio["reviews"]

        print(f"  {new_studio['name']}: {len(merged)} daily rows after merge")

    existing["generated_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False, default=str)

    print(f"  Written → {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--existing", required=True)
    parser.add_argument("--new",      required=True)
    parser.add_argument("--output",   default=None)
    args = parser.parse_args()

    output = args.output or args.existing
    merge(args.existing, args.new, output)


if __name__ == "__main__":
    main()
