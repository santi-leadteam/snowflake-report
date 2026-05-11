"""
merge_array_json.py — Generic merger for JSON files that contain flat arrays of
daily rows (marketing_data.json, nso_google_ads.json).

Usage:
  # marketing_data.json  (keys: meta_ads, ga4_traffic)
  python scripts/merge_array_json.py \
      --existing marketing_data.json \
      --new      marketing_data_new.json \
      --keys     meta_ads ga4_traffic

  # nso_google_ads.json  (key: google_ads)
  python scripts/merge_array_json.py \
      --existing nso_google_ads.json \
      --new      nso_google_ads_new.json \
      --keys     google_ads

Logic:
  For every key listed in --keys, removes rows from the existing array whose
  'date' value falls within the date range present in the new array, then
  appends the new rows. Rows without a 'date' field are always kept.
"""

import argparse
import json
from datetime import datetime


def merge_array(existing_rows: list, new_rows: list) -> list:
    if not new_rows:
        return existing_rows

    new_dates = {r["date"] for r in new_rows if "date" in r}
    kept = [r for r in existing_rows if r.get("date") not in new_dates]
    merged = kept + new_rows
    merged.sort(key=lambda r: (r.get("date", ""), r.get("studio", "")))
    return merged


def merge(existing_path: str, new_path: str, output_path: str, keys: list) -> None:
    with open(existing_path, encoding="utf-8") as f:
        existing = json.load(f)
    with open(new_path, encoding="utf-8") as f:
        new = json.load(f)

    for key in keys:
        existing_rows = existing.get(key, [])
        new_rows      = new.get(key, [])
        if new_rows:
            merged = merge_array(existing_rows, new_rows)
            existing[key] = merged
            print(f"  {key}: {len(existing_rows)} → {len(merged)} rows")

    existing["generated_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False, default=str)

    print(f"  Written → {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--existing", required=True)
    parser.add_argument("--new",      required=True)
    parser.add_argument("--output",   default=None)
    parser.add_argument("--keys",     nargs="+", required=True,
                        help="Top-level JSON keys that hold daily-row arrays")
    args = parser.parse_args()

    output = args.output or args.existing
    merge(args.existing, args.new, output, args.keys)


if __name__ == "__main__":
    main()
