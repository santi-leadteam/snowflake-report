#!/usr/bin/env python3
"""
build_scorecard_from_data.py

Rebuilds leads (total_leads) and presales (presales_count) in nso_scorecard_data.json
from daily MindBody data in data.json.

Week boundaries come from the date_range strings in nso_scorecard_data.json, which
match the exact dates defined in the Excel scorecard (e.g. Week 1 = 2/10-2/15, not a
calendar Mon-Sun week).

Fields NOT modified: targets, co_week, go_week, date_range, estimated_day1_rmr,
total_marketing_spend, blended_cpl, blended_cpa.

Run:
  python scripts/build_scorecard_from_data.py
  python scripts/build_scorecard_from_data.py --dry-run
  python scripts/build_scorecard_from_data.py --studio "Naples - Mercato"
"""

import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
TODAY = date.today()


def parse_week_dates(date_range: str, year: int):
    """
    Parse a date_range string into (start_date_or_None, end_date).

    Handles:
      "2/10 - 2/15"                    → (2026-02-10, 2026-02-15)
      "Ads Go Live 2/10 - 2/15"        → (2026-02-10, 2026-02-15)
      "Move to $129 on 3/3 3/2 - 3/8"  → (2026-03-02, 2026-03-08)
      "Target C/O 7/23  7/20 - 7/26"   → (2026-07-20, 2026-07-26)
      "Pre 2/10"                        → (None, 2026-02-09)
    """
    # Two dates: M/D - M/D
    m = re.search(r'(\d{1,2})/(\d{1,2})\s*[-]\s*(\d{1,2})/(\d{1,2})', date_range)
    if m:
        try:
            start = date(year, int(m.group(1)), int(m.group(2)))
            end   = date(year, int(m.group(3)), int(m.group(4)))
            if end < start:                      # year rollover (e.g. 12/30-1/5)
                end = date(year + 1, int(m.group(3)), int(m.group(4)))
            return start, end
        except ValueError:
            pass

    # "Pre M/D" → everything before that date
    m = re.search(r'Pre\s+(\d{1,2})/(\d{1,2})', date_range, re.IGNORECASE)
    if m:
        try:
            end = date(year, int(m.group(1)), int(m.group(2))) - timedelta(days=1)
            return None, end
        except ValueError:
            pass

    return None, None


def main():
    dry_run  = "--dry-run"  in sys.argv
    studio_filter = None
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--studio" and i < len(sys.argv):
            studio_filter = sys.argv[i + 1]

    sc_path   = ROOT / "nso_scorecard_data.json"
    data_path = ROOT / "data.json"

    print(f"Loading {data_path.name} ...")
    with open(data_path, encoding="utf-8") as f:
        raw = json.load(f)

    print(f"Loading {sc_path.name} ...")
    with open(sc_path, encoding="utf-8") as f:
        sc = json.load(f)

    daily = raw.get("daily_detail", [])

    for studio in sc["studios"]:
        full_name = studio["full_name"]
        if studio_filter and studio_filter.lower() not in full_name.lower():
            continue

        weeks = studio["weeks"]
        print(f"\n{'='*72}")
        print(f"  {full_name}")
        print(f"{'='*72}")

        # Daily rows for this studio
        studio_rows = [r for r in daily if r.get("studio") == full_name]
        if not studio_rows:
            print("  No rows found in data.json — skipping.")
            continue

        # Aggregate to day-level (signups = new leads, first_sales = presales/Won)
        by_date: dict[str, dict] = {}
        for r in studio_rows:
            d = str(r.get("date", ""))[:10]
            if len(d) < 10:
                continue
            if d not in by_date:
                by_date[d] = {"leads": 0, "sales": 0}
            by_date[d]["leads"] += int(r.get("signups")    or 0)
            by_date[d]["sales"] += int(r.get("first_sales") or 0)

        all_days = sorted(by_date)
        base_year = int(all_days[0][:4]) if all_days else 2026
        print(f"  data.json range : {all_days[0]} to {all_days[-1]}  (year: {base_year})")

        # Parse week boundaries
        bounds: list[tuple] = []
        for wk in weeks:
            start, end = parse_week_dates(wk["date_range"], base_year)
            bounds.append((start, end))

        # Bucket each day into exactly one week
        weekly_leads = [0] * len(weeks)
        weekly_sales = [0] * len(weeks)
        unassigned   = []

        for day_str, vals in sorted(by_date.items()):
            day = date.fromisoformat(day_str)
            hit = False
            for i, (start, end) in enumerate(bounds):
                if end is None:
                    continue
                if start is None:           # Week 0 style
                    if day <= end:
                        weekly_leads[i] += vals["leads"]
                        weekly_sales[i] += vals["sales"]
                        hit = True
                        break
                else:
                    if start <= day <= end:
                        weekly_leads[i] += vals["leads"]
                        weekly_sales[i] += vals["sales"]
                        hit = True
                        break
            if not hit:
                unassigned.append(day_str)

        if unassigned:
            print(f"  ⚠  {len(unassigned)} day(s) not assigned to any week: {unassigned[:8]}")

        # Print comparison table
        hdr = f"  {'Week':<10} {'Date Range':<33} {'New L':>6} {'New S':>6}  {'Cum L':>6} {'Cum S':>6}  {'Prev L':>7} {'Prev S':>7}"
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))

        cum_leads = cum_sales = 0
        current_week_idx = 0

        for i, wk in enumerate(weeks):
            cum_leads += weekly_leads[i]
            cum_sales += weekly_sales[i]

            prev_l = wk.get("total_leads")
            prev_s = wk.get("presales_count")
            prev_l_str = f"{int(prev_l)}" if prev_l is not None else "null"
            prev_s_str = f"{int(prev_s)}" if prev_s is not None else "null"

            _, end = bounds[i]
            flag = ""
            if end is not None and end <= TODAY:
                current_week_idx = i
                if weekly_leads[i] == 0 and weekly_sales[i] == 0:
                    flag = "  (no data)"

            print(
                f"  {wk['week']:<10} {wk['date_range'][:32]:<33} "
                f"{weekly_leads[i]:>6} {weekly_sales[i]:>6}  "
                f"{cum_leads:>6} {cum_sales:>6}  "
                f"{prev_l_str:>7} {prev_s_str:>7}{flag}"
            )

            if not dry_run:
                wk["total_leads"] = float(cum_leads)
                # Only set presales_count if we have any data for this or earlier weeks
                if cum_sales > 0 or weekly_sales[i] > 0:
                    wk["presales_count"] = float(cum_sales)
                elif wk.get("presales_count") is not None and wk["presales_count"] == 0:
                    wk["presales_count"] = None   # keep future weeks as null

        if not dry_run:
            studio["current_week"] = current_week_idx

        print(f"\n  -> current_week set to {current_week_idx}  ({weeks[current_week_idx]['week']})")

    if dry_run:
        print("\n[DRY RUN] Nothing written.")
        return

    with open(sc_path, "w", encoding="utf-8") as f:
        json.dump(sc, f, indent=2)

    print(f"\nOK  Wrote {sc_path}")


if __name__ == "__main__":
    main()
