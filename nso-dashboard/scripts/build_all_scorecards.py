#!/usr/bin/env python3
"""
build_all_scorecards.py

Generates nso_scorecard_data.json for all 5 NSO studios.

Naples      : parses existing Excel week structure (irregular Week 1), keeps spend/RMR
Dallas      : Week 1 = Mon 11/10/2025  (7-day Mon-Sun windows)
Reston      : Week 1 = Mon 11/17/2025
Pinecrest   : Week 1 = Mon 12/15/2025
Herriman    : Week 1 = Mon 05/18/2026  (not started yet)

Fills per week from existing data:
  total_leads / presales_count   from data.json  (cumulative)
  ig_new_followers               from social_insights.json (last 30-day API window only)
  spend, RMR, CPL, CPA           Naples only (from Excel); null for others

Usage:
  python scripts/build_all_scorecards.py
  python scripts/build_all_scorecards.py --dry-run
"""

import json, sys, re
from datetime import date, timedelta
from collections import defaultdict
from pathlib import Path

ROOT      = Path(__file__).parent.parent        # nso-dashboard/
REPO_ROOT = ROOT.parent                          # repo root (where data.json lives)
TODAY = date.today()

# Map Snowflake studio_id → full_name (same as fetch_nso_snowflake.py)
STUDIO_ID_TO_NAME = {
    "5751381": "SWEAT440 Naples - Mercato",
    "5752080": "SWEAT440 Herriman",
    "5750138": "SWEAT440 Dallas - Prestonwood",
    "5750128": "SWEAT440 Pinecrest - Palmetto Bay",
    "5750130": "SWEAT440 Reston",
}
NUM_WEEKS = 28   # weeks to generate per studio (W0..W28)

# ── Studio definitions ──────────────────────────────────────────────────────
STUDIO_CFG = [
    {
        "full_name"   : "SWEAT440 Naples - Mercato",
        "name"        : "Naples - Mercato",
        "code"        : "FL-019",
        "week1_start" : date(2026, 2, 10),   # Tuesday — keep Excel week structure
        "use_excel_weeks": True,              # parse weeks from existing JSON
        "go_week"     : 27,
        "co_week"     : 24,
        "ig_name"     : None,
        "targets"     : {
            "total_leads": 1257.142857,
            "presales_count": 440.0,
            "estimated_day1_rmr": 48660.0,
            "blended_cpl": "$28-$41",
            "blended_cpa": "$80-$116",
        },
    },
    {
        "full_name"   : "SWEAT440 Dallas - Prestonwood",
        "name"        : "Dallas - Prestonwood",
        "code"        : "TX-003",
        "week1_start" : date(2025, 11, 10),  # Monday
        "use_excel_weeks": False,
        "go_week"     : None,
        "co_week"     : None,
        "ig_name"     : "Dallas - Prestonwood",
        "targets"     : {},
    },
    {
        "full_name"   : "SWEAT440 Reston",
        "name"        : "Reston",
        "code"        : "VA-001",
        "week1_start" : date(2025, 11, 17),  # Monday
        "use_excel_weeks": False,
        "go_week"     : None,
        "co_week"     : None,
        "ig_name"     : "Reston",
        "targets"     : {},
    },
    {
        "full_name"   : "SWEAT440 Pinecrest - Palmetto Bay",
        "name"        : "Pinecrest - Palmetto Bay",
        "code"        : "FL-017",
        "week1_start" : date(2025, 12, 15),  # Monday
        "use_excel_weeks": False,
        "go_week"     : None,
        "co_week"     : None,
        "ig_name"     : "Pinecrest - Palmetto Bay",
        "targets"     : {},
    },
    {
        "full_name"   : "SWEAT440 Herriman",
        "name"        : "Herriman",
        "code"        : "UT-001",
        "week1_start" : date(2026, 5, 18),   # Monday — not started yet
        "use_excel_weeks": False,
        "go_week"     : None,
        "co_week"     : None,
        "ig_name"     : "Herriman",
        "targets"     : {},
    },
]

# Naples spend/RMR/CPL/CPA from Excel (only for weeks with data)
NAPLES_FINANCIAL = {
    1:  {"estimated_day1_rmr": 1287.0,  "total_marketing_spend": 3225.77,  "blended_cpl": 71.68,  "blended_cpa": 248.14},
    2:  {"estimated_day1_rmr": 2376.0,  "total_marketing_spend": 4008.85,  "blended_cpl": 47.72,  "blended_cpa": 167.04},
    3:  {"estimated_day1_rmr": 6039.0,  "total_marketing_spend": 4725.33,  "blended_cpl": 28.99,  "blended_cpa": 77.46},
    4:  {"estimated_day1_rmr": 9495.0,  "total_marketing_spend": 5416.68,  "blended_cpl": 24.73,  "blended_cpa": 57.02},
    5:  {"estimated_day1_rmr": 10209.0, "total_marketing_spend": 7976.91,  "blended_cpl": 34.68,  "blended_cpa": 78.98},
    6:  {"estimated_day1_rmr": 11757.0, "total_marketing_spend": 8700.05,  "blended_cpl": 27.71,  "blended_cpa": 76.99},
    7:  {"estimated_day1_rmr": 12888.0, "total_marketing_spend": 9254.55,  "blended_cpl": 27.14,  "blended_cpa": 75.86},
    8:  {"estimated_day1_rmr": 14436.0, "total_marketing_spend": 10145.29, "blended_cpl": 27.35,  "blended_cpa": 75.71},
    9:  {"estimated_day1_rmr": 15210.0, "total_marketing_spend": 10718.01, "blended_cpl": 26.80,  "blended_cpa": 76.56},
    10: {"estimated_day1_rmr": 16143.0, "total_marketing_spend": 12430.45, "blended_cpl": 29.46,  "blended_cpa": 84.56},
}

NAPLES_DATE_RANGES = {
    0 : "Pre 2/10",
    1 : "Ads Go Live 2/10 - 2/15",
    2 : "2/16 - 2/22",
    3 : "2/23 - 3/1",
    4 : "Move to $129 on 3/3 3/2 - 3/8",
    5 : "3/9 - 3/15",
    6 : "3/16 - 3/22",
    7 : "3/23 - 3/29",
    8 : "3/30 - 4/5",
    9 : "4/6 - 4/12",
    10: "4/13 - 4/19",
    24: "Target C/O 7/23  7/20 - 7/26",
    27: "Target Grand Open 8/10 - 8/16",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_dates_from_range(dr: str, base_year: int = 2026):
    """Parse 'M/D - M/D' strings into (date_start, date_end). Returns (None,None) on failure."""
    m = re.search(r'(\d{1,2})/(\d{1,2})\s*[-]\s*(\d{1,2})/(\d{1,2})', dr)
    if m:
        sm, sd, em, ed = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
        try:
            s = date(base_year, sm, sd)
            e = date(base_year, em, ed)
            if e < s:
                e = date(base_year + 1, em, ed)
            return s, e
        except ValueError:
            pass
    return None, None


def fmt_dr(s: date, e: date) -> str:
    return f"{s.month}/{s.day} - {e.month}/{e.day}"


def make_mon_sun_bounds(week1_start: date, num_weeks: int):
    """Generate (wn, date_start, date_end) list for standard Mon-Sun 7-day weeks."""
    assert week1_start.weekday() == 0, f"week1_start must be Monday, got {week1_start.strftime('%A')}"
    bounds = [(0, None, week1_start - timedelta(days=1))]
    for i in range(1, num_weeks + 1):
        ws = week1_start + timedelta(weeks=i - 1)
        we = ws + timedelta(days=6)
        bounds.append((i, ws, we))
    return bounds


def make_naples_bounds(base_year: int = 2026):
    """Build Naples week bounds from the known Excel date ranges."""
    bounds = [(0, None, date(base_year, 2, 9))]   # Pre 2/10
    # Weeks with explicit date ranges
    explicit = {
        1 : (date(2026, 2, 10), date(2026, 2, 15)),
        2 : (date(2026, 2, 16), date(2026, 2, 22)),
        3 : (date(2026, 2, 23), date(2026, 3,  1)),
        4 : (date(2026, 3,  2), date(2026, 3,  8)),
        5 : (date(2026, 3,  9), date(2026, 3, 15)),
        6 : (date(2026, 3, 16), date(2026, 3, 22)),
        7 : (date(2026, 3, 23), date(2026, 3, 29)),
        8 : (date(2026, 3, 30), date(2026, 4,  5)),
        9 : (date(2026, 4,  6), date(2026, 4, 12)),
        10: (date(2026, 4, 13), date(2026, 4, 19)),
        24: (date(2026, 7, 20), date(2026, 7, 26)),
        27: (date(2026, 8, 10), date(2026, 8, 16)),
    }
    # Fill remaining weeks 11-28 as standard Mon-Sun from week 11 start
    last_explicit_end = date(2026, 4, 19)  # Week 10 end
    next_mon = last_explicit_end + timedelta(days=1)  # 4/20 = Monday
    for wn in range(1, NUM_WEEKS + 1):
        if wn in explicit:
            bounds.append((wn, explicit[wn][0], explicit[wn][1]))
        else:
            offset = wn - 11
            ws = next_mon + timedelta(weeks=offset)
            we = ws + timedelta(days=6)
            bounds.append((wn, ws, we))
    return bounds


def aggregate_daily(daily_rows, full_name):
    by = defaultdict(lambda: {"leads": 0, "sales": 0, "grassroots": 0})
    for r in daily_rows:
        if r.get("studio") != full_name:
            continue
        d = str(r.get("date", ""))[:10]
        if len(d) < 10:
            continue
        by[d]["leads"]     += int(r.get("signups") or 0)
        by[d]["sales"]     += int(r.get("first_sales") or 0)
        if str(r.get("source", "")).lower() == "grassroots":
            by[d]["grassroots"] += int(r.get("signups") or 0)
    return by


def ig_follower_by_date(social_raw, ig_name):
    if not social_raw or not ig_name:
        return {}
    for ig in social_raw.get("instagram", []):
        if ig.get("studio") == ig_name:
            return {r["date"]: r["follower_count"]
                    for r in ig.get("daily", []) if "follower_count" in r}
    return {}


def build_sales_lookup(sales_raw):
    """Build {full_name: {date_str: {presales, cancellations}}} from nso_sales_data.json."""
    lookup = defaultdict(lambda: defaultdict(lambda: {"presales": 0, "cancellations": 0}))
    if not sales_raw:
        return lookup
    for sid, s in sales_raw.get("studios", {}).items():
        full = STUDIO_ID_TO_NAME.get(sid)
        if not full:
            continue
        for row in s.get("daily", []):
            d = row["date"]
            lookup[full][d]["presales"]      += row.get("presales", 0)
            lookup[full][d]["cancellations"] += row.get("cancellations", 0)
    return lookup


def sum_week(bounds_entry, by_date, ig_fc, sales_by_date=None):
    """Sum leads/presales/cancellations/ig for a single (wn, ws, we) bound."""
    wn, ws, we = bounds_entry
    leads = grassroots = ig_sum = 0
    presales = cancellations = 0
    has_ig = False
    day = ws if ws else None
    if sales_by_date is None:
        sales_by_date = {}

    if day is None:
        # Week 0: everything <= we
        for d_str, v in by_date.items():
            if d_str <= we.isoformat():
                leads      += v["leads"]
                grassroots += v["grassroots"]
        for d_str, sv in sales_by_date.items():
            if d_str <= we.isoformat():
                presales      += sv["presales"]
                cancellations += sv["cancellations"]
        for d_str, fc in ig_fc.items():
            if d_str <= we.isoformat():
                ig_sum += fc; has_ig = True
    else:
        while day <= we:
            ds = day.isoformat()
            v  = by_date.get(ds)
            if v:
                leads      += v["leads"]
                grassroots += v["grassroots"]
            sv = sales_by_date.get(ds)
            if sv:
                presales      += sv["presales"]
                cancellations += sv["cancellations"]
            fc = ig_fc.get(ds)
            if fc is not None:
                ig_sum += fc; has_ig = True
            day += timedelta(days=1)
    return leads, presales, cancellations, grassroots, (ig_sum if has_ig else None)


def current_week_num(bounds):
    """Week number of the latest bound whose date_start <= TODAY."""
    cw = 0
    for wn, ws, we in bounds:
        if ws is None:
            continue
        if ws <= TODAY:
            cw = wn
    return cw


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv

    print("Loading data.json ...")
    with open(REPO_ROOT / "data.json", encoding="utf-8") as f:
        raw = json.load(f)
    daily_rows = raw.get("daily_detail", [])

    try:
        with open(ROOT / "social_insights.json", encoding="utf-8") as f:
            social_raw = json.load(f)
        print("Loaded social_insights.json")
    except FileNotFoundError:
        social_raw = {}
        print("social_insights.json not found — IG data will be null")

    try:
        with open(ROOT / "nso_sales_data.json", encoding="utf-8") as f:
            sales_raw = json.load(f)
        print("Loaded nso_sales_data.json")
    except FileNotFoundError:
        sales_raw = {}
        print("nso_sales_data.json not found — presales/cancellations from sales will be null")

    sales_lookup = build_sales_lookup(sales_raw)

    output_studios = []

    for cfg in STUDIO_CFG:
        full = cfg["full_name"]
        print(f"\n{'='*60}\n  {full}\n{'='*60}")

        # Build week bounds
        if cfg["use_excel_weeks"]:
            bounds = make_naples_bounds()
        else:
            bounds = make_mon_sun_bounds(cfg["week1_start"], NUM_WEEKS)

        by_date        = aggregate_daily(daily_rows, full)
        ig_fc          = ig_follower_by_date(social_raw, cfg.get("ig_name"))
        sales_by_date  = sales_lookup[full]
        cw_num         = current_week_num(bounds)

        print(f"  current_week: {cw_num}  (today={TODAY})")

        cum_leads = cum_presales = cum_canc = 0
        weeks_out = []

        for bound in bounds:
            wn, ws, we = bound
            wl, wp, wc, gr, ig_val = sum_week(bound, by_date, ig_fc, sales_by_date)
            cum_leads   += wl
            cum_presales += wp
            cum_canc    += wc

            # Labels
            if wn == 0:
                wk_label = "Week 0"
                if cfg["use_excel_weeks"]:
                    dr = NAPLES_DATE_RANGES.get(0, f"Pre {cfg['week1_start'].month}/{cfg['week1_start'].day}")
                else:
                    dr = f"Pre {cfg['week1_start'].month}/{cfg['week1_start'].day}"
            else:
                wk_label = f"WEEK {wn}"
                if cfg["use_excel_weeks"] and wn in NAPLES_DATE_RANGES:
                    dr = NAPLES_DATE_RANGES[wn]
                else:
                    dr = fmt_dr(ws, we) if ws else ""

            # Cumulative totals: null if no data yet (avoids false 0s in chart)
            tl = float(cum_leads)    if cum_leads    > 0 else (0.0 if wn == 0 else None)
            pc = float(cum_presales) if cum_presales > 0 else (0.0 if wn == 0 else None)
            cc = float(cum_canc)     if cum_canc     > 0 else (0.0 if wn == 0 else None)

            entry = {
                "week"                  : wk_label,
                "date_range"            : dr,
                "date_start"            : ws.isoformat() if ws else None,
                "date_end"              : we.isoformat() if we else None,
                "total_leads"           : tl,
                "presales_count"        : pc,
                "presales_week"         : wp if (wp > 0 or (wn > 0 and ws and ws <= TODAY)) else None,
                "cancellations_count"   : cc,
                "cancellations_week"    : wc if (wc > 0 or (wn > 0 and ws and ws <= TODAY)) else None,
                "ig_new_followers"      : ig_val,
                "estimated_day1_rmr"    : None,
                "total_marketing_spend" : None,
                "blended_cpl"           : None,
                "blended_cpa"           : None,
            }

            # Naples financial data from Excel
            if cfg["use_excel_weeks"] and wn in NAPLES_FINANCIAL:
                entry.update(NAPLES_FINANCIAL[wn])

            ig_str = str(ig_val) if ig_val is not None else "-"
            print(f"  W{wn:02d} {dr[:22]:<23} +{wl:4d}L +{wp:3d}P -{wc:2d}C  cum={cum_leads:4d}/{cum_presales:3d}  ig={ig_str}")

            weeks_out.append(entry)

        studio_entry = {
            "name"         : cfg["name"],
            "code"         : cfg["code"],
            "full_name"    : full,
            "targets"      : cfg["targets"],
            "co_week"      : cfg["co_week"],
            "go_week"      : cfg["go_week"],
            "current_week" : cw_num,
            "weeks"        : weeks_out,
        }
        output_studios.append(studio_entry)

    if dry_run:
        print("\n[DRY RUN] Not writing.")
        return

    out_path = ROOT / "nso_scorecard_data.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"studios": output_studios}, f, indent=2)
    size_kb = out_path.stat().st_size // 1000
    print(f"\nOK  Written {out_path}  ({size_kb} KB)")


if __name__ == "__main__":
    main()
