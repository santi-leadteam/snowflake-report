#!/usr/bin/env python3
"""
fetch_nso_sales.py — Pull presales and cancellations for all 5 NSO studios
from MART_SALES_DETAILS in Snowflake.

Outputs: nso_sales_data.json

Key logic:
  - Presales : PRODUCT_DESCRIPTION IN ('Pre Sale Membership','Pre Sales Membership'),
               QUANTITY=1, IS_RETURN=0
  - Cancellations: QUANTITY=-1, IS_RETURN=1
  - Revenue  : GROSS_PAYMENTAMT_LOCAL (PAYMENTAMT_LOCAL is always 0 for presales)
  - Dedup    : every transaction appears twice (LOCATION_ID 1 + 98).
               ROW_NUMBER() OVER (PARTITION BY CLIENT_ID, PRODUCT_DESCRIPTION,
               SALE_DATE::DATE, QUANTITY ORDER BY UNIQUE_SALE_ID) keeps rn=1 only.

Usage:
  python scripts/fetch_nso_sales.py
  python scripts/fetch_nso_sales.py --start 2025-01-01 --end 2026-05-07
  python scripts/fetch_nso_sales.py --output nso_sales_data.json
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import snowflake.connector

NSO_STUDIOS = {
    5751381: {"name": "SWEAT440 Naples - Mercato",        "code": "FL-019"},
    5752080: {"name": "SWEAT440 Herriman",                "code": "UT-001"},
    5750138: {"name": "SWEAT440 Dallas - Prestonwood",    "code": "TX-003"},
    5750128: {"name": "SWEAT440 Pinecrest - Palmetto Bay","code": "FL-017"},
    5750130: {"name": "SWEAT440 Reston",                  "code": "VA-001"},
}
ID_LIST = ",".join(str(i) for i in NSO_STUDIOS)


def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )


def fetch_sales(cur, start_date, end_date):
    sql = f"""
        WITH leads_dedup AS (
            SELECT
                CLIENT_EMAIL,
                STUDIO_ID,
                LEAD_SOURCE,
                ROW_NUMBER() OVER (
                    PARTITION BY CLIENT_EMAIL, STUDIO_ID
                    ORDER BY IFF(LEAD_SOURCE IS NULL, 1, 0), STAGE_START ASC
                ) AS rn
            FROM MINDBODY_REPORTING_ANALYTICS.MART_LEADS_LOG
        ),
        clients AS (
            SELECT
                EMAIL_ID,
                STUDIO_ID,
                REFERRED_BY,
                ROW_NUMBER() OVER (
                    PARTITION BY EMAIL_ID, STUDIO_ID
                    ORDER BY SIGNEDUP_DATE ASC
                ) AS rn
            FROM MINDBODY_REPORTING_ANALYTICS.MART_CLIENTS
            WHERE LOWER(TRIM(EMAIL_ID)) NOT LIKE '%sweat440%'
              AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%leadteam%'
              AND LOWER(TRIM(EMAIL_ID)) NOT LIKE '%test%'
        )
        SELECT
            s.STUDIO_ID,
            s.SALE_DATE::DATE AS date,
            CASE
                WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'facebook lead ad'                                         THEN 'Meta Ads'
                WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'instagram'                                                THEN 'Meta Ads'
                WHEN LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%facebook%' AND LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%lead%' THEN 'Meta Ads'
                WHEN LOWER(TRIM(l.LEAD_SOURCE)) LIKE '%instagram%'                                           THEN 'Meta Ads'
                WHEN LOWER(TRIM(c.REFERRED_BY)) = 'internet / ai search'                                    THEN 'Google Ads'
                WHEN LOWER(TRIM(c.REFERRED_BY)) = 'google ads'                                              THEN 'Google Ads'
                WHEN LOWER(TRIM(c.REFERRED_BY)) = 'tiktok ads'                                              THEN 'TikTok Ads'
                WHEN LOWER(TRIM(c.REFERRED_BY)) = 'local listings'                                          THEN 'Local Listings'
                WHEN LOWER(TRIM(c.REFERRED_BY)) = 'local event'                                             THEN 'Grassroots'
                WHEN LOWER(TRIM(c.REFERRED_BY)) = 'print ads / signs'                                       THEN 'Print Ads / Signs'
                WHEN LOWER(TRIM(c.REFERRED_BY)) = 'social media'                                            THEN 'Social Media Organic'
                WHEN LOWER(TRIM(l.LEAD_SOURCE)) = 'branded web app (bwa)'                                   THEN 'Website (unattributed)'
                WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('branded mobile app (bma)', 'consumer mode')            THEN 'SWEAT440 App'
                WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('business app', 'business mode', 'public api')          THEN 'Business Mode'
                WHEN LOWER(TRIM(l.LEAD_SOURCE)) IN ('mindbody app', 'mindbody web')                         THEN 'MindBody App'
                WHEN LOWER(TRIM(c.REFERRED_BY)) IN ('another client', 'word of mouth')                      THEN 'Word of Mouth'
                WHEN LOWER(TRIM(c.REFERRED_BY)) IN (
                    'drive by', 'flyer', 'frederick', 'holly met outside of gym.',
                    'internet', 'n/a', 'newspaper', 'other', 'radio', 'tv / streaming'
                )                                                                                            THEN 'Other'
                WHEN LOWER(TRIM(c.REFERRED_BY)) IN ('classpass', 'wellhub', 'wellness passport')            THEN 'ClassPass / Platforms'
                ELSE 'N/A'
            END AS source,
            COUNT(CASE WHEN LOWER(s.PRODUCT_DESCRIPTION) LIKE '%pre%sale%'
                            AND s.QUANTITY = 1 AND s.IS_RETURN = 0
                       THEN 1 END)                                           AS presales,
            COUNT(CASE WHEN LOWER(s.PRODUCT_DESCRIPTION) LIKE '%pre%sale%'
                            AND s.QUANTITY = -1 AND s.IS_RETURN = 1
                       THEN 1 END)                                           AS cancellations,
            SUM(CASE WHEN LOWER(s.PRODUCT_DESCRIPTION) LIKE '%pre%sale%'
                            AND s.QUANTITY = 1 AND s.IS_RETURN = 0
                     THEN COALESCE(s.GROSS_PAYMENTAMT_LOCAL, 0) ELSE 0 END)  AS presale_gross_revenue
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY CLIENT_ID, PRODUCT_DESCRIPTION,
                                 SALE_DATE::DATE, QUANTITY
                    ORDER BY UNIQUE_SALE_ID
                ) AS rn
            FROM MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS
            WHERE STUDIO_ID IN ({ID_LIST})
              AND SALE_DATE::DATE >= '{start_date}'
              AND SALE_DATE::DATE <= '{end_date}'
        ) s
        LEFT JOIN leads_dedup l
            ON LOWER(TRIM(l.CLIENT_EMAIL)) = LOWER(TRIM(s.EMAIL_ID))
           AND l.STUDIO_ID = s.STUDIO_ID
           AND l.rn = 1
        LEFT JOIN clients c
            ON LOWER(TRIM(c.EMAIL_ID)) = LOWER(TRIM(s.EMAIL_ID))
           AND c.STUDIO_ID = s.STUDIO_ID
           AND c.rn = 1
        WHERE s.rn = 1
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """
    cur.execute(sql)
    cols = [d[0].lower() for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    print(f"  {len(rows)} rows returned")
    return rows


def build_output(rows):
    studios = {}
    for sid, meta in NSO_STUDIOS.items():
        studios[str(sid)] = {
            "name": meta["name"],
            "code": meta["code"],
            "daily": [],
            "totals": {"presales": 0, "cancellations": 0, "net_presales": 0, "gross_revenue": 0.0},
        }

    for r in rows:
        sid = str(int(r["studio_id"]))
        if sid not in studios:
            continue
        day = str(r["date"])[:10]
        source        = r.get("source") or "Unknown"
        presales      = int(r.get("presales") or 0)
        cancellations = int(r.get("cancellations") or 0)
        gross         = float(r.get("presale_gross_revenue") or 0)

        studios[sid]["daily"].append({
            "date": day,
            "source": source,
            "presales": presales,
            "cancellations": cancellations,
            "gross_revenue": round(gross, 2),
        })
        studios[sid]["totals"]["presales"]      += presales
        studios[sid]["totals"]["cancellations"] += cancellations
        studios[sid]["totals"]["gross_revenue"] += gross

    for sid in studios:
        t = studios[sid]["totals"]
        t["net_presales"]  = t["presales"] - t["cancellations"]
        t["gross_revenue"] = round(t["gross_revenue"], 2)

    return studios


def main():
    parser = argparse.ArgumentParser(description="Fetch NSO presales/cancellations from Snowflake")
    parser.add_argument("--start", default="2025-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   default=None,         help="End date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--output", default="nso_sales_data.json")
    args = parser.parse_args()

    end_date   = args.end or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = args.start

    print("=" * 60)
    print("NSO Sales Fetch (MART_SALES_DETAILS)")
    print(f"Date range : {start_date} to {end_date}")
    print(f"Studios    : {', '.join(m['name'].replace('SWEAT440 ','') for m in NSO_STUDIOS.values())}")
    print("=" * 60)

    try:
        conn = get_conn()
    except Exception as e:
        print(f"ERROR connecting to Snowflake: {e}")
        sys.exit(1)

    cur = conn.cursor()
    try:
        print("\nQuerying MART_SALES_DETAILS...")
        rows = fetch_sales(cur, start_date, end_date)
    finally:
        cur.close()
        conn.close()

    studios = build_output(rows)

    print("\nSummary:")
    for sid, s in studios.items():
        t = s["totals"]
        print(f"  {s['name'].replace('SWEAT440 ',''):<28}  presales={t['presales']:>4}  "
              f"canc={t['cancellations']:>3}  net={t['net_presales']:>4}  "
              f"revenue=${t['gross_revenue']:>10,.2f}")

    output = {
        "generated_at": datetime.now().isoformat(),
        "date_range": {"start": start_date, "end": end_date},
        "studios": studios,
    }

    out_path = Path(args.output)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    size_kb = out_path.stat().st_size / 1000
    print(f"\nDone. Written to {out_path} ({size_kb:.1f} KB)")
    print("=" * 60)


if __name__ == "__main__":
    main()
