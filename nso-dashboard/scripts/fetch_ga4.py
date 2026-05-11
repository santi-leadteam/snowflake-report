"""
fetch_ga4.py — Pull Google Analytics 4 traffic data.

Writes THREE tabs to Google Sheets:
  - ga4_traffic     : All-site daily traffic (sessions/users) by source+medium
  - ga4_pages       : All pages — sessions, users, new_users per landing_page
  - ga4_studio_pages: Only studio location pages (filtered from ga4_pages),
                      tagged with studio_slug for per-location dashboards

The GA4 property is SHARED across the entire SWEAT440 site.
Per-location metrics are derived by filtering `landing_page` against each
franchise's studio_page_path (e.g. "/gyms/locations-florida-pinecrest/").

Usage in run_all.py:
    fetch_ga4.run(franchise_config, start_date, end_date)

Standalone (test mode):
    python fetch_ga4.py --start 2025-01-01 --end 2025-01-31
"""

from __future__ import annotations

import os
import argparse
from datetime import date, timedelta

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    FilterExpression,
    Filter,
)
from google.oauth2.service_account import Credentials

# Local helper — writes a list[dict] to a named Google Sheet tab
from sheets_writer import write_data


# ─────────────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────────────

def _get_client() -> BetaAnalyticsDataClient:
    key_path = os.environ["GOOGLE_SERVICE_ACCOUNT_KEY_PATH"]
    creds = Credentials.from_service_account_file(key_path)
    return BetaAnalyticsDataClient(credentials=creds)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Site-wide traffic (existing — source/medium level)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_traffic(
    property_id: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    Fetch daily site-wide sessions/users aggregated by source+medium.
    This covers ALL pages — used for the top-level traffic KPIs.
    """
    client = _get_client()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"),
        ],
        limit=100_000,
    )

    print(f"  Fetching GA4 site-wide traffic for property {property_id}...")
    response = client.run_report(request)

    rows = []
    for row in response.rows:
        d = row.dimension_values[0].value
        rows.append({
            "date":                   f"{d[:4]}-{d[4:6]}-{d[6:]}",
            "source":                 row.dimension_values[1].value,
            "medium":                 row.dimension_values[2].value,
            "sessions":               int(row.metric_values[0].value),
            "total_users":            int(row.metric_values[1].value),
            "new_users":              int(row.metric_values[2].value),
            "bounce_rate":            round(float(row.metric_values[3].value), 4),
            "avg_session_duration_s": round(float(row.metric_values[4].value), 1),
        })

    print(f"  → {len(rows)} site-wide traffic rows.")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# 2. All-pages fetch (NEW)
# ─────────────────────────────────────────────────────────────────────────────

# Studio location page slugs across the entire SWEAT440 site.
# Add new locations here as they launch.
STUDIO_PAGE_SLUGS: list[str] = [
    # Florida
    "/gyms/locations-florida-pinecrest/",
    "/gyms/locations-florida-doral/",
    "/gyms/locations-florida-miami-beach/",
    "/gyms/locations-florida-brickell/",
    "/gyms/locations-florida-coral-gables/",
    "/gyms/locations-florida-aventura/",
    "/gyms/locations-florida-ft-lauderdale/",
    "/gyms/locations-florida-pembroke-pines/",
    "/gyms/locations-florida-boca-raton/",
    "/gyms/locations-florida-west-palm-beach/",
    "/gyms/locations-florida-orlando/",
    "/gyms/locations-florida-naples-mercato/",
    # Texas
    "/gyms/locations-texas-austin-highland/",
    "/gyms/locations-texas-austin-domain/",
    "/gyms/locations-texas-dallas-prestonwood/",
    "/gyms/locations-texas-houston/",
    # Utah
    "/gyms/utah-herriman/",
    # Virginia
    "/gyms/virginia-reston/",
    # California
    "/gyms/locations-california-los-angeles/",
    "/gyms/locations-california-san-diego/",
    # Add more as needed
]


def fetch_all_pages(
    property_id: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    Fetch sessions/users for EVERY landing page on the SWEAT440 site.

    Returns one row per (date × landing_page × source × medium) so that
    any downstream filter — Pinecrest, Herriman, Doral, etc. — can be
    applied without re-querying GA4.

    The column `is_studio_page` is True for known studio location pages.
    """
    client = _get_client()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="landingPagePlusQueryString"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
        ],
        limit=100_000,
    )

    print(f"  Fetching GA4 all-pages traffic for property {property_id}...")
    response = client.run_report(request)

    rows = []
    for row in response.rows:
        d = row.dimension_values[0].value
        raw_page = row.dimension_values[1].value
        # Normalize: strip query strings, lowercase
        page_path = raw_page.split("?")[0].lower().rstrip("/") + "/"

        rows.append({
            "date":            f"{d[:4]}-{d[4:6]}-{d[6:]}",
            "landing_page":    page_path,
            "source":          row.dimension_values[2].value,
            "medium":          row.dimension_values[3].value,
            "sessions":        int(row.metric_values[0].value),
            "total_users":     int(row.metric_values[1].value),
            "new_users":       int(row.metric_values[2].value),
            "is_studio_page":  page_path in [s.lower() for s in STUDIO_PAGE_SLUGS],
        })

    print(f"  → {len(rows)} page-level rows across all pages.")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# 3. Per-location filter helpers
# ─────────────────────────────────────────────────────────────────────────────

def filter_by_page(
    all_pages_rows: list[dict],
    studio_page_path: str,
) -> list[dict]:
    """
    Return only rows whose landing_page matches the given studio_page_path.

    studio_page_path should be the slug stored in franchise_config.json,
    e.g. "/gyms/locations-florida-pinecrest/"

    Matching is normalized (lowercase, trailing slash).
    """
    slug = studio_page_path.lower().rstrip("/") + "/"
    filtered = [r for r in all_pages_rows if r["landing_page"] == slug]
    print(f"  → {len(filtered)} rows for studio page '{slug}'")
    return filtered


def build_studio_pages_summary(
    all_pages_rows: list[dict],
    slugs: list[str] | None = None,
) -> list[dict]:
    """
    Build a filtered view of all studio location pages (or a custom slug list).
    Adds a `studio_slug` column to identify the location.

    This is written to the `ga4_studio_pages` Sheet tab so every location
    dashboard can filter by `studio_slug` without separate API calls.
    """
    target_slugs = {s.lower().rstrip("/") + "/" for s in (slugs or STUDIO_PAGE_SLUGS)}
    rows = []
    for r in all_pages_rows:
        if r["landing_page"] in target_slugs:
            rows.append({**r, "studio_slug": r["landing_page"]})
    print(f"  → {len(rows)} rows across all studio location pages.")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# 4. Conversions
# ─────────────────────────────────────────────────────────────────────────────

def fetch_conversions(
    property_id: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """Fetch conversion/lead events from GA4 (site-wide)."""
    client = _get_client()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="eventName"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
        ],
        metrics=[Metric(name="eventCount")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="eventName",
                in_list_filter=Filter.InListFilter(
                    values=[
                        "generate_lead",
                        "form_submit",
                        "sign_up",
                        "purchase",
                        "contact",
                    ]
                ),
            )
        ),
        limit=10_000,
    )

    print(f"  Fetching GA4 conversions for property {property_id}...")
    response = client.run_report(request)

    rows = []
    for row in response.rows:
        d = row.dimension_values[0].value
        rows.append({
            "date":        f"{d[:4]}-{d[4:6]}-{d[6:]}",
            "event_name":  row.dimension_values[1].value,
            "source":      row.dimension_values[2].value,
            "medium":      row.dimension_values[3].value,
            "conversions": int(row.metric_values[0].value),
        })

    print(f"  → {len(rows)} conversion rows.")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# 5. Main entry point (called by run_all.py)
# ─────────────────────────────────────────────────────────────────────────────

def run(franchise_config: dict, start_date: str, end_date: str) -> None:
    """
    Main entry point — called by run_all.py for each franchise.

    Writes four Sheet tabs:
      ga4_traffic        — site-wide traffic by source/medium
      ga4_pages          — all pages (unfiltered, full site)
      ga4_studio_pages   — only studio location pages, all franchises
      ga4_conversions    — site-wide conversion events
    """
    ga4_cfg     = franchise_config["ga4"]
    property_id = ga4_cfg["property_id"]
    studio_page = ga4_cfg.get("studio_page_path")   # e.g. "/gyms/locations-florida-pinecrest/"

    # 1. Site-wide traffic
    traffic_rows = fetch_traffic(property_id, start_date, end_date)
    write_data(traffic_rows, tab_name="ga4_traffic")

    # 2. All pages (fetch once, reuse for all filters)
    all_pages_rows = fetch_all_pages(property_id, start_date, end_date)
    write_data(all_pages_rows, tab_name="ga4_pages")

    # 3. Studio pages summary (all known location pages in one tab)
    studio_pages_rows = build_studio_pages_summary(all_pages_rows)
    write_data(studio_pages_rows, tab_name="ga4_studio_pages")

    # 4. Franchise-specific page rows (bonus — written to named tab)
    if studio_page:
        loc_rows = filter_by_page(all_pages_rows, studio_page)
        # Derive a clean tab name: "pinecrest", "herriman", etc.
        slug_parts = studio_page.strip("/").split("-")
        loc_key = slug_parts[-1]   # last segment, e.g. "pinecrest"
        write_data(loc_rows, tab_name=f"ga4_{loc_key}")

    # 5. Conversions
    conv_rows = fetch_conversions(property_id, start_date, end_date)
    write_data(conv_rows, tab_name="ga4_conversions")

    print("  ✅ GA4 done.")


# ─────────────────────────────────────────────────────────────────────────────
# Standalone / test mode
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test GA4 fetch with page filtering")
    parser.add_argument("--start",    default=str(date.today() - timedelta(days=30)))
    parser.add_argument("--end",      default=str(date.today() - timedelta(days=1)))
    parser.add_argument("--property", default=os.environ.get("GA4_PROPERTY_ID", "341934364"))
    parser.add_argument(
        "--page",
        default="/gyms/locations-florida-pinecrest/",
        help="Studio page path to preview (default: Pinecrest)",
    )
    args = parser.parse_args()

    print(f"\n📊 GA4 Test — {args.start} → {args.end}")
    print(f"   Property : {args.property}")
    print(f"   Page filter: {args.page}\n")

    # Fetch once
    all_pages = fetch_all_pages(args.property, args.start, args.end)

    # Show Pinecrest (or whichever page)
    pinecrest = filter_by_page(all_pages, args.page)
    print(f"\n  Sample rows for {args.page}:")
    for r in pinecrest[:5]:
        print(f"    {r['date']}  {r['source']}/{r['medium']}  "
              f"sessions={r['sessions']}  new_users={r['new_users']}")

    # Show all studio pages summary
    studio_summary = build_studio_pages_summary(all_pages)
    slugs_found = {r["studio_slug"] for r in studio_summary}
    print(f"\n  Studio pages found in GA4 ({len(slugs_found)}):")
    for slug in sorted(slugs_found):
        count = sum(r["sessions"] for r in studio_summary if r["studio_slug"] == slug)
        print(f"    {slug:<55} {count:>6} sessions")
