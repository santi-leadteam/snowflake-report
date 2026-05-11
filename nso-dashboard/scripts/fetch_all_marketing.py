#!/usr/bin/env python3
"""
fetch_all_marketing.py — Pull ALL marketing data from all platforms into one JSON.

No studio filtering — pulls everything raw so we can analyze naming patterns
and build the studio mapping from actual data.

Outputs: marketing_data.json

Usage:
    python fetch_all_marketing.py --days 90
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
if not os.environ.get("META_ACCESS_TOKEN"):
    load_dotenv(Path.cwd() / ".env")


def safe(val, default=0):
    try:
        return float(val) if val is not None else default
    except:
        return default


# ════════════════════════════════════════════════════════════════════════════
# META ADS
# ════════════════════════════════════════════════════════════════════════════
def fetch_meta_ads(start_date, end_date):
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount

    print("\n>> META ADS")
    token = os.environ.get("META_ACCESS_TOKEN")
    app_id = os.environ.get("META_APP_ID", "")
    app_secret = os.environ.get("META_APP_SECRET", "")
    ad_account_id = os.environ.get("META_AD_ACCOUNT_ID", "act_1553887681409034")

    if not token:
        print("  WARNING: META_ACCESS_TOKEN not set - skipping")
        return []

    FacebookAdsApi.init(app_id, app_secret, token)
    account = AdAccount(ad_account_id)

    # Fetch creative metadata (thumbnail, image, preview link) keyed by ad_name
    print(f"  Fetching ad creatives...")
    creatives = {}
    try:
        ads = account.get_ads(fields=[
            "id", "name",
            "creative{thumbnail_url,image_url,object_type}",
            "preview_shareable_link",
        ])
        for ad in ads:
            creative = ad.get("creative") or {}
            creatives[ad["name"]] = {
                "thumbnail_url": creative.get("thumbnail_url", ""),
                "image_url": creative.get("image_url", ""),
                "preview_link": ad.get("preview_shareable_link", ""),
                "object_type": creative.get("object_type", ""),
            }
        print(f"  {len(creatives)} ad creatives fetched")
    except Exception as e:
        print(f"  WARNING: Could not fetch ad creatives: {e}")

    # Ad-level daily data
    params = {
        "time_range": {"since": start_date, "until": end_date},
        "time_increment": 1,
        "level": "ad",
        "filtering": [
            {"field": "ad.effective_status", "operator": "IN",
             "value": ["ACTIVE", "PAUSED", "COMPLETED"]}
        ],
    }
    fields = [
        "date_start",
        "campaign_name",
        "campaign_id",
        "adset_name",
        "ad_name",
        "spend",
        "impressions",
        "clicks",
        "ctr",
        "cpc",
        "actions",
        "cost_per_action_type",
    ]

    print(f"  Fetching ad-level data from {ad_account_id} ({start_date} to {end_date})...")
    rows = []
    try:
        insights = account.get_insights(params=params, fields=fields)
        for row in insights:
            leads = 0
            cost_per_lead = 0.0
            for action in row.get("actions", []):
                if action["action_type"] in ("lead", "offsite_conversion.fb_pixel_lead"):
                    leads += int(action["value"])
            for cpa in row.get("cost_per_action_type", []):
                if cpa["action_type"] in ("lead", "offsite_conversion.fb_pixel_lead"):
                    cost_per_lead = float(cpa["value"])

            ad_name = row.get("ad_name", "")
            creative = creatives.get(ad_name, {})

            rows.append({
                "date": row["date_start"],
                "campaign_name": row.get("campaign_name", ""),
                "campaign_id": row.get("campaign_id", ""),
                "adset_name": row.get("adset_name", ""),
                "ad_name": ad_name,
                "spend": round(safe(row.get("spend")), 2),
                "impressions": int(safe(row.get("impressions"))),
                "clicks": int(safe(row.get("clicks"))),
                "ctr": round(safe(row.get("ctr")), 2),
                "cpc": round(safe(row.get("cpc")), 2),
                "leads": leads,
                "cost_per_lead": round(cost_per_lead, 2),
                "thumbnail_url": creative.get("thumbnail_url", ""),
                "image_url": creative.get("image_url", ""),
                "preview_link": creative.get("preview_link", ""),
                "object_type": creative.get("object_type", ""),
            })
        print(f"  {len(rows)} ad-day rows fetched")
    except Exception as e:
        print(f"  ERROR: Meta Ads error: {e}")

    return rows


# ════════════════════════════════════════════════════════════════════════════
# GOOGLE ADS
# ════════════════════════════════════════════════════════════════════════════
def fetch_google_ads(start_date, end_date):
    from google.ads.googleads.client import GoogleAdsClient

    print("\n>> GOOGLE ADS")
    dev_token = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN")
    if not dev_token:
        print("  WARNING: GOOGLE_ADS_DEVELOPER_TOKEN not set - skipping")
        return []

    mcc_id = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", os.environ.get("GOOGLE_ADS_MCC_ID", "")).replace("-", "")

    config = {
        "developer_token": dev_token,
        "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "login_customer_id": mcc_id,
        "use_proto_plus": True,
    }
    client = GoogleAdsClient.load_from_dict(config)
    ga_service = client.get_service("GoogleAdsService")

    # First, list all accessible customer accounts under the MCC
    customer_service = client.get_service("CustomerService")
    
    # Query the MCC to get all child accounts
    query_accounts = """
        SELECT
            customer_client.id,
            customer_client.descriptive_name,
            customer_client.status,
            customer_client.manager
        FROM customer_client
        WHERE customer_client.manager = false
          AND customer_client.status = 'ENABLED'
    """

    print(f"  Listing accounts under MCC {mcc_id}...")
    accounts = []
    try:
        response = ga_service.search_stream(customer_id=mcc_id, query=query_accounts)
        for batch in response:
            for row in batch.results:
                accounts.append({
                    "id": str(row.customer_client.id),
                    "name": row.customer_client.descriptive_name,
                })
        print(f"  Found {len(accounts)} accounts")
    except Exception as e:
        print(f"  ERROR listing accounts: {e}")
        return []

    # Now query each account for campaign performance
    all_rows = []
    for acct in accounts:
        cid = acct["id"]
        query = f"""
            SELECT
                segments.date,
                campaign.name,
                campaign.id,
                campaign.status,
                customer.descriptive_name,
                metrics.cost_micros,
                metrics.impressions,
                metrics.clicks,
                metrics.ctr,
                metrics.average_cpc,
                metrics.conversions,
                metrics.cost_per_conversion
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND campaign.status != 'REMOVED'
            ORDER BY segments.date DESC
        """
        try:
            response = ga_service.search_stream(customer_id=cid, query=query)
            count = 0
            for batch in response:
                for row in batch.results:
                    spend = row.metrics.cost_micros / 1_000_000
                    avg_cpc = row.metrics.average_cpc / 1_000_000 if row.metrics.average_cpc else 0
                    cost_per_conv = row.metrics.cost_per_conversion / 1_000_000 if row.metrics.cost_per_conversion else 0

                    all_rows.append({
                        "date": row.segments.date,
                        "account_name": row.customer.descriptive_name,
                        "account_id": cid,
                        "campaign_name": row.campaign.name,
                        "campaign_id": str(row.campaign.id),
                        "status": row.campaign.status.name,
                        "spend": round(spend, 2),
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                        "ctr": round(row.metrics.ctr * 100, 2),
                        "avg_cpc": round(avg_cpc, 2),
                        "conversions": round(row.metrics.conversions, 1),
                        "cost_per_conversion": round(cost_per_conv, 2),
                    })
                    count += 1
            if count > 0:
                print(f"  OK {acct['name']}: {count} rows")
        except Exception as e:
            print(f"  SKIP {acct['name']} ({cid}): {e}")

    print(f"  Total: {len(all_rows)} Google Ads rows")
    return all_rows


# ════════════════════════════════════════════════════════════════════════════
# GA4
# ════════════════════════════════════════════════════════════════════════════
def fetch_ga4(start_date, end_date, location=None):
    """
    Fetch GA4 data for all pages (or a specific location slug).

    location: optional substring to filter page paths, e.g. "pinecrest"
              Filters both landing-page sessions and all-page views.
              If None, all pages are returned.
    """
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        DateRange, Dimension, Metric, RunReportRequest,
    )
    from google.oauth2.service_account import Credentials

    print("\n>> GA4")
    key_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY_PATH")
    property_id = os.environ.get("GA4_PROPERTY_ID", "341934364")

    if not key_path:
        print("  WARNING: GOOGLE_SERVICE_ACCOUNT_KEY_PATH not set - skipping")
        return []

    creds = Credentials.from_service_account_file(key_path)
    client = BetaAnalyticsDataClient(credentials=creds)

    # ── 1. Landing-page sessions ─────────────────────────────────────────────
    traffic_request = RunReportRequest(
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
        limit=100000,
    )

    print(f"  Fetching all landing-page sessions...")
    rows = []
    try:
        response = client.run_report(traffic_request)
        for row in response.rows:
            d = row.dimension_values[0].value
            rows.append({
                "date": f"{d[:4]}-{d[4:6]}-{d[6:]}",
                "landing_page": row.dimension_values[1].value,
                "source": row.dimension_values[2].value,
                "medium": row.dimension_values[3].value,
                "sessions": int(row.metric_values[0].value),
                "total_users": int(row.metric_values[1].value),
                "new_users": int(row.metric_values[2].value),
            })
        print(f"  OK {len(rows)} landing-page session rows")
    except Exception as e:
        print(f"  ERROR GA4 sessions: {e}")

    # ── 2. All-page views (pagePath) ─────────────────────────────────────────
    # Captures every page visited, not only the entry page of a session.
    pageview_request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="pagePath"),
            Dimension(name="pageTitle"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
        ],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="totalUsers"),
            Metric(name="averageSessionDuration"),
            Metric(name="bounceRate"),
        ],
        limit=100000,
    )

    print(f"  Fetching all page views...")
    page_views = []
    try:
        resp = client.run_report(pageview_request)
        for row in resp.rows:
            d = row.dimension_values[0].value
            page_views.append({
                "date": f"{d[:4]}-{d[4:6]}-{d[6:]}",
                "page_path": row.dimension_values[1].value,
                "page_title": row.dimension_values[2].value,
                "source": row.dimension_values[3].value,
                "medium": row.dimension_values[4].value,
                "page_views": int(row.metric_values[0].value),
                "total_users": int(row.metric_values[1].value),
                "avg_session_duration": round(float(row.metric_values[2].value), 1),
                "bounce_rate": round(float(row.metric_values[3].value) * 100, 1),
            })
        print(f"  OK {len(page_views)} page-view rows")
    except Exception as e:
        print(f"  WARNING GA4 page views: {e}")

    # ── 3. Conversions ───────────────────────────────────────────────────────
    conv_request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="eventName"),
            Dimension(name="landingPagePlusQueryString"),
            Dimension(name="sessionSource"),
        ],
        metrics=[Metric(name="eventCount")],
        limit=50000,
    )

    conversions = []
    try:
        resp = client.run_report(conv_request)
        for row in resp.rows:
            d = row.dimension_values[0].value
            event = row.dimension_values[1].value
            if event in ("generate_lead", "form_submit", "sign_up", "purchase", "contact", "submit_lead_form"):
                conversions.append({
                    "date": f"{d[:4]}-{d[4:6]}-{d[6:]}",
                    "event_name": event,
                    "landing_page": row.dimension_values[2].value,
                    "source": row.dimension_values[3].value,
                    "conversions": int(row.metric_values[0].value),
                })
        print(f"  OK {len(conversions)} conversion rows")
    except Exception as e:
        print(f"  WARNING GA4 conversions: {e}")

    return {"traffic": rows, "page_views": page_views, "conversions": conversions}


# ════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE (all studios)
# ════════════════════════════════════════════════════════════════════════════
def fetch_snowflake(start_date, end_date):
    import snowflake.connector

    print("\n>> SNOWFLAKE")
    acct = os.environ.get("SNOWFLAKE_ACCOUNT")
    if not acct:
        print("  WARNING: SNOWFLAKE_ACCOUNT not set - skipping")
        return {}

    conn = snowflake.connector.connect(
        account=acct,
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "PLAYLIST_DATA_MART"),
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )

    def query(sql):
        cur = conn.cursor()
        try:
            cur.execute(sql)
            cols = [d[0].lower() for d in cur.description]
            return [dict(zip(cols, row)) for row in cur]
        finally:
            cur.close()

    results = {}

    # Studios list
    print("  Fetching studio list...")
    studios = query("SELECT DISTINCT STUDIO_ID, STUDIO_NAME FROM MINDBODY_REPORTING_ANALYTICS.MART_CLIENTS WHERE STUDIO_NAME LIKE '%SWEAT440%' ORDER BY STUDIO_NAME")
    results["studios"] = [{"id": r["studio_id"], "name": r["studio_name"]} for r in studios]
    print(f"  OK {len(studios)} studios")

    # Leads for all studios
    print("  Fetching leads...")
    leads = query(f"""
        SELECT LEAD_CREATED_ON AS date, STUDIO_ID, LEAD_SOURCE, COUNT(DISTINCT OPPORTUNITY_ID) AS leads
        FROM MINDBODY_REPORTING_ANALYTICS.MART_LEADS_LOG
        WHERE LEAD_CREATED_ON BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY LEAD_CREATED_ON, STUDIO_ID, LEAD_SOURCE
        ORDER BY LEAD_CREATED_ON DESC
    """)
    results["leads"] = [{"date": str(r["date"])[:10], "studio_id": r["studio_id"], "lead_source": r["lead_source"], "leads": r["leads"]} for r in leads]
    print(f"  OK {len(leads)} lead rows")

    # Memberships for all studios
    print("  Fetching memberships...")
    members = query(f"""
        SELECT DATE, STUDIO_ID,
            SUM(TOTAL_NEW_MEMBERSHIPS) AS new_members,
            SUM(TOTAL_ACTIVE_MEMBERSHIPS) AS active_members
        FROM MINDBODY_REPORTING_ANALYTICS.MART_MEMBERSHIP_DAILY
        WHERE DATE BETWEEN '{start_date}' AND '{end_date}'
          AND IS_CLASSPASS_MEMBERSHIP = FALSE
        GROUP BY DATE, STUDIO_ID
        ORDER BY DATE DESC
    """)
    results["memberships"] = [{"date": str(r["date"])[:10], "studio_id": r["studio_id"], "new_members": r["new_members"], "active_members": r["active_members"]} for r in members]
    print(f"  OK {len(members)} membership rows")

    # Sales for all studios
    print("  Fetching sales...")
    sales = query(f"""
        SELECT SALE_DATE AS date, STUDIO_ID, ITEM_TYPE, REVENUE_CATEGORY,
            COUNT(*) AS transactions,
            SUM(COALESCE(GROSS_PAYMENTAMT_LOCAL, 0)) AS gross_revenue,
            SUM(COALESCE(PAYMENTAMT_LOCAL, 0)) AS net_revenue,
            SUM(CASE WHEN IS_INTRO_OFFER = TRUE THEN 1 ELSE 0 END) AS intro_offers
        FROM MINDBODY_REPORTING_ANALYTICS.MART_SALES_DETAILS
        WHERE SALE_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY SALE_DATE, STUDIO_ID, ITEM_TYPE, REVENUE_CATEGORY
        ORDER BY SALE_DATE DESC
    """)
    results["sales"] = [{"date": str(r["date"])[:10], "studio_id": r["studio_id"], "item_type": r["item_type"],
                         "revenue_category": r["revenue_category"], "transactions": r["transactions"],
                         "gross_revenue": round(float(r["gross_revenue"] or 0), 2), "net_revenue": round(float(r["net_revenue"] or 0), 2),
                         "intro_offers": r["intro_offers"]} for r in sales]
    print(f"  OK {len(sales)} sales rows")

    conn.close()
    return results


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Fetch ALL marketing data into one JSON")
    parser.add_argument("--days", type=int, default=90, help="Days to fetch (default: 90)")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (overrides --days)")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument("--source", choices=["meta", "google_ads", "ga4", "snowflake", "all"], default="all")
    parser.add_argument("--location", default=None,
                        help="Filter GA4 to pages containing this slug, e.g. 'pinecrest' or 'florida-pinecrest'")
    parser.add_argument("--output", default="marketing_data.json")
    args = parser.parse_args()

    if args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    print("=" * 60)
    print("SWEAT440 — Full Marketing Data Fetch")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Source: {args.source}")
    if args.location:
        print(f"GA4 location filter: {args.location}")
    print("=" * 60)

    output = {
        "generated_at": datetime.now().isoformat(),
        "date_range": {"start": start_date, "end": end_date},
    }

    if args.source in ("meta", "all"):
        output["meta_ads"] = fetch_meta_ads(start_date, end_date)

    if args.source in ("google_ads", "all"):
        output["google_ads"] = fetch_google_ads(start_date, end_date)

    if args.source in ("ga4", "all"):
        ga4_data = fetch_ga4(start_date, end_date, location=args.location)
        output["ga4_traffic"] = ga4_data.get("traffic", []) if isinstance(ga4_data, dict) else ga4_data
        output["ga4_page_views"] = ga4_data.get("page_views", []) if isinstance(ga4_data, dict) else []
        output["ga4_conversions"] = ga4_data.get("conversions", []) if isinstance(ga4_data, dict) else []

    if args.source in ("snowflake", "all"):
        sf_data = fetch_snowflake(start_date, end_date)
        output["snowflake"] = sf_data

    # Summary
    print("\n" + "=" * 60)
    print("Summary:")
    if "meta_ads" in output:
        print(f"  Meta Ads: {len(output['meta_ads'])} rows")
        campaigns = set(r["campaign_name"] for r in output["meta_ads"])
        print(f"    Unique campaigns: {len(campaigns)}")
        for c in sorted(campaigns)[:10]:
            print(f"      {c}")
        if len(campaigns) > 10:
            print(f"      ... and {len(campaigns) - 10} more")
    if "google_ads" in output:
        print(f"  Google Ads: {len(output['google_ads'])} rows")
        accounts = set(r["account_name"] for r in output["google_ads"])
        print(f"    Accounts: {len(accounts)}")
        for a in sorted(accounts):
            print(f"      {a}")
    if "ga4_traffic" in output:
        traffic = output["ga4_traffic"]
        loc = args.location
        filtered = [r for r in traffic if loc.lower() in r["landing_page"].lower()] if loc else traffic
        label = f" (showing '{loc}' only)" if loc else ""
        print(f"  GA4 Landing-page sessions (total {len(traffic)} rows){label}: {len(filtered)} matching")
        pages = sorted(set(r["landing_page"] for r in filtered if "/gyms/" in r["landing_page"]))
        for p in pages[:20]:
            print(f"      {p}")
        if len(pages) > 20:
            print(f"      ... and {len(pages) - 20} more")
    if "ga4_page_views" in output:
        pv = output["ga4_page_views"]
        loc = args.location
        filtered_pv = [r for r in pv if loc.lower() in r["page_path"].lower()] if loc else pv
        label = f" (showing '{loc}' only)" if loc else ""
        print(f"  GA4 All page views (total {len(pv)} rows){label}: {len(filtered_pv)} matching")
        unique_paths = sorted(set(r["page_path"] for r in filtered_pv if "/gyms/" in r["page_path"]))
        for p in unique_paths[:20]:
            total = sum(r["page_views"] for r in filtered_pv if r["page_path"] == p)
            print(f"      {p.encode('ascii','replace').decode()}  ({total} views)")
        if len(unique_paths) > 20:
            print(f"      ... and {len(unique_paths) - 20} more")
    if "ga4_conversions" in output:
        print(f"  GA4 Conversions: {len(output['ga4_conversions'])} rows")
    if "snowflake" in output:
        sf = output["snowflake"]
        print(f"  Snowflake Studios: {len(sf.get('studios', []))}")
        for s in sf.get("studios", []):
            print(f"    {s['name']} (ID: {s['id']})")
        print(f"  Snowflake Leads: {len(sf.get('leads', []))} rows")
        print(f"  Snowflake Memberships: {len(sf.get('memberships', []))} rows")
        print(f"  Snowflake Sales: {len(sf.get('sales', []))} rows")

    # Write
    out_path = Path(args.output)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    size_mb = out_path.stat().st_size / 1_000_000
    print(f"\nDone. Written to {out_path} ({size_mb:.1f} MB)")
    print("=" * 60)


if __name__ == "__main__":
    main()
