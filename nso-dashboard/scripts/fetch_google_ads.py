"""
fetch_google_ads.py — Pull Google Ads campaign performance data.

Writes one tab:
  - google_ads: Daily campaign-level spend, clicks, impressions, conversions, cost/conv

Uses the Google Ads API v17 via the google-ads Python client.
"""

import os
from google.ads.googleads.client import GoogleAdsClient
from sheets_writer import write_data


def _get_client(mcc_id: str) -> GoogleAdsClient:
    """Build a GoogleAdsClient from environment variables."""
    config = {
        "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "login_customer_id": mcc_id.replace("-", ""),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


def fetch_campaign_performance(
    customer_id: str,
    start_date: str,
    end_date: str,
    mcc_id: str = "",
) -> list[dict]:
    """
    Fetch daily campaign-level metrics from Google Ads.

    Args:
        customer_id: Google Ads customer ID (no dashes, e.g. '1234567890')
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        mcc_id: MCC manager account ID

    Returns:
        List of dicts with: date, campaign_name, campaign_id, status,
        spend, impressions, clicks, ctr, avg_cpc, conversions, cost_per_conversion
    """
    client = _get_client(mcc_id)
    ga_service = client.get_service("GoogleAdsService")

    # Remove dashes from customer ID if present
    cid = customer_id.replace("-", "")

    query = f"""
        SELECT
            segments.date,
            campaign.name,
            campaign.id,
            campaign.status,
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
        ORDER BY segments.date DESC, metrics.cost_micros DESC
    """

    print(f"  Fetching Google Ads for customer {cid} ({start_date} → {end_date})...")

    rows = []
    try:
        response = ga_service.search_stream(customer_id=cid, query=query)

        for batch in response:
            for row in batch.results:
                spend = row.metrics.cost_micros / 1_000_000  # micros → dollars
                avg_cpc = row.metrics.average_cpc / 1_000_000 if row.metrics.average_cpc else 0
                cost_per_conv = row.metrics.cost_per_conversion / 1_000_000 if row.metrics.cost_per_conversion else 0

                rows.append({
                    "date": row.segments.date,
                    "campaign_name": row.campaign.name,
                    "campaign_id": str(row.campaign.id),
                    "status": row.campaign.status.name,
                    "spend": round(spend, 2),
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "ctr": round(row.metrics.ctr * 100, 2),  # as percentage
                    "avg_cpc": round(avg_cpc, 2),
                    "conversions": round(row.metrics.conversions, 1),
                    "cost_per_conversion": round(cost_per_conv, 2),
                })

    except Exception as e:
        print(f"  ⚠ Google Ads API error: {e}")
        return []

    print(f"  → {len(rows)} campaign-day rows fetched.")
    return rows


def fetch_top_campaigns(
    customer_id: str,
    start_date: str,
    end_date: str,
    mcc_id: str = "",
    top_n: int = 10,
) -> list[dict]:
    """Fetch top N campaigns by spend for the period (for summary section)."""
    client = _get_client(mcc_id)
    ga_service = client.get_service("GoogleAdsService")
    cid = customer_id.replace("-", "")

    query = f"""
        SELECT
            campaign.name,
            campaign.id,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.cost_per_conversion
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND campaign.status != 'REMOVED'
        ORDER BY metrics.cost_micros DESC
        LIMIT {top_n}
    """

    rows = []
    try:
        response = ga_service.search_stream(customer_id=cid, query=query)
        for batch in response:
            for rank, row in enumerate(batch.results, 1):
                spend = row.metrics.cost_micros / 1_000_000
                cost_per_conv = row.metrics.cost_per_conversion / 1_000_000 if row.metrics.cost_per_conversion else 0
                rows.append({
                    "rank": rank,
                    "campaign_name": row.campaign.name,
                    "spend": round(spend, 2),
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "conversions": round(row.metrics.conversions, 1),
                    "cost_per_conversion": round(cost_per_conv, 2),
                })
    except Exception as e:
        print(f"  ⚠ Google Ads API error (top campaigns): {e}")

    return rows


def run(franchise_config: dict, start_date: str, end_date: str):
    """Main entry point — called by run_all.py."""
    gads_cfg = franchise_config["google_ads"]
    customer_id = gads_cfg["customer_id"]
    mcc_id = gads_cfg["mcc_id"]

    # 1. Daily campaign performance
    daily_rows = fetch_campaign_performance(customer_id, start_date, end_date, mcc_id)
    write_data(daily_rows, tab_name="google_ads")

    # 2. Top campaigns for the period
    top = fetch_top_campaigns(customer_id, start_date, end_date, mcc_id)
    write_data(top, tab_name="google_ads_top_campaigns")

    print("  ✅ Google Ads done.")
