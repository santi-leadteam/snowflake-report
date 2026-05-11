"""
fetch_meta_ads.py — Pull Meta Ads (Facebook + Instagram) and Instagram organic data.

Writes two tabs:
  - meta_ads: Ad account spend, clicks, impressions, conversions, CPL, best ads
  - meta_organic: Instagram reach, likes, comments, top posts

Uses Meta Marketing API v21.0.
"""

import os
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.iguser import IGUser
from facebook_business.adobjects.igmedia import IGMedia
from sheets_writer import write_data


def init_api():
    """Initialize the Meta Marketing API."""
    access_token = os.environ["META_ACCESS_TOKEN"]
    app_id = os.environ.get("META_APP_ID", "")
    app_secret = os.environ.get("META_APP_SECRET", "")
    FacebookAdsApi.init(app_id, app_secret, access_token)


def fetch_ad_creatives(ad_account_id: str) -> dict:
    """
    Fetch creative metadata for all ads in the account.
    Returns dict keyed by ad_name: {thumbnail_url, image_url, preview_link, object_type}
    """
    account = AdAccount(ad_account_id)
    print(f"  Fetching ad creatives for {ad_account_id}...")
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
        print(f"  → {len(creatives)} ad creatives fetched.")
    except Exception as e:
        print(f"  ⚠ Could not fetch ad creatives: {e}")
    return creatives


def fetch_ad_performance(
    ad_account_id: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    Fetch daily ad performance from a Meta ad account.

    Returns rows with: date, campaign_name, adset_name, ad_name,
    spend, impressions, clicks, ctr, cpc, conversions (leads),
    cost_per_lead, thumbnail_url, image_url, preview_link, object_type.
    """
    account = AdAccount(ad_account_id)

    # Fetch creative metadata once and join by ad_name
    creatives = fetch_ad_creatives(ad_account_id)

    params = {
        "time_range": {"since": start_date, "until": end_date},
        "time_increment": 1,  # daily breakdown
        "level": "ad",
        "filtering": [
            {"field": "ad.effective_status", "operator": "IN",
             "value": ["ACTIVE", "PAUSED", "COMPLETED"]}
        ],
    }
    fields = [
        "date_start",
        "campaign_name",
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

    print(f"  Fetching Meta Ads for {ad_account_id} ({start_date} → {end_date})...")
    insights = account.get_insights(params=params, fields=fields)

    rows = []
    for row in insights:
        # Extract lead conversions from the actions array
        leads = 0
        cost_per_lead = 0.0
        for action in row.get("actions", []):
            if action["action_type"] == "lead":
                leads = int(action["value"])
        for cpa in row.get("cost_per_action_type", []):
            if cpa["action_type"] == "lead":
                cost_per_lead = float(cpa["value"])

        ad_name = row.get("ad_name", "")
        creative = creatives.get(ad_name, {})

        rows.append({
            "date": row["date_start"],
            "campaign_name": row.get("campaign_name", ""),
            "adset_name": row.get("adset_name", ""),
            "ad_name": ad_name,
            "spend": float(row.get("spend", 0)),
            "impressions": int(row.get("impressions", 0)),
            "clicks": int(row.get("clicks", 0)),
            "ctr": float(row.get("ctr", 0)),
            "cpc": float(row.get("cpc", 0)),
            "leads": leads,
            "cost_per_lead": cost_per_lead,
            "thumbnail_url": creative.get("thumbnail_url", ""),
            "image_url": creative.get("image_url", ""),
            "preview_link": creative.get("preview_link", ""),
            "object_type": creative.get("object_type", ""),
        })

    print(f"  → {len(rows)} ad-level daily rows fetched.")
    return rows


def fetch_best_performing_ads(
    ad_account_id: str,
    start_date: str,
    end_date: str,
    top_n: int = 5,
) -> list[dict]:
    """
    Fetch top N ads by spend for the period.
    Used for the 'Best Performing Ads' section.
    """
    account = AdAccount(ad_account_id)

    params = {
        "time_range": {"since": start_date, "until": end_date},
        "level": "ad",
        "sort": ["spend_descending"],
        "limit": top_n,
    }
    fields = [
        "ad_name",
        "spend",
        "impressions",
        "clicks",
        "ctr",
        "actions",
        "cost_per_action_type",
    ]

    insights = account.get_insights(params=params, fields=fields)

    rows = []
    for rank, row in enumerate(insights, 1):
        leads = 0
        cost_per_lead = 0.0
        for action in row.get("actions", []):
            if action["action_type"] == "lead":
                leads = int(action["value"])
        for cpa in row.get("cost_per_action_type", []):
            if cpa["action_type"] == "lead":
                cost_per_lead = float(cpa["value"])

        rows.append({
            "rank": rank,
            "ad_name": row.get("ad_name", ""),
            "spend": float(row.get("spend", 0)),
            "impressions": int(row.get("impressions", 0)),
            "clicks": int(row.get("clicks", 0)),
            "ctr": float(row.get("ctr", 0)),
            "leads": leads,
            "cost_per_lead": cost_per_lead,
        })

    return rows


def fetch_instagram_organic(
    ig_account_id: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    Fetch Instagram organic metrics: reach, impressions, likes, comments.
    Also returns the top posts by engagement.
    """
    ig_user = IGUser(ig_account_id)

    # Account-level insights — daily reach
    print(f"  Fetching Instagram organic for {ig_account_id}...")
    daily_data = {}

    try:
        # Reach works with period=day
        params_daily = {
            "metric": ["reach"],
            "period": "day",
            "since": start_date,
            "until": end_date,
        }
        account_insights = ig_user.get_insights(params=params_daily)
        for metric_obj in account_insights:
            metric_name = metric_obj["name"]
            for val in metric_obj.get("values", []):
                day = val["end_time"][:10]
                if day not in daily_data:
                    daily_data[day] = {"date": day}
                daily_data[day][metric_name] = val["value"]
    except Exception as e:
        print(f"  ⚠ Could not fetch daily reach: {e}")

    try:
        # These metrics require metric_type=total_value
        params_total = {
            "metric": ["accounts_engaged", "total_interactions"],
            "period": "day",
            "metric_type": "total_value",
            "since": start_date,
            "until": end_date,
        }
        total_insights = ig_user.get_insights(params=params_total)
        for metric_obj in total_insights:
            metric_name = metric_obj["name"]
            for val in metric_obj.get("values", []):
                day = val["end_time"][:10]
                if day not in daily_data:
                    daily_data[day] = {"date": day}
                daily_data[day][metric_name] = val["value"]
    except Exception as e:
        print(f"  ⚠ Could not fetch engagement metrics: {e}")

    # Recent media for engagement metrics
    media_items = ig_user.get_media(fields=[
        "id", "caption", "timestamp", "like_count",
        "comments_count", "media_type", "permalink",
        "thumbnail_url", "media_url",
    ])

    posts = []
    for media in media_items:
        ts = media.get("timestamp", "")[:10]
        if start_date <= ts <= end_date:
            # For VIDEO/REEL use thumbnail_url, for IMAGE/CAROUSEL use media_url
            image_url = media.get("thumbnail_url") or media.get("media_url") or ""
            posts.append({
                "date": ts,
                "caption": (media.get("caption", "") or "")[:100],
                "likes": media.get("like_count", 0),
                "comments": media.get("comments_count", 0),
                "media_type": media.get("media_type", ""),
                "permalink": media.get("permalink", ""),
                "image_url": image_url,
                "engagement": media.get("like_count", 0) + media.get("comments_count", 0),
            })

    # Sort by engagement for "best performing posts"
    posts.sort(key=lambda x: x["engagement"], reverse=True)

    return list(daily_data.values()), posts


def run(franchise_config: dict, start_date: str, end_date: str):
    """Main entry point — called by run_all.py."""
    init_api()

    meta_cfg = franchise_config["meta"]
    ad_account_id = meta_cfg["ad_account_id"]
    ig_account_id = meta_cfg["instagram_account_id"]

    # 1. Ad performance (daily, ad-level)
    ad_rows = fetch_ad_performance(ad_account_id, start_date, end_date)
    write_data(ad_rows, tab_name="meta_ads")

    # 2. Best performing ads (period summary)
    best_ads = fetch_best_performing_ads(ad_account_id, start_date, end_date)
    write_data(best_ads, tab_name="meta_best_ads")

    # 3. Instagram organic
    daily_organic, top_posts = fetch_instagram_organic(ig_account_id, start_date, end_date)
    write_data(daily_organic, tab_name="meta_organic")
    write_data(top_posts[:10], tab_name="instagram_top_posts")

    print("  ✅ Meta Ads + Instagram done.")
