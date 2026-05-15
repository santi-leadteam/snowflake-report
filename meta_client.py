"""
meta_client.py
──────────────────────────────────────────────────────────────────────
Thin wrapper around the Meta Marketing API v20.0.
Used by both fetch_paid_ads.py (Daniel) and fetch_meta_ads.py (Santi).

Public surface
──────────────
MetaClient
  .get_insights(object_id, *, level, date_start, date_end,
                time_increment=None, extra_fields=None) -> list[dict]
  .get_daily_insights(campaign_id, *, date_start, date_end) -> list[dict]
  .list_ad_sets(campaign_id) -> list[dict]
  .list_ads(campaign_id) -> list[dict]
  .list_ads_for_account(ad_account_id) -> list[dict]   ← used by fetch_meta_ads.py
  .get_creatives_by_ids(creative_ids) -> dict[id, dict]

Helper functions (imported directly by pipeline scripts)
  leads_of(row)     -> int
  trials_of(row)    -> int
  purchases_of(row) -> int
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

log = logging.getLogger(__name__)

# ── API constants ─────────────────────────────────────────────────────
API_VERSION = "v20.0"
BASE_URL    = f"https://graph.facebook.com/{API_VERSION}"

# Default insight fields requested for all get_insights calls
DEFAULT_INSIGHT_FIELDS = [
    "ad_id", "ad_name", "adset_id", "adset_name",
    "campaign_id", "campaign_name",
    "spend", "impressions", "clicks", "reach",
    "actions", "action_values",
    "date_start", "date_stop",
]

# Creative fields for get_creatives_by_ids
CREATIVE_FIELDS = [
    "id", "name", "object_type", "thumbnail_url", "image_url",
    "image_hash", "video_id", "object_story_spec", "asset_feed_spec",
]

# Ad fields for list_ads / list_ads_for_account
AD_FIELDS = "id,name,status,creative{id}"

# Adset fields for list_ad_sets
ADSET_FIELDS = "id,name,status,campaign_id"

# Retry settings
MAX_RETRIES    = 5
RETRY_WAIT_S   = 60   # seconds to wait on rate-limit (error code 17 / 32)
BACKOFF_BASE_S = 2    # exponential backoff base


# ── helper functions ──────────────────────────────────────────────────

def _action_value(row: dict, action_types: list[str]) -> int:
    """Sum 'actions' entries whose action_type matches any of action_types."""
    total = 0
    for entry in (row.get("actions") or []):
        if entry.get("action_type") in action_types:
            try:
                total += int(float(entry.get("value", 0)))
            except (TypeError, ValueError):
                pass
    return total


def leads_of(row: dict) -> int:
    return _action_value(row, [
        "lead", "onsite_conversion.lead_grouped",
        "offsite_conversion.fb_pixel_lead",
    ])


def trials_of(row: dict) -> int:
    return _action_value(row, [
        "offsite_conversion.fb_pixel_custom",
        "onsite_conversion.custom",
        "trial",
    ])


def purchases_of(row: dict) -> int:
    return _action_value(row, [
        "offsite_conversion.fb_pixel_purchase",
        "purchase",
        "onsite_conversion.purchase",
    ])


# ── MetaClient ────────────────────────────────────────────────────────

class MetaClient:
    """
    Wraps Meta Marketing API with automatic pagination and retry logic.
    Reads access token from META_TOKEN environment variable.
    """

    def __init__(self, token: str | None = None) -> None:
        self.token = token or os.environ["META_TOKEN"]
        self.session = requests.Session()
        self.session.params = {"access_token": self.token}  # type: ignore[assignment]

    # ── low-level ────────────────────────────────────────────────────

    def _get(self, path: str, params: dict | None = None) -> dict:
        """Single GET request with retry on rate-limit errors."""
        url = f"{BASE_URL}/{path.lstrip('/')}"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, params=params or {}, timeout=120)
                data = resp.json()
                if "error" in data:
                    err  = data["error"]
                    code = err.get("code")
                    # Rate limit or temporary server error — wait and retry
                    if code in (17, 32, 4, 613) or err.get("is_transient"):
                        wait = RETRY_WAIT_S * attempt
                        log.warning(f"Rate limit (code {code}), waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                        time.sleep(wait)
                        continue
                    raise RuntimeError(f"Meta API error: {err.get('message')} (code {code})")
                resp.raise_for_status()
                return data
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    raise
                wait = BACKOFF_BASE_S ** attempt
                log.warning(f"Request error: {e}, retrying in {wait}s ...")
                time.sleep(wait)
        raise RuntimeError(f"Exhausted {MAX_RETRIES} retries for {url}")

    def _paginate(self, path: str, params: dict | None = None) -> list[dict]:
        """Follow cursor-based pagination, return all rows."""
        results: list[dict] = []
        data = self._get(path, params)
        while True:
            results.extend(data.get("data") or [])
            paging = data.get("paging") or {}
            next_url = paging.get("next")
            if not next_url:
                break
            # next_url is a full URL; strip base and re-request
            next_path = next_url.replace(BASE_URL, "").lstrip("/")
            # next_url already contains access_token — pass no extra params
            resp = self.session.get(next_url, timeout=120)
            data = resp.json()
            if "error" in data:
                raise RuntimeError(f"Meta pagination error: {data['error'].get('message')}")
        return results

    def _paginate_insights(self, path: str, params: dict) -> list[dict]:
        """
        Insights endpoints use async jobs for large date ranges.
        Uses synchronous mode (default) with cursor pagination.
        """
        return self._paginate(path, params)

    # ── public methods ───────────────────────────────────────────────

    def get_insights(
        self,
        object_id: str,
        *,
        level: str,
        date_start: str,
        date_end: str,
        time_increment: int | None = None,
        extra_fields: list[str] | None = None,
    ) -> list[dict]:
        """
        Fetch insights for any object (account, campaign, adset, ad).
        object_id: e.g. "act_1553887681409034", "120243785513570249"
        level: "account" | "campaign" | "adset" | "ad"
        """
        fields = DEFAULT_INSIGHT_FIELDS + (extra_fields or [])
        params: dict[str, Any] = {
            "level":      level,
            "fields":     ",".join(fields),
            "time_range": f'{{"since":"{date_start}","until":"{date_end}"}}',
            "limit":      500,
        }
        if time_increment is not None:
            params["time_increment"] = time_increment

        return self._paginate_insights(f"{object_id}/insights", params)

    def get_daily_insights(
        self,
        campaign_id: str,
        *,
        date_start: str,
        date_end: str,
    ) -> list[dict]:
        """Daily campaign-level insights. Used by Daniel's pipeline."""
        return self.get_insights(
            campaign_id,
            level="campaign",
            date_start=date_start,
            date_end=date_end,
            time_increment=1,
        )

    def list_ad_sets(self, campaign_id: str) -> list[dict]:
        """List adsets under a campaign. Used by Daniel's pipeline."""
        return self._paginate(
            f"{campaign_id}/adsets",
            {"fields": ADSET_FIELDS, "limit": 500},
        )

    def list_ads(self, campaign_id: str) -> list[dict]:
        """List ads under a campaign. Used by Daniel's pipeline."""
        return self._paginate(
            f"{campaign_id}/ads",
            {"fields": AD_FIELDS, "limit": 500},
        )

    def list_ads_for_account(self, ad_account_id: str) -> list[dict]:
        """
        List all ads under an ad account (all campaigns).
        Used by fetch_meta_ads.py (Santi's pipeline).
        ad_account_id: e.g. "act_1553887681409034"
        """
        return self._paginate(
            f"{ad_account_id}/ads",
            {"fields": AD_FIELDS, "limit": 500},
        )

    def get_creatives_by_ids(self, creative_ids: list[str]) -> dict[str, dict]:
        """
        Batch-fetch creative details by ID.
        Returns dict keyed by creative ID.
        Meta batch API allows up to 50 per request.
        """
        if not creative_ids:
            return {}

        results: dict[str, dict] = {}
        chunk_size = 50
        fields = ",".join(CREATIVE_FIELDS)

        for i in range(0, len(creative_ids), chunk_size):
            chunk = creative_ids[i : i + chunk_size]
            params = {"ids": ",".join(chunk), "fields": fields}
            data = self._get("/", params)
            for cid, cdata in data.items():
                if isinstance(cdata, dict):
                    results[cid] = cdata

        return results
