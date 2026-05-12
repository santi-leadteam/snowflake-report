"""
fetch_google_ads.py
─────────────────────────────────────────────────────────────
ETL: Google Ads MCC → google-ads-data.json

Pulls daily data for current quarter + previous quarter,
monthly for older history (3-year cap) — matching the same
granularity as Snowflake data in data.json.

Metrics per studio per day/month:
  spend, impressions, clicks,
  leads   (form submissions / lead conversions),
  calls   (call extension clicks),
  directions (location click: get directions),
  opportunities = leads + 0.3*directions + 0.4*calls  (computed here)

Asset performance (headlines + descriptions) is monthly only.

Studio attribution: campaign name regex matching using the same
logic as the Looker Studio CASE formula, campaign name only.

Writes to google-ads-data.json (additive, never breaks data.json
or paid-ads-data.json).

Requires env vars / GitHub Secrets:
  GOOGLE_ADS_DEVELOPER_TOKEN
  GOOGLE_ADS_CLIENT_ID
  GOOGLE_ADS_CLIENT_SECRET
  GOOGLE_ADS_REFRESH_TOKEN
  GOOGLE_ADS_LOGIN_CUSTOMER_ID   (MCC account ID, no dashes)
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ── Google Ads client ────────────────────────────────────────────────────────
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

REPO_ROOT = Path(__file__).resolve().parent
OUT_PATH  = REPO_ROOT / "google-ads-data.json"

# Daily window: current Q + previous Q; anything older → monthly only
DAILY_WINDOW_DAYS = 180  # ~2 quarters

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("google-ads-etl")


# ════════════════════════════════════════════════════════════════════════════
# STUDIO ATTRIBUTION
# Campaign name regex map derived from Looker Studio CASE formula.
# Returns the canonical studio name (no "SWEAT440 " prefix) used across
# all dashboard data sources as the shared merge/filter key.
# ════════════════════════════════════════════════════════════════════════════
# Each entry: (regex_pattern, canonical_name)
# Patterns are tested in order; first match wins.
CAMPAIGN_STUDIO_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(r"iramar",         re.IGNORECASE), "Miramar"),
    (re.compile(r"FIDI|FiDi|Financial District", re.IGNORECASE), "NYC - Financial District"),
    (re.compile(r"helsea",         re.IGNORECASE), "NYC - Chelsea"),
    (re.compile(r"rickell",        re.IGNORECASE), "Miami - Brickell"),
    (re.compile(r"Sobe|Miami Beach|SoBe", re.IGNORECASE), "Miami Beach"),
    (re.compile(r"Doral",          re.IGNORECASE), "Doral"),
    (re.compile(r"Gables",         re.IGNORECASE), "Coral Gables"),
    (re.compile(r"eerfield",       re.IGNORECASE), "Deerfield Beach"),
    (re.compile(r"pper|iscayne",   re.IGNORECASE), "Miami - Upper East Side"),
    (re.compile(r"Springs",        re.IGNORECASE), "Coral Springs"),
    (re.compile(r"River|Toms",     re.IGNORECASE), "Toms River"),
    (re.compile(r"usic Row|Music", re.IGNORECASE), "Music Row"),
    (re.compile(r"ighland",        re.IGNORECASE), "Austin - Highland"),
    (re.compile(r"ilker|Zilker",   re.IGNORECASE), "Austin - Zilker"),
    (re.compile(r"idtown",         re.IGNORECASE), "Miami - Midtown"),
    (re.compile(r"ulch|apitol",    re.IGNORECASE), "Capitol View"),
    (re.compile(r"cean",           re.IGNORECASE), "Ocean Township"),
    (re.compile(r"NODA",           re.IGNORECASE), "Charlotte - Noda"),
    (re.compile(r"South Miami",    re.IGNORECASE), "South Miami"),
    (re.compile(r"oconut|Coconut|Grove|FL011", re.IGNORECASE), "Miami - Coconut Grove"),
    (re.compile(r"untsville",      re.IGNORECASE), "Huntsville"),
    (re.compile(r"adison",         re.IGNORECASE), "Madison"),
    (re.compile(r"Lakes",          re.IGNORECASE), "Miami Lakes"),
    (re.compile(r"Olas",           re.IGNORECASE), "Fort Lauderdale - Las Olas"),
    (re.compile(r"embroke",        re.IGNORECASE), "Pembroke Pines"),
    (re.compile(r"Boca",           re.IGNORECASE), "Boca Raton"),
    (re.compile(r"West Palm",      re.IGNORECASE), "West Palm Beach"),
    (re.compile(r"Wall",           re.IGNORECASE), "Wall Township"),
    (re.compile(r"astchester",     re.IGNORECASE), "Eastchester"),
    (re.compile(r"Slope",          re.IGNORECASE), "NYC - Park Slope"),
    (re.compile(r"Prestonwood",    re.IGNORECASE), "Dallas - Prestonwood"),
    (re.compile(r"Reston",         re.IGNORECASE), "Reston"),
    (re.compile(r"inecrest",       re.IGNORECASE), "Pinecrest"),
    (re.compile(r"Naples",         re.IGNORECASE), "Naples Mercato"),
    (re.compile(r"Aventura",       re.IGNORECASE), "Aventura"),
    (re.compile(r"Herriman",       re.IGNORECASE), "Herriman"),
    (re.compile(r"Phillips|Dr.?\s*Phillips", re.IGNORECASE), "Dr Phillips"),
    (re.compile(r"NYC|New York",   re.IGNORECASE), "NYC - Financial District"),
]


def studio_from_campaign(campaign_name: str) -> str | None:
    """Return canonical studio name from campaign name, or None if no match."""
    for pattern, studio in CAMPAIGN_STUDIO_MAP:
        if pattern.search(campaign_name):
            return studio
    return None


# ════════════════════════════════════════════════════════════════════════════
# CREDENTIALS
# ════════════════════════════════════════════════════════════════════════════
def build_client() -> GoogleAdsClient:
    config = {
        "developer_token":    os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id":          os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret":      os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token":      os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "login_customer_id":  os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"],
        "use_proto_plus":     True,
    }
    return GoogleAdsClient.load_from_dict(config)


# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════
def _run_query(client: GoogleAdsClient, customer_id: str, query: str) -> list:
    """Run a GAQL query against a single customer account."""
    svc = client.get_service("GoogleAdsService")
    try:
        stream = svc.search_stream(customer_id=customer_id, query=query)
        rows = []
        for batch in stream:
            rows.extend(batch.results)
        return rows
    except GoogleAdsException as ex:
        for err in ex.failure.errors:
            log.warning(f"  GAQL error [{customer_id}]: {err.message}")
        return []


def _list_accessible_customers(client: GoogleAdsClient, mcc_id: str) -> list[str]:
    """Return all leaf (non-manager) account IDs under the MCC."""
    svc = client.get_service("CustomerServiceClient" if False else "GoogleAdsService")

    # Use CustomerClient resource to enumerate MCC children
    query = """
        SELECT
            customer_client.client_customer,
            customer_client.level,
            customer_client.manager,
            customer_client.status
        FROM customer_client
        WHERE
            customer_client.level <= 2
            AND customer_client.status = 'ENABLED'
    """
    rows = _run_query(client, mcc_id, query)
    leaf_ids = []
    for row in rows:
        cc = row.customer_client
        if not cc.manager:          # leaf accounts only
            cid = cc.client_customer.split("/")[-1]
            leaf_ids.append(cid)
    log.info(f"  MCC {mcc_id}: found {len(leaf_ids)} leaf accounts")
    return leaf_ids


def _date_range_params(today: date) -> dict:
    """
    Returns date windows matching the same granularity as Snowflake data:
      daily:   current Q start → today  (and previous full Q)
      monthly: anything before that, capped at 3 years back
    """
    # Current quarter start
    q_month = ((today.month - 1) // 3) * 3 + 1
    curr_q_start = date(today.year, q_month, 1)
    # Previous quarter start
    prev_q_start = (curr_q_start - timedelta(days=1)).replace(day=1)
    prev_q_start = date(prev_q_start.year, ((prev_q_start.month - 1) // 3) * 3 + 1, 1)

    daily_from  = prev_q_start
    daily_to    = today
    monthly_from = date(today.year - 3, today.month, 1)
    monthly_to   = prev_q_start - timedelta(days=1)

    return {
        "daily_from":   daily_from,
        "daily_to":     daily_to,
        "monthly_from": monthly_from,
        "monthly_to":   monthly_to,
    }


def safe_micro(val) -> float:
    """Convert micros (int) to dollars."""
    try:
        return float(val) / 1_000_000
    except (TypeError, ValueError):
        return 0.0


def safe_int(val) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def compute_opportunities(leads: int, calls: int, directions: int) -> float:
    """opportunities = leads + 0.3*directions + 0.4*calls"""
    return leads + 0.3 * directions + 0.4 * calls


# ════════════════════════════════════════════════════════════════════════════
# DAILY PERFORMANCE QUERY
# Pulls campaign-level metrics for a given date range.
# Studio attribution is done by matching the campaign name.
# ════════════════════════════════════════════════════════════════════════════
PERF_QUERY_TEMPLATE = """
    SELECT
        segments.date,
        campaign.name,
        metrics.cost_micros,
        metrics.impressions,
        metrics.clicks,
        metrics.conversions_by_conversion_date,
        metrics.phone_calls,
        metrics.all_conversions_by_conversion_date
    FROM campaign
    WHERE
        segments.date BETWEEN '{date_from}' AND '{date_to}'
        AND campaign.status != 'REMOVED'
        AND metrics.impressions > 0
"""

# Separate query for direction clicks via click type breakdown
DIRECTION_QUERY_TEMPLATE = """
    SELECT
        segments.date,
        campaign.name,
        segments.click_type,
        metrics.clicks
    FROM campaign
    WHERE
        segments.date BETWEEN '{date_from}' AND '{date_to}'
        AND segments.click_type = 'GET_DIRECTIONS'
        AND campaign.status != 'REMOVED'
"""

# Form submission conversions query
CONVERSION_QUERY_TEMPLATE = """
    SELECT
        segments.date,
        campaign.name,
        segments.conversion_action_name,
        metrics.conversions
    FROM campaign
    WHERE
        segments.date BETWEEN '{date_from}' AND '{date_to}'
        AND campaign.status != 'REMOVED'
"""


def _fetch_daily_rows(
    client: GoogleAdsClient,
    customer_id: str,
    date_from: date,
    date_to: date,
) -> tuple[dict, dict, dict]:
    """
    Returns three dicts keyed by (studio, date_str):
      perf_map:  spend, impressions, clicks, calls
      lead_map:  leads (form conversions)
      dir_map:   directions (GET_DIRECTIONS clicks)
    """
    df = date_from.isoformat()
    dt = date_to.isoformat()

    # ---- Performance (spend, impressions, clicks, calls) ----
    perf_map: dict[tuple, dict] = {}
    rows = _run_query(client, customer_id, PERF_QUERY_TEMPLATE.format(date_from=df, date_to=dt))
    for row in rows:
        studio = studio_from_campaign(row.campaign.name)
        if not studio:
            continue
        key = (studio, row.segments.date)
        if key not in perf_map:
            perf_map[key] = {"spend": 0.0, "impressions": 0, "clicks": 0, "calls": 0}
        m = perf_map[key]
        m["spend"]       += safe_micro(row.metrics.cost_micros)
        m["impressions"] += safe_int(row.metrics.impressions)
        m["clicks"]      += safe_int(row.metrics.clicks)
        m["calls"]       += safe_int(row.metrics.phone_calls)

    # ---- Direction clicks ----
    dir_map: dict[tuple, int] = {}
    rows = _run_query(client, customer_id, DIRECTION_QUERY_TEMPLATE.format(date_from=df, date_to=dt))
    for row in rows:
        studio = studio_from_campaign(row.campaign.name)
        if not studio:
            continue
        key = (studio, row.segments.date)
        dir_map[key] = dir_map.get(key, 0) + safe_int(row.metrics.clicks)

    # ---- Form submission conversions (leads) ----
    lead_map: dict[tuple, int] = {}
    rows = _run_query(client, customer_id, CONVERSION_QUERY_TEMPLATE.format(date_from=df, date_to=dt))
    for row in rows:
        studio = studio_from_campaign(row.campaign.name)
        if not studio:
            continue
        # Only count conversion actions that look like form submissions / leads.
        # Exclude calls (tracked separately), directions, purchases etc.
        action_name = (row.segments.conversion_action_name or "").lower()
        is_lead = any(kw in action_name for kw in [
            "lead", "form", "submit", "signup", "sign up", "contact",
            "prospect", "inquiry", "enquiry", "registration", "register",
        ])
        # Explicitly exclude call/direction conversion types
        is_excluded = any(kw in action_name for kw in [
            "call", "direction", "store visit", "visit", "purchase",
        ])
        if is_lead and not is_excluded:
            key = (studio, row.segments.date)
            lead_map[key] = lead_map.get(key, 0) + int(float(row.metrics.conversions))

    return perf_map, lead_map, dir_map


# ════════════════════════════════════════════════════════════════════════════
# ASSET PERFORMANCE QUERY (monthly only)
# RSA headlines + descriptions with performance labels
# ════════════════════════════════════════════════════════════════════════════
ASSET_QUERY_TEMPLATE = """
    SELECT
        ad_group_ad_asset_view.field_type,
        asset.text_asset.text,
        asset.type,
        metrics.impressions,
        metrics.clicks,
        ad_group_ad_asset_view.performance_label
    FROM ad_group_ad_asset_view
    WHERE
        segments.date BETWEEN '{date_from}' AND '{date_to}'
        AND ad_group_ad_asset_view.field_type IN ('HEADLINE', 'DESCRIPTION')
        AND asset.type = 'TEXT'
        AND metrics.impressions > 0
"""


def _fetch_assets(
    client: GoogleAdsClient,
    customer_id: str,
    date_from: date,
    date_to: date,
) -> tuple[list[dict], list[dict]]:
    """
    Returns (headlines, descriptions) — each is a list of dicts:
      {text, impressions, clicks, ctr, performance_label}
    Aggregated across all campaigns for the account.
    """
    df = date_from.isoformat()
    dt = date_to.isoformat()
    rows = _run_query(client, customer_id, ASSET_QUERY_TEMPLATE.format(date_from=df, date_to=dt))

    hl_map: dict[str, dict] = {}
    ds_map: dict[str, dict] = {}

    for row in rows:
        text   = (row.asset.text_asset.text or "").strip()
        if not text:
            continue
        ftype  = str(row.ad_group_ad_asset_view.field_type.name)
        impr   = safe_int(row.metrics.impressions)
        clicks = safe_int(row.metrics.clicks)
        label  = str(row.ad_group_ad_asset_view.performance_label.name)

        target = hl_map if ftype == "HEADLINE" else ds_map if ftype == "DESCRIPTION" else None
        if target is None:
            continue

        if text not in target:
            target[text] = {"text": text, "impressions": 0, "clicks": 0, "performance_label": label}
        target[text]["impressions"] += impr
        target[text]["clicks"]      += clicks
        # Keep highest-performing label seen
        label_rank = {"BEST": 3, "GOOD": 2, "LOW": 1, "LEARNING": 0, "UNKNOWN": -1}
        if label_rank.get(label, -1) > label_rank.get(target[text]["performance_label"], -1):
            target[text]["performance_label"] = label

    def _finalize(m: dict) -> list[dict]:
        out = []
        for v in m.values():
            ctr = round(v["clicks"] / v["impressions"] * 100, 2) if v["impressions"] else 0
            out.append({
                "text":              v["text"],
                "impressions":       v["impressions"],
                "clicks":            v["clicks"],
                "ctr":               ctr,
                "performance_label": v["performance_label"],
            })
        return sorted(out, key=lambda x: -x["impressions"])

    return _finalize(hl_map), _finalize(ds_map)


# ════════════════════════════════════════════════════════════════════════════
# MERGE HELPERS
# ════════════════════════════════════════════════════════════════════════════
def _merge_day(
    perf: dict,
    leads: dict,
    dirs: dict,
    studio: str,
    date_str: str,
) -> dict:
    key   = (studio, date_str)
    p     = perf.get(key, {})
    l_val = leads.get(key, 0)
    d_val = dirs.get(key, 0)
    c_val = p.get("calls", 0)
    opp   = compute_opportunities(l_val, c_val, d_val)
    spend = round(p.get("spend", 0.0), 2)
    return {
        "date":             date_str,
        "studio":           studio,
        "spend":            spend,
        "impressions":      p.get("impressions", 0),
        "clicks":           p.get("clicks", 0),
        "leads":            l_val,
        "calls":            c_val,
        "directions":       d_val,
        "opportunities":    round(opp, 2),
        "cost_per_opp":     round(spend / opp, 2) if opp > 0 else 0,
    }


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
def run():
    today   = date.today()
    windows = _date_range_params(today)

    log.info(f"Date windows:")
    log.info(f"  daily:   {windows['daily_from']} → {windows['daily_to']}")
    log.info(f"  monthly: {windows['monthly_from']} → {windows['monthly_to']}")

    client = build_client()
    mcc_id = os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"].replace("-", "")

    # Get all leaf customer IDs under the MCC
    customer_ids = _list_accessible_customers(client, mcc_id)
    if not customer_ids:
        log.warning("No accessible customer accounts found under MCC. Exiting.")
        sys.exit(0)

    # ---- Accumulators ----
    # Keyed by (studio, date_str) for daily, (studio, month_str) for monthly
    daily_perf:   dict = {}
    daily_leads:  dict = {}
    daily_dirs:   dict = {}
    monthly_perf: dict = {}
    monthly_leads: dict = {}
    monthly_dirs: dict = {}

    # Asset accumulators (monthly only, across all studios)
    all_headlines:    list[dict] = []
    all_descriptions: list[dict] = []

    for cid in customer_ids:
        log.info(f"Processing account {cid}...")

        # -- Daily performance --
        p, l, d = _fetch_daily_rows(
            client, cid,
            windows["daily_from"], windows["daily_to"]
        )
        for k, v in p.items():
            if k not in daily_perf:
                daily_perf[k] = {"spend": 0.0, "impressions": 0, "clicks": 0, "calls": 0}
            for fld in ["spend", "impressions", "clicks", "calls"]:
                daily_perf[k][fld] += v.get(fld, 0)
        for k, v in l.items():
            daily_leads[k] = daily_leads.get(k, 0) + v
        for k, v in d.items():
            daily_dirs[k] = daily_dirs.get(k, 0) + v

        # -- Monthly performance (older history) --
        if windows["monthly_from"] < windows["monthly_to"]:
            mp, ml, md = _fetch_daily_rows(
                client, cid,
                windows["monthly_from"], windows["monthly_to"]
            )
            # Roll up to month
            for (studio, date_str), v in mp.items():
                month_key = (studio, date_str[:7] + "-01")
                if month_key not in monthly_perf:
                    monthly_perf[month_key] = {"spend": 0.0, "impressions": 0, "clicks": 0, "calls": 0}
                for fld in ["spend", "impressions", "clicks", "calls"]:
                    monthly_perf[month_key][fld] += v.get(fld, 0)
            for (studio, date_str), v in ml.items():
                month_key = (studio, date_str[:7] + "-01")
                monthly_leads[month_key] = monthly_leads.get(month_key, 0) + v
            for (studio, date_str), v in md.items():
                month_key = (studio, date_str[:7] + "-01")
                monthly_dirs[month_key] = monthly_dirs.get(month_key, 0) + v

        # -- Asset performance (monthly, last 90 days) --
        asset_from = today - timedelta(days=90)
        hl, ds = _fetch_assets(client, cid, asset_from, today)
        all_headlines.extend(hl)
        all_descriptions.extend(ds)

    # ---- Build daily rows ----
    # Collect all unique (studio, date) keys
    all_daily_keys = set(daily_perf.keys()) | set(daily_leads.keys()) | set(daily_dirs.keys())
    daily_rows = []
    for (studio, date_str) in sorted(all_daily_keys):
        daily_rows.append(_merge_day(daily_perf, daily_leads, daily_dirs, studio, date_str))

    # ---- Build monthly rows ----
    all_monthly_keys = set(monthly_perf.keys()) | set(monthly_leads.keys()) | set(monthly_dirs.keys())
    monthly_rows = []
    for (studio, month_str) in sorted(all_monthly_keys):
        p = monthly_perf.get((studio, month_str), {})
        l_val = monthly_leads.get((studio, month_str), 0)
        d_val = monthly_dirs.get((studio, month_str), 0)
        c_val = p.get("calls", 0)
        opp   = compute_opportunities(l_val, c_val, d_val)
        spend = round(p.get("spend", 0.0), 2)
        monthly_rows.append({
            "month":         month_str,
            "studio":        studio,
            "spend":         spend,
            "impressions":   p.get("impressions", 0),
            "clicks":        p.get("clicks", 0),
            "leads":         l_val,
            "calls":         c_val,
            "directions":    d_val,
            "opportunities": round(opp, 2),
            "cost_per_opp":  round(spend / opp, 2) if opp > 0 else 0,
        })

    # ---- De-duplicate and aggregate asset lists ----
    def _dedup_assets(assets: list[dict]) -> list[dict]:
        agg: dict[str, dict] = {}
        for a in assets:
            t = a["text"]
            if t not in agg:
                agg[t] = {**a}
            else:
                agg[t]["impressions"] += a["impressions"]
                agg[t]["clicks"]      += a["clicks"]
                label_rank = {"BEST": 3, "GOOD": 2, "LOW": 1, "LEARNING": 0, "UNKNOWN": -1}
                if label_rank.get(a["performance_label"], -1) > label_rank.get(agg[t]["performance_label"], -1):
                    agg[t]["performance_label"] = a["performance_label"]
        out = []
        for v in agg.values():
            ctr = round(v["clicks"] / v["impressions"] * 100, 2) if v["impressions"] else 0
            out.append({**v, "ctr": ctr})
        return sorted(out, key=lambda x: -x["impressions"])[:50]  # top 50

    headlines    = _dedup_assets(all_headlines)
    descriptions = _dedup_assets(all_descriptions)

    # ---- Normalize studio names to Snowflake canonical (source of truth) ----
    _CANONICAL = {
        'Miami Brickell':        'Miami - Brickell',
        'Miami Upper East Side': 'Miami - Upper East Side',
        'Midtown Miami':         'Miami - Midtown',
        'Coconut Grove':         'Miami - Coconut Grove',
        'NYC Chelsea':           'NYC - Chelsea',
        'NYC Park Slope':        'NYC - Park Slope',
    }
    for r in daily_rows + monthly_rows:
        if r['studio'] in _CANONICAL:
            r['studio'] = _CANONICAL[r['studio']]

    # ---- Studios list (canonical names seen in this data) ----
    studios_seen = sorted(set(
        r["studio"] for r in daily_rows + monthly_rows
    ))

    # ---- Summary totals (daily window only) ----
    total_spend  = sum(r["spend"]        for r in daily_rows)
    total_leads  = sum(r["leads"]        for r in daily_rows)
    total_calls  = sum(r["calls"]        for r in daily_rows)
    total_dirs   = sum(r["directions"]   for r in daily_rows)
    total_opp    = sum(r["opportunities"] for r in daily_rows)
    total_impr   = sum(r["impressions"]  for r in daily_rows)
    total_clicks = sum(r["clicks"]       for r in daily_rows)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "date_windows": {
            "daily_from":   windows["daily_from"].isoformat(),
            "daily_to":     windows["daily_to"].isoformat(),
            "monthly_from": windows["monthly_from"].isoformat(),
            "monthly_to":   windows["monthly_to"].isoformat(),
        },
        "studios": studios_seen,
        "totals": {
            "spend":        round(total_spend, 2),
            "impressions":  total_impr,
            "clicks":       total_clicks,
            "leads":        total_leads,
            "calls":        total_calls,
            "directions":   total_dirs,
            "opportunities": round(total_opp, 2),
            "cost_per_opp": round(total_spend / total_opp, 2) if total_opp else 0,
        },
        # Daily rows: current Q + previous Q, one row per studio per day
        "daily":   daily_rows,
        # Monthly rows: older history (3yr cap), one row per studio per month
        "monthly": monthly_rows,
        # Asset performance tables (monthly, not date-filterable)
        "assets": {
            "period_from": (today - timedelta(days=90)).isoformat(),
            "period_to":   today.isoformat(),
            "headlines":    headlines,
            "descriptions": descriptions,
        },
    }

    OUT_PATH.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    size_kb = OUT_PATH.stat().st_size / 1024
    log.info(f"✅ Wrote {OUT_PATH}  ({size_kb:.1f} KB)")
    log.info(f"   Daily rows:   {len(daily_rows):,} ({len(studios_seen)} studios)")
    log.info(f"   Monthly rows: {len(monthly_rows):,}")
    log.info(f"   Totals: spend=${total_spend:,.2f}, leads={total_leads}, "
             f"calls={total_calls}, dirs={total_dirs}, opp={total_opp:.0f}")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        log.exception(f"❌ Google Ads ETL failed: {e}")
        sys.exit(1)