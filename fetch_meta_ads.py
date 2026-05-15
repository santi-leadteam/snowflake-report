"""
fetch_meta_ads.py
─────────────────────────────────────────────────────────────
ETL Meta Ads (account-level) → meta-ads-data.json

Output shape
────────────
{
  "generated_at":   "2026-05-14T...",

  "ad_daily":       [{date, studio_code, ad_id, ad_name,
                       spend, impressions, clicks, leads, trials}, ...]
                    // Apr 1 2026 → today; grows indefinitely; upsert on (date, studio_code, ad_id)

  "studio_daily":   [{date, studio_code,
                       spend, impressions, clicks, leads, trials}, ...]
                    // Apr 1 2026 → today; grows indefinitely; upsert on (date, studio_code)

  "studio_monthly": [{month, studio_code, spend}, ...]
                    // Jan 2025–Mar 2026: baked (preserved from file)
                    // Apr 2026+: computed from studio_daily each run

  "ad_meta":        {ad_id: {name, status, media_type, studio_code,
                              thumbnail_url, library_url}, ...}
                    // current snapshot of all known ads; merged each run
}

No campaign IDs in config. Queries the ad account directly.
"""
from __future__ import annotations

import json
import logging
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import yaml

from meta_client import MetaClient, leads_of, purchases_of, trials_of

# ── paths ────────────────────────────────────────────────────────────
REPO_ROOT   = Path(__file__).resolve().parent
CONFIG_PATH = REPO_ROOT / "config-meta.yaml"
OUT_PATH    = REPO_ROOT / "meta-ads-data.json"

# Earliest date we want daily data for (grows forward from here indefinitely)
DAILY_START = "2026-04-01"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("meta-ads-etl")


# ── classification helpers (unchanged) ──────────────────────────────

def match_studio(name: str, studios: list[dict]) -> dict | None:
    n = (name or "").lower()
    for s in studios:
        if s.get("match") and s["match"].lower() in n:
            return s
    return None



_VIDEO_KEYWORDS  = {"VIDEO", "REEL", "REELS", "GIF", "STORY", "STORIES"}
_STATIC_KEYWORDS = {"STATIC", "IMAGE", "PHOTO", "CAROUSEL"}


def _media_type_from_creative(creative: dict) -> str | None:
    if not creative:
        return None
    ot = (creative.get("object_type") or "").upper()
    if ot == "VIDEO": return "Video"
    if ot == "PHOTO": return "Static"
    if creative.get("video_id"):  return "Video"
    if creative.get("image_hash"): return "Static"
    oss = creative.get("object_story_spec") or {}
    if isinstance(oss, dict):
        vd = oss.get("video_data") or {}
        if isinstance(vd, dict) and (vd.get("video_id") or vd.get("image_url")):
            return "Video"
        ld = oss.get("link_data") or {}
        if isinstance(ld, dict):
            if ld.get("video_id"):                          return "Video"
            if ld.get("image_hash") or ld.get("picture"):  return "Static"
        pd = oss.get("photo_data") or {}
        if isinstance(pd, dict) and pd.get("image_hash"):  return "Static"
    afs = creative.get("asset_feed_spec") or {}
    if isinstance(afs, dict):
        if afs.get("videos"): return "Video"
        if afs.get("images"): return "Static"
    return None


def _media_type_from_name(ad_name: str) -> str:
    if not ad_name: return "Other"
    words = {w.upper() for w in re.findall(r"\w+", ad_name)}
    if words & _VIDEO_KEYWORDS:  return "Video"
    if words & _STATIC_KEYWORDS: return "Static"
    return "Other"


def safe_float(x, default=0.0):
    try:    return float(x)
    except: return default


# ── quarter helpers ───────────────────────────────────────────────────

def current_quarter_bounds(today: date) -> tuple[str, str]:
    """Return (start, end) ISO strings for the current quarter."""
    q = (today.month - 1) // 3
    q_start = date(today.year, q * 3 + 1, 1)
    q_end_month = q * 3 + 3
    q_end = date(today.year, q_end_month,
                 [31,28,31,30,31,30,31,31,30,31,30,31][q_end_month - 1])
    # leap year adjustment
    if q_end_month == 2 and (today.year % 4 == 0 and (today.year % 100 != 0 or today.year % 400 == 0)):
        q_end = date(today.year, 2, 29)
    return q_start.isoformat(), q_end.isoformat()


def previous_quarter_bounds(today: date) -> tuple[str, str]:
    q = (today.month - 1) // 3
    if q == 0:
        pq_year = today.year - 1; pq = 3
    else:
        pq_year = today.year;     pq = q - 1
    pq_start = date(pq_year, pq * 3 + 1, 1)
    pq_end_month = pq * 3 + 3
    pq_end = date(pq_year, pq_end_month,
                  [31,28,31,30,31,30,31,31,30,31,30,31][pq_end_month - 1])
    if pq_end_month == 2 and (pq_year % 4 == 0 and (pq_year % 100 != 0 or pq_year % 400 == 0)):
        pq_end = date(pq_year, 2, 29)
    return pq_start.isoformat(), min(pq_end.isoformat(), date.today().isoformat())


# ── main ETL ─────────────────────────────────────────────────────────

def run():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    account_cfg  = cfg["meta_account"]
    ad_account   = account_cfg["ad_account_id"]   # e.g. "act_1553887681409034"
    studios_cfg  = account_cfg["studios"]

    today     = date.today()
    today_iso = today.isoformat()

    # ── date windows ─────────────────────────────────────────────────
    # ad_daily + studio_daily: DAILY_START → today
    daily_start = DAILY_START
    daily_end   = today_iso

    # ── load existing output (for upsert + baked monthly) ────────────
    existing: dict = {}
    if OUT_PATH.exists():
        try:
            existing = json.loads(OUT_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass

    existing_ad_daily     = existing.get("ad_daily",      [])
    existing_studio_daily = existing.get("studio_daily",  [])
    existing_ad_meta      = existing.get("ad_meta",        {})
    # studio_monthly: baked Jan2025–Mar2026 rows are preserved; Apr2026+ recomputed
    baked_monthly = [
        r for r in existing.get("studio_monthly", [])
        if r.get("month", "") < "2026-04"
    ]

    meta = MetaClient()

    # ── 1. Fetch daily ad-level insights (account level) ─────────────
    # Returns one row per (ad_id, date) with adset_name for studio matching.
    log.info(f"Fetching daily ad insights [{daily_start} → {daily_end}] from {ad_account} ...")
    try:
        raw_daily_ad = meta.get_insights(
            ad_account,
            level="ad",
            date_start=daily_start,
            date_end=daily_end,
            time_increment=1,
        )
        log.info(f"  {len(raw_daily_ad)} ad×day rows")
    except Exception as e:
        log.exception(f"❌ Daily ad insights failed: {e}")
        sys.exit(1)

    # ── 2. Fetch ad metadata (status, creative) ───────────────────────
    # Get all active/paused ads on the account for thumbnails + status.
    log.info("Fetching ad list from account ...")
    try:
        all_ads = meta.list_ads_for_account(ad_account)
        log.info(f"  {len(all_ads)} ads")
    except Exception as e:
        log.warning(f"  list_ads_for_account failed: {e} — using existing ad_meta")
        all_ads = []

    # Batch-fetch creatives
    cids_per_ad: dict[str, str] = {}
    for ad in all_ads:
        ad_id = ad.get("id")
        if not ad_id: continue
        cid = (ad.get("creative") or {}).get("id")
        if cid: cids_per_ad[ad_id] = cid

    unique_cids = list({c for c in cids_per_ad.values()})
    if unique_cids:
        log.info(f"Fetching {len(unique_cids)} creatives in batch ...")
        try:
            cdetails = meta.get_creatives_by_ids(unique_cids)
        except Exception as e:
            log.warning(f"  creatives batch failed: {e}")
            cdetails = {}
    else:
        cdetails = {}

    status_by_ad = {ad["id"]: ad.get("status", "UNKNOWN") for ad in all_ads if ad.get("id")}

    # ── 3. Process daily ad rows → ad_daily + studio_daily ───────────
    # Index buckets keyed by (date, studio_code, ad_id) and (date, studio_code)
    ad_daily_idx:     dict[tuple, dict] = {}
    studio_daily_idx: dict[tuple, dict] = {}

    # Seed from existing data (will be overwritten for dates we're refetching)
    for r in existing_ad_daily:
        k = (r["date"], r["studio_code"], r["ad_id"])
        ad_daily_idx[k] = r

    for r in existing_studio_daily:
        k = (r["date"], r["studio_code"])
        studio_daily_idx[k] = r

    # Track ad name per ad_id (for ad_meta, in case list_ads_for_account failed)
    ad_name_seen: dict[str, str] = {}
    ad_studio_seen: dict[str, str] = {}

    rows_written = 0
    rows_skipped = 0

    for row in raw_daily_ad:
        ad_id     = row.get("ad_id")
        adset_name = row.get("adset_name", "")
        d          = row.get("date_start")
        if not ad_id or not d:
            rows_skipped += 1
            continue

        studio = match_studio(adset_name, studios_cfg)
        if not studio:
            rows_skipped += 1
            continue

        sc       = studio["code"]
        ad_name  = row.get("ad_name", "")
        spend    = round(safe_float(row.get("spend")), 2)
        impr     = int(safe_float(row.get("impressions")))
        clicks   = int(safe_float(row.get("clicks")))
        leads    = leads_of(row)
        trials   = trials_of(row)

        ad_name_seen[ad_id]  = ad_name
        ad_studio_seen[ad_id] = sc

        # ad_daily upsert
        ak = (d, sc, ad_id)
        ad_daily_idx[ak] = {
            "date":        d,
            "studio_code": sc,
            "ad_id":       ad_id,
            "ad_name":     ad_name,
            "spend":       spend,
            "impressions": impr,
            "clicks":      clicks,
            "leads":       leads,
            "trials":      trials,
        }

        # studio_daily upsert — accumulate within this run then write
        sk = (d, sc)
        if sk not in studio_daily_idx:
            studio_daily_idx[sk] = {
                "date": d, "studio_code": sc,
                "spend": 0.0, "impressions": 0, "clicks": 0, "leads": 0, "trials": 0,
            }
        # Re-aggregate from scratch for dates in this fetch window
        # (handled below after full pass)

        rows_written += 1

    log.info(f"  processed: {rows_written} matched, {rows_skipped} skipped (no studio match)")

    # Re-aggregate studio_daily for the fetch window from scratch
    # (avoids double-counting if we re-process same dates)
    studio_daily_fresh: dict[tuple, dict] = {}
    for row in raw_daily_ad:
        ad_id     = row.get("ad_id")
        adset_name = row.get("adset_name", "")
        d          = row.get("date_start")
        if not ad_id or not d: continue
        studio = match_studio(adset_name, studios_cfg)
        if not studio: continue
        sc = studio["code"]
        sk = (d, sc)
        if sk not in studio_daily_fresh:
            studio_daily_fresh[sk] = {
                "date": d, "studio_code": sc,
                "spend": 0.0, "impressions": 0, "clicks": 0, "leads": 0, "trials": 0,
            }
        b = studio_daily_fresh[sk]
        b["spend"]       = round(b["spend"] + safe_float(row.get("spend")), 2)
        b["impressions"] += int(safe_float(row.get("impressions")))
        b["clicks"]      += int(safe_float(row.get("clicks")))
        b["leads"]       += leads_of(row)
        b["trials"]      += trials_of(row)

    # Merge fresh aggregations into the full index
    # Preserve existing rows outside the fetch window, overwrite within it
    for k, v in studio_daily_fresh.items():
        studio_daily_idx[k] = v

    log.info(f"  studio_daily: {len(studio_daily_idx)} total (date×studio) rows")
    log.info(f"  ad_daily:     {len(ad_daily_idx)} total (date×studio×ad) rows")

    # ── 4. Build ad_meta snapshot ─────────────────────────────────────
    # Start from existing, then update with fresh data
    ad_meta: dict[str, dict] = dict(existing_ad_meta)

    for ad_id in set(list(ad_name_seen.keys()) + list(status_by_ad.keys())):
        creative   = cdetails.get(cids_per_ad.get(ad_id, ""), {})
        media_type = _media_type_from_creative(creative) or _media_type_from_name(ad_name_seen.get(ad_id, ""))
        thumb = creative.get("thumbnail_url") or creative.get("image_url") or ""

        ad_meta[ad_id] = {
            "name":          ad_name_seen.get(ad_id) or (ad_meta.get(ad_id) or {}).get("name", ""),
            "status":        status_by_ad.get(ad_id, (ad_meta.get(ad_id) or {}).get("status", "UNKNOWN")),
            "media_type":    media_type,
            "studio_code":   ad_studio_seen.get(ad_id) or (ad_meta.get(ad_id) or {}).get("studio_code"),
            "thumbnail_url": thumb or (ad_meta.get(ad_id) or {}).get("thumbnail_url", ""),
            "library_url":   f"https://www.facebook.com/ads/library/?id={ad_id}&country=US",
        }

    log.info(f"  ad_meta: {len(ad_meta)} ads")

    # ── 5. Compute studio_monthly for Apr 2026+ from studio_daily ────
    monthly_fresh: dict[tuple, float] = defaultdict(float)
    for r in studio_daily_idx.values():
        if r["date"] >= "2026-04-01":
            month = r["date"][:7]  # "YYYY-MM"
            monthly_fresh[(month, r["studio_code"])] += r["spend"]

    computed_monthly = [
        {"month": m, "studio_code": sc, "spend": round(spend, 2)}
        for (m, sc), spend in sorted(monthly_fresh.items())
    ]

    studio_monthly = baked_monthly + computed_monthly
    log.info(f"  studio_monthly: {len(baked_monthly)} baked + {len(computed_monthly)} computed = {len(studio_monthly)} rows")

    # ── 6. Sort and write ─────────────────────────────────────────────
    ad_daily_out = sorted(
        ad_daily_idx.values(),
        key=lambda r: (r["date"], r["studio_code"], r["ad_id"])
    )
    studio_daily_out = sorted(
        studio_daily_idx.values(),
        key=lambda r: (r["date"], r["studio_code"])
    )

    output = {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "ad_daily":       ad_daily_out,
        "studio_daily":   studio_daily_out,
        "studio_monthly": studio_monthly,
        "ad_meta":        ad_meta,
    }

    OUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(
        f"✅ Wrote {OUT_PATH} ({OUT_PATH.stat().st_size:,} bytes) — "
        f"{len(ad_daily_out)} ad_daily, {len(studio_daily_out)} studio_daily, "
        f"{len(studio_monthly)} studio_monthly, {len(ad_meta)} ad_meta"
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        log.exception(f"❌ ETL failed: {e}")
        sys.exit(1)
