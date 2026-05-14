"""
fetch_paid_ads.py
─────────────────────────────────────────────────────────────
ETL Meta Ads → paid-ads-data.json
"""
from __future__ import annotations
import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from datetime import datetime, timezone
from pathlib import Path

import yaml

from meta_client import MetaClient, leads_of, purchases_of, trials_of


REPO_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = REPO_ROOT / "config.yaml"
OUT_PATH = REPO_ROOT / "paid-ads-data.json"

DAILY_AD_STUDIO_START = "2026-04-01"  # earliest date for daily_ad_studio; grows forward indefinitely


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("paid-ads-etl")


# ── helpers de clasificación ─────────────────────────────────────────
def match_studio(name: str, studios: list[dict]) -> dict | None:
    n = (name or "").lower()
    for s in studios:
        if s.get("match") and s["match"].lower() in n:
            return s
    return None


def _has_token(name: str, token: str) -> bool:
    if not name or not token:
        return False
    norm = re.sub(r"[_\-/|]+", " ", name.upper())
    tok = token.upper()
    pattern = r"(?:(?<=^)|(?<=\s))" + re.escape(tok) + r"(?=$|\s)"
    return re.search(pattern, norm) is not None


def match_audience(name: str, tokens_by_aud: dict[str, list[str]]) -> str | None:
    for aud, tokens in tokens_by_aud.items():
        for tok in tokens:
            if _has_token(name, tok):
                return aud
    return None


def match_pillar(name: str, tokens_by_pillar: dict[str, list[str]]) -> str | None:
    for pillar, tokens in tokens_by_pillar.items():
        for tok in tokens:
            if _has_token(name, tok):
                return pillar
    return None


_STOPWORDS = {
    "V1", "V2", "V3", "V4", "V5", "A", "B", "C", "TEST", "VER", "VERSION",
    "WAFM", "WIN", "FREE", "MONTH", "CLASS", "OPEN", "STUDIOS", "STUDIO",
    "PROMO", "AD", "ADS", "COPY", "CREATIVE",
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
    "ENE", "ABR", "AGO", "DIC",
}

_GENERIC_PREFIXES = {
    "VIDEO", "REEL", "REELS", "IMAGE", "PHOTO", "STATIC",
    "CAROUSEL", "GIF", "STORY", "STORIES",
}

# Fallback por nombre cuando Meta no devuelve creative info.
_VIDEO_KEYWORDS = {"VIDEO", "REEL", "REELS", "GIF", "STORY", "STORIES"}
_STATIC_KEYWORDS = {"STATIC", "IMAGE", "PHOTO", "CAROUSEL"}


def _media_type_from_creative(creative: dict) -> str | None:
    """
    Determina Static vs Video usando el objeto `creative` de Meta.
    Devuelve "Video", "Static" o None si no se puede decidir.

    Para SWEAT440 la mayoría de ads son object_type=LINK con la info de
    imagen/video DENTRO de object_story_spec, así que también miramos ahí.

    Orden de prioridad:
      1. object_type explícito ("VIDEO" / "PHOTO") → decide directo.
      2. video_id / image_hash a nivel top.
      3. object_story_spec.video_data → Video.
      4. object_story_spec.link_data: video_id → Video, image_hash/picture → Static.
      5. object_story_spec.photo_data → Static.
      6. asset_feed_spec (si por alguna razón viene): videos → Video, images → Static.
    """
    if not creative:
        return None

    ot = (creative.get("object_type") or "").upper()
    if ot == "VIDEO":
        return "Video"
    if ot == "PHOTO":
        return "Static"

    # Top-level
    if creative.get("video_id"):
        return "Video"
    if creative.get("image_hash"):
        return "Static"

    # Anidado en object_story_spec — donde Meta esconde los datos cuando
    # object_type es LINK / SHARE.
    oss = creative.get("object_story_spec") or {}
    if isinstance(oss, dict):
        vd = oss.get("video_data") or {}
        if isinstance(vd, dict) and (vd.get("video_id") or vd.get("image_url")):
            return "Video"
        ld = oss.get("link_data") or {}
        if isinstance(ld, dict):
            if ld.get("video_id"):
                return "Video"
            if ld.get("image_hash") or ld.get("picture"):
                return "Static"
        pd = oss.get("photo_data") or {}
        if isinstance(pd, dict) and pd.get("image_hash"):
            return "Static"

    # Asset feed spec (dynamic creatives) — fallback histórico
    afs = creative.get("asset_feed_spec") or {}
    if isinstance(afs, dict):
        videos = afs.get("videos") or []
        images = afs.get("images") or []
        if videos:
            return "Video"
        if images:
            return "Static"

    return None


def _media_type_from_name(ad_name: str) -> str:
    """Fallback por nombre — usado solo si no hay creative info."""
    if not ad_name:
        return "Other"
    words = {w.upper() for w in re.findall(r"\w+", ad_name)}
    if words & _VIDEO_KEYWORDS:
        return "Video"
    if words & _STATIC_KEYWORDS:
        return "Static"
    return "Other"


def detect_media_type(ad_name: str, creative: dict | None = None) -> str:
    """
    Determina "Video" / "Static" / "Other".
    Prioriza la info del creative de Meta (object_type / video_id / image_hash).
    Cae al heurístico de nombre si Meta no devolvió esos campos.
    """
    via_creative = _media_type_from_creative(creative or {})
    if via_creative:
        return via_creative
    return _media_type_from_name(ad_name)


def detect_concept(
    ad_name: str,
    *,
    studio_match: str | None,
    audience_tokens_flat: set[str],
    pillar_tokens_flat: set[str],
    state_code: str | None = None,
) -> str:
    if not ad_name:
        return "(other)"
    text = ad_name

    if studio_match:
        text = re.sub(re.escape(studio_match), " ", text, flags=re.IGNORECASE)

    text = re.sub(r"\b[A-Z]{2}[\-\s]?\d{2,3}\b", " ", text)
    text = re.sub(r"[_\-/|]+", " ", text)

    all_class_tokens = {t.upper() for t in audience_tokens_flat} | {t.upper() for t in pillar_tokens_flat}
    words_out = []
    for raw in re.split(r"\s+", text):
        w = raw.strip()
        if not w:
            continue
        upper = w.upper()
        if upper in all_class_tokens:
            continue
        if upper in _STOPWORDS:
            continue
        if len(w) == 2 and w.isalpha() and w.isupper():
            continue
        if re.fullmatch(r"\d+", w):
            continue
        if re.fullmatch(r"[Vv]\d+", w):
            continue
        if len(w) < 3:
            continue
        words_out.append(w)

    if not words_out:
        return "(other)"

    primary = None
    primary_idx = 0
    for i, w in enumerate(words_out):
        if w[0].isupper():
            primary = w
            primary_idx = i
            break
    if primary is None:
        primary = words_out[0]
        primary_idx = 0

    if primary.upper() in _GENERIC_PREFIXES:
        for w in words_out[primary_idx + 1:]:
            if len(w) >= 3 and w[0].isupper():
                return f"{primary} - {w}"

    return primary


def safe_float(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


# ── núcleo: procesar 1 campaña ───────────────────────────────────────
def run_one(meta: MetaClient, campaign_key: str, c: dict) -> dict:
    log.info(f"── Campaign: {c['display_name']} ({c['period_label']}) [{campaign_key}]")

    ad_sets = meta.list_ad_sets(c["campaign_id"])
    log.info(f"  {len(ad_sets)} ad sets")
    adset_by_id = {a["id"]: a for a in ad_sets}

    ads = meta.list_ads(c["campaign_id"])
    log.info(f"  {len(ads)} ads")

    # Recolectar creative_ids únicos y traerlos en batch desde Meta.
    # La expansión `creative{...}` desde el endpoint de ads dropea sub-fields
    # silenciosamente — por eso fetcheamos los creatives por ID directamente.
    cids_per_ad: dict[str, str] = {}
    for ad in ads:
        ad_id = ad.get("id")
        if not ad_id:
            continue
        cr = ad.get("creative") or {}
        cid = cr.get("id")
        if cid:
            cids_per_ad[ad_id] = cid
    unique_cids = list({cid for cid in cids_per_ad.values()})
    log.info(f"  fetching {len(unique_cids)} unique creatives in batch...")
    cdetails = meta.get_creatives_by_ids(unique_cids)
    log.info(f"  got detail for {len(cdetails)}/{len(unique_cids)} creatives")

    # Mapa ad_id → creative DETALLADO (incluye asset_feed_spec.images/videos).
    creative_by_ad: dict[str, dict] = {}
    for ad_id, cid in cids_per_ad.items():
        creative_by_ad[ad_id] = cdetails.get(cid) or {}

    # Conteos para diagnosticar la cobertura
    n_with_creative = sum(1 for cr in creative_by_ad.values() if cr)
    n_without_creative = len(creative_by_ad) - n_with_creative
    log.info(
        f"  creative info: {n_with_creative}/{len(creative_by_ad)} ads "
        f"con creative ({n_without_creative} sin info → fallback por nombre)"
    )

    # Insights a nivel AD
    ad_insights: list[dict] = []
    for adset in ad_sets:
        try:
            rows = meta.get_insights(
                adset["id"],
                level="ad",
                date_start=c["date_start"],
                date_end=c["date_end"],
            )
            ad_insights.extend(rows)
        except Exception as e:
            log.warning(f"  ad set {adset.get('name','?')} ({adset['id']}) failed: {e}")
    log.info(f"  {len(ad_insights)} ad-level insight rows")

    daily = meta.get_daily_insights(
        c["campaign_id"],
        date_start=c["date_start"],
        date_end=c["date_end"],
    )
    log.info(f"  {len(daily)} daily rows")

    studios_cfg = c["studios"]

    # Normalize studio names to Snowflake canonical
    _CANONICAL = {
        "Charlotte - NoDa":      "Charlotte - Noda",
        "Miami Brickell":        "Miami - Brickell",
        "Miami Upper East Side": "Miami - Upper East Side",
        "Midtown Miami":         "Miami - Midtown",
        "Coconut Grove":         "Miami - Coconut Grove",
        "NYC Chelsea":           "NYC - Chelsea",
        "NYC Park Slope":        "NYC - Park Slope",
    }
    for s in studios_cfg:
        if s.get("name") in _CANONICAL:
            s["name"] = _CANONICAL[s["name"]]

    def _empty_bucket():
        return {"spend": 0.0, "impressions": 0, "leads": 0, "ads": []}

    studio_agg: dict[str, dict] = {
        s["code"]: {
            "code": s["code"], "name": s["name"], "state": s["state"],
            "impressions": 0, "clicks": 0, "spend": 0.0, "reach": 0,
            "leads": 0, "purchases": 0, "trials": 0,
            "_audiences":   defaultdict(_empty_bucket),
            "_pillars":     defaultdict(_empty_bucket),
            "_concepts":    defaultdict(_empty_bucket),
            "_media_types": defaultdict(_empty_bucket),
        } for s in studios_cfg
    }

    global_aud:        dict[str, dict] = defaultdict(_empty_bucket)
    global_pillar:     dict[str, dict] = defaultdict(_empty_bucket)
    global_concept:    dict[str, dict] = defaultdict(_empty_bucket)
    global_media_type: dict[str, dict] = defaultdict(_empty_bucket)

    aud_tokens_cfg    = c.get("audience_tokens", {}) or {}
    pillar_tokens_cfg = c.get("pillar_tokens", {}) or {}
    aud_flat    = {t for toks in aud_tokens_cfg.values() for t in toks}
    pillar_flat = {t for toks in pillar_tokens_cfg.values() for t in toks}

    ad_dims: dict[str, dict] = {}

    # Diagnóstico de clasificación de media_type
    mt_via_creative = 0
    mt_via_name = 0

    for ins in ad_insights:
        adset = adset_by_id.get(ins.get("adset_id"), {})
        studio = match_studio(adset.get("name", ""), studios_cfg)
        if not studio:
            continue

        ad_name = ins.get("ad_name", "")
        aud    = match_audience(ad_name, aud_tokens_cfg)
        pillar = match_pillar(ad_name, pillar_tokens_cfg)
        concept = detect_concept(
            ad_name,
            studio_match=studio.get("match"),
            audience_tokens_flat=aud_flat,
            pillar_tokens_flat=pillar_flat,
            state_code=studio.get("state"),
        )
        # Clasificación de tipo de creatividad. Usamos creative (Meta API)
        # como fuente primaria; ad_name es fallback.
        creative = creative_by_ad.get(ins.get("ad_id")) or {}
        via_creative = _media_type_from_creative(creative)
        if via_creative:
            media_type = via_creative
            mt_via_creative += 1
        else:
            media_type = _media_type_from_name(ad_name)
            mt_via_name += 1

        spend = safe_float(ins.get("spend"))
        impressions = int(safe_float(ins.get("impressions")))
        clicks = int(safe_float(ins.get("clicks")))
        reach = int(safe_float(ins.get("reach")))
        leads = leads_of(ins)
        purchases = purchases_of(ins)
        trials = trials_of(ins)

        agg = studio_agg[studio["code"]]
        agg["impressions"] += impressions
        agg["clicks"] += clicks
        agg["spend"] += spend
        agg["reach"] += reach
        agg["leads"] += leads
        agg["purchases"] += purchases
        agg["trials"] += trials

        def _bump(bucket: dict, ad: str):
            bucket["spend"] += spend
            bucket["impressions"] += impressions
            bucket["leads"] += leads
            if ad and ad not in bucket["ads"]:
                bucket["ads"].append(ad)

        if aud:
            _bump(agg["_audiences"][aud], ad_name)
            _bump(global_aud[aud], ad_name)
        if pillar:
            _bump(agg["_pillars"][pillar], ad_name)
            _bump(global_pillar[pillar], ad_name)
        if concept and concept != "(other)":
            _bump(agg["_concepts"][concept], ad_name)
            _bump(global_concept[concept], ad_name)
        _bump(agg["_media_types"][media_type], ad_name)
        _bump(global_media_type[media_type], ad_name)

        ad_id = ins.get("ad_id")
        if ad_id:
            ad_dims[ad_id] = {
                "studio_code": studio["code"],
                "audience":    aud,
                "pillar":      pillar,
                "concept":     concept if concept and concept != "(other)" else None,
                "media_type":  media_type,
            }

    log.info(
        f"  media_type breakdown: {mt_via_creative} via Meta creative API, "
        f"{mt_via_name} via name fallback"
    )

    # Totales
    totals = {k: 0 for k in ["impressions", "clicks", "reach", "leads", "purchases", "trials"]}
    totals["spend"] = 0.0
    for s in studio_agg.values():
        for k in ["impressions", "clicks", "reach", "leads", "purchases", "trials"]:
            totals[k] += s[k]
        totals["spend"] += s["spend"]
    totals["spend"] = round(totals["spend"], 2)
    totals["ctr"] = round((totals["clicks"] / totals["impressions"] * 100), 2) if totals["impressions"] else 0
    totals["cpm"] = round((totals["spend"] / totals["impressions"] * 1000), 2) if totals["impressions"] else 0
    totals["cpl"] = round((totals["spend"] / totals["leads"]), 2) if totals["leads"] else 0

    log.info(
        f"  totals: spend=${totals['spend']:.2f}  leads={totals['leads']}  "
        f"trials={totals['trials']}  purchases={totals['purchases']}  "
        f"CPL=${totals['cpl']:.2f}"
    )

    # ── armar estructuras JSON ──────────────────────────────────────
    studios_out = []
    for s in studios_cfg:
        a = studio_agg[s["code"]]
        cpl = round(a["spend"] / a["leads"], 2) if a["leads"] else 0
        ctr = round(a["clicks"] / a["impressions"] * 100, 2) if a["impressions"] else 0
        cpm = round(a["spend"] / a["impressions"] * 1000, 2) if a["impressions"] else 0
        studios_out.append({
            "code": a["code"],
            "name": a["name"],
            "state": a["state"],
            "impressions": a["impressions"],
            "clicks": a["clicks"],
            "spend": round(a["spend"], 2),
            "reach": a["reach"],
            "ctr": ctr,
            "cpm": cpm,
            "leads": a["leads"],
            "cpl": cpl,
            "purchases": a["purchases"],
            "trials": a["trials"],
        })

    audiences_out = []
    for code, agg in studio_agg.items():
        for aud, v in agg["_audiences"].items():
            audiences_out.append({
                "studio_code": code,
                "audience": aud,
                "spend": round(v["spend"], 2),
                "impressions": v["impressions"],
                "leads": v["leads"],
                "ads": v["ads"],
            })

    pillars_out = []
    for pillar, v in global_pillar.items():
        cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
        pillars_out.append({
            "pillar": pillar,
            "spend": round(v["spend"], 2),
            "impressions": v["impressions"],
            "leads": v["leads"],
            "cpl": cpl,
            "ads": v["ads"][:20],
        })

    concepts_out = []
    for concept, v in global_concept.items():
        cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
        concepts_out.append({
            "concept": concept,
            "spend": round(v["spend"], 2),
            "impressions": v["impressions"],
            "leads": v["leads"],
            "cpl": cpl,
            "ads": v["ads"][:20],
        })

    studio_pillars_out = []
    for code, agg in studio_agg.items():
        for pillar, v in agg["_pillars"].items():
            cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
            studio_pillars_out.append({
                "studio_code": code,
                "pillar": pillar,
                "spend": round(v["spend"], 2),
                "impressions": v["impressions"],
                "leads": v["leads"],
                "cpl": cpl,
            })

    studio_concepts_out = []
    for code, agg in studio_agg.items():
        for concept, v in agg["_concepts"].items():
            cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
            studio_concepts_out.append({
                "studio_code": code,
                "concept": concept,
                "spend": round(v["spend"], 2),
                "impressions": v["impressions"],
                "leads": v["leads"],
                "cpl": cpl,
            })

    media_types_out = []
    for mt, v in global_media_type.items():
        cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
        media_types_out.append({
            "media_type": mt,
            "spend": round(v["spend"], 2),
            "impressions": v["impressions"],
            "leads": v["leads"],
            "cpl": cpl,
            "ads": v["ads"][:20],
        })

    studio_media_types_out = []
    for code, agg in studio_agg.items():
        for mt, v in agg["_media_types"].items():
            cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
            studio_media_types_out.append({
                "studio_code": code,
                "media_type": mt,
                "spend": round(v["spend"], 2),
                "impressions": v["impressions"],
                "leads": v["leads"],
                "cpl": cpl,
            })

    # daily_out removed — superseded by top-level daily_ad_studio

    # ── daily ad×studio rows (Apr 2026 onward, grows indefinitely) ───────
    # Window: DAILY_AD_STUDIO_START → today, clipped to campaign bounds.
    # These are returned as a list of raw rows; run() merges them into the
    # top-level daily_ad_studio array across all campaigns.
    today_iso    = date.today().isoformat()
    das_start    = max(c["date_start"], DAILY_AD_STUDIO_START)
    das_end      = min(c["date_end"],   today_iso)

    ad_first_seen: dict[str, str] = {}
    das_rows: list[dict] = []  # [{date, studio_code, ad_id, spend, impressions, clicks, leads, trials}]

    if das_start <= das_end:
        log.info(f"  fetching daily ad×studio [{das_start} -> {das_end}] ...")
        daily_ad_insights: list[dict] = []
        for adset in ad_sets:
            try:
                rows = meta.get_insights(
                    adset["id"],
                    level="ad",
                    date_start=das_start,
                    date_end=das_end,
                    time_increment=1,
                )
                daily_ad_insights.extend(rows)
            except Exception as e:
                log.warning(f"  daily ad-level failed for adset {adset.get('name','?')} ({adset['id']}): {e}")
        log.info(f"  {len(daily_ad_insights)} ad×day rows")

        d_ad_studio: dict[tuple, dict] = defaultdict(
            lambda: {"spend": 0.0, "impressions": 0, "clicks": 0, "leads": 0, "trials": 0}
        )

        for row in daily_ad_insights:
            ad_id = row.get("ad_id")
            dims  = ad_dims.get(ad_id)
            if not dims:
                continue
            d = row.get("date_start")
            if not d:
                continue

            spend = safe_float(row.get("spend"))
            if spend > 0 and (ad_id not in ad_first_seen or d < ad_first_seen[ad_id]):
                ad_first_seen[ad_id] = d

            sc = dims["studio_code"]
            b  = d_ad_studio[(d, sc, ad_id)]
            b["spend"]       += spend
            b["impressions"] += int(safe_float(row.get("impressions")))
            b["clicks"]      += int(safe_float(row.get("clicks")))
            b["leads"]       += leads_of(row)
            b["trials"]      += trials_of(row)

        for (d, sc, ad_id), b in sorted(d_ad_studio.keys()):
            b = d_ad_studio[(d, sc, ad_id)]
            das_rows.append({
                "date":        d,
                "studio_code": sc,
                "ad_id":       ad_id,
                "spend":       round(b["spend"], 2),
                "impressions": b["impressions"],
                "clicks":      b["clicks"],
                "leads":       b["leads"],
                "trials":      b["trials"],
            })

        log.info(f"  daily_ad_studio: {len(das_rows)} rows")
    else:
        log.info(f"  daily_ad_studio: window empty ({das_start} > {das_end}), skip.")



    # ── ads — metadata only (thumb, name, status) for the Active Ads table ──
    # Metrics come from daily_ad_studio filtered by date; no metrics stored here.
    # Build status map from list_ads() result
    status_by_ad: dict[str, str] = {
        ad["id"]: ad.get("status", "UNKNOWN")
        for ad in ads
        if ad.get("id")
    }

    ads_out = []
    for ad_id, dims in ad_dims.items():
        creative = creative_by_ad.get(ad_id, {})
        thumb = (
            creative.get("thumbnail_url")
            or creative.get("image_url")
            or ""
        )
        ads_out.append({
            "ad_id":        ad_id,
            "name":         next(
                (ins.get("ad_name", "") for ins in ad_insights if ins.get("ad_id") == ad_id),
                ad_id,
            ),
            "status":       status_by_ad.get(ad_id, "UNKNOWN"),
            "media_type":   dims.get("media_type", "Other"),
            "studio_code":  dims.get("studio_code"),
            "thumbnail_url": thumb,
            "library_url":  f"https://www.facebook.com/ads/library/?id={ad_id}&country=US",
            "first_seen":   ad_first_seen.get(ad_id),
        })

    ads_out.sort(key=lambda x: x.get("first_seen") or "")
    log.info(f"  ads_out: {len(ads_out)} ad metadata rows")

    return {
        "display_name": c["display_name"],
        "period_label": c["period_label"],
        "date_start":   c["date_start"],
        "date_end":     c["date_end"],
        "totals":            totals,
        "studios":           studios_out,
        "audiences":         audiences_out,
        "pillars":           pillars_out,
        "concepts":          concepts_out,
        "media_types":       media_types_out,
        "studio_pillars":    studio_pillars_out,
        "studio_concepts":   studio_concepts_out,
        "studio_media_types": studio_media_types_out,
        "ads":               ads_out,   # metadata only: ad_id, name, status, media_type, studio_code, thumbnail_url, library_url, first_seen
        "_das_rows":         das_rows,  # internal — merged into top-level daily_ad_studio by run()
    }


# ── entry point ──────────────────────────────────────────────────────
def run():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    keys = cfg.get("campaigns_to_track") or [cfg["active_campaign"]]
    active = cfg.get("active_campaign", keys[0])

    meta = MetaClient()

    campaigns_data: dict[str, dict] = {}
    campaigns_index: list[dict] = []

    for key in keys:
        if key not in cfg["campaigns"]:
            log.warning(f"Skipping '{key}' — not in config.campaigns")
            continue
        try:
            data = run_one(meta, key, cfg["campaigns"][key])
        except Exception as e:
            log.exception(f"❌ Campaign '{key}' failed: {e}")
            continue

        campaigns_data[key] = data
        campaigns_index.append({
            "key": key,
            "display_name": data["display_name"],
            "period_label": data["period_label"],
            "date_start": data["date_start"],
            "date_end": data["date_end"],
            "leads": data["totals"]["leads"],
            "spend": data["totals"]["spend"],
            "is_default": key == active,
        })

    # Preserve manually-baked static data (monthly_spend) and the growing
    # daily_ad_studio array from the existing file.
    existing_das: list[dict] = []
    existing_static: dict = {}
    if OUT_PATH.exists():
        try:
            existing = json.loads(OUT_PATH.read_text(encoding="utf-8"))
            for key in ("monthly_spend",):
                if key in existing:
                    existing_static[key] = existing[key]
            existing_das = existing.get("daily_ad_studio", [])
        except Exception:
            pass

    # Merge _das_rows from all campaigns into the existing daily_ad_studio.
    # Key: (date, studio_code, ad_id) — new data overwrites old for same key.
    das_index: dict[tuple, dict] = {
        (r["date"], r["studio_code"], r["ad_id"]): r
        for r in existing_das
    }
    for data in campaigns_data.values():
        for r in data.pop("_das_rows", []):
            das_index[(r["date"], r["studio_code"], r["ad_id"])] = r

    daily_ad_studio = sorted(das_index.values(), key=lambda r: (r["date"], r["studio_code"], r["ad_id"]))
    log.info(f"  daily_ad_studio total rows: {len(daily_ad_studio)} (was {len(existing_das)})")

    output = {
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "active_campaign": active,
        "campaigns_index": campaigns_index,
        "campaigns":       campaigns_data,
        "daily_ad_studio": daily_ad_studio,
        **existing_static,
    }

    OUT_PATH.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info(f"✅ Wrote {OUT_PATH}  ({OUT_PATH.stat().st_size:,} bytes, "
             f"{len(campaigns_data)} campaign(s))")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        log.exception(f"❌ ETL failed: {e}")
        sys.exit(1)