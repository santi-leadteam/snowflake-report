"""
fetch_nso_ads.py
─────────────────────────────────────────────────────────────
ETL Meta Ads → nso-ads-data.json

Descubre automáticamente campañas que contengan "NSO" en el nombre
(configurable en config-nso.yaml) y genera el mismo formato JSON
que paid-ads-data.json para que el dashboard nso-ads.html lo consuma.
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

import yaml

from meta_client import MetaClient, leads_of, purchases_of, trials_of


REPO_ROOT   = Path(__file__).resolve().parent
CONFIG_PATH = REPO_ROOT / "config-nso.yaml"
OUT_PATH    = REPO_ROOT / "nso-ads-data.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("nso-ads-etl")


# ── helpers de clasificación ─────────────────────────────────────────
def match_studio(name: str, studios: list[dict]) -> dict | None:
    """
    Intenta casar el nombre de un ad set contra la lista de studios.
    Primero busca coincidencias específicas (campo 'match' no vacío).
    Si no hay ninguna, devuelve el primer studio catch-all (match='').
    """
    n = (name or "").lower()
    catchall = None
    for s in studios:
        m = s.get("match", "")
        if m and m.lower() in n:
            return s
        if not m and catchall is None:
            catchall = s
    return catchall


def _has_token(name: str, token: str) -> bool:
    if not name or not token:
        return False
    norm = re.sub(r"[_\-/|]+", " ", name.upper())
    tok  = token.upper()
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


def match_tier(name: str, tokens_by_tier: dict[str, list[str]]) -> str | None:
    """Retorna el nombre del tier cuyo token aparece en el nombre del ad."""
    for tier, tokens in tokens_by_tier.items():
        for tok in tokens:
            if _has_token(name, tok):
                return tier
    return None


_STOPWORDS = {
    "V1", "V2", "V3", "V4", "V5", "A", "B", "C", "TEST", "VER", "VERSION",
    "NSO", "NEW", "STUDIO", "STUDIOS", "OPENING",
    "PROMO", "AD", "ADS", "COPY", "CREATIVE",
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
    "ENE", "ABR", "AGO", "DIC",
}

_GENERIC_PREFIXES = {
    "VIDEO", "REEL", "REELS", "IMAGE", "PHOTO", "STATIC",
    "CAROUSEL", "GIF", "STORY", "STORIES",
}

_VIDEO_KEYWORDS  = {"VIDEO", "REEL", "REELS", "GIF", "STORY", "STORIES"}
_STATIC_KEYWORDS = {"STATIC", "IMAGE", "PHOTO", "CAROUSEL"}


def _media_type_from_creative(creative: dict) -> str | None:
    if not creative:
        return None
    ot = (creative.get("object_type") or "").upper()
    if ot == "VIDEO":
        return "Video"
    if ot == "PHOTO":
        return "Static"
    if creative.get("video_id"):
        return "Video"
    if creative.get("image_hash"):
        return "Static"
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
    afs = creative.get("asset_feed_spec") or {}
    if isinstance(afs, dict):
        if afs.get("videos"):
            return "Video"
        if afs.get("images"):
            return "Static"
    return None


def _media_type_from_name(ad_name: str) -> str:
    if not ad_name:
        return "Other"
    words = {w.upper() for w in re.findall(r"\w+", ad_name)}
    if words & _VIDEO_KEYWORDS:
        return "Video"
    if words & _STATIC_KEYWORDS:
        return "Static"
    return "Other"


def detect_media_type(ad_name: str, creative: dict | None = None) -> str:
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
) -> str:
    if not ad_name:
        return "(other)"
    text = ad_name
    if studio_match:
        text = re.sub(re.escape(studio_match), " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b[A-Z]{2}[\-\s]?\d{2,3}\b", " ", text)
    text = re.sub(r"[_\-/|]+", " ", text)

    all_class_tokens = (
        {t.upper() for t in audience_tokens_flat}
        | {t.upper() for t in pillar_tokens_flat}
    )
    words_out = []
    for raw in re.split(r"\s+", text):
        w = raw.strip()
        if not w:
            continue
        upper = w.upper()
        if upper in all_class_tokens or upper in _STOPWORDS:
            continue
        if len(w) == 2 and w.isalpha() and w.isupper():
            continue
        if re.fullmatch(r"\d+", w) or re.fullmatch(r"[Vv]\d+", w):
            continue
        if len(w) < 3:
            continue
        words_out.append(w)

    if not words_out:
        return "(other)"

    primary = next(
        (w for w in words_out if w[0].isupper()),
        words_out[0],
    )
    primary_idx = words_out.index(primary)
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


def _campaign_key(name: str) -> str:
    """Slug limpio para usar como key en el JSON."""
    key = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return key or "campaign"


def _infer_dates(campaign: dict) -> tuple[str, str]:
    """
    Devuelve (date_start, date_end) del campaign de Meta.
    stop_time puede no existir (campañas sin fecha de fin).
    """
    today = date.today().isoformat()
    raw_start = campaign.get("start_time") or ""
    raw_stop  = campaign.get("stop_time")  or ""
    d_start = raw_start[:10] if raw_start else "2026-01-01"
    d_end   = raw_stop[:10]  if raw_stop  else today
    if d_end > today:
        d_end = today
    return d_start, d_end


# ── núcleo: procesar 1 campaña NSO ──────────────────────────────────
def run_one(
    meta: MetaClient,
    campaign: dict,
    cfg: dict,
    daily_window_days: int = 90,
) -> dict:
    campaign_id  = campaign["id"]
    display_name = campaign["name"]
    date_start, date_end = _infer_dates(campaign)

    log.info(f"-- NSO Campaign: {display_name} [{campaign_id}]")
    log.info(f"   period: {date_start} -> {date_end}")

    # Si no hay studios en el config, crea un catch-all con el nombre de la campaña
    studios_cfg: list[dict] = cfg.get("studios") or [{
        "code":  campaign_id,
        "name":  display_name,
        "state": "",
        "match": "",   # catch-all: coincide con todo
    }]

    ad_sets = meta.list_ad_sets(campaign_id)
    log.info(f"  {len(ad_sets)} ad sets")
    adset_by_id = {a["id"]: a for a in ad_sets}

    ads = meta.list_ads(campaign_id)
    log.info(f"  {len(ads)} ads")

    cids_per_ad: dict[str, str] = {}
    for ad in ads:
        ad_id = ad.get("id")
        if not ad_id:
            continue
        cr  = ad.get("creative") or {}
        cid = cr.get("id")
        if cid:
            cids_per_ad[ad_id] = cid

    unique_cids = list({cid for cid in cids_per_ad.values()})
    log.info(f"  fetching {len(unique_cids)} creatives...")
    cdetails = meta.get_creatives_by_ids(unique_cids)
    log.info(f"  got detail for {len(cdetails)}/{len(unique_cids)} creatives")

    creative_by_ad: dict[str, dict] = {
        ad_id: cdetails.get(cid) or {}
        for ad_id, cid in cids_per_ad.items()
    }

    # Insights a nivel AD (agregados en el rango completo)
    ad_insights: list[dict] = []
    for adset in ad_sets:
        try:
            rows = meta.get_insights(
                adset["id"],
                level="ad",
                date_start=date_start,
                date_end=date_end,
            )
            ad_insights.extend(rows)
        except Exception as e:
            log.warning(f"  adset {adset.get('name','?')} ({adset['id']}) failed: {e}")
    log.info(f"  {len(ad_insights)} ad-level insight rows")

    daily = meta.get_daily_insights(campaign_id, date_start=date_start, date_end=date_end)
    log.info(f"  {len(daily)} daily rows")

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

    # tier_agg: { tier_name: {spend, impressions, clicks, reach, leads} }
    def _empty_tier():
        return {"spend": 0.0, "impressions": 0, "clicks": 0, "reach": 0, "leads": 0}

    global_tier: dict[str, dict] = defaultdict(_empty_tier)
    # ads_agg: { ad_name: {tier, spend, impressions, clicks, reach, leads} }
    ads_agg: dict[str, dict] = {}

    aud_tokens_cfg    = cfg.get("audience_tokens") or {}
    pillar_tokens_cfg = cfg.get("pillar_tokens")   or {}
    tier_tokens_cfg   = cfg.get("tier_tokens")     or {}
    aud_flat    = {t for toks in aud_tokens_cfg.values() for t in toks}
    pillar_flat = {t for toks in pillar_tokens_cfg.values() for t in toks}

    ad_dims: dict[str, dict] = {}

    for ins in ad_insights:
        adset  = adset_by_id.get(ins.get("adset_id"), {})
        studio = match_studio(adset.get("name", ""), studios_cfg)
        if not studio:
            continue

        ad_name  = ins.get("ad_name", "")
        aud      = match_audience(ad_name, aud_tokens_cfg)
        pillar   = match_pillar(ad_name, pillar_tokens_cfg)
        tier     = match_tier(ad_name, tier_tokens_cfg)
        concept  = detect_concept(
            ad_name,
            studio_match=studio.get("match") or None,
            audience_tokens_flat=aud_flat,
            pillar_tokens_flat=pillar_flat,
        )
        creative   = creative_by_ad.get(ins.get("ad_id")) or {}
        media_type = detect_media_type(ad_name, creative)

        spend       = safe_float(ins.get("spend"))
        impressions = int(safe_float(ins.get("impressions")))
        clicks      = int(safe_float(ins.get("clicks")))
        reach       = int(safe_float(ins.get("reach")))
        leads       = leads_of(ins)
        purchases   = purchases_of(ins)
        trials      = trials_of(ins)

        agg = studio_agg[studio["code"]]
        agg["impressions"] += impressions
        agg["clicks"]      += clicks
        agg["spend"]       += spend
        agg["reach"]       += reach
        agg["leads"]       += leads
        agg["purchases"]   += purchases
        agg["trials"]      += trials

        def _bump(bucket: dict, ad: str):
            bucket["spend"]       += spend
            bucket["impressions"] += impressions
            bucket["leads"]       += leads
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

        # Acumular tier y ad-level
        if tier:
            t = global_tier[tier]
            t["spend"]       += spend
            t["impressions"] += impressions
            t["clicks"]      += clicks
            t["reach"]       += reach
            t["leads"]       += leads

        if ad_name:
            if ad_name not in ads_agg:
                ads_agg[ad_name] = {
                    "ad_name": ad_name, "tier": tier or "",
                    "spend": 0.0, "impressions": 0, "clicks": 0, "reach": 0, "leads": 0,
                }
            a = ads_agg[ad_name]
            a["spend"]       += spend
            a["impressions"] += impressions
            a["clicks"]      += clicks
            a["reach"]       += reach
            a["leads"]       += leads
            if tier and not a["tier"]:
                a["tier"] = tier

        ad_id = ins.get("ad_id")
        if ad_id:
            ad_dims[ad_id] = {
                "studio_code": studio["code"],
                "audience":    aud,
                "pillar":      pillar,
                "tier":        tier,
                "concept":     concept if concept and concept != "(other)" else None,
                "media_type":  media_type,
            }

    # Totales
    totals = {k: 0 for k in ["impressions", "clicks", "reach", "leads", "purchases", "trials"]}
    totals["spend"] = 0.0
    for s in studio_agg.values():
        for k in ["impressions", "clicks", "reach", "leads", "purchases", "trials"]:
            totals[k] += s[k]
        totals["spend"] += s["spend"]
    totals["spend"]  = round(totals["spend"], 2)
    totals["ctr"]    = round(totals["clicks"] / totals["impressions"] * 100, 2) if totals["impressions"] else 0
    totals["cpm"]    = round(totals["spend"]  / totals["impressions"] * 1000, 2) if totals["impressions"] else 0
    totals["cpl"]    = round(totals["spend"]  / totals["leads"], 2)              if totals["leads"]       else 0

    log.info(
        f"  totals: spend=${totals['spend']:.2f}  leads={totals['leads']}  "
        f"trials={totals['trials']}  purchases={totals['purchases']}  "
        f"CPL=${totals['cpl']:.2f}"
    )

    # ── armar estructuras JSON (misma forma que paid-ads-data.json) ──
    studios_out = []
    for s in studios_cfg:
        a   = studio_agg[s["code"]]
        cpl = round(a["spend"] / a["leads"], 2)                         if a["leads"]       else 0
        ctr = round(a["clicks"] / a["impressions"] * 100, 2)            if a["impressions"] else 0
        cpm = round(a["spend"]  / a["impressions"] * 1000, 2)           if a["impressions"] else 0
        studios_out.append({
            "code":        a["code"],
            "name":        a["name"],
            "state":       a["state"],
            "impressions": a["impressions"],
            "clicks":      a["clicks"],
            "spend":       round(a["spend"], 2),
            "reach":       a["reach"],
            "ctr":         ctr,
            "cpm":         cpm,
            "leads":       a["leads"],
            "cpl":         cpl,
            "purchases":   a["purchases"],
            "trials":      a["trials"],
        })

    audiences_out = []
    for code, agg in studio_agg.items():
        for aud, v in agg["_audiences"].items():
            audiences_out.append({
                "studio_code": code,
                "audience":    aud,
                "spend":       round(v["spend"], 2),
                "impressions": v["impressions"],
                "leads":       v["leads"],
                "ads":         v["ads"],
            })

    pillars_out = []
    for pillar, v in global_pillar.items():
        cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
        pillars_out.append({
            "pillar":      pillar,
            "spend":       round(v["spend"], 2),
            "impressions": v["impressions"],
            "leads":       v["leads"],
            "cpl":         cpl,
            "ads":         v["ads"][:20],
        })

    concepts_out = []
    for concept, v in global_concept.items():
        cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
        concepts_out.append({
            "concept":     concept,
            "spend":       round(v["spend"], 2),
            "impressions": v["impressions"],
            "leads":       v["leads"],
            "cpl":         cpl,
            "ads":         v["ads"][:20],
        })

    media_types_out = []
    for mt, v in global_media_type.items():
        cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
        media_types_out.append({
            "media_type":  mt,
            "spend":       round(v["spend"], 2),
            "impressions": v["impressions"],
            "leads":       v["leads"],
            "cpl":         cpl,
            "ads":         v["ads"][:20],
        })

    studio_pillars_out = []
    for code, agg in studio_agg.items():
        for pillar, v in agg["_pillars"].items():
            cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
            studio_pillars_out.append({
                "studio_code": code,
                "pillar":      pillar,
                "spend":       round(v["spend"], 2),
                "impressions": v["impressions"],
                "leads":       v["leads"],
                "cpl":         cpl,
            })

    studio_concepts_out = []
    for code, agg in studio_agg.items():
        for concept, v in agg["_concepts"].items():
            cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
            studio_concepts_out.append({
                "studio_code": code,
                "concept":     concept,
                "spend":       round(v["spend"], 2),
                "impressions": v["impressions"],
                "leads":       v["leads"],
                "cpl":         cpl,
            })

    studio_media_types_out = []
    for code, agg in studio_agg.items():
        for mt, v in agg["_media_types"].items():
            cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
            studio_media_types_out.append({
                "studio_code": code,
                "media_type":  mt,
                "spend":       round(v["spend"], 2),
                "impressions": v["impressions"],
                "leads":       v["leads"],
                "cpl":         cpl,
            })

    daily_out = [
        {
            "date":        d.get("date_start"),
            "impressions": int(safe_float(d.get("impressions"))),
            "clicks":      int(safe_float(d.get("clicks"))),
            "spend":       round(safe_float(d.get("spend")), 2),
            "reach":       int(safe_float(d.get("reach"))),
            "leads":       leads_of(d),
            "purchases":   purchases_of(d),
            "trials":      trials_of(d),
        }
        for d in daily
    ]

    # ── serie diaria granular (ad×día) ────────────────────────────────
    today       = date.today()
    window_start = (today - timedelta(days=daily_window_days)).isoformat()
    today_iso   = today.isoformat()
    daily_start = max(date_start, window_start)
    daily_end   = min(date_end, today_iso)

    daily_series: dict = {
        "window_start":         daily_start,
        "window_end":           daily_end,
        "window_days":          daily_window_days,
        "campaign":             [],
        "by_studio":            [],
        "by_audience":          [],
        "by_pillar":            [],
        "by_concept":           [],
        "by_media_type":        [],
        "by_studio_audience":   [],
        "by_studio_pillar":     [],
        "by_studio_concept":    [],
        "by_studio_media_type": [],
    }

    if daily_start > daily_end:
        log.info(f"  daily series: ventana vacía ({daily_start} > {daily_end}), skip.")
    else:
        log.info(f"  fetching daily ad-day insights [{daily_start} -> {daily_end}] ...")
        daily_ad_insights: list[dict] = []
        for adset in ad_sets:
            try:
                rows = meta.get_insights(
                    adset["id"],
                    level="ad",
                    date_start=daily_start,
                    date_end=daily_end,
                    time_increment=1,
                )
                daily_ad_insights.extend(rows)
            except Exception as e:
                log.warning(f"  daily ad-level failed for {adset.get('name','?')} ({adset['id']}): {e}")
        log.info(f"  {len(daily_ad_insights)} ad-day rows")

        def _empty_d():
            return {"spend": 0.0, "impressions": 0, "clicks": 0,
                    "reach": 0, "leads": 0, "trials": 0, "purchases": 0}

        camp_d        = defaultdict(_empty_d)
        d_studio      = defaultdict(_empty_d)
        d_aud         = defaultdict(_empty_d)
        d_pillar      = defaultdict(_empty_d)
        d_concept     = defaultdict(_empty_d)
        d_media_type  = defaultdict(_empty_d)
        d_tier        = defaultdict(_empty_d)
        d_stu_aud     = defaultdict(_empty_d)
        d_stu_pillar  = defaultdict(_empty_d)
        d_stu_concept = defaultdict(_empty_d)
        d_stu_media   = defaultdict(_empty_d)

        def _bump_d(bucket, spend, impressions, clicks, reach, leads, trials, purchases):
            bucket["spend"]       += spend
            bucket["impressions"] += impressions
            bucket["clicks"]      += clicks
            bucket["reach"]       += reach
            bucket["leads"]       += leads
            bucket["trials"]      += trials
            bucket["purchases"]   += purchases

        for row in daily_ad_insights:
            ad_id = row.get("ad_id")
            dims  = ad_dims.get(ad_id)
            if not dims:
                continue
            d = row.get("date_start")
            if not d:
                continue
            spend       = safe_float(row.get("spend"))
            impressions = int(safe_float(row.get("impressions")))
            clicks      = int(safe_float(row.get("clicks")))
            reach       = int(safe_float(row.get("reach")))
            leads       = leads_of(row)
            trials      = trials_of(row)
            purchases   = purchases_of(row)

            _bump_d(camp_d[d], spend, impressions, clicks, reach, leads, trials, purchases)
            sc = dims["studio_code"]
            _bump_d(d_studio[(sc, d)], spend, impressions, clicks, reach, leads, trials, purchases)
            if dims["audience"]:
                a = dims["audience"]
                _bump_d(d_aud[(a, d)],         spend, impressions, clicks, reach, leads, trials, purchases)
                _bump_d(d_stu_aud[(sc, a, d)], spend, impressions, clicks, reach, leads, trials, purchases)
            if dims["pillar"]:
                p = dims["pillar"]
                _bump_d(d_pillar[(p, d)],          spend, impressions, clicks, reach, leads, trials, purchases)
                _bump_d(d_stu_pillar[(sc, p, d)],  spend, impressions, clicks, reach, leads, trials, purchases)
            if dims["concept"]:
                co = dims["concept"]
                _bump_d(d_concept[(co, d)],          spend, impressions, clicks, reach, leads, trials, purchases)
                _bump_d(d_stu_concept[(sc, co, d)],  spend, impressions, clicks, reach, leads, trials, purchases)
            mt = dims.get("media_type") or "Other"
            _bump_d(d_media_type[(mt, d)],    spend, impressions, clicks, reach, leads, trials, purchases)
            _bump_d(d_stu_media[(sc, mt, d)], spend, impressions, clicks, reach, leads, trials, purchases)
            if dims.get("tier"):
                _bump_d(d_tier[(dims["tier"], d)], spend, impressions, clicks, reach, leads, trials, purchases)

        def _row_metrics(b: dict) -> dict:
            return {
                "spend":       round(b["spend"], 2),
                "impressions": b["impressions"],
                "clicks":      b["clicks"],
                "reach":       b["reach"],
                "leads":       b["leads"],
                "trials":      b["trials"],
                "purchases":   b["purchases"],
                "cpl": round(b["spend"] / b["leads"],      2) if b["leads"]       else 0,
                "cpt": round(b["spend"] / b["trials"],     2) if b["trials"]      else 0,
                "cpp": round(b["spend"] / b["purchases"],  2) if b["purchases"]   else 0,
                "ctr": round(b["clicks"] / b["impressions"] * 100, 2) if b["impressions"] else 0,
                "cpm": round(b["spend"]  / b["impressions"] * 1000, 2) if b["impressions"] else 0,
            }

        def _emit(data_dict, key_names):
            out = []
            for k in sorted(data_dict.keys()):
                if not isinstance(k, tuple):
                    k = (k,)
                row = dict(zip(key_names, k))
                row.update(_row_metrics(data_dict[k]))
                out.append(row)
            return out

        campaign_series = [
            {"date": dt, **_row_metrics(camp_d[dt])}
            for dt in sorted(camp_d.keys())
        ]

        daily_series.update({
            "campaign":             campaign_series,
            "by_tier":              _emit(d_tier,        ["tier",        "date"]),
            "by_studio":            _emit(d_studio,      ["studio_code", "date"]),
            "by_audience":          _emit(d_aud,         ["audience",    "date"]),
            "by_pillar":            _emit(d_pillar,      ["pillar",      "date"]),
            "by_concept":           _emit(d_concept,     ["concept",     "date"]),
            "by_media_type":        _emit(d_media_type,  ["media_type",  "date"]),
            "by_studio_audience":   _emit(d_stu_aud,     ["studio_code", "audience",   "date"]),
            "by_studio_pillar":     _emit(d_stu_pillar,  ["studio_code", "pillar",     "date"]),
            "by_studio_concept":    _emit(d_stu_concept, ["studio_code", "concept",    "date"]),
            "by_studio_media_type": _emit(d_stu_media,   ["studio_code", "media_type", "date"]),
        })

        log.info(
            f"  daily series: {len(campaign_series)} days | "
            f"{len(daily_series['by_studio'])} studio/day | "
            f"{len(daily_series['by_audience'])} aud/day | "
            f"{len(daily_series['by_pillar'])} pillar/day | "
            f"{len(daily_series['by_concept'])} concept/day | "
            f"{len(daily_series['by_media_type'])} media/day"
        )

    # ── tiers output ────────────────────────────────────────────────
    tiers_out = []
    for tier_name, v in global_tier.items():
        tiers_out.append({
            "tier":        tier_name,
            "spend":       round(v["spend"], 2),
            "impressions": v["impressions"],
            "clicks":      v["clicks"],
            "reach":       v["reach"],
            "leads":       v["leads"],
            "cpl":         round(v["spend"] / v["leads"], 2)                    if v["leads"]       else 0,
            "ctr":         round(v["clicks"] / v["impressions"] * 100, 2)       if v["impressions"] else 0,
        })

    # ── ads output ──────────────────────────────────────────────────
    ads_out = []
    for v in ads_agg.values():
        ads_out.append({
            "ad_name":     v["ad_name"],
            "tier":        v["tier"],
            "spend":       round(v["spend"], 2),
            "impressions": v["impressions"],
            "clicks":      v["clicks"],
            "reach":       v["reach"],
            "leads":       v["leads"],
            "cpl":         round(v["spend"] / v["leads"], 2)                    if v["leads"]       else 0,
            "ctr":         round(v["clicks"] / v["impressions"] * 100, 2)       if v["impressions"] else 0,
        })
    ads_out.sort(key=lambda x: x["spend"], reverse=True)

    log.info(f"  tiers: {len(tiers_out)} | ads: {len(ads_out)}")

    period_label = f"{date_start} → {date_end}"
    return {
        "display_name":       display_name,
        "period_label":       period_label,
        "date_start":         date_start,
        "date_end":           date_end,
        "totals":             totals,
        "tiers":              tiers_out,
        "ads":                ads_out,
        "studios":            studios_out,
        "audiences":          audiences_out,
        "pillars":            pillars_out,
        "concepts":           concepts_out,
        "media_types":        media_types_out,
        "studio_pillars":     studio_pillars_out,
        "studio_concepts":    studio_concepts_out,
        "studio_media_types": studio_media_types_out,
        "daily":              daily_out,
        "daily_series":       daily_series,
    }


# ── entry point ──────────────────────────────────────────────────────
def run():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    ad_account_id    = cfg["ad_account_id"]
    name_filter      = cfg.get("campaign_name_filter", "NSO")
    status_filter    = cfg.get("campaign_statuses")
    daily_window     = int(cfg.get("daily_window_days", 90))

    meta = MetaClient()

    log.info(f"Listing campaigns with '{name_filter}' in name from {ad_account_id}...")
    campaigns = meta.list_campaigns(
        ad_account_id,
        name_filter=name_filter,
        status_filter=status_filter,
    )
    log.info(f"Found {len(campaigns)} matching campaigns")

    if not campaigns:
        log.warning("No NSO campaigns found — nothing to process.")
        return

    campaigns_data:  dict[str, dict] = {}
    campaigns_index: list[dict]      = []

    for campaign in campaigns:
        key = _campaign_key(campaign["name"])
        # Si hay colisión de keys, añade suffix
        if key in campaigns_data:
            key = f"{key}_{campaign['id'][-6:]}"
        try:
            data = run_one(meta, campaign, cfg, daily_window_days=daily_window)
        except Exception as e:
            log.exception(f"[ERR] Campaign '{campaign['name']}' ({campaign['id']}) failed: {e}")
            continue

        campaigns_data[key] = data
        campaigns_index.append({
            "key":          key,
            "display_name": data["display_name"],
            "period_label": data["period_label"],
            "date_start":   data["date_start"],
            "date_end":     data["date_end"],
            "leads":        data["totals"]["leads"],
            "spend":        data["totals"]["spend"],
            "is_default":   len(campaigns_index) == 0,   # primera campaña como default
        })

    output = {
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "active_campaign": campaigns_index[0]["key"] if campaigns_index else "",
        "campaigns_index": campaigns_index,
        "campaigns":       campaigns_data,
    }

    OUT_PATH.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info(
        f"[OK] Wrote {OUT_PATH}  ({OUT_PATH.stat().st_size:,} bytes, "
        f"{len(campaigns_data)} campaign(s))"
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        log.exception(f"[ERR] ETL failed: {e}")
        sys.exit(1)
