"""
Reclassify pillars/audiences for buy_1_get_3 using updated config.yaml pillar_tokens,
without re-fetching from the Meta API. Reads existing ad names from daniel-paid-ads-data.json,
re-runs the classification, and rewrites daily_series.by_pillar, c.pillars, etc.
"""
import json, pathlib, yaml
from collections import defaultdict

ROOT      = pathlib.Path(__file__).parent
DATA_FILE = ROOT / "daniel-paid-ads-data.json"
CFG_FILE  = ROOT / "config.yaml"

data = json.loads(DATA_FILE.read_text("utf-8"))
cfg  = yaml.safe_load(CFG_FILE.read_text("utf-8"))

CAMPAIGN_KEY = "buy_1_get_3_open_studios_2026"
c    = data["campaigns"][CAMPAIGN_KEY]
ccfg = cfg["campaigns"][CAMPAIGN_KEY]

pillar_tokens   = ccfg.get("pillar_tokens", {})
audience_tokens = ccfg.get("audience_tokens", {})

def detect_pillar(name):
    for pil, tokens in pillar_tokens.items():
        for tok in tokens:
            if f" - {tok} - " in name or name.startswith(f"{tok} - ") or name.endswith(f" - {tok}"):
                return pil
    return None

def detect_audience(name):
    for aud, tokens in audience_tokens.items():
        for tok in tokens:
            if f" - {tok} - " in name or name.startswith(f"{tok} - ") or name.endswith(f" - {tok}"):
                return aud
    return None

# Build ad_id → (pillar, audience) lookup from existing ads
ad_dims = {}
for ad in c.get("ads", []):
    name  = ad.get("name", "")
    ad_id = ad.get("ad_id", "")
    pil   = detect_pillar(name)
    aud   = detect_audience(name)
    ad_dims[ad_id] = {"pillar": pil, "audience": aud, "name": name}

print(f"  ad_dims built: {len(ad_dims)} ads")
pillar_counts = defaultdict(int)
for v in ad_dims.values():
    pillar_counts[v['pillar'] or 'None'] += 1
for k, n in sorted(pillar_counts.items()):
    print(f"    pillar={k}: {n} ads")

# Re-aggregate daily_series.by_pillar from by_studio_pillar (cross-breakdown)
# Actually, we need to rebuild from by_studio_audience, etc. using the ad_dims.
# The raw daily data per ad isn't stored — use by_studio_pillar cross-breakdown.
# But we need to reclassify. Since raw ad×day isn't in the JSON, we use
# daily_series.by_studio as proxy and re-map via studio_concepts/studio_pillars.

ds = c.get("daily_series", {})

# The best we can do without raw ad×day data is re-aggregate from studio_pillars
# which stores (studio_code, pillar) tuples with totals (not daily).
# For daily data, we rebuild from by_studio_pillar cross-breakdown if it exists.
bsp = ds.get("by_studio_pillar", [])
bsa = ds.get("by_studio_audience", [])

if bsp:
    # Re-aggregate by (date, pillar) to rebuild by_pillar
    # Problem: bsp was built with old pillar_tokens, so VC/DC/PR rows won't exist.
    # We need to rebuild from scratch using the raw studio×concept×date data.
    # Since we only have by_studio_concept in daily_series, let's use studio_concepts
    # plus ad_dims to re-classify by pillar.
    print("  Rebuilding by_pillar from by_studio_concept + ad classification...")
    bsc = ds.get("by_studio_concept", [])

    # Map concept → pillar by checking ad_dims
    # Normalize concept names: collapse multiple spaces around dashes
    import re
    def norm(s):
        return re.sub(r'\s*-\s*', ' - ', s.strip())

    concept_to_pillar = {}
    for dim in ad_dims.values():
        name = dim['name']
        pil  = dim['pillar']
        # Extract concept token from name (last segment after audience)
        parts = [p.strip() for p in name.split(' - ')]
        if len(parts) >= 4:
            concept_raw = ' - '.join(parts[3:])
            concept = norm(concept_raw)
            if concept and pil:
                concept_to_pillar[concept] = pil

    print(f"  concept->pillar map: {dict(sorted(concept_to_pillar.items()))}")

    # Rebuild by_pillar from daily by_studio_concept rows
    pil_day: dict = defaultdict(lambda: {"spend":0,"impressions":0,"clicks":0,"leads":0,"trials":0,"purchases":0,"cpl":0,"cpt":0,"cpp":0,"ctr":0,"cpm":0})
    for r in bsc:
        concept = norm(r.get("concept",""))
        pil = concept_to_pillar.get(concept)
        if not pil:
            # try matching via detect_pillar on a fake ad name
            # concept might match a token directly
            for p, toks in pillar_tokens.items():
                for tok in toks:
                    if tok in concept:
                        pil = p
                        break
                if pil:
                    break
        if pil:
            key = (r["date"], pil)
            b   = pil_day[key]
            b["spend"]       += r.get("spend",       0)
            b["impressions"] += r.get("impressions", 0)
            b["clicks"]      += r.get("clicks",      0)
            b["leads"]       += r.get("leads",       0)
            b["trials"]      += r.get("trials",      0)
            b["purchases"]   += r.get("purchases",   0)

    by_pillar_new = []
    for (date, pil), b in sorted(pil_day.items()):
        row = {"pillar": pil, "date": date, **b}
        if b["leads"]:      row["cpl"] = round(b["spend"] / b["leads"], 2)
        if b["trials"]:     row["cpt"] = round(b["spend"] / b["trials"], 2)
        if b["purchases"]:  row["cpp"] = round(b["spend"] / b["purchases"], 2)
        if b["impressions"]:
            row["ctr"] = round(b["clicks"] / b["impressions"] * 100, 2)
            row["cpm"] = round(b["spend"]  / b["impressions"] * 1000, 2)
        by_pillar_new.append(row)

    ds["by_pillar"] = by_pillar_new
    print(f"  by_pillar rebuilt: {len(by_pillar_new)} rows")

# Rebuild top-level pillars aggregate
pil_totals: dict = defaultdict(lambda: {"spend":0,"impressions":0,"leads":0,"ads":[]})
for ad in c.get("ads", []):
    name = ad.get("name","")
    pil  = detect_pillar(name)
    if pil:
        t = pil_totals[pil]
        t["ads"].append(name)
        # Add spend/leads from ad-level totals if available
        t["spend"]  += ad.get("spend",  0)
        t["leads"]  += ad.get("leads",  0)
        t["impressions"] += ad.get("impressions", 0)

# If ads don't have spend, use by_pillar aggregated totals
if by_pillar_new:
    pil_sums: dict = defaultdict(lambda: {"spend":0,"leads":0,"impressions":0})
    for r in by_pillar_new:
        p = r["pillar"]
        pil_sums[p]["spend"]       += r["spend"]
        pil_sums[p]["leads"]       += r["leads"]
        pil_sums[p]["impressions"] += r["impressions"]

    pillars_out = []
    for pil in pil_sums:
        s = pil_sums[pil]
        pillars_out.append({
            "pillar": pil,
            "spend":  round(s["spend"], 2),
            "leads":  s["leads"],
            "impressions": s["impressions"],
            "cpl":    round(s["spend"] / s["leads"], 2) if s["leads"] else 0,
            "ads":    [v["name"] for v in ad_dims.values() if v["pillar"] == pil],
        })
    pillars_out.sort(key=lambda x: -x["spend"])
    c["pillars"] = pillars_out
    print(f"  c.pillars rebuilt: {len(pillars_out)} pillars")
    for p in pillars_out:
        print(f"    {p['pillar']}: spend=${p['spend']:.2f}, leads={p['leads']}")

DATA_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
print(f"\nWrote {DATA_FILE} ({DATA_FILE.stat().st_size:,} bytes)")
