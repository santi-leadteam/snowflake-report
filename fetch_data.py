import snowflake.connector
import json
import os
from datetime import datetime, date
from cryptography.hazmat.primitives import serialization

# ── Credentials ───────────────────────────────────────────────────────────────
ACCOUNT   = os.getenv("SF_ACCOUNT",   "MINDBODYORG-PLAYLIST_DATA_MART_SWEAT440")
USER      = os.getenv("SF_USER",      "SWEAT440")
ROLE      = os.getenv("SF_ROLE",      "SYSADMIN")
WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
DATABASE  = os.getenv("SF_DATABASE",  "MARKETING_REPORTS")
SCHEMA    = os.getenv("SF_SCHEMA",    "PUBLIC")
TOKEN     = os.getenv("SF_TOKEN")

def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

conn = snowflake.connector.connect(
    account=ACCOUNT, user=USER, token=TOKEN,
    authenticator="programmatic_access_token",
    role=ROLE, warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA
)
cur = conn.cursor()

# ── 1. DAILY — from start of previous quarter through today ───────────────────
# Snowflake: DATEADD('quarter', -1, DATE_TRUNC('quarter', CURRENT_DATE()))
# gives the first day of the previous quarter, dynamically.
cur.execute("""
    SELECT
        EVENT_DATE,
        STUDIO_NAME,
        SOURCE,
        SUM(SIGNUPS)            AS signups,
        SUM(FIRST_VISITS)       AS first_visits,
        SUM(FIRST_ACTIVATIONS)  AS first_activations,
        SUM(FIRST_SALES)        AS first_sales
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    WHERE EVENT_DATE >= DATEADD('quarter', -1, DATE_TRUNC('quarter', CURRENT_DATE()))
      AND EVENT_DATE <= CURRENT_DATE()
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
""")
daily_detail = [
    {
        "date":              json_serial(r[0]),
        "studio":            r[1],
        "source":            r[2],
        "signups":           int(r[3] or 0),
        "first_visits":      int(r[4] or 0),
        "first_activations": int(r[5] or 0),
        "first_sales":       int(r[6] or 0),
    }
    for r in cur.fetchall()
]

# ── 2. MONTHLY — everything before the previous quarter, back 3 years ─────────
cur.execute("""
    SELECT
        DATE_TRUNC('month', EVENT_DATE) AS month,
        STUDIO_NAME,
        SOURCE,
        SUM(SIGNUPS)            AS signups,
        SUM(FIRST_VISITS)       AS first_visits,
        SUM(FIRST_ACTIVATIONS)  AS first_activations,
        SUM(FIRST_SALES)        AS first_sales
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    WHERE EVENT_DATE <  DATEADD('quarter', -1, DATE_TRUNC('quarter', CURRENT_DATE()))
      AND EVENT_DATE >= DATEADD('year', -3, CURRENT_DATE())
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
""")
monthly_detail = [
    {
        "month":             json_serial(r[0]),
        "studio":            r[1],
        "source":            r[2],
        "signups":           int(r[3] or 0),
        "first_visits":      int(r[4] or 0),
        "first_activations": int(r[5] or 0),
        "first_sales":       int(r[6] or 0),
    }
    for r in cur.fetchall()
]

# ── 3. Studio + source lists ──────────────────────────────────────────────────
cur.execute("""
    SELECT DISTINCT STUDIO_NAME FROM MARKETING_REPORTS.PUBLIC.LEADS
    WHERE STUDIO_NAME IS NOT NULL ORDER BY 1
""")
studios = [r[0] for r in cur.fetchall()]

cur.execute("""
    SELECT DISTINCT SOURCE FROM MARKETING_REPORTS.PUBLIC.LEADS
    WHERE SOURCE IS NOT NULL ORDER BY 1
""")
sources = [r[0] for r in cur.fetchall()]

# ── Write output ──────────────────────────────────────────────────────────────
output = {
    "generated_at":   datetime.utcnow().isoformat() + "Z",
    "studios":        studios,
    "sources":        sources,
    "daily_detail":   daily_detail,    # prev quarter start → today, by day
    "monthly_detail": monthly_detail,  # older history, by month, 3yr cap
}

with open("data.json", "w") as f:
    json.dump(output, f, indent=2, default=json_serial)

conn.close()

size_kb = os.path.getsize("data.json") / 1024
print(f"✅  data.json written")
print(f"    Daily rows:   {len(daily_detail):,}  (prev quarter start → today)")
print(f"    Monthly rows: {len(monthly_detail):,}  (3yr history)")
print(f"    Studios:      {len(studios)}")
print(f"    Sources:      {len(sources)}")
print(f"    File size:    {size_kb:.1f} KB")
