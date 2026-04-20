import snowflake.connector
import json
import os
from datetime import datetime, date

# ── Credentials (loaded from env vars in GitHub Actions, hardcoded for local test) ──
ACCOUNT  = os.getenv("SF_ACCOUNT",   "MINDBODYORG-PLAYLIST_DATA_MART_SWEAT440")
USER     = os.getenv("SF_USER",      "SWEAT440")
PASSWORD = os.getenv("SF_PASSWORD",  "myPq3?GfF08ulJCtvCB!poJP1vQHWD")
ROLE     = os.getenv("SF_ROLE",      "SYSADMIN")
WAREHOUSE= os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
DATABASE = os.getenv("SF_DATABASE",  "MARKETING_REPORTS")
SCHEMA   = os.getenv("SF_SCHEMA",    "PUBLIC")

def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

conn = snowflake.connector.connect(
    account=ACCOUNT, user=USER, password=PASSWORD,
    role=ROLE, warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA
)
cur = conn.cursor()

# ── 1. KPI Totals ──────────────────────────────────────────────────────────────
cur.execute("""
    SELECT
        SUM(SIGNUPS)            AS total_signups,
        SUM(FIRST_VISITS)       AS total_first_visits,
        SUM(FIRST_ACTIVATIONS)  AS total_first_activations,
        SUM(FIRST_SALES)        AS total_first_sales
    FROM MARKETING_REPORTS.PUBLIC.LEADS
""")
row = cur.fetchone()
kpis = {
    "total_signups":           row[0],
    "total_first_visits":      row[1],
    "total_first_activations": row[2],
    "total_first_sales":       row[3],
}

# ── 2. Monthly trend (last 12 months) ─────────────────────────────────────────
cur.execute("""
    SELECT
        DATE_TRUNC('month', EVENT_DATE)  AS month,
        SUM(SIGNUPS)                     AS signups,
        SUM(FIRST_VISITS)                AS first_visits,
        SUM(FIRST_ACTIVATIONS)           AS first_activations,
        SUM(FIRST_SALES)                 AS first_sales
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    WHERE EVENT_DATE >= DATEADD('month', -12, CURRENT_DATE())
    GROUP BY 1
    ORDER BY 1
""")
monthly = [
    {
        "month":             json_serial(r[0]),
        "signups":           r[1],
        "first_visits":      r[2],
        "first_activations": r[3],
        "first_sales":       r[4],
    }
    for r in cur.fetchall()
]

# ── 3. By Studio ──────────────────────────────────────────────────────────────
cur.execute("""
    SELECT
        STUDIO_NAME,
        SUM(SIGNUPS)            AS signups,
        SUM(FIRST_VISITS)       AS first_visits,
        SUM(FIRST_ACTIVATIONS)  AS first_activations,
        SUM(FIRST_SALES)        AS first_sales
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    GROUP BY STUDIO_NAME
    ORDER BY signups DESC
""")
by_studio = [
    {
        "studio":            r[0],
        "signups":           r[1],
        "first_visits":      r[2],
        "first_activations": r[3],
        "first_sales":       r[4],
    }
    for r in cur.fetchall()
]

# ── 4. By Source ──────────────────────────────────────────────────────────────
cur.execute("""
    SELECT
        SOURCE,
        SUM(SIGNUPS)            AS signups,
        SUM(FIRST_VISITS)       AS first_visits,
        SUM(FIRST_ACTIVATIONS)  AS first_activations,
        SUM(FIRST_SALES)        AS first_sales
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    GROUP BY SOURCE
    ORDER BY signups DESC
""")
by_source = [
    {
        "source":            r[0],
        "signups":           r[1],
        "first_visits":      r[2],
        "first_activations": r[3],
        "first_sales":       r[4],
    }
    for r in cur.fetchall()
]

# ── 5. Conversion funnel (overall) ────────────────────────────────────────────
funnel = [
    {"stage": "Signups",           "value": kpis["total_signups"]},
    {"stage": "First Visits",      "value": kpis["total_first_visits"]},
    {"stage": "First Activations", "value": kpis["total_first_activations"]},
    {"stage": "First Sales",       "value": kpis["total_first_sales"]},
]

# ── Write output ──────────────────────────────────────────────────────────────
output = {
    "generated_at": datetime.utcnow().isoformat() + "Z",
    "kpis":         kpis,
    "monthly":      monthly,
    "by_studio":    by_studio,
    "by_source":    by_source,
    "funnel":       funnel,
}

with open("data.json", "w") as f:
    json.dump(output, f, indent=2, default=json_serial)

conn.close()
print(f"✅  data.json written — {len(monthly)} months, {len(by_studio)} studios, {len(by_source)} sources")
