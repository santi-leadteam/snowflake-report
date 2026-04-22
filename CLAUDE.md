# SWEAT440 Dashboard Project — Contributor Guide

Read this before starting any dashboard work.

## Repository
- GitHub: https://github.com/dev-leadteam/dashboards
- Live URL: https://dev-leadteam.github.io/dashboards/
- Open Studio Dashboard: https://dev-leadteam.github.io/dashboards/index.html

## Repo structure

```
dashboards/
├── data.json              ← auto-refreshed daily (all dashboards share this)
├── fetch_data.py          ← Snowflake + other data sources → data.json
├── logo.png               ← SWEAT440 logo (white on transparent)
├── index.html             ← Open Studio Dashboard
├── new-studio.html        ← New Studio Opening Dashboard
├── CLAUDE.md              ← this file
├── shared/
│   ├── style.css          ← brand CSS — import in every dashboard
│   └── utils.js           ← shared JS functions — import in every dashboard
└── .github/workflows/
    └── refresh.yml        ← daily data refresh via GitHub Actions
```

## Starting a new dashboard

Copy this HTML skeleton:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>SWEAT440 — [Dashboard Name]</title>
<link href="https://fonts.googleapis.com/css2?family=Oswald:wght@400;500;600;700&family=Open+Sans:wght@400;600&display=swap" rel="stylesheet"/>
<link rel="stylesheet" href="shared/style.css"/>
</head>
<body>
  <!-- nav, controls, tabs go here -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <script src="shared/utils.js"></script>
  <script>
    // Dashboard-specific JS only
    let RAW = null;

    async function loadData() {
      try {
        const res = await fetch('data.json?_=' + Date.now());
        RAW = await res.json();
        if (!RAW.daily_detail)   RAW.daily_detail   = [];
        if (!RAW.monthly_detail) RAW.monthly_detail = [];
      } catch(e) {
        RAW = getMock(); // getMock() is in utils.js
      }
      // build your dashboard...
    }

    loadData();
  </script>
</body>
</html>
```

## data.json structure

```json
{
  "generated_at": "ISO timestamp",
  "studios":        ["SWEAT440 Miami Beach", ...],
  "sources":        ["Meta Ads", "Google Ads", ...],
  "daily_detail":   [{ "date": "2026-01-15", "studio": "...", "source": "...",
                       "signups": 12, "first_visits": 8,
                       "first_activations": 2, "first_sales": 1 }],
  "monthly_detail": [{ "month": "2025-12-01", "studio": "...", "source": "...",
                       "signups": 340, "first_visits": 240,
                       "first_activations": 51, "first_sales": 28 }]
}
```

Daily: previous quarter start → today (by day, per studio+source)
Monthly: 3 years of history (by month, per studio+source)

## Shared functions (available from utils.js)

**Data**
- `filterRows(arr, fromDate, toDate, studios, sources)` — filter any row array
- `sumRows(rows)` → `{leads, ft, mem}` — sum signups/first_visits/first_activations
- `getRowDate(r)` — returns Date from r.date or r.month
- `toTimeSeries(dailyRows, monthlyRows)` — collapse to time series by GRAN
- `toSourceTimeSeries(dailyRows, monthlyRows, gran)` — time series with source dimension
- `computeWindows(from, to)` → `{ppFrom, ppTo, pyFrom, pyTo}` — prev period + prev year
- `getMock()` — deterministic mock data for preview/testing

**Charts**
- `buildAreaChart(canvasId, togglesId, series, srcList, valueKey)` — stacked area
- `buildRingChart(canvasId, legendId, labels, values)` — donut ring
- `buildStudioRankTable(tableId, rows, valueKey)` — ranked table with bar

**UI components**
- `kpiCard(label, val, pp, py, fmt)` — metric card with prev period/year deltas
- `cprKpiCard(label, val, pp, py, fmt)` — same but inverted colors (decrease=good)
- `buildMultiSelect(menuId, labelId, items, defaultExcluded)` — checkbox dropdown
- `getSelected(menuId)` — returns array of checked values
- `srcColor(src, i)` — consistent color for a source name

**Date utilities**
- `fmtDate(d)` — "Jan 2026"
- `fmtDayLabel(iso)` — "Jan 15" (UTC-safe)
- `fmtMonthLabel(iso)` — "Jan '26" (UTC-safe)
- `localDateStr(d)` — "2026-01-15"
- `getQuarterBounds()` → `{dailyFrom, dailyTo}`

**Granularity**
- `setGran(gran)` — set global granularity (daily/weekly/monthly), calls applyFilters
- `updateGranButtons(from, to)` — enable/disable gran buttons based on date range

## Brand guidelines

**Colors**
- Primary blue: `#00A3E0`
- Deep blue: `#0084B5` (nav background)
- Cyan: `#00FFC2` (digital only)
- Yellow: `#DDFF00`
- Light blue: `#99DAF3`
- Pale blue: `#D2EFFF`

**Typography**
- Titles/labels: Oswald, italic, ALL CAPS
- Body/numbers: Open Sans

**CSS classes (from style.css)**
- `.kpi-card` / `.kpi-card-label` / `.kpi-card-val` / `.kpi-card-delta.pos` / `.kpi-card-delta.neg`
- `.chart-card` / `.chart-card-title`
- `.data-table` / `.data-table th.num` / `.data-table td.num`
- `.kpi-grid` — 4-col grid for KPI cards
- `.kpi-grid.three` — 3-col variant
- `.two-col` — 2-col grid
- `.toggle-group` / `.tog-btn` — series toggle buttons
- `.gran-group` / `.gran-btn` — granularity toggle buttons
- `.multi-select` / `.ms-trigger` / `.ms-menu` / `.ms-item` — checkbox dropdown
- `.delta.pos` / `.delta.neg` / `.delta.neu` — inline delta indicators
- `.funnel-wrap` / `.funnel-row` / `.funnel-bar-track` — funnel chart components

## Excluded studios (default)
These are excluded by default in all dashboards (override if needed):
- SWEAT440 Dallas - Prestonwood
- SWEAT440 Herriman
- SWEAT440 Naples - Mercato
- SWEAT440 Nashville - Capitol View
- SWEAT440 Pinecrest - Palmetto Bay
- SWEAT440 Reston

## Excluded sources (default)
- ClassPass / Platforms
- Grassroots

## Git workflow
1. Clone: `git clone https://github.com/dev-leadteam/dashboards.git`
2. Create a branch: `git checkout -b your-dashboard-name`
2. Build your `.html` file
3. Test locally with `python3 -m http.server 8080`
4. Push and create a PR for review before merging to main
