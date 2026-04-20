# SWEAT440 Leads Dashboard

Live dashboard pulling data from Snowflake → GitHub Pages, auto-refreshed daily.

## Files

| File | Purpose |
|------|---------|
| `index.html` | The dashboard (served via GitHub Pages) |
| `fetch_data.py` | Pulls data from Snowflake, writes `data.json` |
| `data.json` | Generated data file (committed by Actions) |
| `.github/workflows/refresh.yml` | Scheduled GitHub Actions workflow |

## Setup Instructions

### 1. Create GitHub repo & enable Pages
- Create a new GitHub repo (e.g. `sweat440-leads-dashboard`)
- Go to **Settings → Pages → Source**: set to `Deploy from branch: main / root`

### 2. Add GitHub Secrets
Go to **Settings → Secrets and variables → Actions → New repository secret** and add:

| Secret name | Value |
|-------------|-------|
| `SF_ACCOUNT` | `MINDBODYORG-PLAYLIST_DATA_MART_SWEAT440` |
| `SF_USER` | `SWEAT440` |
| `SF_PASSWORD` | *(your Snowflake password)* |
| `SF_ROLE` | `SYSADMIN` |
| `SF_WAREHOUSE` | `COMPUTE_WH` |
| `SF_DATABASE` | `MARKETING_REPORTS` |
| `SF_SCHEMA` | `PUBLIC` |

### 3. Push all files
```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_ORG/sweat440-leads-dashboard.git
git push -u origin main
```

### 4. Run first data fetch manually
```bash
pip install snowflake-connector-python
python fetch_data.py
git add data.json
git commit -m "Initial data"
git push
```

### 5. Trigger workflow manually (optional)
Go to **Actions → Refresh Leads Data → Run workflow** to test the automation.

The dashboard will be live at:
`https://YOUR_ORG.github.io/sweat440-leads-dashboard/`
