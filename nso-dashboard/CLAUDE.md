SWEAT440 Marketing Dashboard — Full Project Summary
What We're Building
A branded marketing performance dashboard for SWEAT440 franchise owners. Replaces Looker Studio with a custom React dashboard, pulling data from 4 APIs into Snowflake, served via Vercel serverless functions, embedded in the WordPress franchise portal.

Architecture (3 Phases)
Phase 1 — DONE (Testing)

Python scripts pull from 4 APIs → write to Google Sheets for visual verification
Test location: Austin Highland / Herriman (Utah)
Files built: fetch_meta_ads.py, fetch_google_ads.py, fetch_ga4.py, fetch_gbp.py, sheets_writer.py, run_all.py, .env.example, franchise_config.json

Phase 2 — IN PROGRESS

Same Python scripts adapted to write to Snowflake instead of Sheets
NSO (New Studio Opening) dashboard with 3 data sources:

data.json — leads/first timers/members from Snowflake (MindBody data, already exists)
nso_marketing_data.json — from Meta, Google Ads, GA4, GBP APIs
nso_scorecard_data.json — from Google Sheets scorecards (goals, targets, opening dates)


Dashboard tabs: Leads, Sales, CPR Fees, Meta Ads, Google Ads, Instagram, GBP
GitHub Actions runs daily at 3am LA time (11:00 UTC) to refresh JSON files

Phase 3 — Production

React dashboard embedded in WordPress via iframe
Calls Vercel serverless function /api/metrics
JWT token validates franchisee → queries Snowflake filtered by franchise_id
Loads 90 days of pre-aggregated data into browser memory; all filtering is client-side JavaScript
No more API calls until user requests dates beyond the cached window


Data Sources & Credentials
Meta Marketing API

One shared corporate ad account: act_1553887681409034
Studios are differentiated by campaign name filters (each studio has its name in campaign names)
Page ID (Herriman): 1016504601542354
Instagram Account ID (Herriman): 17841447639266583
Credentials: META_ACCESS_TOKEN, META_APP_ID, META_APP_SECRET in .env

Google Ads

MCC (LeadTeam): 605-546-2417
Each studio has its own Customer ID (Herriman: 385-801-4125)
Credentials: GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN

Google Analytics 4 (GA4)

Single property 341934364 covers all sweat440.com locations
Filtering is done by studio_page_path (e.g., /gyms/utah-herriman/)
Uses same Google Service Account as GBP/Sheets
Herriman path: /gyms/utah-herriman/
Pinecrest path: /gyms/locations-florida-pinecrest/

Google Business Profile (GBP)

API quota was 0 — quota increase request submitted
Herriman Location ID: 01689314637450990290
GBP Account ID: 4243744174605320602
Fallback: CSV import script for manual GBP exports


5 NSO Studios
StudioStateSnowflake IDGoogle Ads CIDGA4 Page PathHerrimanUT5752080385-801-4125/gyms/utah-herriman/Naples - MercatoFLTBDTBDTBDDallas - PrestonwoodTXTBDTBDTBDPinecrest - Palmetto BayFLTBDTBDTBDRestonVATBDTBDTBD
A config spreadsheet (nso_studio_config.xlsx) was created with all fields that need to be filled in per studio.

Key Files Built So Far
sweat440-test/
├── config/
│   └── franchise_config.json       # Per-studio platform account IDs
├── scripts/
│   ├── fetch_meta_ads.py
│   ├── fetch_google_ads.py
│   ├── fetch_ga4.py                # Filters by studio_page_path
│   ├── fetch_gbp.py
│   ├── sheets_writer.py            # Phase 1 output; swap for snowflake_writer.py in Phase 2
│   └── run_all.py
├── .env.example                    # All credential variable names
├── requirements.txt
└── .github/workflows/daily_sync.yml

Current Status / What's Next

GA4 per-location filtering — Fixed. Uses server-side BEGINS_WITH filter on landingPagePlusQueryString. Confirmed working for Herriman and Pinecrest.
GBP — Quota increase requested, not yet active. CSV fallback available.
NSO dashboard — Being built. Currently working on: marketing API data ingestion for all 5 studios, dashboard UI (Meta Ads sort/filter, Instagram column reorder, larger thumbnails, Cost Per Result time-series chart, date filter in English MM/DD/YYYY format).
Missing: Snowflake IDs and platform account IDs for Naples, Dallas, Pinecrest, Reston (config spreadsheet shared with client to fill in).
Production deploy: Not started. Needs Vercel setup, JWT auth, Snowflake table schema design for marketing data.


Tech Stack

Python 3 — data ingestion scripts
React — dashboard frontend
Snowflake — data warehouse (MindBody + ClassPass data already there; adding marketing data)
Vercel — serverless API functions for production
WordPress — franchise portal (dashboard embedded via iframe)
GitHub Actions — daily scheduled data refresh
Google Sheets — Phase 1 verification only, not production