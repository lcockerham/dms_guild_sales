# DMs Guild Sales Dashboard

Fetches monthly royalty reports from DMs Guild and generates an interactive sales dashboard.

## Monthly workflow

**Step 1 — Fetch data**

Ask Claude Code to fetch this month's royalty data. It logs into DMs Guild via Playwright, extracts the report table, and saves it to `reports/dmsguild_report_YYYYMM.csv`.

**Step 2 — Regenerate dashboard**

```bash
python generate_dashboard.py
```

Opens `dashboard.html` in your browser with:
- Monthly royalties trend + 12-month forecast
- Revenue by title over time (stacked area)
- Top products by total royalties
- Year-over-year comparison
- Per-title performance table (total $, units, avg $/month, estimated bestseller metal)

It also writes `../dms-guild-publishing/data/title_metals.json` (title → metal, `null` if none) so the publishing project can pick up badge changes.

## Setup

```bash
pip install -r requirements.txt
```

Credentials are stored in `credentials.txt` (gitignored). The encryption key lives in `env_variables.txt` (also gitignored).

## Project structure

```
├── parse_report.py          # HTML → CSV parser (used by Claude during data fetch)
├── generate_dashboard.py    # Reads reports/, writes dashboard.html
├── import_history.py        # One-time import of pre-2025 history from the old Google Sheet (.xlsx)
├── dashboard.html           # Generated output — open in any browser (gitignored)
├── reports/                 # Monthly CSV files (gitignored)
└── DND_Classics/            # Separate analysis of D&D Classics product catalogue
    ├── get_product_info.py
    └── DND_Classics_Analysis.ipynb
```
