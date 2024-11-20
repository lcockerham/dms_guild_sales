# DMs Guild Royalty Report Fetcher

An automated tool to fetch royalty reports from DMs Guild and sync them to Google Sheets.

## Overview

This script automates the process of:
1. Logging into DMs Guild
2. Retrieving last month's royalty report
3. Saving the data locally as CSV
4. Syncing the data to a specified Google Sheet

## Prerequisites

- Python 3.x
- Chrome browser
- Google Cloud project with Sheets API enabled
- Service account credentials for Google Sheets API

### Required Python Packages

```
selenium
pandas
beautifulsoup4
google-api-python-client
python-dateutil
numpy
```

## Setup

1. Install required packages:
   ```bash
   pip install selenium pandas beautifulsoup4 google-api-python-client python-dateutil numpy
   ```

2. Configure credentials:
   - `credentials.txt` for DMs Guild login will be created on first run
   - Place Google Sheets service account JSON file in project directory

3. Create the following environment variables:
    - Encryption key for credentials file: $env:DMSGUILD_ENCRYPTION_KEY = "my_key_here"
    - Google Sheets API Key: $env:GOOGLE_SHEETS_CREDENTIALS = "my_key_name_here.json"
    - Google Sheets spredsheet ID: env:GOOGLE_SHEETS_SPREADSHEET_ID = "my_spreadsheet_id_here"

## Usage

Run the script:
```bash
python fetch_dmsguild_royalties.py
```

The script will:
- Check for existing report for current month
- If none exists, fetch new data from DMs Guild
- Save report locally in `reports/` directory
- Update configured Google Sheet

## Features

- Secure credential storage with basic encryption
- Automated web navigation and data extraction
- Local CSV backup of reports
- Duplicate prevention in Google Sheets
- Currency formatting in Google Sheets
- Data verification before upload

## Security Notes

- DMs Guild credentials are stored with basic encryption
- Google service account credentials should be kept secure
- Update encryption key in production deployment