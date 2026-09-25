"""
import_history.py

One-time import of historical sales from the old Google Sheet export
(dms_guild_sales_testing.xlsx, Sheet1) into reports/dmsguild_report_YYYYMM.csv.

Months that already have a CSV in reports/ are skipped: those were fetched
directly from DMs Guild and are more complete than the sheet.

Usage:
    python import_history.py [path/to/file.xlsx]
"""
from __future__ import annotations

import calendar
import os
import sys

import pandas as pd

from parse_report import save_report

DEFAULT_XLSX = "dms_guild_sales_testing.xlsx"

COLUMN_MAP = {
    "Units Sold": "Units_Sold",
    "Current Royalty Rate": "Royalty_Rate",
    "Your Royalties": "Royalties",
}


def import_history(xlsx_path: str, reports_dir: str = "reports") -> list[str]:
    """Write one CSV per month from the sheet, skipping months already in reports/."""
    df = pd.read_excel(xlsx_path, sheet_name="Sheet1").rename(columns=COLUMN_MAP)
    # The sheet stores the rate as a fraction (0.5); reports/ uses percent (50.0).
    df["Royalty_Rate"] = df["Royalty_Rate"].where(df["Royalty_Rate"] > 1, df["Royalty_Rate"] * 100)
    df["SKU"] = df["SKU"].fillna("")

    written = []
    for (year, month), month_df in df.groupby(["Year", "Month"], sort=False):
        month_num = list(calendar.month_name).index(month)
        target = os.path.join(reports_dir, f"dmsguild_report_{year}{month_num:02d}.csv")
        if os.path.exists(target):
            print(f"Skipping {month} {year}: {target} already exists")
            continue
        written.append(save_report(month_df[[
            "Month", "Year", "Publisher", "Title", "SKU",
            "Units_Sold", "Net", "Royalty_Rate", "Royalties",
        ]], reports_dir))
    return written


if __name__ == "__main__":
    files = import_history(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_XLSX)
    print(f"Imported {len(files)} months")
