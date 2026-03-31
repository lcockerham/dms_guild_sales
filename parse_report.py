"""
parse_report.py

Parses the royalty report HTML table (copied from fetch_dmsguild_royalties.py)
into a CSV file. Called after Claude fetches the raw HTML via Playwright.

Usage:
    from parse_report import process_sales_table, save_report
    df = process_sales_table(table_html, month=2, year=2026)
    save_report(df)
"""
import calendar
import os
from datetime import date

import pandas as pd
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta


def get_last_month() -> tuple[int, int]:
    """Return (month, year) for last month."""
    first_of_this_month = date.today().replace(day=1)
    last_month = first_of_this_month - relativedelta(days=1)
    return last_month.month, last_month.year


def process_sales_table(html: str, month: int, year: int) -> pd.DataFrame:
    """Parse the royalty report HTML table into a DataFrame."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")

    data_rows = []
    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) != 7:
            continue
        try:
            data_rows.append({
                "Publisher":    cols[0].text.strip(),
                "Title":        cols[1].text.strip(),
                "SKU":          cols[2].text.strip(),
                "Units_Sold":   int(cols[3].text.strip() or "0"),
                "Net":          float(cols[4].text.strip().replace("$", "").replace(",", "") or "0"),
                "Royalty_Rate": float(cols[5].text.strip().replace("%", "") or "0"),
                "Royalties":    float(cols[6].text.strip().replace("$", "").replace(",", "") or "0"),
            })
        except (IndexError, ValueError) as exc:
            print(f"Skipping row: {exc}")

    if not data_rows:
        raise ValueError("No data rows found in table HTML")

    df = pd.DataFrame(data_rows)
    df["Month"] = calendar.month_name[month]
    df["Year"] = year
    return df[["Month", "Year", "Publisher", "Title", "SKU", "Units_Sold", "Net", "Royalty_Rate", "Royalties"]]


def save_report(df: pd.DataFrame, output_dir: str = "reports") -> str:
    """Save DataFrame to reports/dmsguild_report_YYYYMM.csv."""
    os.makedirs(output_dir, exist_ok=True)
    month_num = list(calendar.month_name).index(df["Month"].iloc[0])
    year = df["Year"].iloc[0]
    filepath = os.path.join(output_dir, f"dmsguild_report_{year}{month_num:02d}.csv")
    df.to_csv(filepath, index=False)
    print(f"Saved {len(df)} rows → {filepath}")
    return filepath


def load_all_reports(reports_dir: str = "reports") -> pd.DataFrame:
    """Load and concatenate all monthly CSV files."""
    files = sorted(f for f in os.listdir(reports_dir) if f.startswith("dmsguild_report_") and f.endswith(".csv"))
    if not files:
        raise FileNotFoundError(f"No report CSVs found in {reports_dir}/")
    frames = [pd.read_csv(os.path.join(reports_dir, f)) for f in files]
    df = pd.concat(frames, ignore_index=True)
    month_order = list(calendar.month_name)[1:]
    df["Month"] = pd.Categorical(df["Month"], categories=month_order, ordered=True)
    df["Period"] = pd.to_datetime(df["Year"].astype(str) + "-" + df["Month"].cat.codes.add(1).astype(str).str.zfill(2))
    return df.sort_values("Period")
