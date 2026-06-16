"""
generate_dashboard.py

Reads all monthly CSV files from reports/ and writes a self-contained
dashboard.html with interactive Plotly.js charts.

Usage:
    python generate_dashboard.py
    # Then open dashboard.html in any browser.
"""
import json
import os
import webbrowser
from datetime import date

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from parse_report import load_all_reports

OUTPUT_FILE = "dashboard.html"


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------

def build_monthly_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Total royalties and units per month."""
    return (
        df.groupby("Period", observed=True)
        .agg(Royalties=("Royalties", "sum"), Units_Sold=("Units_Sold", "sum"))
        .reset_index()
        .sort_values("Period")
    )


def build_title_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Total royalties and units per product title, sorted descending."""
    return (
        df.groupby("Title")
        .agg(
            Total_Royalties=("Royalties", "sum"),
            Total_Units=("Units_Sold", "sum"),
            Months_Active=("Period", "nunique"),
        )
        .reset_index()
        .sort_values("Total_Royalties", ascending=False)
    )


def build_title_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Royalties per title per month (for stacked area chart)."""
    return (
        df.pivot_table(index="Period", columns="Title", values="Royalties", aggfunc="sum", observed=True)
        .fillna(0)
        .reset_index()
        .sort_values("Period")
    )


def build_yearly_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Royalties and units per calendar year."""
    return (
        df.groupby("Year")
        .agg(Royalties=("Royalties", "sum"), Units_Sold=("Units_Sold", "sum"))
        .reset_index()
        .sort_values("Year")
    )


def build_forecast(monthly: pd.DataFrame, periods: int = 12) -> dict:
    """
    12-month forward forecast using Holt-Winters exponential smoothing.
    Returns dict with future dates, forecast values, and confidence bounds.
    """
    series = monthly.set_index("Period")["Royalties"]
    series.index = pd.DatetimeIndex(series.index).to_period("M").to_timestamp("M")

    if len(series) < 4:
        return {}

    try:
        model = ExponentialSmoothing(
            series,
            trend="add",
            damped_trend=True,
            seasonal=None,
            initialization_method="estimated",
        ).fit(optimized=True)

        forecast = model.forecast(periods)
        # Simple confidence interval: ±1.5 std of residuals, growing over horizon
        resid_std = model.resid.std()
        horizon = np.arange(1, periods + 1)
        margin = resid_std * np.sqrt(horizon) * 1.5

        last_date = series.index[-1]
        future_dates = [
            (last_date + relativedelta(months=i)).strftime("%Y-%m-%d")
            for i in range(1, periods + 1)
        ]

        return {
            "dates": future_dates,
            "forecast": [round(max(v, 0), 2) for v in forecast.values],
            "upper": [round(max(v + m, 0), 2) for v, m in zip(forecast.values, margin)],
            "lower": [round(max(v - m, 0), 2) for v, m in zip(forecast.values, margin)],
        }
    except Exception as exc:
        print(f"Forecast warning: {exc}")
        return {}


# ---------------------------------------------------------------------------
# JSON serialisation helpers
# ---------------------------------------------------------------------------

def to_json(obj) -> str:
    """Convert pandas/numpy objects to JSON-serialisable form."""
    if isinstance(obj, pd.DataFrame):
        obj = obj.copy()
        for col in obj.select_dtypes(include=["datetime64[ns]", "period[M]"]).columns:
            obj[col] = obj[col].astype(str)
        return json.dumps(obj.to_dict(orient="list"))
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            if isinstance(v, list):
                cleaned[k] = [
                    x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else x for x in v
                ]
            else:
                cleaned[k] = v
        return json.dumps(cleaned)
    return json.dumps(obj)


def period_labels(periods) -> list[str]:
    """Convert Period series to 'Mon YYYY' strings."""
    return [p.strftime("%b %Y") for p in periods]


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

def generate_html(df: pd.DataFrame) -> str:
    monthly = build_monthly_totals(df)
    titles = build_title_totals(df)
    title_monthly = build_title_monthly(df)
    yearly = build_yearly_totals(df)
    forecast = build_forecast(monthly)

    # Scalars for summary cards
    total_royalties = df["Royalties"].sum()
    total_units = df["Units_Sold"].sum()
    num_titles = df["Title"].nunique()
    num_months = monthly.shape[0]
    avg_monthly = total_royalties / num_months if num_months else 0
    best_month_row = monthly.loc[monthly["Royalties"].idxmax()]
    best_month_label = best_month_row["Period"].strftime("%b %Y")
    best_month_val = best_month_row["Royalties"]

    # Periods as labels
    month_labels = period_labels(monthly["Period"])
    forecast_labels = forecast.get("dates", [])
    forecast_labels_fmt = [
        date.fromisoformat(d).strftime("%b %Y") for d in forecast_labels
    ] if forecast_labels else []

    # Top-10 titles for bar chart
    top10 = titles.head(10)

    # Per-title avg monthly revenue
    titles_with_avg = titles.copy()
    titles_with_avg["Avg_Monthly"] = (
        titles_with_avg["Total_Royalties"] / titles_with_avg["Months_Active"]
    ).round(2)

    # Stacked area: top 5 titles + "Other"
    top5_titles = titles.head(5)["Title"].tolist()
    tm = title_monthly.copy()
    other_cols = [c for c in tm.columns if c != "Period" and c not in top5_titles]
    if other_cols:
        tm["Other"] = tm[other_cols].sum(axis=1)
    area_titles = top5_titles + (["Other"] if other_cols else [])
    area_data = {t: tm[t].round(2).tolist() if t in tm.columns else [] for t in area_titles}

    # Colours
    COLORS = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948", "#b07aa1"]

    plotly_cdn = "https://cdn.plot.ly/plotly-2.32.0.min.js"

    def js_array(lst) -> str:
        return json.dumps(lst)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>DMs Guild Sales Dashboard</title>
  <script src="{plotly_cdn}"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f5f5f5;
      color: #111;
      margin: 0;
      padding: 24px;
      font-size: 16px;
    }}
    h1 {{
      color: #111;
      text-align: center;
      font-size: 2.2rem;
      margin: 0 0 6px;
    }}
    .subtitle {{
      text-align: center;
      color: #444;
      font-size: 1rem;
      margin-bottom: 28px;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 16px;
      margin-bottom: 28px;
    }}
    .card {{
      background: #fff;
      border: 1px solid #ddd;
      border-radius: 8px;
      padding: 18px 20px;
    }}
    .card-label {{
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: .06em;
      color: #555;
      margin-bottom: 6px;
    }}
    .card-value {{
      font-size: 1.8rem;
      font-weight: 700;
      color: #111;
    }}
    .card-sub {{
      font-size: 0.85rem;
      color: #555;
      margin-top: 4px;
    }}
    .charts {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 24px;
    }}
    .chart-box {{
      background: #fff;
      border: 1px solid #ddd;
      border-radius: 8px;
      padding: 20px;
    }}
    .chart-title {{
      font-size: 1.1rem;
      font-weight: 600;
      color: #111;
      margin-bottom: 12px;
    }}
    @media (min-width: 900px) {{
      .charts {{ grid-template-columns: 1fr 1fr; }}
      .chart-box.wide {{ grid-column: 1 / -1; }}
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.95rem;
    }}
    th {{
      text-align: left;
      color: #111;
      padding: 8px 10px;
      border-bottom: 2px solid #ddd;
    }}
    td {{
      padding: 7px 10px;
      border-bottom: 1px solid #eee;
      color: #111;
    }}
    tr:last-child td {{ border-bottom: none; }}
    .num {{ text-align: right; }}
    .generated {{
      text-align: center;
      font-size: 0.82rem;
      color: #777;
      margin-top: 28px;
    }}
  </style>
</head>
<body>
  <h1>DMs Guild Sales Dashboard</h1>
  <p class="subtitle">Generated {date.today().strftime("%B %d, %Y")} &mdash; {num_months} months of data</p>

  <!-- Summary cards -->
  <div class="cards">
    <div class="card">
      <div class="card-label">Total Royalties</div>
      <div class="card-value">${total_royalties:,.2f}</div>
      <div class="card-sub">across {num_months} months</div>
    </div>
    <div class="card">
      <div class="card-label">Avg / Month</div>
      <div class="card-value">${avg_monthly:,.2f}</div>
    </div>
    <div class="card">
      <div class="card-label">Best Month</div>
      <div class="card-value">${best_month_val:,.2f}</div>
      <div class="card-sub">{best_month_label}</div>
    </div>
    <div class="card">
      <div class="card-label">Total Units Sold</div>
      <div class="card-value">{total_units:,}</div>
    </div>
    <div class="card">
      <div class="card-label">Active Titles</div>
      <div class="card-value">{num_titles}</div>
    </div>
  </div>

  <!-- Charts -->
  <div class="charts">

    <!-- Monthly royalties trend + forecast -->
    <div class="chart-box wide">
      <div class="chart-title">Monthly Royalties &amp; 12-Month Forecast</div>
      <div id="chart-trend"></div>
    </div>

    <!-- Stacked area by title -->
    <div class="chart-box wide">
      <div class="chart-title">Revenue by Title Over Time</div>
      <div id="chart-stacked"></div>
    </div>

    <!-- Top 10 titles -->
    <div class="chart-box">
      <div class="chart-title">Top Products — Total Royalties</div>
      <div id="chart-top10"></div>
    </div>

    <!-- Year-over-year -->
    <div class="chart-box">
      <div class="chart-title">Year-over-Year Revenue</div>
      <div id="chart-yoy"></div>
    </div>

    <!-- Per-title table -->
    <div class="chart-box wide">
      <div class="chart-title">All Titles — Performance Summary</div>
      <table>
        <thead>
          <tr>
            <th>Title</th>
            <th class="num">Total $</th>
            <th class="num">Units</th>
            <th class="num">Months</th>
            <th class="num">Avg $/Mo</th>
          </tr>
        </thead>
        <tbody>
"""

    for _, row in titles_with_avg.iterrows():
        html += f"""          <tr>
            <td>{row['Title']}</td>
            <td class="num">${row['Total_Royalties']:,.2f}</td>
            <td class="num">{int(row['Total_Units'])}</td>
            <td class="num">{int(row['Months_Active'])}</td>
            <td class="num">${row['Avg_Monthly']:,.2f}</td>
          </tr>
"""

    html += f"""        </tbody>
      </table>
    </div>

  </div>
  <p class="generated">Generated by generate_dashboard.py</p>

<script>
const COLORS = {js_array(COLORS)};
const layout_base = {{
  paper_bgcolor: "transparent",
  plot_bgcolor: "#fff",
  font: {{ color: "#111", size: 13 }},
  margin: {{ t: 10, r: 10, b: 60, l: 70 }},
  xaxis: {{ gridcolor: "#e0e0e0", linecolor: "#ccc", tickfont: {{ size: 13 }} }},
  yaxis: {{ gridcolor: "#e0e0e0", linecolor: "#ccc", tickprefix: "$", tickfont: {{ size: 13 }} }},
  showlegend: false,
  hovermode: "x unified",
}};

// ── Monthly trend + forecast ─────────────────────────────────────────────
(function() {{
  const labels = {js_array(month_labels)};
  const royalties = {js_array(monthly['Royalties'].round(2).tolist())};
  const traces = [{{
    x: labels,
    y: royalties,
    type: "scatter",
    mode: "lines+markers",
    name: "Actual",
    line: {{ color: COLORS[0], width: 2 }},
    marker: {{ size: 5 }},
    hovertemplate: "%{{x}}<br>$%{{y:.2f}}<extra></extra>",
  }}];
"""

    if forecast:
        html += f"""
  const fLabels = {js_array(forecast_labels_fmt)};
  const fVals   = {js_array(forecast['forecast'])};
  const fUpper  = {js_array(forecast['upper'])};
  const fLower  = {js_array(forecast['lower'])};
  traces.push({{
    x: [...[labels[labels.length-1]], ...fLabels],
    y: [...[royalties[royalties.length-1]], ...fVals],
    type: "scatter", mode: "lines",
    name: "Forecast",
    line: {{ color: COLORS[1], width: 2, dash: "dot" }},
    hovertemplate: "%{{x}}<br>Forecast: $%{{y:.2f}}<extra></extra>",
  }});
  traces.push({{
    x: [...fLabels, ...[...fLabels].reverse()],
    y: [...fUpper, ...[...fLower].reverse()],
    type: "scatter", fill: "toself", fillcolor: "rgba(242,142,43,0.12)",
    line: {{ color: "transparent" }}, name: "95% CI",
    hoverinfo: "skip",
  }});
"""

    html += f"""
  Plotly.newPlot("chart-trend", traces,
    {{...layout_base, height: 420, showlegend: true,
      legend: {{ orientation: "h", y: -0.18, font: {{ size: 13 }} }} }},
    {{responsive: true}});
}})();

// ── Stacked area by title ────────────────────────────────────────────────
(function() {{
  const labels = {js_array(month_labels)};
  const areaData = {json.dumps(area_data)};
  const areaTitles = {js_array(area_titles)};
  const traces = areaTitles.map((t, i) => ({{
    x: labels, y: areaData[t],
    type: "scatter", mode: "lines", stackgroup: "one",
    name: t.length > 35 ? t.slice(0, 33) + "…" : t,
    line: {{ color: COLORS[i % COLORS.length], width: 1 }},
    fillcolor: COLORS[i % COLORS.length] + "99",
    hovertemplate: "%{{x}}<br>" + t + ": $%{{y:.2f}}<extra></extra>",
  }}));
  Plotly.newPlot("chart-stacked", traces,
    {{...layout_base, height: 420, showlegend: true,
      legend: {{ orientation: "h", y: -0.22, font: {{ size: 12 }} }} }},
    {{responsive: true}});
}})();

// ── Top 10 titles ────────────────────────────────────────────────────────
(function() {{
  const names = {js_array(top10['Title'].tolist())}.map(t => t.length > 30 ? t.slice(0,28)+"…" : t);
  const vals  = {js_array(top10['Total_Royalties'].round(2).tolist())};
  Plotly.newPlot("chart-top10",
    [{{ x: vals, y: names, type: "bar", orientation: "h",
       marker: {{ color: COLORS[0] }},
       hovertemplate: "%{{y}}<br>$%{{x:.2f}}<extra></extra>" }}],
    {{...layout_base,
      height: 420,
      xaxis: {{...layout_base.xaxis, tickprefix: "$"}},
      yaxis: {{...layout_base.yaxis, tickprefix: "", automargin: true, tickfont: {{ size: 13 }} }},
      margin: {{t:10, r:10, b:50, l:240}} }},
    {{responsive: true}});
}})();

// ── Year-over-year ───────────────────────────────────────────────────────
(function() {{
  const years = {js_array([str(y) for y in yearly['Year'].tolist()])};
  const vals  = {js_array(yearly['Royalties'].round(2).tolist())};
  Plotly.newPlot("chart-yoy",
    [{{ x: years, y: vals, type: "bar",
       marker: {{ color: vals.map((v, i) => COLORS[i % COLORS.length]) }},
       hovertemplate: "%{{x}}<br>$%{{y:.2f}}<extra></extra>" }}],
    {{...layout_base, height: 420, xaxis: {{...layout_base.xaxis, tickprefix: ""}} }},
    {{responsive: true}});
}})();
</script>
</body>
</html>"""

    return html


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Loading reports...")
    df = load_all_reports()
    print(f"  {len(df)} rows across {df['Period'].nunique()} months, {df['Title'].nunique()} titles")

    print("Generating dashboard...")
    html = generate_html(df)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Saved -> {OUTPUT_FILE}")

    abs_path = os.path.abspath(OUTPUT_FILE)
    webbrowser.open(f"file:///{abs_path}")
    print("Opened in browser.")
