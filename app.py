from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

from get_vendor_summary import DATABASE_PATH, SUMMARY_TABLE_NAME, run_vendor_summary_pipeline
from ingestion_db import DEFAULT_DATA_DIR, load_raw_data


REQUIRED_RAW_TABLES = {"purchases", "purchase_prices", "sales", "vendor_invoice"}

ACCENT = "#111827"
MUTED = "#6B7280"
SURFACE = "#F8FAFC"

st.set_page_config(
    page_title="Vendor Performance Dashboard",
    page_icon="📊",
    layout="wide",
)

st.markdown(
    f"""
    <style>
        .stApp {{
            background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
            color: {ACCENT};
        }}
        .block-container {{
            padding-top: 2rem;
            padding-bottom: 2rem;
        }}
        .hero {{
            background: white;
            border: 1px solid #e5e7eb;
            border-radius: 24px;
            padding: 1.5rem 1.75rem;
            box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05);
            margin-bottom: 1rem;
        }}
        .eyebrow {{
            color: {MUTED};
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-size: 0.8rem;
            margin-bottom: 0.5rem;
        }}
        .hero h1 {{
            font-size: 2.2rem;
            margin: 0;
            color: {ACCENT};
        }}
        .hero p {{
            color: {MUTED};
            margin-top: 0.65rem;
            margin-bottom: 0;
            max-width: 60rem;
        }}
        .metric-card {{
            background: white;
            border: 1px solid #e5e7eb;
            border-radius: 22px;
            padding: 1.1rem 1.25rem;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.04);
        }}
        .metric-label {{
            color: {MUTED};
            font-size: 0.85rem;
            margin-bottom: 0.35rem;
        }}
        .metric-value {{
            color: {ACCENT};
            font-size: 1.75rem;
            font-weight: 700;
            line-height: 1.1;
        }}
        .section-title {{
            font-size: 1.1rem;
            font-weight: 600;
            color: {ACCENT};
            margin-bottom: 0.5rem;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)


def format_currency(value: float) -> str:
    return f"${value:,.0f}"


def format_percent(value: float) -> str:
    return f"{value:.1f}%"


@st.cache_data(show_spinner=False)
def load_summary_data(database_path: str) -> pd.DataFrame:
    db_path = Path(database_path)
    if not db_path.exists():
        return pd.DataFrame()

    with sqlite3.connect(db_path) as conn:
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            conn,
            params=(SUMMARY_TABLE_NAME,),
        )
        if tables.empty:
            return pd.DataFrame()
        return pd.read_sql_query(f"SELECT * FROM {SUMMARY_TABLE_NAME}", conn)


def get_missing_raw_tables(database_path: str) -> set[str]:
    db_path = Path(database_path)
    if not db_path.exists():
        return set(REQUIRED_RAW_TABLES)

    with sqlite3.connect(db_path) as conn:
        table_df = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
    available_tables = set(table_df["name"].tolist())
    return REQUIRED_RAW_TABLES.difference(available_tables)


@st.cache_data(show_spinner=False)
def compute_vendor_rollup(data: pd.DataFrame) -> pd.DataFrame:
    vendor_view = (
        data.groupby(["VendorNumber", "VendorName"], as_index=False)
        .agg(
            TotalSalesDollars=("TotalSalesDollars", "sum"),
            TotalPurchaseDollars=("TotalPurchaseDollars", "sum"),
            TotalSalesQuantity=("TotalSalesQuantity", "sum"),
            TotalPurchaseQuantity=("TotalPurchaseQuantity", "sum"),
            FreightCost=("FreightCost", "max"),
            GrossProfit=("GrossProfit", "sum"),
        )
        .sort_values("TotalSalesDollars", ascending=False)
    )
    # Plotly marker sizes must be non-negative; use magnitude for bubble size.
    vendor_view["GrossProfitMagnitude"] = vendor_view["GrossProfit"].abs().clip(lower=1)
    vendor_view["ProfitMargin"] = (
        vendor_view["GrossProfit"]
        .div(vendor_view["TotalSalesDollars"].where(vendor_view["TotalSalesDollars"].ne(0)))
        .mul(100)
        .fillna(0)
    )
    vendor_view["StockTurnover"] = (
        vendor_view["TotalSalesQuantity"]
        .div(vendor_view["TotalPurchaseQuantity"].where(vendor_view["TotalPurchaseQuantity"].ne(0)))
        .fillna(0)
    )
    return vendor_view


@st.cache_data(show_spinner=False)
def compute_brand_rollup(data: pd.DataFrame) -> pd.DataFrame:
    brand_view = (
        data.groupby("Brand", as_index=False)
        .agg(
            TotalSalesDollars=("TotalSalesDollars", "sum"),
            TotalPurchaseDollars=("TotalPurchaseDollars", "sum"),
            GrossProfit=("GrossProfit", "sum"),
            AvgMargin=("ProfitMargin", "mean"),
        )
        .sort_values("TotalSalesDollars", ascending=False)
    )
    return brand_view


def metric_card(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div class=\"metric-card\">
            <div class=\"metric-label\">{label}</div>
            <div class=\"metric-value\">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


st.markdown(
    """
    <section class="hero">
        <div class="eyebrow">Vendor intelligence</div>
        <h1>Vendor Performance Dashboard</h1>
        <p>
            A calm, minimalist workspace for tracking spend, sales, profitability, and stock efficiency
            across vendors and brands from the curated <code>vendor_sales_summary</code> table.
        </p>
    </section>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("### Data source")
    database_path = st.text_input("SQLite database", value=str(DATABASE_PATH))
    reload_requested = st.button("Refresh summary table", width="stretch")
    if reload_requested:
        try:
            missing_tables = get_missing_raw_tables(database_path)
            if missing_tables:
                with st.spinner("Raw tables missing. Ingesting CSV files into database..."):
                    db_path = Path(database_path)
                    engine = create_engine(f"sqlite:///{db_path.as_posix()}")
                    load_raw_data(data_folder=DEFAULT_DATA_DIR, engine=engine)

                remaining_missing = get_missing_raw_tables(database_path)
                if remaining_missing:
                    missing_list = ", ".join(sorted(remaining_missing))
                    raise RuntimeError(
                        f"Missing required raw tables after ingestion: {missing_list}. "
                        "Check that matching CSV files exist in the data directory."
                    )

            with st.spinner("Rebuilding vendor summary table..."):
                run_vendor_summary_pipeline(Path(database_path))
            load_summary_data.clear()
            st.success("Summary table refreshed.")
        except Exception as exc:  # noqa: BLE001
            st.error(f"Could not refresh data: {exc}")

summary_df = load_summary_data(database_path)

if summary_df.empty:
    st.warning(
        "No `vendor_sales_summary` table was found yet. Run `python get_vendor_summary.py` or use the sidebar refresh button after loading the raw tables."
    )
    st.stop()

vendor_options = sorted(summary_df["VendorName"].dropna().unique().tolist())
brand_options = sorted(summary_df["Brand"].dropna().unique().tolist())

with st.sidebar:
    st.markdown("### Filters")
    selected_vendors = st.multiselect("Vendor", options=vendor_options)
    selected_brands = st.multiselect("Brand", options=brand_options)
    min_margin = st.slider("Minimum margin (%)", min_value=-100, max_value=100, value=-100)

filtered_df = summary_df.copy()
if selected_vendors:
    filtered_df = filtered_df[filtered_df["VendorName"].isin(selected_vendors)]
if selected_brands:
    filtered_df = filtered_df[filtered_df["Brand"].isin(selected_brands)]
filtered_df = filtered_df[filtered_df["ProfitMargin"] >= min_margin]

if filtered_df.empty:
    st.info("No data matched the current filters. Try widening the selection.")
    st.stop()

vendor_rollup = compute_vendor_rollup(filtered_df)
brand_rollup = compute_brand_rollup(filtered_df)

sales_total = filtered_df["TotalSalesDollars"].sum()
gross_profit_total = filtered_df["GrossProfit"].sum()
purchase_total = filtered_df["TotalPurchaseDollars"].sum()
avg_margin = filtered_df["ProfitMargin"].mean()

metric_columns = st.columns(4)
with metric_columns[0]:
    metric_card("Total sales", format_currency(sales_total))
with metric_columns[1]:
    metric_card("Gross profit", format_currency(gross_profit_total))
with metric_columns[2]:
    metric_card("Purchase spend", format_currency(purchase_total))
with metric_columns[3]:
    metric_card("Average margin", format_percent(avg_margin))

st.write("")
overview_tab, explorer_tab, table_tab = st.tabs(["Overview", "Vendor explorer", "Data table"])

with overview_tab:
    col_left, col_right = st.columns((1.15, 1), gap="large")

    with col_left:
        st.markdown('<div class="section-title">Top vendors by sales</div>', unsafe_allow_html=True)
        top_vendor_chart = px.bar(
            vendor_rollup.head(12),
            x="TotalSalesDollars",
            y="VendorName",
            orientation="h",
            color="ProfitMargin",
            color_continuous_scale=["#cbd5e1", "#0f172a"],
            labels={
                "TotalSalesDollars": "Sales ($)",
                "VendorName": "Vendor",
                "ProfitMargin": "Margin %",
            },
        )
        top_vendor_chart.update_layout(
            height=480,
            margin=dict(l=0, r=0, t=10, b=0),
            yaxis=dict(categoryorder="total ascending"),
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        st.plotly_chart(top_vendor_chart, width="stretch")

    with col_right:
        st.markdown('<div class="section-title">Portfolio mix</div>', unsafe_allow_html=True)
        mix_chart = px.treemap(
            brand_rollup.head(15),
            path=[px.Constant("Brands"), "Brand"],
            values="TotalSalesDollars",
            color="GrossProfit",
            color_continuous_scale=["#e2e8f0", "#0f172a"],
        )
        mix_chart.update_layout(
            height=480,
            margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="white",
        )
        st.plotly_chart(mix_chart, width="stretch")

    insight_col1, insight_col2 = st.columns(2, gap="large")

    with insight_col1:
        st.markdown('<div class="section-title">Profitability vs scale</div>', unsafe_allow_html=True)
        scatter_chart = px.scatter(
            vendor_rollup,
            x="TotalSalesDollars",
            y="ProfitMargin",
            size="GrossProfitMagnitude",
            color="StockTurnover",
            hover_name="VendorName",
            color_continuous_scale=["#cbd5e1", "#0f172a"],
            labels={
                "TotalSalesDollars": "Sales ($)",
                "ProfitMargin": "Margin %",
                "StockTurnover": "Stock turnover",
                "GrossProfitMagnitude": "|Gross profit|",
            },
        )
        scatter_chart.update_layout(
            height=420,
            margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        st.plotly_chart(scatter_chart, width="stretch")

    with insight_col2:
        st.markdown('<div class="section-title">Highest gross profit brands</div>', unsafe_allow_html=True)
        brand_profit_chart = px.bar(
            brand_rollup.nlargest(10, "GrossProfit"),
            x="Brand",
            y="GrossProfit",
            color="AvgMargin",
            color_continuous_scale=["#dbeafe", "#1d4ed8"],
            labels={"GrossProfit": "Gross profit ($)", "AvgMargin": "Avg margin %"},
        )
        brand_profit_chart.update_layout(
            height=420,
            margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="white",
            paper_bgcolor="white",
            xaxis_tickangle=-30,
        )
        st.plotly_chart(brand_profit_chart, width="stretch")

with explorer_tab:
    st.markdown('<div class="section-title">Vendor deep dive</div>', unsafe_allow_html=True)
    selected_vendor = st.selectbox(
        "Choose a vendor",
        options=vendor_rollup["VendorName"].tolist(),
    )
    vendor_slice = filtered_df[filtered_df["VendorName"] == selected_vendor].sort_values(
        "TotalSalesDollars", ascending=False
    )
    vendor_kpis = vendor_rollup[vendor_rollup["VendorName"] == selected_vendor].iloc[0]

    drill_cols = st.columns(4)
    with drill_cols[0]:
        metric_card("Vendor sales", format_currency(float(vendor_kpis["TotalSalesDollars"])))
    with drill_cols[1]:
        metric_card("Vendor gross profit", format_currency(float(vendor_kpis["GrossProfit"])))
    with drill_cols[2]:
        metric_card("Vendor margin", format_percent(float(vendor_kpis["ProfitMargin"])))
    with drill_cols[3]:
        metric_card("Stock turnover", f"{float(vendor_kpis['StockTurnover']):.2f}x")

    vendor_chart = px.bar(
        vendor_slice.head(15),
        x="Brand",
        y=["TotalSalesDollars", "GrossProfit"],
        barmode="group",
        labels={"value": "Dollars ($)", "variable": "Metric"},
        color_discrete_sequence=["#0f172a", "#94a3b8"],
    )
    vendor_chart.update_layout(
        height=420,
        margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis_tickangle=-30,
    )
    st.plotly_chart(vendor_chart, width="stretch")

    st.dataframe(
        vendor_slice[
            [
                "Brand",
                "TotalPurchaseDollars",
                "TotalSalesDollars",
                "GrossProfit",
                "ProfitMargin",
                "StockTurnover",
            ]
        ].style.format(
            {
                "TotalPurchaseDollars": "${:,.0f}",
                "TotalSalesDollars": "${:,.0f}",
                "GrossProfit": "${:,.0f}",
                "ProfitMargin": "{:.1f}%",
                "StockTurnover": "{:.2f}x",
            }
        ),
        width="stretch",
        hide_index=True,
    )

with table_tab:
    st.markdown('<div class="section-title">Filtered vendor-brand dataset</div>', unsafe_allow_html=True)
    st.caption("Use this table for audit-friendly inspection or to export slices into downstream analysis.")
    st.dataframe(
        filtered_df.sort_values("TotalSalesDollars", ascending=False).style.format(
            {
                "PurchasePrice": "${:,.2f}",
                "ActualPrice": "${:,.2f}",
                "TotalPurchaseDollars": "${:,.0f}",
                "TotalSalesDollars": "${:,.0f}",
                "TotalSalesPrice": "${:,.0f}",
                "TotalExciseTax": "${:,.0f}",
                "FreightCost": "${:,.0f}",
                "GrossProfit": "${:,.0f}",
                "ProfitMargin": "{:.1f}%",
                "StockTurnover": "{:.2f}x",
                "SalesToPurchaseRatio": "{:.2f}x",
            }
        ),
        width="stretch",
        hide_index=True,
    )
