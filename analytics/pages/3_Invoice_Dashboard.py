"""
Invoice Analytics Dashboard
Source: load_test.invoices (10 000 rows) loaded via ETL Manager (local_dw_con).
Columns: first_name, last_name, email, product_id, qty, amount,
         invoice_date (DD/MM/YYYY text), address, city, stock_code, job
"""
import os

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from cryptography.fernet import Fernet
from sqlalchemy import create_engine, text

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Invoice Dashboard",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Connection helpers (same pattern as other pages) ──────────────────────────
AIRFLOW_DB = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres/airflow",
)
FERNET_KEY = os.getenv("AIRFLOW__CORE__FERNET_KEY", "")
CONN_ID    = "local_dw_con"


def _fernet_decrypt(enc: str) -> str:
    if not FERNET_KEY or not enc:
        return enc or ""
    try:
        f = Fernet(FERNET_KEY.encode())
        if enc.startswith("gAAA"):
            return f.decrypt(enc.encode()).decode()
    except Exception:
        pass
    return enc


@st.cache_data(ttl=300, show_spinner=False)
def _get_uri() -> str | None:
    try:
        engine = create_engine(AIRFLOW_DB)
        with engine.connect() as c:
            row = c.execute(
                text("SELECT host, port, schema, login, password "
                     "FROM connection WHERE conn_id = :id"),
                {"id": CONN_ID},
            ).fetchone()
        if not row:
            return None
        host, port, schema, login, password = row
        pwd = _fernet_decrypt(password or "")
        return f"postgresql+psycopg2://{login}:{pwd}@{host}:{int(port or 5432)}/{schema}"
    except Exception:
        return None


def _get_engine():
    uri = _get_uri()
    if not uri:
        return None
    return create_engine(uri, connect_args={"connect_timeout": 15}, pool_pre_ping=True)


# ── Data loader ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def load_invoices() -> pd.DataFrame:
    engine = _get_engine()
    if engine is None:
        return pd.DataFrame()
    sql = """
        SELECT first_name, last_name, email,
               product_id, qty, amount,
               invoice_date, city, job
        FROM load_test.invoices
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)

    # Parse DD/MM/YYYY text dates; invalid → NaT
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], format="%d/%m/%Y", errors="coerce")
    df["year"]  = df["invoice_date"].dt.year
    df["month"] = df["invoice_date"].dt.to_period("M").astype(str)
    df["revenue"] = df["qty"] * df["amount"]
    return df


# ── Colour palette ─────────────────────────────────────────────────────────────
PALETTE = px.colors.qualitative.Safe

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — filters
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("🧾 Invoice Filters")

with st.sidebar:
    with st.spinner("Loading data…"):
        df_raw = load_invoices()

if df_raw.empty:
    st.error("⚠️ Could not load invoice data. Check the `local_dw_con` Airflow connection.")
    st.stop()

# Year range
min_year = int(df_raw["year"].dropna().min())
max_year = int(df_raw["year"].dropna().max())
year_range = st.sidebar.slider(
    "Invoice Year",
    min_value=min_year, max_value=max_year,
    value=(max(min_year, max_year - 9), max_year),
)

# Top-N cities
top_n_cities = st.sidebar.selectbox("Top N cities in charts", [5, 10, 15, 20], index=1)

# Job filter (multi-select, default = all)
all_jobs = sorted(df_raw["job"].dropna().unique().tolist())
job_filter = st.sidebar.multiselect(
    "Filter by job/profession",
    options=all_jobs,
    default=[],
    placeholder="All professions",
)

# Apply filters
df = df_raw[df_raw["year"].between(year_range[0], year_range[1])]
if job_filter:
    df = df[df["job"].isin(job_filter)]

# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════
st.title("🧾 Invoice Analytics Dashboard")
st.caption(
    f"Showing **{len(df):,}** invoices · "
    f"{year_range[0]}–{year_range[1]}"
    + (f" · {len(job_filter)} professions selected" if job_filter else "")
)

# ══════════════════════════════════════════════════════════════════════════════
# ROW 1 — KPI cards
# ══════════════════════════════════════════════════════════════════════════════
total_revenue  = df["revenue"].sum()
total_invoices = len(df)
avg_order_val  = df["revenue"].mean() if total_invoices else 0
avg_qty        = df["qty"].mean() if total_invoices else 0
top_city       = df.groupby("city")["revenue"].sum().idxmax() if total_invoices else "—"
unique_cities  = df["city"].nunique()

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Revenue",    f"${total_revenue:,.2f}")
k2.metric("Total Invoices",   f"{total_invoices:,}")
k3.metric("Avg Order Value",  f"${avg_order_val:,.2f}")
k4.metric("Avg Qty / Invoice",f"{avg_qty:.1f}")
k5.metric("Cities",           f"{unique_cities:,}")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# ROW 2 — Revenue over time  +  Revenue by year
# ══════════════════════════════════════════════════════════════════════════════
col_left, col_right = st.columns([3, 1])

with col_left:
    st.subheader("Monthly Revenue Trend")
    monthly = (
        df.dropna(subset=["invoice_date"])
        .groupby("month", as_index=False)
        .agg(revenue=("revenue", "sum"), invoices=("revenue", "count"))
        .sort_values("month")
    )
    if not monthly.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(x=monthly["month"], y=monthly["revenue"],
                   name="Revenue ($)", marker_color=PALETTE[0], opacity=0.75),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(x=monthly["month"], y=monthly["invoices"],
                       name="Invoices", mode="lines+markers",
                       line=dict(color=PALETTE[2], width=2)),
            secondary_y=True,
        )
        fig.update_layout(
            height=320, margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            plot_bgcolor="rgba(0,0,0,0)",
        )
        fig.update_yaxes(title_text="Revenue ($)",  secondary_y=False, gridcolor="#eee")
        fig.update_yaxes(title_text="Invoice Count", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for selected filters.")

with col_right:
    st.subheader("Revenue by Year")
    yearly = (
        df.dropna(subset=["year"])
        .groupby("year", as_index=False)["revenue"].sum()
        .sort_values("year")
    )
    if not yearly.empty:
        fig2 = px.bar(
            yearly, x="year", y="revenue",
            color="revenue", color_continuous_scale="Blues",
            labels={"revenue": "Revenue ($)", "year": "Year"},
            height=320,
        )
        fig2.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            coloraxis_showscale=False,
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No data.")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# ROW 3 — Top cities  +  Revenue distribution
# ══════════════════════════════════════════════════════════════════════════════
col3, col4 = st.columns(2)

with col3:
    st.subheader(f"Top {top_n_cities} Cities by Revenue")
    city_rev = (
        df.groupby("city", as_index=False)["revenue"].sum()
        .sort_values("revenue", ascending=False)
        .head(top_n_cities)
    )
    fig3 = px.bar(
        city_rev.sort_values("revenue"), x="revenue", y="city",
        orientation="h", color="revenue",
        color_continuous_scale="Teal",
        labels={"revenue": "Revenue ($)", "city": "City"},
        height=360,
    )
    fig3.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        coloraxis_showscale=False,
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(tickfont=dict(size=11)),
    )
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("Invoice Amount Distribution")
    fig4 = px.histogram(
        df, x="amount", nbins=60,
        color_discrete_sequence=[PALETTE[1]],
        labels={"amount": "Unit Amount ($)", "count": "Invoices"},
        height=360,
    )
    fig4.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="rgba(0,0,0,0)",
        bargap=0.05,
    )
    # Add median line
    median_amount = df["amount"].median()
    fig4.add_vline(
        x=median_amount, line_dash="dash", line_color="#e45",
        annotation_text=f"Median ${median_amount:.2f}",
        annotation_position="top right",
    )
    st.plotly_chart(fig4, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# ROW 4 — Top products  +  Qty vs Revenue scatter
# ══════════════════════════════════════════════════════════════════════════════
col5, col6 = st.columns(2)

with col5:
    st.subheader("Top 10 Products by Revenue")
    prod_rev = (
        df.groupby("product_id", as_index=False)
        .agg(revenue=("revenue", "sum"), invoices=("revenue", "count"), total_qty=("qty", "sum"))
        .sort_values("revenue", ascending=False)
        .head(10)
    )
    prod_rev["product_id"] = "P-" + prod_rev["product_id"].astype(str)
    fig5 = px.bar(
        prod_rev.sort_values("revenue"), x="revenue", y="product_id",
        orientation="h", color="total_qty",
        color_continuous_scale="Oranges",
        labels={"revenue": "Revenue ($)", "product_id": "Product", "total_qty": "Total Qty"},
        height=360,
    )
    fig5.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig5, use_container_width=True)

with col6:
    st.subheader("Qty vs Unit Amount")
    sample = df.sample(min(2000, len(df)), random_state=42) if len(df) > 2000 else df
    fig6 = px.scatter(
        sample, x="qty", y="amount",
        color="product_id", opacity=0.5,
        labels={"qty": "Quantity", "amount": "Unit Amount ($)", "product_id": "Product ID"},
        color_continuous_scale="Viridis",
        height=360,
    )
    fig6.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig6, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# ROW 5 — Top professions  +  Revenue heatmap (year × month)
# ══════════════════════════════════════════════════════════════════════════════
col7, col8 = st.columns(2)

with col7:
    st.subheader("Top 10 Professions by Revenue")
    job_rev = (
        df.groupby("job", as_index=False)["revenue"].sum()
        .sort_values("revenue", ascending=False)
        .head(10)
    )
    fig7 = px.bar(
        job_rev.sort_values("revenue"), x="revenue", y="job",
        orientation="h", color="revenue",
        color_continuous_scale="Purpor",
        labels={"revenue": "Revenue ($)", "job": "Profession"},
        height=380,
    )
    fig7.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        coloraxis_showscale=False,
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(tickfont=dict(size=10)),
    )
    st.plotly_chart(fig7, use_container_width=True)

with col8:
    st.subheader("Monthly Revenue Heatmap (Year × Month)")
    heatmap_df = (
        df.dropna(subset=["invoice_date"])
        .assign(
            yr=lambda d: d["invoice_date"].dt.year,
            mo=lambda d: d["invoice_date"].dt.month,
        )
        .groupby(["yr", "mo"], as_index=False)["revenue"].sum()
    )
    if not heatmap_df.empty:
        pivot = heatmap_df.pivot(index="yr", columns="mo", values="revenue").fillna(0)
        pivot.columns = [
            "Jan","Feb","Mar","Apr","May","Jun",
            "Jul","Aug","Sep","Oct","Nov","Dec",
        ][:len(pivot.columns)]
        fig8 = px.imshow(
            pivot,
            color_continuous_scale="YlOrRd",
            aspect="auto",
            labels={"color": "Revenue ($)", "x": "Month", "y": "Year"},
            height=380,
        )
        fig8.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig8, use_container_width=True)
    else:
        st.info("Not enough date data for heatmap.")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# ROW 6 — Raw data explorer
# ══════════════════════════════════════════════════════════════════════════════
with st.expander("🔍 Raw Invoice Data Explorer", expanded=False):
    st.caption(f"{len(df):,} rows after filters")
    show_cols = ["invoice_date","first_name","last_name","city","job",
                 "product_id","qty","amount","revenue"]
    st.dataframe(
        df[show_cols].sort_values("invoice_date", ascending=False).reset_index(drop=True),
        use_container_width=True,
        height=400,
    )
