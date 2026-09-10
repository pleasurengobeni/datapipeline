"""
Wind Speed Analytics Dashboard
Reads from weather.wind_speed (local_dw_con) as defined in the ETL Manager config.
Covers 36 cities across North America and Israel.
"""
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from cryptography.fernet import Fernet
from sqlalchemy import create_engine, text

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Wind Speed Analytics",
    page_icon="🌬️",
    layout="wide",
    initial_sidebar_state="expanded",
)

AIRFLOW_DB  = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
                        "postgresql+psycopg2://airflow:airflow@postgres/airflow")
FERNET_KEY  = os.getenv("AIRFLOW__CORE__FERNET_KEY", "")
CONN_ID     = "local_dw_con"
SCHEMA      = "weather"
TABLE       = "wind_speed"

# All 36 cities from the ETL Manager data dictionary (file_local_dw_con__wind_speed.json)
CITY_REGIONS = {
    "US West":    ["vancouver", "portland", "san_francisco", "seattle",
                   "los_angeles", "san_diego", "las_vegas", "phoenix"],
    "US Central": ["albuquerque", "denver", "san_antonio", "dallas", "houston",
                   "kansas_city", "minneapolis", "saint_louis", "chicago"],
    "US East":    ["nashville", "indianapolis", "atlanta", "detroit",
                   "jacksonville", "charlotte", "miami", "pittsburgh",
                   "philadelphia", "new_york", "boston"],
    "Canada":     ["toronto", "montreal"],
    "Israel":     ["beersheba", "tel_aviv_district", "eilat",
                   "haifa", "nahariyya", "jerusalem"],
}
CITIES      = [c for cities in CITY_REGIONS.values() for c in cities]
CITY_LABELS = {c: c.replace("_", " ").title() for c in CITIES}
CITY_COLORS = px.colors.qualitative.Alphabet[:len(CITIES)]

# ── Helpers ───────────────────────────────────────────────────────────────────
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
    except Exception as e:
        st.error(f"Could not resolve Airflow connection '{CONN_ID}': {e}")
        return None


@st.cache_data(ttl=120, show_spinner=False)
def load_data() -> pd.DataFrame:
    uri = _get_uri()
    if not uri:
        return pd.DataFrame()
    engine = create_engine(uri, connect_args={"connect_timeout": 15})
    city_cols = ", ".join(f'"{c}"::float AS {c}' for c in CITIES)
    query = f"""
        SELECT
            datetime AS datetime,
            {city_cols},
            _pipeline_inserted_at
        FROM "{SCHEMA}"."{TABLE}"
        WHERE datetime IS NOT NULL
        ORDER BY datetime
    """
    with engine.connect() as c:
        df = pd.read_sql(query, c, parse_dates=["datetime", "_pipeline_inserted_at"])
    return df


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/7/77/"
        "Flag_of_Rwanda.svg/320px-Flag_of_Rwanda.svg.png",
        width=60,
    )
    st.title("🌬️ Wind Speed")
    st.caption(f"weather.wind_speed · {len(CITIES)} cities")
    st.divider()

    with st.spinner("Loading data…"):
        df_raw = load_data()

    if df_raw.empty:
        st.error("No data loaded.")
        st.stop()

    date_min = df_raw["datetime"].dt.date.min()
    date_max = df_raw["datetime"].dt.date.max()

    st.subheader("Filters")
    date_range = st.date_input(
        "Date range",
        value=(date_min, date_max),
        min_value=date_min,
        max_value=date_max,
        key="dr",
    )
    if len(date_range) == 2:
        d_from, d_to = date_range
    else:
        d_from, d_to = date_min, date_max

    region_filter = st.multiselect(
        "Region",
        options=list(CITY_REGIONS.keys()),
        default=list(CITY_REGIONS.keys()),
        key="region",
    )
    region_cities = [
        c for r in (region_filter or CITY_REGIONS.keys())
        for c in CITY_REGIONS[r]
    ]
    selected_cities = st.multiselect(
        "Cities",
        options=region_cities,
        default=region_cities,
        format_func=lambda c: CITY_LABELS[c],
    )
    if not selected_cities:
        selected_cities = region_cities

    resample_freq = st.selectbox(
        "Time resolution",
        ["1h (raw)", "6H", "1D", "1W", "1ME"],
        index=2,
    )
    freq = None if resample_freq == "1h (raw)" else resample_freq

    st.divider()
    if st.button("🔄 Refresh data", width="stretch"):
        st.cache_data.clear()
        st.rerun()

# ── Filter data ───────────────────────────────────────────────────────────────
df = df_raw[
    (df_raw["datetime"].dt.date >= d_from)
    & (df_raw["datetime"].dt.date <= d_to)
].copy()

if freq:
    df = (
        df.set_index("datetime")[selected_cities]
        .resample(freq)
        .mean()
        .reset_index()
    )
else:
    df = df[["datetime"] + selected_cities].copy()

city_colors = {c: CITY_COLORS[i % len(CITY_COLORS)] for i, c in enumerate(selected_cities)}

# ── Page title ────────────────────────────────────────────────────────────────
st.title("🌬️ Wind Speed Analytics")
st.caption(
    f"Source: `{SCHEMA}.{TABLE}` (ETL Manager · `local_dw_con`) · "
    f"{df_raw['datetime'].dt.date.min()} → {df_raw['datetime'].dt.date.max()} · "
    f"{len(df_raw):,} records · {len(CITIES)} cities"
)
st.divider()

# ── KPI Row ───────────────────────────────────────────────────────────────────
kpi_cols = st.columns(len(selected_cities))
for i, city in enumerate(selected_cities):
    vals = df[city].dropna()
    avg = vals.mean()
    peak = vals.max()
    label = CITY_LABELS[city]
    kpi_cols[i].metric(
        label=f"🏙️ {label}",
        value=f"{avg:.1f} m/s" if not pd.isna(avg) else "—",
        delta=f"peak {peak:.1f}" if not pd.isna(peak) else None,
    )

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_ts, tab_compare, tab_patterns, tab_dist, tab_corr, tab_raw = st.tabs([
    "📈 Time Series",
    "🏙️ City Comparison",
    "🕐 Temporal Patterns",
    "📊 Distributions",
    "🔗 Correlations",
    "🗃️ Raw Data",
])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — Time Series
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_ts:
    df_melt = df.melt(
        id_vars="datetime", value_vars=selected_cities,
        var_name="city", value_name="wind_speed"
    )
    df_melt["city"] = df_melt["city"].map(CITY_LABELS)
    df_melt = df_melt.dropna(subset=["wind_speed"])

    st.subheader("Wind Speed Over Time — All Cities")
    fig_ts = px.line(
        df_melt, x="datetime", y="wind_speed", color="city",
        title="Wind Speed (m/s) Over Time",
        labels={"wind_speed": "Wind Speed (m/s)", "datetime": "Date", "city": "City"},
        template="plotly_dark",
        height=420,
    )
    fig_ts.update_traces(line_width=1.5)
    fig_ts.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig_ts, width="stretch")

    st.subheader("7-Day Rolling Average")
    df_roll = df.set_index("datetime")[selected_cities].rolling("7D").mean().reset_index()
    df_roll_melt = df_roll.melt(
        id_vars="datetime", value_vars=selected_cities,
        var_name="city", value_name="wind_speed"
    )
    df_roll_melt["city"] = df_roll_melt["city"].map(CITY_LABELS)
    df_roll_melt = df_roll_melt.dropna(subset=["wind_speed"])
    fig_roll = px.line(
        df_roll_melt, x="datetime", y="wind_speed", color="city",
        title="7-Day Rolling Avg Wind Speed (m/s)",
        labels={"wind_speed": "Rolling Avg (m/s)", "datetime": "Date", "city": "City"},
        template="plotly_dark",
        height=400,
    )
    fig_roll.update_traces(line_width=2)
    fig_roll.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig_roll, width="stretch")

    st.subheader("Stacked Area — Total Wind Energy")
    fig_area = px.area(
        df_melt, x="datetime", y="wind_speed", color="city",
        title="Stacked Wind Speed Contribution",
        labels={"wind_speed": "Wind Speed (m/s)", "datetime": "Date"},
        template="plotly_dark",
        height=380,
    )
    fig_area.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig_area, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — City Comparison
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_compare:
    stats = pd.DataFrame({
        "City": [CITY_LABELS[c] for c in selected_cities],
        "Mean":   [df[c].mean()   for c in selected_cities],
        "Median": [df[c].median() for c in selected_cities],
        "Max":    [df[c].max()    for c in selected_cities],
        "Std":    [df[c].std()    for c in selected_cities],
    }).round(2).sort_values("Mean", ascending=False)

    left, right = st.columns(2)

    with left:
        st.subheader("Average Wind Speed by City")
        fig_avg = px.bar(
            stats, x="Mean", y="City",
            orientation="h",
            color="Mean",
            color_continuous_scale="Blues",
            title="Mean Wind Speed (m/s)",
            labels={"Mean": "m/s", "City": ""},
            template="plotly_dark",
            height=380,
        )
        fig_avg.update_layout(coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_avg, width="stretch")

    with right:
        st.subheader("Peak Wind Speed by City")
        fig_max = px.bar(
            stats, x="Max", y="City",
            orientation="h",
            color="Max",
            color_continuous_scale="Reds",
            title="Peak Wind Speed (m/s)",
            labels={"Max": "m/s", "City": ""},
            template="plotly_dark",
            height=380,
        )
        fig_max.update_layout(coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_max, width="stretch")

    st.subheader("Wind Speed Statistics Table")
    st.dataframe(stats, width="stretch", hide_index=True)

    st.subheader("Grouped Bar — Mean / Median / Max")
    fig_grouped = go.Figure()
    for metric, color in [("Mean", "#4C72B0"), ("Median", "#55A868"), ("Max", "#C44E52")]:
        fig_grouped.add_trace(go.Bar(
            name=metric,
            x=stats["City"],
            y=stats[metric],
        ))
    fig_grouped.update_layout(
        barmode="group",
        title="City Wind Speed Comparison",
        xaxis_title="City",
        yaxis_title="Wind Speed (m/s)",
        template="plotly_dark",
        height=380,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig_grouped, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3 — Temporal Patterns
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_patterns:
    df_tp = df_raw[
        (df_raw["datetime"].dt.date >= d_from)
        & (df_raw["datetime"].dt.date <= d_to)
    ].copy()

    df_tp["hour"]       = df_tp["datetime"].dt.hour
    df_tp["month"]      = df_tp["datetime"].dt.month
    df_tp["month_name"] = df_tp["datetime"].dt.strftime("%b")
    df_tp["day_of_week"]= df_tp["datetime"].dt.day_name()
    df_tp["year"]       = df_tp["datetime"].dt.year
    df_tp["avg_speed"]  = df_tp[selected_cities].mean(axis=1)

    left, right = st.columns(2)

    with left:
        st.subheader("Average Speed by Hour of Day")
        hourly = df_tp.groupby("hour")["avg_speed"].mean().reset_index()
        fig_hour = px.line(
            hourly, x="hour", y="avg_speed", markers=True,
            title="Avg Wind Speed by Hour (all cities combined)",
            labels={"avg_speed": "Avg Speed (m/s)", "hour": "Hour of Day"},
            template="plotly_dark",
            height=340,
        )
        fig_hour.update_traces(line_color="#FDB515", line_width=2)
        st.plotly_chart(fig_hour, width="stretch")

    with right:
        st.subheader("Average Speed by Month")
        month_order = ["Jan","Feb","Mar","Apr","May","Jun",
                       "Jul","Aug","Sep","Oct","Nov","Dec"]
        monthly = df_tp.groupby("month_name")["avg_speed"].mean().reindex(month_order).reset_index()
        monthly.columns = ["Month", "avg_speed"]
        fig_month = px.bar(
            monthly.dropna(), x="Month", y="avg_speed",
            color="avg_speed",
            color_continuous_scale="Tealgrn",
            title="Avg Wind Speed by Month",
            labels={"avg_speed": "Avg Speed (m/s)", "Month": ""},
            template="plotly_dark",
            height=340,
        )
        fig_month.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_month, width="stretch")

    st.subheader("Heatmap — Hour of Day vs Month (Avg Wind Speed)")
    pivot = (
        df_tp.groupby(["hour", "month_name"])["avg_speed"]
        .mean()
        .unstack("month_name")
        .reindex(columns=month_order)
    )
    fig_hmap = px.imshow(
        pivot,
        aspect="auto",
        color_continuous_scale="Viridis",
        title="Wind Speed Heatmap: Hour × Month",
        labels={"x": "Month", "y": "Hour of Day", "color": "Avg m/s"},
        template="plotly_dark",
        height=400,
    )
    st.plotly_chart(fig_hmap, width="stretch")

    st.subheader("Per-City Monthly Trend")
    monthly_city = (
        df_tp.groupby(["month_name"])[selected_cities].mean().reindex(month_order)
        .reset_index().rename(columns={"month_name": "Month"})
    )
    mc_melt = monthly_city.melt(id_vars="Month", var_name="city", value_name="wind_speed")
    mc_melt["city"] = mc_melt["city"].map(CITY_LABELS)
    fig_mc = px.line(
        mc_melt.dropna(), x="Month", y="wind_speed", color="city", markers=True,
        title="Monthly Wind Speed Trend by City",
        labels={"wind_speed": "Avg Speed (m/s)", "Month": "", "city": "City"},
        template="plotly_dark",
        height=380,
    )
    fig_mc.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig_mc, width="stretch")

    st.subheader("Day of Week Pattern")
    dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    dow = df_tp.groupby("day_of_week")["avg_speed"].mean().reindex(dow_order).reset_index()
    dow.columns = ["Day", "avg_speed"]
    fig_dow = px.bar(
        dow.dropna(), x="Day", y="avg_speed",
        color="avg_speed", color_continuous_scale="Purples",
        title="Avg Wind Speed by Day of Week",
        labels={"avg_speed": "Avg Speed (m/s)", "Day": ""},
        template="plotly_dark",
        height=340,
    )
    fig_dow.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig_dow, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4 — Distributions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_dist:
    df_melt_full = df.melt(
        id_vars="datetime", value_vars=selected_cities,
        var_name="city", value_name="wind_speed"
    )
    df_melt_full["city"] = df_melt_full["city"].map(CITY_LABELS)
    df_melt_full = df_melt_full.dropna(subset=["wind_speed"])

    left, right = st.columns(2)

    with left:
        st.subheader("Box Plot — Speed Distribution by City")
        fig_box = px.box(
            df_melt_full, x="city", y="wind_speed",
            color="city",
            title="Wind Speed Distribution",
            labels={"wind_speed": "Speed (m/s)", "city": ""},
            template="plotly_dark",
            height=420,
        )
        fig_box.update_layout(showlegend=False)
        st.plotly_chart(fig_box, width="stretch")

    with right:
        st.subheader("Violin Plot — Speed Distribution")
        fig_violin = px.violin(
            df_melt_full, x="city", y="wind_speed",
            color="city", box=True,
            title="Wind Speed Violin Plot",
            labels={"wind_speed": "Speed (m/s)", "city": ""},
            template="plotly_dark",
            height=420,
        )
        fig_violin.update_layout(showlegend=False)
        st.plotly_chart(fig_violin, width="stretch")

    st.subheader("Histogram — Wind Speed Frequency")
    fig_hist = px.histogram(
        df_melt_full, x="wind_speed", color="city",
        nbins=50, barmode="overlay", opacity=0.6,
        title="Wind Speed Frequency Distribution",
        labels={"wind_speed": "Speed (m/s)", "city": "City"},
        template="plotly_dark",
        height=380,
    )
    fig_hist.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig_hist, width="stretch")

    st.subheader("🏆 Top 15 Windiest Hours (any city)")
    df_any = df.copy()
    df_any["max_speed"] = df_any[selected_cities].max(axis=1)
    df_any["windiest_city"] = df_any[selected_cities].idxmax(axis=1).map(CITY_LABELS)
    top15 = (
        df_any[["datetime", "max_speed", "windiest_city"]]
        .dropna()
        .nlargest(15, "max_speed")
        .reset_index(drop=True)
    )
    top15.index += 1
    top15.columns = ["Date/Time", "Max Speed (m/s)", "Windiest City"]
    top15["Date/Time"] = top15["Date/Time"].dt.strftime("%Y-%m-%d %H:%M")
    st.dataframe(top15, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 5 — Correlations
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_corr:
    if len(selected_cities) >= 2:
        corr_matrix = df[selected_cities].rename(columns=CITY_LABELS).corr()

        st.subheader("Correlation Matrix — Wind Speeds Between Cities")
        fig_corr = px.imshow(
            corr_matrix,
            text_auto=".2f",
            color_continuous_scale="RdBu_r",
            zmin=-1, zmax=1,
            title="Pearson Correlation of Wind Speed Between Cities",
            template="plotly_dark",
            height=480,
        )
        st.plotly_chart(fig_corr, width="stretch")

        st.subheader("Scatter — City vs City")
        sc1, sc2 = st.columns(2)
        city_a = sc1.selectbox("City A", selected_cities, index=0,
                               format_func=lambda c: CITY_LABELS[c], key="sc_a")
        city_b = sc2.selectbox("City B", selected_cities,
                               index=min(1, len(selected_cities)-1),
                               format_func=lambda c: CITY_LABELS[c], key="sc_b")

        if city_a != city_b:
            scatter_df = df[["datetime", city_a, city_b]].dropna()
            fig_sc = px.scatter(
                scatter_df.sample(min(5000, len(scatter_df)), random_state=42),
                x=city_a, y=city_b,
                color="datetime",
                color_continuous_scale="Plasma",
                title=f"{CITY_LABELS[city_a]} vs {CITY_LABELS[city_b]}",
                labels={
                    city_a: f"{CITY_LABELS[city_a]} (m/s)",
                    city_b: f"{CITY_LABELS[city_b]} (m/s)",
                },
                template="plotly_dark",
                height=420,
            )
            st.plotly_chart(fig_sc, width="stretch")
        else:
            st.info("Select two different cities to compare.")
    else:
        st.info("Select at least 2 cities in the sidebar to view correlations.")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 6 — Raw Data
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_raw:
    st.subheader(f"Raw Data — {len(df):,} rows (filtered)")
    display_df = df.copy()
    display_df.columns = [
        "Date/Time" if c == "datetime" else CITY_LABELS.get(c, c)
        for c in display_df.columns
    ]
    st.dataframe(display_df, width="stretch")

    csv = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇ Download CSV",
        data=csv,
        file_name=f"wind_speed_{d_from}_{d_to}.csv",
        mime="text/csv",
    )
