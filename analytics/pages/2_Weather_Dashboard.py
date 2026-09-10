"""
Weather Analytics Dashboard
Queries weather.wind_speed, weather.humidity, weather.temperature, weather.weather_description
and weather.city_attributes — all loaded via ETL Manager (local_dw_con), 36 cities,
Oct 2012 – Nov 2017.  Temperature stored in Kelvin; displayed in Celsius (K − 273.15).
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
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, GradientBoostingRegressor, ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVR
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import confusion_matrix, mean_absolute_error, r2_score, mean_squared_error

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Weather Dashboard",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Constants from ETL Manager configs ────────────────────────────────────────
AIRFLOW_DB = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres/airflow",
)
FERNET_KEY = os.getenv("AIRFLOW__CORE__FERNET_KEY", "")
CONN_ID    = "local_dw_con"
SCHEMA     = "weather"
KELVIN_OFFSET = 273.15   # temperature stored in K; display in °C

# All 36 cities from file_local_dw_con__wind_speed.json column_names
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

# ── Helpers ────────────────────────────────────────────────────────────────────
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


def _get_engine():
    uri = _get_uri()
    if not uri:
        return None
    return create_engine(uri, connect_args={"connect_timeout": 15}, pool_pre_ping=True)


@st.cache_data(ttl=300, show_spinner=False)
def load_all() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load wind_speed, humidity, temperature (→ °C), weather_description from direct table queries."""
    engine = _get_engine()
    if engine is None:
        return (pd.DataFrame(),) * 4

    city_num  = ", ".join(f'"{c}"::float AS "{c}"' for c in CITIES)
    city_text = ", ".join(f'"{c}"' for c in CITIES)

    def _load(tbl, cols):
        with engine.connect() as conn:
            return pd.read_sql(
                f'SELECT datetime, {cols} FROM "{SCHEMA}"."{tbl}" '
                f'WHERE datetime IS NOT NULL ORDER BY datetime',
                conn, parse_dates=["datetime"],
            )

    ws  = _load("wind_speed",          city_num)
    hum = _load("humidity",            city_num)
    tmp = _load("temperature",         city_num)
    dsc = _load("weather_description", city_text)

    # Convert temperature Kelvin → Celsius
    for c in CITIES:
        if c in tmp.columns:
            tmp[c] = tmp[c] - KELVIN_OFFSET

    return ws, hum, tmp, dsc


@st.cache_data(ttl=300, show_spinner=False)
def load_joined(city: str) -> pd.DataFrame:
    """Single-city cross-variable join of wind_speed, humidity, temperature (°C), weather_description."""
    engine = _get_engine()
    if engine is None:
        return pd.DataFrame()
    sql = f"""
        SELECT ws.datetime,
               ws."{city}"::float                        AS wind_speed,
               h."{city}"::float                         AS humidity,
               t."{city}"::float - {KELVIN_OFFSET}       AS temperature,
               wd."{city}"                               AS weather_description
        FROM weather.wind_speed ws
        JOIN weather.humidity            h  USING (datetime)
        JOIN weather.temperature         t  USING (datetime)
        JOIN weather.weather_description wd USING (datetime)
        WHERE ws.datetime IS NOT NULL
        ORDER BY ws.datetime
    """
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, parse_dates=["datetime"])


@st.cache_data(ttl=300, show_spinner=False)
def load_city_attributes() -> pd.DataFrame:
    engine = _get_engine()
    if engine is None:
        return pd.DataFrame()
    with engine.connect() as conn:
        return pd.read_sql(
            'SELECT city, country, latitude, longitude FROM weather.city_attributes',
            conn,
        )


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/7/77/"
        "Flag_of_Rwanda.svg/320px-Flag_of_Rwanda.svg.png",
        width=60,
    )
    st.title("🌦️ Weather")
    st.caption("weather.* · local_dw_con · 36 cities")
    st.divider()

    with st.spinner("Loading…"):
        ws_raw, hum_raw, tmp_raw, dsc_raw = load_all()

    if ws_raw.empty:
        st.error("No data loaded.")
        st.stop()

    date_min = ws_raw["datetime"].dt.date.min()
    date_max = ws_raw["datetime"].dt.date.max()

    st.subheader("Filters")
    date_range = st.date_input(
        "Date range", value=(date_min, date_max),
        min_value=date_min, max_value=date_max, key="dr",
    )
    d_from, d_to = (date_range if len(date_range) == 2 else (date_min, date_max))

    region_filter = st.multiselect(
        "Region", options=list(CITY_REGIONS.keys()),
        default=list(CITY_REGIONS.keys()), key="region",
    )
    region_cities = [c for r in (region_filter or CITY_REGIONS.keys()) for c in CITY_REGIONS[r]]

    selected_cities = st.multiselect(
        "Cities", options=region_cities, default=region_cities[:8],
        format_func=lambda c: CITY_LABELS[c],
    )
    if not selected_cities:
        selected_cities = region_cities[:8]

    resample_freq = st.selectbox(
        "Time resolution", ["Raw (3h)", "1D", "1W", "1ME"], index=1,
    )
    freq = None if resample_freq == "Raw (3h)" else resample_freq

    single_city = st.selectbox(
        "City for cross-variable analysis",
        options=CITIES, index=CITIES.index("new_york"),
        format_func=lambda c: CITY_LABELS[c],
    )

    st.divider()
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ── Filter helper ──────────────────────────────────────────────────────────────
def _filter_resample(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df[(df["datetime"].dt.date >= d_from) & (df["datetime"].dt.date <= d_to)].copy()
    if freq:
        out = out.set_index("datetime")[cols].resample(freq).mean().reset_index()
    else:
        out = out[["datetime"] + cols].copy()
    return out


ws  = _filter_resample(ws_raw,  selected_cities)
hum = _filter_resample(hum_raw, selected_cities)
tmp = _filter_resample(tmp_raw, selected_cities)

# ── Page title ─────────────────────────────────────────────────────────────────
st.title("🌦️ Weather Analytics Dashboard")
st.caption(
    f"Sources: `weather.wind_speed`, `humidity`, `temperature`, `weather_description` "
    f"(ETL Manager · `local_dw_con`) · "
    f"**{date_min}** → **{date_max}** · "
    f"**{len(ws_raw):,}** records · **{len(CITIES)}** cities"
)
st.divider()

# ── KPI row ────────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Cities selected",    len(selected_cities))
k2.metric("Avg Wind Speed",     f"{ws_raw[selected_cities].mean().mean():.1f} m/s")
k3.metric("Avg Humidity",       f"{hum_raw[selected_cities].mean().mean():.1f} %")
k4.metric("Avg Temperature",    f"{tmp_raw[selected_cities].mean().mean():.1f} °C")
k5.metric("Date range (days)",  str((d_to - d_from).days))

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_overview, tab_compare, tab_cross, tab_desc, tab_map, tab_corr, tab_raw, tab_forecast, tab_ml = st.tabs([
    "📈 Overview",
    "🏙️ City Comparison",
    "🔗 Cross-Variable",
    "☁️ Conditions",
    "🗺️ Map",
    "📊 Correlations",
    "🗃️ Raw Data",
    "🔮 Forecast",
    "🤖 ML Models",
])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — Overview: time-series for all 3 numeric variables side-by-side
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_overview:
    def _melt(df: pd.DataFrame, var_name: str) -> pd.DataFrame:
        m = df.melt(id_vars="datetime", value_vars=selected_cities,
                    var_name="city", value_name=var_name)
        m["city"] = m["city"].map(CITY_LABELS)
        return m.dropna(subset=[var_name])

    for label, unit, df_t in [
        ("Wind Speed",   "m/s", ws),
        ("Humidity",     "%",   hum),
        ("Temperature",  "°C",  tmp),
    ]:
        df_m = _melt(df_t, label)
        fig = px.line(
            df_m, x="datetime", y=label, color="city",
            title=f"{label} Over Time",
            labels={label: f"{label} ({unit})", "datetime": "Date", "city": "City"},
            template="plotly_dark", height=340,
        )
        fig.update_traces(line_width=1.5)
        fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, width="stretch")
        st.divider()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — City Comparison: mean of each variable per city
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_compare:
    stats = pd.DataFrame({
        "City":            [CITY_LABELS[c] for c in selected_cities],
        "Wind (m/s)":      [ws[c].mean()   for c in selected_cities],
        "Humidity (%)":    [hum[c].mean()  for c in selected_cities],
        "Temperature (°C)":[tmp[c].mean()  for c in selected_cities],
    }).round(2).sort_values("Wind (m/s)", ascending=False)

    c1, c2, c3 = st.columns(3)
    for col, metric, scale in [
        (c1, "Wind (m/s)",       "Blues"),
        (c2, "Humidity (%)",     "Greens"),
        (c3, "Temperature (°C)", "Reds"),
    ]:
        fig = px.bar(
            stats, x=metric, y="City", orientation="h",
            color=metric, color_continuous_scale=scale,
            title=f"Mean {metric}", labels={metric: metric, "City": ""},
            template="plotly_dark", height=420,
        )
        fig.update_layout(coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
        col.plotly_chart(fig, width="stretch")

    st.divider()
    st.subheader("All Metrics Table")
    st.dataframe(stats, width="stretch", hide_index=True)

    # Radar chart — normalised metrics per city (top 10 by wind)
    st.subheader("Radar — Multi-metric Profile (top 10 by wind)")
    top10 = stats.head(10)
    cats = ["Wind (m/s)", "Humidity (%)", "Temperature (°C)"]
    scale_max = {c: stats[c].max() for c in cats}
    fig_radar = go.Figure()
    for _, row in top10.iterrows():
        vals = [row[c] / scale_max[c] * 100 for c in cats]
        vals += [vals[0]]
        fig_radar.add_trace(go.Scatterpolar(
            r=vals, theta=cats + [cats[0]],
            fill="toself", name=row["City"],
        ))
    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 110])),
        template="plotly_dark", height=480,
        legend=dict(orientation="h", yanchor="bottom", y=-0.25),
    )
    st.plotly_chart(fig_radar, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3 — Cross-Variable: joined single-city deep-dive
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_cross:
    city_label = CITY_LABELS[single_city]
    st.subheader(f"Cross-Variable Analysis — {city_label}")
    st.caption("Joined on `datetime` across all 4 weather tables")

    with st.spinner(f"Joining tables for {city_label}…"):
        jdf = load_joined(single_city)

    if jdf.empty:
        st.warning("No joined data available.")
    else:
        jdf = jdf[
            (jdf["datetime"].dt.date >= d_from) &
            (jdf["datetime"].dt.date <= d_to)
        ].copy()
        if freq:
            num_cols = ["wind_speed", "humidity", "temperature"]
            jdf = jdf.set_index("datetime")[num_cols].resample(freq).mean().reset_index()

        # KPIs
        m1, m2, m3 = st.columns(3)
        m1.metric("Avg Wind Speed",   f"{jdf['wind_speed'].mean():.2f} m/s")
        m2.metric("Avg Humidity",     f"{jdf['humidity'].mean():.1f} %")
        m3.metric("Avg Temperature",  f"{jdf['temperature'].mean():.1f} °C")

        # Subplot: 3 variables on aligned time axis
        fig_sub = make_subplots(
            rows=3, cols=1, shared_xaxes=True,
            subplot_titles=["Wind Speed (m/s)", "Humidity (%)", "Temperature (°C)"],
            vertical_spacing=0.06,
        )
        fig_sub.add_trace(go.Scatter(x=jdf["datetime"], y=jdf["wind_speed"],
                                     name="Wind", line=dict(color="#4FC3F7")), row=1, col=1)
        fig_sub.add_trace(go.Scatter(x=jdf["datetime"], y=jdf["humidity"],
                                     name="Humidity", line=dict(color="#81C784")), row=2, col=1)
        fig_sub.add_trace(go.Scatter(x=jdf["datetime"], y=jdf["temperature"],
                                     name="Temperature", line=dict(color="#FFB74D")), row=3, col=1)
        fig_sub.update_layout(
            title=f"{city_label} — Wind / Humidity / Temperature",
            template="plotly_dark", height=580, showlegend=False,
        )
        st.plotly_chart(fig_sub, width="stretch")

        # Scatter matrix for the 3 numeric variables
        st.subheader(f"Scatter Matrix — {city_label}")
        if "weather_description" in jdf.columns:
            color_col = "weather_description"
        else:
            color_col = None
        fig_sm = px.scatter_matrix(
            jdf.dropna(),
            dimensions=["wind_speed", "humidity", "temperature"],
            color=color_col,
            title=f"Scatter Matrix — {city_label}",
            labels={"wind_speed": "Wind (m/s)", "humidity": "Humidity (%)",
                    "temperature": "Temperature (°C)"},
            template="plotly_dark", height=520,
        )
        fig_sm.update_traces(diagonal_visible=False, marker=dict(size=2, opacity=0.5))
        st.plotly_chart(fig_sm, width="stretch")

        # Scatter: wind vs humidity coloured by temperature
        st.subheader("Wind Speed vs Humidity (colour = Temperature)")
        _sc_df = jdf.dropna()
        fig_sc = px.scatter(
            _sc_df.sample(min(5000, len(_sc_df)), random_state=42),
            x="wind_speed", y="humidity", color="temperature",
            color_continuous_scale="Plasma",
            title=f"{city_label} — Wind vs Humidity",
            labels={"wind_speed": "Wind Speed (m/s)", "humidity": "Humidity (%)",
                    "temperature": "Temperature (°C)"},
            template="plotly_dark", height=420,
        )
        st.plotly_chart(fig_sc, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4 — Weather Conditions (weather_description)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_desc:
    dsc_f = dsc_raw[
        (dsc_raw["datetime"].dt.date >= d_from) &
        (dsc_raw["datetime"].dt.date <= d_to)
    ].copy()

    # Condition frequency for selected city
    st.subheader(f"Condition Frequency — {CITY_LABELS[single_city]}")
    cond_counts = (
        dsc_f[single_city]
        .dropna()
        .str.strip()
        .value_counts()
        .reset_index()
    )
    cond_counts.columns = ["Condition", "Count"]

    left, right = st.columns(2)
    with left:
        fig_pie = px.pie(
            cond_counts.head(10), names="Condition", values="Count",
            title=f"{CITY_LABELS[single_city]} — Top 10 Conditions",
            template="plotly_dark", height=400,
        )
        st.plotly_chart(fig_pie, width="stretch")
    with right:
        fig_bar = px.bar(
            cond_counts.head(15), x="Count", y="Condition", orientation="h",
            color="Count", color_continuous_scale="Teal",
            title=f"{CITY_LABELS[single_city]} — Condition Counts",
            template="plotly_dark", height=400,
        )
        fig_bar.update_layout(coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_bar, width="stretch")

    # Condition heatmap across selected cities
    st.subheader("Dominant Condition per City (most frequent over date range)")
    rows = []
    for c in selected_cities:
        top = dsc_f[c].dropna().str.strip().mode()
        rows.append({"City": CITY_LABELS[c], "Dominant Condition": top.iloc[0] if len(top) else "—"})
    top_df = pd.DataFrame(rows)
    st.dataframe(top_df, width="stretch", hide_index=True)

    # Condition-linked wind speed: for single city, avg wind per condition
    st.subheader(f"Avg Wind Speed by Condition — {CITY_LABELS[single_city]}")
    if not jdf.empty and "weather_description" in load_joined(single_city).columns:
        jdf_full = load_joined(single_city)
        jdf_full = jdf_full[
            (jdf_full["datetime"].dt.date >= d_from) &
            (jdf_full["datetime"].dt.date <= d_to)
        ]
        cond_wind = (
            jdf_full.groupby("weather_description")["wind_speed"]
            .mean().reset_index().sort_values("wind_speed", ascending=False)
        )
        cond_wind.columns = ["Condition", "Avg Wind Speed (m/s)"]
        fig_cw = px.bar(
            cond_wind.head(20), x="Avg Wind Speed (m/s)", y="Condition",
            orientation="h", color="Avg Wind Speed (m/s)",
            color_continuous_scale="Blues",
            title=f"{CITY_LABELS[single_city]} — Avg Wind Speed per Condition",
            template="plotly_dark", height=480,
        )
        fig_cw.update_layout(coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_cw, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 5 — City Map: scatter_map coloured by avg temperature / humidity / wind
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_map:
    st.subheader("🗺️ City Map — Average Metrics")
    geo_df = load_city_attributes()
    if geo_df.empty:
        st.warning("city_attributes table unavailable.")
    else:
        # Normalise city name to match column labels (e.g. "New York" → "new_york")
        geo_df["city_col"] = (
            geo_df["city"].str.lower()
            .str.replace(" ", "_", regex=False)
            .str.replace("-", "_", regex=False)
        )
        # Build avg metrics for each city in city_attributes
        avg_rows = []
        for _, row in geo_df.iterrows():
            cc = row["city_col"]
            if cc in CITIES:
                avg_rows.append({
                    "City":            row["city"],
                    "Country":         row["country"],
                    "lat":             row["latitude"],
                    "lon":             row["longitude"],
                    "Avg Temp (°C)":   round(tmp_raw[cc].mean(), 1),
                    "Avg Humidity (%)":round(hum_raw[cc].mean(), 1),
                    "Avg Wind (m/s)":  round(ws_raw[cc].mean(),  1),
                })
        map_df = pd.DataFrame(avg_rows)

        map_metric = st.selectbox(
            "Colour cities by",
            ["Avg Temp (°C)", "Avg Humidity (%)", "Avg Wind (m/s)"],
            key="map_metric",
        )
        scale_map = {
            "Avg Temp (°C)":   "RdBu_r",
            "Avg Humidity (%)":"Blues",
            "Avg Wind (m/s)":  "Greens",
        }
        fig_map = px.scatter_map(
            map_df, lat="lat", lon="lon",
            color=map_metric, size=map_metric,
            hover_name="City",
            hover_data={"Country": True, "Avg Temp (°C)": True,
                        "Avg Humidity (%)": True, "Avg Wind (m/s)": True,
                        "lat": False, "lon": False},
            color_continuous_scale=scale_map[map_metric],
            size_max=30, zoom=1,
            title=f"36 Cities — {map_metric}",
            map_style="carto-darkmatter",
            height=600,
        )
        fig_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
        st.plotly_chart(fig_map, width="stretch")

        st.subheader("City Metrics Table")
        st.dataframe(
            map_df.drop(columns=["lat", "lon"]).sort_values(map_metric, ascending=False),
            width="stretch", hide_index=True,
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 6 — Correlations between variables across all selected cities
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_corr:
    st.subheader("Cross-Variable Correlation per City")
    st.caption("Pearson r between wind speed, humidity, and temperature for each selected city")

    corr_rows = []
    for c in selected_cities:
        city_df = pd.DataFrame({
            "wind_speed":  ws_raw[c],
            "humidity":    hum_raw[c],
            "temperature": tmp_raw[c],
        }).dropna()
        if len(city_df) > 5:
            corr_rows.append({
                "City":               CITY_LABELS[c],
                "Wind↔Humidity":      round(city_df["wind_speed"].corr(city_df["humidity"]),     3),
                "Wind↔Temperature":   round(city_df["wind_speed"].corr(city_df["temperature"]),  3),
                "Humidity↔Temperature": round(city_df["humidity"].corr(city_df["temperature"]),  3),
            })

    corr_df = pd.DataFrame(corr_rows)

    if not corr_df.empty:
        left, right = st.columns(2)

        with left:
            fig_wh = px.bar(
                corr_df.sort_values("Wind↔Humidity"),
                x="Wind↔Humidity", y="City", orientation="h",
                color="Wind↔Humidity", color_continuous_scale="RdBu_r",
                color_continuous_midpoint=0,
                title="Wind ↔ Humidity Correlation",
                template="plotly_dark", height=420,
            )
            fig_wh.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig_wh, width="stretch")

        with right:
            fig_wp = px.bar(
                corr_df.sort_values("Wind↔Temperature"),
                x="Wind↔Temperature", y="City", orientation="h",
                color="Wind↔Temperature", color_continuous_scale="RdBu_r",
                color_continuous_midpoint=0,
                title="Wind ↔ Temperature Correlation",
                template="plotly_dark", height=420,
            )
            fig_wp.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig_wp, width="stretch")

        # Heatmap of all 3 pairs per city
        heat_df = corr_df.set_index("City")[["Wind↔Humidity", "Wind↔Temperature", "Humidity↔Temperature"]]
        fig_heat = px.imshow(
            heat_df, text_auto=".2f",
            color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
            title="Correlation Heatmap — All Cities × Variable Pairs",
            labels={"color": "Pearson r"},
            template="plotly_dark", height=max(400, len(selected_cities) * 18),
        )
        st.plotly_chart(fig_heat, width="stretch")

        st.subheader("Correlation Table")
        st.dataframe(corr_df, width="stretch", hide_index=True)

    # Wind-speed correlation between cities
    st.subheader("Wind Speed — City-to-City Correlation")
    ws_corr = ws_raw[selected_cities].rename(columns=CITY_LABELS).corr()
    fig_wcc = px.imshow(
        ws_corr, text_auto=".2f",
        color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
        title="Wind Speed Correlation Between Cities",
        template="plotly_dark",
        height=max(420, len(selected_cities) * 20),
    )
    st.plotly_chart(fig_wcc, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 6 — Raw joined data export
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_raw:
    raw_city = st.selectbox(
        "City", options=CITIES, index=CITIES.index("new_york"),
        format_func=lambda c: CITY_LABELS[c], key="raw_city",
    )
    with st.spinner("Loading joined data…"):
        raw_jdf = load_joined(raw_city)

    raw_jdf = raw_jdf[
        (raw_jdf["datetime"].dt.date >= d_from) &
        (raw_jdf["datetime"].dt.date <= d_to)
    ].copy()

    st.subheader(f"Joined Data — {CITY_LABELS[raw_city]} ({len(raw_jdf):,} rows)")
    st.caption("Source: `JOIN weather.wind_speed, humidity, temperature, weather_description ON datetime`")
    st.dataframe(raw_jdf, width="stretch")

    csv = raw_jdf.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇ Download CSV",
        data=csv,
        file_name=f"weather_{raw_city}_{d_from}_{d_to}.csv",
        mime="text/csv",
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 7 — 7-day Forecast using Holt-Winters Exponential Smoothing
# Trains on full history per city, forecasts 56 steps (7 days × 8 obs/day at 3h)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_forecast:
    st.subheader("🔮 7-Day Forecast — Holt-Winters Exponential Smoothing")
    st.caption(
        "Trained on full history (Oct 2012 – Nov 2017). "
        "Forecasts 7 days beyond 2017-11-30 00:00 using additive trend + seasonality. "
        "Model: `statsmodels.tsa.holtwinters.ExponentialSmoothing`."
    )

    fc1, fc2 = st.columns([1, 2])
    with fc1:
        fc_variable = st.selectbox(
            "Variable to forecast",
            ["wind_speed", "humidity", "temperature"],
            format_func=lambda v: {"wind_speed": "Wind Speed (m/s)",
                                   "humidity": "Humidity (%)",
                                   "temperature": "Temperature (°C)"}[v],
            key="fc_var",
        )
        fc_cities = st.multiselect(
            "Cities", options=CITIES, default=["new_york", "chicago", "los_angeles",
                                               "toronto", "tel_aviv_district"],
            format_func=lambda c: CITY_LABELS[c], key="fc_cities",
        )
        if not fc_cities:
            fc_cities = ["new_york"]
        fc_horizon_days = st.slider("Forecast horizon (days)", 1, 14, 7, key="fc_days")
        history_months = st.slider(
            "Training window (most recent months)", 3, 60, 24, key="fc_train",
            help="Limit training to the N most recent months for faster fitting."
        )

    STEPS_PER_DAY   = 8          # one obs every 3 h
    SEASONAL_PERIOD = 56         # 7-day weekly cycle at 3-hourly = 56 steps
    HORIZON         = fc_horizon_days * STEPS_PER_DAY
    LAST_DT         = pd.Timestamp("2017-11-30 00:00:00")
    future_index    = pd.date_range(
        start=LAST_DT + pd.Timedelta(hours=3),
        periods=HORIZON,
        freq="3h",
    )

    # Map variable → source table raw df
    _src_map = {"wind_speed": ws_raw, "humidity": hum_raw, "temperature": tmp_raw}
    _unit_map = {"wind_speed": "m/s", "humidity": "%", "temperature": "°C"}
    src_df = _src_map[fc_variable]
    unit   = _unit_map[fc_variable]

    @st.cache_data(ttl=600, show_spinner=False)
    def _fit_forecast(variable: str, city: str, horizon: int,
                      seasonal_period: int, history_months: int) -> pd.DataFrame:
        """Fit Holt-Winters on the most recent `history_months` of data and forecast."""
        raw = _src_map[variable][["datetime", city]].dropna().sort_values("datetime")
        cutoff = raw["datetime"].max() - pd.DateOffset(months=history_months)
        train  = raw[raw["datetime"] >= cutoff][city].values

        if len(train) < seasonal_period * 2:
            return pd.DataFrame()   # not enough data

        try:
            model = ExponentialSmoothing(
                train,
                trend="add",
                seasonal="add",
                seasonal_periods=seasonal_period,
                initialization_method="estimated",
            ).fit(optimized=True, use_brute=False)
            forecast = model.forecast(horizon)
        except Exception:
            # fallback: no seasonality
            model = ExponentialSmoothing(
                train, trend="add", seasonal=None,
            ).fit(optimized=True)
            forecast = model.forecast(horizon)

        # Confidence interval approximation: ±1.96 × residual std
        resid_std = np.std(model.resid) if hasattr(model, "resid") else 0.0
        ci = 1.96 * resid_std

        fi = pd.date_range(
            start=LAST_DT + pd.Timedelta(hours=3),
            periods=horizon, freq="3h",
        )
        return pd.DataFrame({
            "datetime":  fi,
            "forecast":  forecast,
            "upper":     forecast + ci,
            "lower":     np.maximum(0, forecast - ci),
        })

    with fc2:
        st.info(
            f"Fitting **{len(fc_cities)} city model(s)** · horizon = **{fc_horizon_days} days** "
            f"({HORIZON} steps @ 3h) · training window = last **{history_months} months**"
        )

    progress = st.progress(0, text="Fitting models…")
    all_fc_traces = []
    errors = []
    for i, city in enumerate(fc_cities):
        progress.progress((i) / len(fc_cities), text=f"Fitting {CITY_LABELS[city]}…")
        fc_df = _fit_forecast(fc_variable, city, HORIZON, SEASONAL_PERIOD, history_months)
        if not fc_df.empty:
            fc_df["city"] = CITY_LABELS[city]
            all_fc_traces.append(fc_df)
        else:
            errors.append(CITY_LABELS[city])
    progress.empty()

    if errors:
        st.warning(f"Insufficient data to model: {', '.join(errors)}")

    if all_fc_traces:
        # ── Plot 1: forecast lines with confidence bands ──────────────────────
        fig_fc = go.Figure()
        palette = px.colors.qualitative.Bold
        for idx, fc_df in enumerate(all_fc_traces):
            colour = palette[idx % len(palette)]
            city_name = fc_df["city"].iloc[0]

            # Historical tail (last 30 days for context)
            hist_tail = src_df[["datetime", fc_df["city"].map(
                {v: k for k, v in CITY_LABELS.items()}).iloc[0]
                if False else fc_cities[idx]
            ]].tail(STEPS_PER_DAY * 30)

            fig_fc.add_trace(go.Scatter(
                x=hist_tail["datetime"], y=hist_tail[fc_cities[idx]],
                name=f"{city_name} (history)",
                line=dict(color=colour, width=1, dash="dot"),
                opacity=0.5,
            ))
            # CI band
            fig_fc.add_trace(go.Scatter(
                x=pd.concat([fc_df["datetime"], fc_df["datetime"].iloc[::-1]]),
                y=pd.concat([fc_df["upper"], fc_df["lower"].iloc[::-1]]),
                fill="toself", fillcolor=colour,
                line=dict(color="rgba(255,255,255,0)"),
                opacity=0.15, showlegend=False, hoverinfo="skip",
            ))
            fig_fc.add_trace(go.Scatter(
                x=fc_df["datetime"], y=fc_df["forecast"],
                name=f"{city_name} (forecast)",
                line=dict(color=colour, width=2),
            ))

        fig_fc.add_vline(
            x=LAST_DT.timestamp() * 1000,
            line_dash="dash", line_color="white",
            annotation_text="Last observation", annotation_position="top left",
        )
        fig_fc.update_layout(
            title=f"{fc_variable.replace('_', ' ').title()} — 7-Day Forecast",
            xaxis_title="Date",
            yaxis_title=f"{fc_variable.replace('_', ' ').title()} ({unit})",
            template="plotly_dark", height=480,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_fc, width="stretch")

        # ── Plot 2: bar chart of forecast mean per city ───────────────────────
        st.subheader("Forecast Summary — Mean Predicted Value per City")
        summary = pd.DataFrame([
            {"City": fc["city"].iloc[0],
             f"Forecast Mean ({unit})": round(fc["forecast"].mean(), 2),
             f"Forecast Max ({unit})":  round(fc["forecast"].max(), 2),
             f"Forecast Min ({unit})":  round(fc["forecast"].min(), 2)}
            for fc in all_fc_traces
        ]).sort_values(f"Forecast Mean ({unit})", ascending=False)

        col_bar, col_tbl = st.columns([3, 2])
        with col_bar:
            fig_bar = px.bar(
                summary, x=f"Forecast Mean ({unit})", y="City",
                orientation="h",
                color=f"Forecast Mean ({unit})",
                color_continuous_scale="Blues",
                title=f"Mean Predicted {fc_variable.replace('_', ' ').title()} (next {fc_horizon_days}d)",
                template="plotly_dark", height=380,
                error_x_minus=summary[f"Forecast Mean ({unit})"] - summary[f"Forecast Min ({unit})"],
                error_x=summary[f"Forecast Max ({unit})"] - summary[f"Forecast Mean ({unit})"],
            )
            fig_bar.update_layout(coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_bar, width="stretch")
        with col_tbl:
            st.dataframe(summary, width="stretch", hide_index=True)

        # ── Download forecast data ────────────────────────────────────────────
        combined_fc = pd.concat(all_fc_traces, ignore_index=True)
        combined_fc["variable"] = fc_variable
        st.download_button(
            label="⬇ Download Forecast CSV",
            data=combined_fc.to_csv(index=False).encode("utf-8"),
            file_name=f"forecast_{fc_variable}_7d.csv",
            mime="text/csv",
        )
    else:
        st.error("No forecasts could be generated. Select at least one city with sufficient data.")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 8 — ML Models
# Two tasks:
#   A) Classification  — predict weather_description from wind/humidity/temperature
#   B) Regression      — predict one numeric variable from the other two
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_ml:
    st.subheader("🤖 ML Model Comparison")
    st.caption(
        "Trains multiple scikit-learn models on the joined weather data for a single city. "
        "Compares classifiers (predict weather condition) and regressors (predict a numeric variable)."
    )

    ml_c1, ml_c2 = st.columns([1, 2])
    with ml_c1:
        ml_city = st.selectbox(
            "City", CITIES, index=CITIES.index("new_york"),
            format_func=lambda c: CITY_LABELS[c], key="ml_city",
        )
        ml_task = st.radio("Task", ["Classification", "Regression"], key="ml_task")
        ml_sample = st.slider(
            "Max training rows", 500, 10000, 3000, 500, key="ml_sample",
            help="Randomly subsample for faster fitting. Use more for better accuracy.",
        )
        if ml_task == "Regression":
            reg_target = st.selectbox(
                "Target variable",
                ["wind_speed", "humidity", "temperature"],
                format_func=lambda v: v.replace("_", " ").title(),
                key="ml_reg_target",
            )
        run_ml = st.button("▶ Run Models", use_container_width=True, key="run_ml")

    with ml_c2:
        st.info(
            "**Classification** — 4 classifiers predict the weather condition label "
            "(from `weather_description`) using wind speed, humidity, and temperature as features.\n\n"
            "**Regression** — 4 regressors predict the chosen numeric variable "
            "from the other two numeric variables + hour-of-day + month."
        )

    if run_ml:
        with st.spinner(f"Loading joined data for {CITY_LABELS[ml_city]}…"):
            ml_df = load_joined(ml_city).dropna()

        if ml_df.empty:
            st.error("No data available for this city.")
        else:
            ml_df["hour"]  = ml_df["datetime"].dt.hour
            ml_df["month"] = ml_df["datetime"].dt.month
            ml_df["dow"]   = ml_df["datetime"].dt.dayofweek

            # Subsample
            if len(ml_df) > ml_sample:
                ml_df = ml_df.sample(ml_sample, random_state=42)

            NUMERIC_FEATURES = ["wind_speed", "humidity", "temperature", "hour", "month", "dow"]

            # ── A: CLASSIFICATION ────────────────────────────────────────────
            if ml_task == "Classification":
                # Drop rare classes that have < 2 samples (stratified split needs ≥ 2)
                desc_col = ml_df["weather_description"].astype(str).str.strip()
                class_counts = desc_col.value_counts()
                valid_classes = class_counts[class_counts >= 2].index
                ml_df_clf = ml_df[desc_col.isin(valid_classes)].copy()

                le = LabelEncoder()
                y = le.fit_transform(ml_df_clf["weather_description"].astype(str).str.strip())
                X = ml_df_clf[NUMERIC_FEATURES].fillna(0)
                scaler = StandardScaler()
                X_s = scaler.fit_transform(X)

                X_train, X_test, y_train, y_test = train_test_split(
                    X_s, y, test_size=0.2, random_state=42, stratify=y
                )

                classifiers = {
                    "Random Forest":      RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1),
                    "Decision Tree":      DecisionTreeClassifier(max_depth=8, random_state=42),
                    "Logistic Regression": LogisticRegression(max_iter=500, random_state=42),
                    "Gradient Boosting":  GradientBoostingRegressor,   # replaced below
                }
                # Gradient Boosting doesn't do multiclass natively — use RF variant
                classifiers = {
                    "Random Forest":       RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1),
                    "Decision Tree":       DecisionTreeClassifier(max_depth=8, random_state=42),
                    "Logistic Regression": LogisticRegression(max_iter=500, random_state=42, C=1.0),
                    "Extra Trees":         __import__('sklearn.ensemble', fromlist=['ExtraTreesClassifier']).ExtraTreesClassifier(
                                               n_estimators=100, random_state=42, n_jobs=-1),
                }

                results = []
                bar = st.progress(0, "Training classifiers…")
                for i, (name, clf) in enumerate(classifiers.items()):
                    bar.progress((i) / len(classifiers), f"Training {name}…")
                    clf.fit(X_train, y_train)
                    acc   = clf.score(X_test, y_test)
                    cv    = cross_val_score(clf, X_s, y, cv=3, scoring="accuracy", n_jobs=-1).mean()
                    results.append({"Model": name, "Test Accuracy": round(acc, 4),
                                    "CV Accuracy (3-fold)": round(cv, 4)})
                bar.empty()

                res_df = pd.DataFrame(results).sort_values("Test Accuracy", ascending=False)
                st.subheader(f"Classification Results — {CITY_LABELS[ml_city]}")
                st.caption(f"Features: {NUMERIC_FEATURES} · Target: weather_description · "
                           f"{len(np.unique(y))} unique classes · {len(X_train)} train / {len(X_test)} test rows")

                # Model comparison bar chart
                fig_acc = px.bar(
                    res_df.melt(id_vars="Model", var_name="Metric", value_name="Score"),
                    x="Score", y="Model", color="Metric", barmode="group",
                    orientation="h", title="Model Accuracy Comparison",
                    template="plotly_dark", height=320,
                    color_discrete_sequence=px.colors.qualitative.Bold,
                    range_x=[0, 1],
                )
                fig_acc.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_acc, width="stretch")
                st.dataframe(res_df, width="stretch", hide_index=True)

                # Confusion matrix for best model
                best_name = res_df.iloc[0]["Model"]
                best_clf = classifiers[best_name]
                best_clf.fit(X_train, y_train)
                y_pred = best_clf.predict(X_test)

                # Limit to top-15 classes by frequency for readability
                top_labels_idx = np.argsort(np.bincount(y))[-15:]
                mask = np.isin(y_test, top_labels_idx) & np.isin(y_pred, top_labels_idx)
                if mask.sum() > 0:
                    cm = confusion_matrix(y_test[mask], y_pred[mask],
                                         labels=top_labels_idx)
                    cm_labels = le.inverse_transform(top_labels_idx)
                    fig_cm = px.imshow(
                        cm, x=cm_labels, y=cm_labels,
                        text_auto=True, color_continuous_scale="Blues",
                        title=f"Confusion Matrix — {best_name} (top 15 classes)",
                        labels={"x": "Predicted", "y": "Actual"},
                        template="plotly_dark",
                        height=520,
                    )
                    fig_cm.update_layout(xaxis_tickangle=-35)
                    st.plotly_chart(fig_cm, width="stretch")

                # Feature importance (tree-based models)
                if hasattr(best_clf, "feature_importances_"):
                    fi_df = pd.DataFrame({
                        "Feature":   NUMERIC_FEATURES,
                        "Importance": best_clf.feature_importances_,
                    }).sort_values("Importance", ascending=True)
                    fig_fi = px.bar(
                        fi_df, x="Importance", y="Feature", orientation="h",
                        title=f"Feature Importance — {best_name}",
                        template="plotly_dark", height=320,
                        color="Importance", color_continuous_scale="Teal",
                    )
                    fig_fi.update_layout(coloraxis_showscale=False)
                    st.plotly_chart(fig_fi, width="stretch")

            # ── B: REGRESSION ────────────────────────────────────────────────
            else:
                feat_cols = [c for c in ["wind_speed", "humidity", "temperature",
                                         "hour", "month", "dow"] if c != reg_target]
                X = ml_df[feat_cols].fillna(0)
                y = ml_df[reg_target].fillna(ml_df[reg_target].median())
                scaler = StandardScaler()
                X_s = scaler.fit_transform(X)

                X_train, X_test, y_train, y_test = train_test_split(
                    X_s, y, test_size=0.2, random_state=42
                )

                regressors = {
                    "Random Forest":      RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
                    "Gradient Boosting":  GradientBoostingRegressor(n_estimators=100, random_state=42),
                    "Ridge Regression":   Ridge(alpha=1.0),
                    "SVR (RBF kernel)":   SVR(kernel="rbf", C=1.0, epsilon=0.2),
                }

                results = []
                preds_dict = {}
                bar = st.progress(0, "Training regressors…")
                for i, (name, reg) in enumerate(regressors.items()):
                    bar.progress(i / len(regressors), f"Training {name}…")
                    reg.fit(X_train, y_train)
                    y_hat = reg.predict(X_test)
                    mae  = mean_absolute_error(y_test, y_hat)
                    rmse = np.sqrt(mean_squared_error(y_test, y_hat))
                    r2   = r2_score(y_test, y_hat)
                    cv_r2 = cross_val_score(reg, X_s, y, cv=3, scoring="r2", n_jobs=-1).mean()
                    results.append({"Model": name,
                                    "MAE":  round(mae,  4),
                                    "RMSE": round(rmse, 4),
                                    "R²":   round(r2,   4),
                                    "CV R² (3-fold)": round(cv_r2, 4)})
                    preds_dict[name] = y_hat
                bar.empty()

                res_df = pd.DataFrame(results).sort_values("R²", ascending=False)
                unit_map = {"wind_speed": "m/s", "humidity": "%", "temperature": "°C"}
                tgt_label = reg_target.replace("_", " ").title()

                st.subheader(f"Regression Results — Predicting {tgt_label} · {CITY_LABELS[ml_city]}")
                st.caption(f"Features: {feat_cols} · {len(X_train)} train / {len(X_test)} test rows")

                # Metric comparison chart
                r2_df = res_df[["Model", "R²", "CV R² (3-fold)"]].melt(
                    id_vars="Model", var_name="Metric", value_name="Score"
                )
                fig_r2 = px.bar(
                    r2_df, x="Score", y="Model", color="Metric", barmode="group",
                    orientation="h", title="R² Score Comparison",
                    template="plotly_dark", height=320,
                    color_discrete_sequence=px.colors.qualitative.Safe,
                )
                fig_r2.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_r2, width="stretch")

                err_df = res_df[["Model", "MAE", "RMSE"]].melt(
                    id_vars="Model", var_name="Metric", value_name="Error"
                )
                fig_err = px.bar(
                    err_df, x="Error", y="Model", color="Metric", barmode="group",
                    orientation="h",
                    title=f"Error Comparison (lower is better) — {unit_map.get(reg_target, '')}",
                    template="plotly_dark", height=320,
                    color_discrete_sequence=["#EF553B", "#FF7F0E"],
                )
                fig_err.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_err, width="stretch")
                st.dataframe(res_df, width="stretch", hide_index=True)

                # Predicted vs Actual for best model
                best_name = res_df.iloc[0]["Model"]
                y_hat_best = preds_dict[best_name]
                pva = pd.DataFrame({"Actual": y_test.values, "Predicted": y_hat_best})
                _sc_pva = pva.sample(min(2000, len(pva)), random_state=42)
                fig_pva = px.scatter(
                    _sc_pva, x="Actual", y="Predicted",
                    title=f"Predicted vs Actual — {best_name}",
                    labels={"Actual": f"Actual {tgt_label} ({unit_map.get(reg_target, '')})",
                            "Predicted": f"Predicted {tgt_label}"},
                    template="plotly_dark", height=420,
                    opacity=0.5,
                )
                # Perfect prediction line
                lo = float(min(pva["Actual"].min(), pva["Predicted"].min()))
                hi = float(max(pva["Actual"].max(), pva["Predicted"].max()))
                fig_pva.add_shape(type="line", x0=lo, y0=lo, x1=hi, y1=hi,
                                  line=dict(color="white", dash="dash"))
                st.plotly_chart(fig_pva, width="stretch")

                # Residual distribution for best model
                residuals = pva["Actual"] - pva["Predicted"]
                fig_resid = px.histogram(
                    residuals, nbins=60,
                    title=f"Residual Distribution — {best_name}",
                    labels={"value": f"Residual ({unit_map.get(reg_target, '')})"},
                    template="plotly_dark", height=320,
                )
                fig_resid.update_traces(marker_color="#636EFA")
                st.plotly_chart(fig_resid, width="stretch")

                # Feature importance
                best_reg = regressors[best_name]
                best_reg.fit(X_train, y_train)
                if hasattr(best_reg, "feature_importances_"):
                    fi_df = pd.DataFrame({
                        "Feature":    feat_cols,
                        "Importance": best_reg.feature_importances_,
                    }).sort_values("Importance", ascending=True)
                    fig_fi = px.bar(
                        fi_df, x="Importance", y="Feature", orientation="h",
                        title=f"Feature Importance — {best_name}",
                        template="plotly_dark", height=320,
                        color="Importance", color_continuous_scale="Teal",
                    )
                    fig_fi.update_layout(coloraxis_showscale=False)
                    st.plotly_chart(fig_fi, width="stretch")
