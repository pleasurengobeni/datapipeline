"""
WASAC Analytics Dashboard
Reads ETL Manager config files + data dictionaries and renders AI-suggested charts.
"""
import json
import os
import urllib.request
import urllib.error
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from cryptography.fernet import Fernet
from sqlalchemy import create_engine, text

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="WASAC Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Navigation hint (shown in sidebar by Streamlit multipage auto-discovery) ──


# ── Environment ───────────────────────────────────────────────────────────────
CONFIG_DIR = Path(os.getenv("CONFIG_DIR", "/app/dags/config"))
AIRFLOW_DB = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres/airflow",
)
FERNET_KEY = os.getenv("AIRFLOW__CORE__FERNET_KEY", "")
ACTIVE_ENV = os.getenv("AIRFLOW_VAR_environment", os.getenv("AIRFLOW_VAR_ENVIRONMENT", "dev"))


# ── Config loading ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def load_all_configs() -> list[dict]:
    configs = []
    for path in sorted(CONFIG_DIR.rglob("*.json")):
        try:
            cfg = json.loads(path.read_text())
            cfg["_file"] = path.name
            configs.append(cfg)
        except Exception:
            pass
    return configs


# ── Airflow connection helpers ─────────────────────────────────────────────────
def _fernet_decrypt(enc: str) -> str:
    if not FERNET_KEY or not enc:
        return enc or ""
    try:
        f = Fernet(FERNET_KEY.encode())
        if enc.startswith("gAAA"):
            return f.decrypt(enc.encode()).decode()
        return enc
    except Exception:
        return enc


@st.cache_data(ttl=300)
def get_airflow_conn(conn_id: str) -> dict | None:
    """Fetch and decrypt an Airflow connection from the metadata DB."""
    if not conn_id or not AIRFLOW_DB:
        return None
    try:
        engine = create_engine(AIRFLOW_DB)
        with engine.connect() as c:
            row = c.execute(
                text(
                    "SELECT conn_type, host, port, schema, login, password "
                    "FROM connection WHERE conn_id = :id"
                ),
                {"id": conn_id},
            ).fetchone()
        if not row:
            return None
        conn_type, host, port, schema, login, password = row
        return {
            "type": conn_type,
            "host": host,
            "port": int(port or 5432),
            "schema": schema,
            "login": login,
            "password": _fernet_decrypt(password or ""),
        }
    except Exception:
        return None


def build_sqlalchemy_uri(conn: dict | None) -> str | None:
    if not conn:
        return None
    return (
        f"postgresql+psycopg2://{conn['login']}:{conn['password']}"
        f"@{conn['host']}:{conn['port']}/{conn['schema']}"
    )


@st.cache_data(ttl=60, show_spinner=False)
def fetch_table(uri: str, schema: str, table: str, limit: int = 1000):
    """Load a target table into a dataframe; return (df, error_str)."""
    try:
        engine = create_engine(uri, connect_args={"connect_timeout": 10})
        with engine.connect() as c:
            df = pd.read_sql(f'SELECT * FROM "{schema}"."{table}" LIMIT {limit}', c)
        return df, None
    except Exception as e:
        return None, str(e)


# ── AI chart suggestions ───────────────────────────────────────────────────────
def _post_json(url: str, payload: dict, headers: dict) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def _strip_fences(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1]
        if raw.lower().startswith("json"):
            raw = raw[4:]
    return raw.strip()


def _extract_nested(obj: dict, path: str):
    for key in path.split("."):
        obj = obj[int(key)] if key.isdigit() else obj[key]
    return obj


def suggest_charts(columns_info: list[dict], sample_df: pd.DataFrame, table: str) -> list[dict]:
    """Ask AI providers (with fallback) for a list of chart specs."""
    col_lines = "\n".join(
        f"- {c['name']} ({c.get('dtype', 'unknown')}): {c.get('description', '')}"
        for c in columns_info
    )
    sample_rows = sample_df.head(5).to_dict(orient="records")

    prompt = (
        f'You are a data analyst. The table "{table}" has these columns:\n{col_lines}\n\n'
        f"Sample rows: {json.dumps(sample_rows, default=str)}\n\n"
        "Suggest 4-6 insightful charts. Return ONLY a JSON array, no markdown, no explanation. "
        "Each item must be an object with:\n"
        '  "type": "bar" | "line" | "pie" | "scatter" | "histogram"\n'
        '  "x": column name (skip for pie)\n'
        '  "y": numeric column name (skip for pie, histogram)\n'
        '  "color": column name or null\n'
        '  "agg": "sum" | "count" | "avg" | "none"\n'
        '  "title": short chart title\n'
        '  "description": one sentence explaining why this chart is useful\n'
        "For pie charts use \"names\" and \"values\" instead of x/y. "
        "For histogram use only \"x\". "
        "Only use column names from the list above."
    )

    chat_payload = lambda k: {
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        "max_tokens": 1000,
        **k,
    }
    chat_path = "choices.0.message.content"

    providers = [
        (
            "GROQ_API_KEY",
            "https://api.groq.com/openai/v1/chat/completions",
            chat_payload({"model": "llama-3.3-70b-versatile"}),
            chat_path,
        ),
        (
            "GOOGLE_AI_API_KEY",
            None,  # URL built below
            {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2, "maxOutputTokens": 1000},
            },
            "candidates.0.content.parts.0.text",
        ),
        (
            "MISTRAL_API_KEY",
            "https://api.mistral.ai/v1/chat/completions",
            chat_payload({"model": "mistral-small-latest"}),
            chat_path,
        ),
        (
            "DEEPSEEK_API_KEY",
            "https://api.deepseek.com/chat/completions",
            chat_payload({"model": "deepseek-chat"}),
            chat_path,
        ),
        (
            "OPENROUTER_API_KEY",
            "https://openrouter.ai/api/v1/chat/completions",
            chat_payload({"model": "meta-llama/llama-3.3-70b-instruct"}),
            chat_path,
        ),
        (
            "CEREBRAS_API_KEY",
            "https://api.cerebras.ai/v1/chat/completions",
            chat_payload({"model": "llama-3.3-70b"}),
            chat_path,
        ),
        (
            "SAMBANOVA_API_KEY",
            "https://api.sambanova.ai/v1/chat/completions",
            chat_payload({"model": "Meta-Llama-3.3-70B-Instruct"}),
            chat_path,
        ),
    ]

    for env_var, url, payload, path in providers:
        api_key = os.getenv(env_var, "")
        if not api_key:
            continue
        try:
            if env_var == "GOOGLE_AI_API_KEY":
                actual_url = (
                    "https://generativelanguage.googleapis.com/v1beta/models/"
                    f"gemini-2.0-flash:generateContent?key={api_key}"
                )
                raw = _extract_nested(_post_json(actual_url, payload, {}), path)
            else:
                raw = _extract_nested(
                    _post_json(url, payload, {"Authorization": f"Bearer {api_key}"}),
                    path,
                )
            charts = json.loads(_strip_fences(raw))
            if isinstance(charts, list) and charts:
                return charts
        except Exception:
            continue

    return []


# ── Chart rendering ─────────────────────────────────────────────────────────────
def render_chart(spec: dict, df: pd.DataFrame) -> None:
    chart_type = spec.get("type", "bar")
    title = spec.get("title", "")
    agg = spec.get("agg", "none")
    valid_cols = set(df.columns)

    def vc(col: str | None) -> bool:
        return bool(col and col in valid_cols)

    try:
        plot_df = df.copy()

        if chart_type == "histogram":
            x = spec.get("x")
            if not vc(x):
                st.warning(f"Column '{x}' not found — skipping: {title}")
                return
            fig = px.histogram(plot_df, x=x, title=title)

        elif chart_type == "pie":
            names = spec.get("names") or spec.get("x")
            values = spec.get("values") or spec.get("y")
            if not vc(names) or not vc(values):
                st.warning(f"Invalid columns for pie chart: {title}")
                return
            if agg in ("sum", "count", "avg"):
                fn = {"sum": "sum", "count": "count", "avg": "mean"}[agg]
                plot_df = getattr(plot_df.groupby(names)[values], fn)().reset_index()
            fig = px.pie(plot_df, names=names, values=values, title=title)

        elif chart_type in ("bar", "line"):
            x, y = spec.get("x"), spec.get("y")
            color = spec.get("color")
            if not vc(x) or not vc(y):
                st.warning(f"Invalid columns for chart: {title}")
                return
            if agg in ("sum", "count", "avg"):
                group = [x] + ([color] if vc(color) else [])
                fn = {"sum": "sum", "count": "count", "avg": "mean"}[agg]
                plot_df = getattr(plot_df.groupby(group)[y], fn)().reset_index()
            kwargs = dict(x=x, y=y, title=title)
            if vc(color):
                kwargs["color"] = color
            fig = (px.bar if chart_type == "bar" else px.line)(plot_df, **kwargs)

        elif chart_type == "scatter":
            x, y = spec.get("x"), spec.get("y")
            color = spec.get("color")
            if not vc(x) or not vc(y):
                st.warning(f"Invalid columns for scatter: {title}")
                return
            kwargs = dict(x=x, y=y, title=title)
            if vc(color):
                kwargs["color"] = color
            fig = px.scatter(plot_df, **kwargs)

        else:
            st.info(f"Unsupported chart type: {chart_type}")
            return

        fig.update_layout(margin=dict(t=40, b=10, l=10, r=10))
        st.plotly_chart(fig, width="stretch")
        if spec.get("description"):
            st.caption(spec["description"])

    except Exception as e:
        st.error(f"Chart render error — {title}: {e}")


# ── Main app ────────────────────────────────────────────────────────────────────
def main() -> None:
    st.title("📊 WASAC Analytics")

    configs = load_all_configs()
    if not configs:
        st.warning("No pipeline configs found. Create pipelines in ETL Manager first.")
        st.stop()

    # ── Sidebar ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Pipelines")
        env = st.radio(
            "Environment",
            ["dev", "prod"],
            index=0 if ACTIVE_ENV == "dev" else 1,
            horizontal=True,
        )

        if st.button("🏠 Home", width="stretch"):
            st.session_state.pop("selected_dag_id", None)

        st.divider()

        # Group configs by target connection
        groups: dict[str, list] = {}
        for cfg in configs:
            tgt_env = cfg.get("target", {})
            conn = (
                tgt_env.get(env, tgt_env.get("dev", {})).get("target_db_conn_id")
                or "unknown"
            )
            groups.setdefault(conn, []).append(cfg)

        for conn_id, cfgs in sorted(groups.items()):
            with st.expander(f"🔌 {conn_id}", expanded=True):
                for cfg in cfgs:
                    dag_id = cfg.get("dag_id", cfg["_file"])
                    tgt = cfg.get("target", {}).get(env, cfg.get("target", {}).get("dev", {}))
                    table = tgt.get("target_table", dag_id)
                    schema = tgt.get("target_schema", "")
                    label = f"{schema}.{table}" if schema else table
                    if st.button(f"📋 {label}", key=f"nav_{dag_id}", width="stretch"):
                        st.session_state["selected_dag_id"] = dag_id
                        # Clear stale chart cache when switching pipelines
                        for k in list(st.session_state.keys()):
                            if k.startswith("charts_") or k == "explore_df":
                                del st.session_state[k]

        st.divider()
        st.caption("WASAC ETL Analytics v1.0")

    # ── Landing page ──────────────────────────────────────────────────────────
    dag_id = st.session_state.get("selected_dag_id")
    if not dag_id:
        st.subheader("Select a pipeline from the sidebar to explore its data")

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Pipelines", len(configs))
        c2.metric(
            "Connections",
            len(
                {
                    c.get("target", {}).get("dev", {}).get("target_db_conn_id", "?")
                    for c in configs
                }
            ),
        )
        c3.metric(
            "File Pipelines",
            sum(1 for c in configs if c.get("pipeline_type") == "file_based"),
        )

        st.divider()
        st.subheader("All Pipelines")
        rows = []
        for cfg in configs:
            tgt = cfg.get("target", {}).get("dev", {})
            rows.append(
                {
                    "Pipeline": cfg.get("dag_id", ""),
                    "Table": f"{tgt.get('target_schema','')}.{tgt.get('target_table','')}",
                    "Connection": tgt.get("target_db_conn_id", ""),
                    "Documented Columns": len(cfg.get("data_dictionary", {})),
                    "Schedule": cfg.get("schedule_interval", ""),
                    "Write Mode": cfg.get("write_mode", ""),
                }
            )
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
        return

    # ── Pipeline detail ───────────────────────────────────────────────────────
    cfg = next((c for c in configs if c.get("dag_id") == dag_id), None)
    if not cfg:
        st.error(f"Config not found for: {dag_id}")
        return

    tgt = cfg.get("target", {}).get(env, cfg.get("target", {}).get("dev", {}))
    conn_id = tgt.get("target_db_conn_id", "")
    schema = tgt.get("target_schema", "public")
    table = tgt.get("target_table", "")
    dd: dict = cfg.get("data_dictionary", {})

    st.header(f"📋 {dag_id}")
    st.caption(
        f"Table: **{schema}.{table}** · Connection: **{conn_id}** · Env: **{env}**"
    )

    # ── Load data ─────────────────────────────────────────────────────────────
    conn_details = get_airflow_conn(conn_id)
    uri = build_sqlalchemy_uri(conn_details)
    df: pd.DataFrame | None = None

    if uri and table:
        with st.spinner("Connecting and loading data…"):
            df, err = fetch_table(uri, schema, table)
        if err:
            st.error(f"Could not load data: {err}")

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_overview, tab_charts, tab_explore = st.tabs(
        ["📄 Overview", "🤖 AI Charts", "🔍 Explore"]
    )

    # ── Overview ─────────────────────────────────────────────────────────────
    with tab_overview:
        left, right = st.columns([1, 1])

        with left:
            st.subheader("Data Dictionary")
            if dd:
                dd_rows = []
                for col_name, meta in dd.items():
                    if isinstance(meta, dict):
                        dd_rows.append(
                            {
                                "Column": col_name,
                                "Type": meta.get("dtype", ""),
                                "Description": meta.get("description", ""),
                            }
                        )
                    else:
                        dd_rows.append(
                            {"Column": col_name, "Type": "", "Description": str(meta)}
                        )
                st.dataframe(
                    pd.DataFrame(dd_rows), width="stretch", hide_index=True
                )
            else:
                st.info(
                    "No data dictionary defined yet. "
                    "Open this pipeline in ETL Manager and add column descriptions."
                )

        with right:
            st.subheader("Pipeline Config")
            st.json(
                {
                    "dag_id": dag_id,
                    "schedule": cfg.get("schedule_interval"),
                    "write_mode": cfg.get("write_mode"),
                    "source": cfg.get("source", {}).get(env, {}),
                    "target": tgt,
                }
            )

        if df is not None:
            st.divider()
            st.subheader(f"Data Preview — {len(df):,} rows loaded")
            st.dataframe(df, width="stretch")

    # ── AI Charts ─────────────────────────────────────────────────────────────
    with tab_charts:
        if df is None:
            st.warning("No data available — check the database connection above.")
        else:
            # Build column metadata: data dictionary + fallback to pandas dtype
            columns_info = []
            for col_name in df.columns:
                meta = dd.get(col_name, {})
                if isinstance(meta, dict):
                    columns_info.append(
                        {
                            "name": col_name,
                            "dtype": meta.get("dtype") or str(df[col_name].dtype),
                            "description": meta.get("description", ""),
                        }
                    )
                else:
                    columns_info.append(
                        {
                            "name": col_name,
                            "dtype": str(df[col_name].dtype),
                            "description": str(meta) if meta else "",
                        }
                    )

            cache_key = f"charts_{dag_id}_{env}"

            btn_col, _ = st.columns([1, 3])
            if btn_col.button("🤖 Suggest Charts with AI", width="stretch"):
                with st.spinner("Asking AI for chart recommendations…"):
                    charts = suggest_charts(
                        columns_info, df, f"{schema}.{table}"
                    )
                if charts:
                    st.session_state[cache_key] = charts
                else:
                    st.error(
                        "All AI providers failed or returned no suggestions. "
                        "Check that at least one API key is configured."
                    )

            charts: list = st.session_state.get(cache_key, [])

            if not charts:
                st.info(
                    'Click **"Suggest Charts with AI"** to generate chart recommendations '
                    "based on the data dictionary and table contents."
                )
            else:
                st.success(f"AI suggested {len(charts)} charts")
                # Render in a 2-column grid
                pairs = [charts[i : i + 2] for i in range(0, len(charts), 2)]
                for pair in pairs:
                    cols = st.columns(len(pair))
                    for col, spec in zip(cols, pair):
                        with col:
                            render_chart(spec, df)

    # ── Explore ───────────────────────────────────────────────────────────────
    with tab_explore:
        st.subheader("Custom Query")
        default_sql = f'SELECT *\nFROM "{schema}"."{table}"\nLIMIT 100'
        sql = st.text_area("SQL", value=default_sql, height=120, key="explore_sql")

        run_btn, _ = st.columns([1, 4])
        if run_btn.button("▶ Run Query", width="stretch"):
            if not uri:
                st.error("No database connection available for this pipeline.")
            else:
                with st.spinner("Running…"):
                    try:
                        engine = create_engine(uri, connect_args={"connect_timeout": 10})
                        with engine.connect() as c:
                            result_df = pd.read_sql(sql, c)
                        st.session_state["explore_df"] = result_df
                    except Exception as e:
                        st.error(f"Query failed: {e}")
                        st.session_state.pop("explore_df", None)

        result_df: pd.DataFrame | None = st.session_state.get("explore_df")
        if result_df is not None:
            st.dataframe(result_df, width="stretch")
            st.divider()

            st.subheader("Quick Chart")
            all_cols = result_df.columns.tolist()
            num_cols = result_df.select_dtypes(include="number").columns.tolist()

            if len(all_cols) >= 2:
                qc1, qc2, qc3, qc4, qc5 = st.columns(5)
                chart_type = qc1.selectbox(
                    "Type", ["bar", "line", "scatter", "pie", "histogram"], key="qc_type"
                )
                x_col = qc2.selectbox("X axis", all_cols, key="qc_x")
                y_options = num_cols if num_cols else all_cols
                y_col = qc3.selectbox(
                    "Y axis", y_options, index=min(1, len(y_options) - 1), key="qc_y"
                )
                color_options = ["(none)"] + all_cols
                color_sel = qc4.selectbox("Color", color_options, key="qc_color")
                color_col = None if color_sel == "(none)" else color_sel

                if qc5.button("📈 Plot", width="stretch"):
                    try:
                        kwargs: dict = dict(title=f"{y_col} by {x_col}")
                        if chart_type == "bar":
                            fig = px.bar(result_df, x=x_col, y=y_col, color=color_col, **kwargs)
                        elif chart_type == "line":
                            fig = px.line(result_df, x=x_col, y=y_col, color=color_col, **kwargs)
                        elif chart_type == "scatter":
                            fig = px.scatter(result_df, x=x_col, y=y_col, color=color_col, **kwargs)
                        elif chart_type == "pie":
                            fig = px.pie(result_df, names=x_col, values=y_col, **kwargs)
                        elif chart_type == "histogram":
                            fig = px.histogram(result_df, x=x_col, **kwargs)
                        st.plotly_chart(fig, width="stretch")
                    except Exception as e:
                        st.error(f"Chart error: {e}")


if __name__ == "__main__":
    main()
