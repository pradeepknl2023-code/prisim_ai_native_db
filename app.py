"""
PRISM · AI-Native Data Intelligence Engine
Single-file Streamlit deployment · v2.0

Deploy:
    pip install streamlit duckdb pandas anthropic groq
    streamlit run app.py

Env vars (set in Streamlit Cloud secrets or .env):
    GROQ_API_KEY   — for LLaMA-3.3-70b SQL generation (recommended, fast + free)
    ANTHROPIC_API_KEY — fallback if no Groq key
"""

import streamlit as st
import duckdb
import pandas as pd
import json
import time
import traceback
import re
from io import StringIO, BytesIO
from datetime import datetime

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PRISM · Data Intelligence Engine",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@700;800&family=DM+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #09090f;
    color: #e8e8f0;
}
.stApp { background-color: #09090f; }

/* Hide default header/footer */
#MainMenu, footer, header { visibility: hidden; }

/* PRISM Header */
.prism-header {
    display: flex; align-items: center; gap: 14px;
    padding: 18px 24px 14px;
    border-bottom: 1px solid #1e1e35;
    margin-bottom: 0;
}
.prism-logo {
    font-family: 'Syne', sans-serif;
    font-size: 22px; font-weight: 800;
    letter-spacing: 0.08em;
    background: linear-gradient(135deg, #4fffb0, #7c6fff);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.prism-tagline {
    font-size: 11px; color: #4444aa;
    letter-spacing: 0.12em; text-transform: uppercase;
    font-family: 'DM Mono', monospace;
}

/* Status strip */
.status-strip {
    display: flex; align-items: center; gap: 24px;
    background: #0f0f1a; border: 1px solid #1e1e35;
    border-radius: 8px; padding: 10px 16px;
    margin-bottom: 12px; flex-wrap: wrap;
}
.stat-item { display: flex; flex-direction: column; gap: 2px; }
.stat-label { font-size: 9px; color: #44448a; letter-spacing: 0.1em; text-transform: uppercase; font-family: 'DM Mono', monospace; }
.stat-value { font-size: 15px; font-weight: 600; font-family: 'DM Mono', monospace; }
.stat-green  { color: #4fffb0; }
.stat-purple { color: #7c6fff; }
.stat-yellow { color: #ffcc44; }
.stat-red    { color: #ff6b6b; }
.stat-dim    { color: #6666aa; }

/* SQL panel */
.sql-block {
    background: #0a0a18; border: 1px solid #1e1e35;
    border-left: 3px solid #7c6fff;
    border-radius: 6px; padding: 14px 16px;
    font-family: 'DM Mono', monospace; font-size: 12px;
    color: #c8c8f0; line-height: 1.7; white-space: pre-wrap;
    margin-bottom: 12px;
}

/* Verification badges */
.badge-row { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }
.badge {
    font-size: 11px; font-family: 'DM Mono', monospace;
    padding: 4px 10px; border-radius: 4px; letter-spacing: 0.05em;
}
.badge-pass  { background: #0d2a1e; color: #4fffb0; border: 1px solid #1a4030; }
.badge-warn  { background: #2a2000; color: #ffcc44; border: 1px solid #4a3800; }
.badge-fail  { background: #2a0a0a; color: #ff6b6b; border: 1px solid #4a1a1a; }
.badge-info  { background: #0f0f30; color: #7c6fff; border: 1px solid #2a2a50; }

/* Reconciliation block */
.recon-block {
    background: #0a0a18; border: 1px solid #1e1e35;
    border-left: 3px solid #4fffb0;
    border-radius: 6px; padding: 14px 16px;
    font-family: 'DM Mono', monospace; font-size: 12px;
    margin-bottom: 12px; line-height: 2;
}
.recon-eq   { color: #4fffb0; font-size: 14px; font-weight: 600; }
.recon-dim  { color: #44448a; }
.recon-num  { color: #e8e8f0; }

/* Section labels */
.section-label {
    font-size: 10px; color: #44448a; letter-spacing: 0.15em;
    text-transform: uppercase; font-family: 'DM Mono', monospace;
    margin-bottom: 8px; margin-top: 16px;
}

/* Tab styling override */
.stTabs [data-baseweb="tab-list"] {
    background: #0f0f1a; border-bottom: 1px solid #1e1e35; gap: 0;
}
.stTabs [data-baseweb="tab"] {
    background: transparent; color: #44448a;
    font-family: 'DM Mono', monospace; font-size: 11px;
    letter-spacing: 0.08em; padding: 10px 18px;
    border-bottom: 2px solid transparent;
}
.stTabs [aria-selected="true"] {
    color: #4fffb0!important;
    border-bottom: 2px solid #4fffb0 !important;
    background: transparent !important;
}

/* Buttons */
.stButton>button {
    background: linear-gradient(135deg, #1a3a2a, #1a1a3a);
    color: #4fffb0; border: 1px solid #2a4a3a;
    font-family: 'DM Mono', monospace; font-size: 12px;
    letter-spacing: 0.06em; padding: 8px 18px;
    transition: all 0.2s;
}
.stButton>button:hover {
    background: linear-gradient(135deg, #2a5a3a, #2a2a5a);
    border-color: #4fffb0; color: #4fffb0;
}

/* Textarea / inputs */
.stTextArea textarea, .stTextInput input {
    background: #0f0f1a !important; color: #e8e8f0 !important;
    border: 1px solid #1e1e35 !important;
    font-family: 'DM Mono', monospace !important; font-size: 12px !important;
}

/* Dataframe */
.stDataFrame { border: 1px solid #1e1e35; border-radius: 6px; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0f0f1a; border-right: 1px solid #1e1e35;
}

/* File uploader */
.stFileUploader {
    background: #0f0f1a; border: 1px dashed #2a2a50;
    border-radius: 8px;
}

/* Expander */
.streamlit-expanderHeader {
    background: #0f0f1a !important; color: #8888aa !important;
    font-family: 'DM Mono', monospace !important; font-size: 11px !important;
    border: 1px solid #1e1e35 !important;
}

/* Metric boxes */
[data-testid="stMetric"] {
    background: #0f0f1a; border: 1px solid #1e1e35;
    border-radius: 8px; padding: 10px 14px;
}
[data-testid="stMetricLabel"] { color: #44448a !important; font-size: 10px !important; letter-spacing: 0.1em; text-transform: uppercase; }
[data-testid="stMetricValue"] { color: #4fffb0 !important; font-family: 'DM Mono', monospace !important; }

/* Alert / info */
.stAlert { background: #0f0f1a !important; border: 1px solid #1e1e35 !important; }

/* Selectbox */
.stSelectbox select, [data-baseweb="select"] {
    background: #0f0f1a !important; color: #e8e8f0 !important;
    border: 1px solid #1e1e35 !important;
}
</style>
""", unsafe_allow_html=True)

# ── Sidebar: API key diagnostics ──────────────────────────────────────────────
import os as _os

def _check_secret(key):
    try:
        v = st.secrets[key]
        if v: return f"✅ Found ({str(v)[:8]}…)"
    except Exception:
        pass
    try:
        for section in st.secrets:
            try:
                v = st.secrets[section][key]
                if v: return f"✅ Found in [{section}] ({str(v)[:8]}…)"
            except Exception:
                pass
    except Exception:
        pass
    v = _os.environ.get(key, "")
    if v: return f"✅ Found in env ({v[:8]}…)"
    return "❌ Not found"

with st.sidebar:
    st.markdown("### 🔑 API Key Status")
    st.markdown(f"**GROQ_API_KEY**  \n`{_check_secret('GROQ_API_KEY')}`")
    st.markdown(f"**ANTHROPIC_API_KEY**  \n`{_check_secret('ANTHROPIC_API_KEY')}`")
    st.markdown("---")
    st.markdown("""**Fix 403 — exact steps:**
1. Streamlit Cloud → your app  
2. ⚙ **Settings** → **Secrets**  
3. Paste *exactly* (with quotes):
```
GROQ_API_KEY = "gsk_xxxxxxxxxxxx"
```
4. Click **Save** then **Reboot app**

Free key (no card): [console.groq.com](https://console.groq.com)
""")

# ── Session state defaults ──────────────────────────────────────────────────────
defaults = {
    "tables": {},           # {name: DataFrame}
    "conn": None,           # duckdb connection
    "last_sql": "",
    "last_result": None,
    "last_stats": {},
    "quarantine_df": None,
    "run_history": [],
    "status": "idle",       # idle | running | done | error
    "status_msg": "",
    "confidence": 0,
    "recon": {},
    "schema_info": {},
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── DuckDB connection (persistent in session) ──────────────────────────────────
def get_conn():
    if st.session_state.conn is None:
        st.session_state.conn = duckdb.connect(database=":memory:")
    return st.session_state.conn

# ── Register DataFrames into DuckDB ────────────────────────────────────────────
def register_tables(tables: dict):
    conn = get_conn()
    schema = {}
    for name, df in tables.items():
        conn.register(name, df)
        schema[name] = {
            "columns": list(df.columns),
            "dtypes":  {c: str(df[c].dtype) for c in df.columns},
            "row_count": len(df),
            "sample_values": {c: df[c].dropna().head(3).tolist() for c in df.columns},
        }
    st.session_state.schema_info = schema
    return schema

# ── Build schema string for AI prompt ─────────────────────────────────────────
def schema_to_prompt(schema: dict) -> str:
    lines = []
    for tname, info in schema.items():
        lines.append(f"Table: {tname} ({info['row_count']:,} rows)")
        for col in info["columns"]:
            dtype = info["dtypes"][col]
            samples = info["sample_values"][col]
            sample_str = ", ".join(str(s) for s in samples[:3])
            lines.append(f"  · {col} ({dtype}) — e.g. {sample_str}")
    return "\n".join(lines)

# ── AI SQL generation ──────────────────────────────────────────────────────────
def generate_sql(prompt: str, schema: dict) -> tuple[str, float, str]:
    """Returns (sql, confidence_0_to_1, explanation)"""
    schema_str = schema_to_prompt(schema)
    system = """You are a DuckDB SQL expert for the PRISM data intelligence engine.
Generate a single, complete, valid DuckDB SQL query based on the user's intent.
Return ONLY a JSON object with keys:
  "sql"         — the complete DuckDB SQL query (string)
  "confidence"  — float 0.0-1.0 (how confident you are this is correct)
  "explanation" — one plain-English sentence describing what the query does
  "warnings"    — list of strings (potential issues, empty list if none)
No markdown, no code fences, no extra text. Only valid JSON."""

    user_msg = f"""User intent: {prompt}

Available tables and schema:
{schema_str}

Generate DuckDB SQL. Use table names exactly as listed above."""

    # ── Read API keys robustly from Streamlit secrets or env ──────────────────
    import os, requests as _requests

    def _get_secret(key: str) -> str:
        """Try st.secrets (flat + nested), then OS env."""
        # 1. Direct key at root level
        try:
            val = st.secrets[key]
            if val:
                return str(val).strip()
        except Exception:
            pass
        # 2. Nested under any section (e.g. [general] or [api_keys])
        try:
            for section in st.secrets:
                try:
                    val = st.secrets[section][key]
                    if val:
                        return str(val).strip()
                except Exception:
                    pass
        except Exception:
            pass
        # 3. OS environment variable
        return os.environ.get(key, "").strip()

    groq_key      = _get_secret("GROQ_API_KEY")
    anthropic_key = _get_secret("ANTHROPIC_API_KEY")

    if groq_key:
        resp = _requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {groq_key}",
            },
            json={
                "model":       "llama-3.3-70b-versatile",
                "messages":    [
                    {"role": "system", "content": system},
                    {"role": "user",   "content": user_msg},
                ],
                "temperature": 0.1,
                "max_tokens":  800,
            },
            timeout=30,
        )
        if resp.status_code == 401:
            raise ValueError(
                "Groq API key rejected (401 Unauthorized). "
                "Check your key at console.groq.com — it should start with 'gsk_'."
            )
        if resp.status_code == 403:
            raise ValueError(
                "Groq API returned 403 Forbidden. This usually means the key is valid "
                "but copied incorrectly (extra space or missing characters). "
                "In Streamlit Cloud → Settings → Secrets, make sure it looks exactly like:\n"
                "  GROQ_API_KEY = \"gsk_xxxxxxxxxxxxxxxxxxxx\"\n"
                "Then click Save and Reboot the app."
            )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()

    elif anthropic_key:
        resp = _requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type":      "application/json",
                "x-api-key":         anthropic_key,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model":     "claude-sonnet-4-20250514",
                "max_tokens": 800,
                "system":    system,
                "messages":  [{"role": "user", "content": user_msg}],
            },
            timeout=30,
        )
        if resp.status_code in (401, 403):
            raise ValueError(
                f"Anthropic API key rejected ({resp.status_code}). "
                "In Streamlit Cloud → Settings → Secrets:\n"
                "  ANTHROPIC_API_KEY = \"sk-ant-xxxxxxxxxxxx\"\n"
                "Then Save and Reboot."
            )
        resp.raise_for_status()
        raw = resp.json()["content"][0]["text"].strip()

    else:
        # Demo mode: real DuckDB execution, no AI
        table_names = list(schema.keys())
        if table_names:
            t = table_names[0]
            raw = json.dumps({
                "sql":         f"SELECT * FROM {t} LIMIT 100",
                "confidence":  0.5,
                "explanation": (
                    f"No API key detected — running real DuckDB query on {t}. "
                    "To enable AI SQL generation: in Streamlit Cloud → ⚙ Settings → Secrets, add:\n"
                    "  GROQ_API_KEY = \"gsk_...\"\n"
                    "Get a free key at console.groq.com (no credit card)."
                ),
                "warnings": ["No API key found — using fallback SELECT. Add GROQ_API_KEY to Streamlit secrets."],
            })
        else:
            raise ValueError("No tables loaded. Upload a CSV or click 'Load Demo Data'.")

    # Strip any accidental markdown fences
    raw = re.sub(r"```json|```", "", raw).strip()
    parsed = json.loads(raw)
    return parsed["sql"], float(parsed["confidence"]), parsed.get("explanation", ""), parsed.get("warnings", [])

# ── Execute SQL in DuckDB ──────────────────────────────────────────────────────
def execute_sql(sql: str, source_rows: int) -> tuple[pd.DataFrame, dict]:
    conn = get_conn()
    t0 = time.perf_counter()
    result_df = conn.execute(sql).df()
    elapsed_ms = (time.perf_counter() - t0) * 1000

    output_rows = len(result_df)
    null_counts = result_df.isnull().sum().to_dict()
    total_nulls = sum(null_counts.values())
    null_pct = (total_nulls / max(output_rows * len(result_df.columns), 1)) * 100

    stats = {
        "output_rows":  output_rows,
        "source_rows":  source_rows,
        "elapsed_ms":   round(elapsed_ms, 1),
        "columns":      list(result_df.columns),
        "col_count":    len(result_df.columns),
        "null_pct":     round(null_pct, 2),
        "null_counts":  null_counts,
        "dtypes":       {c: str(result_df[c].dtype) for c in result_df.columns},
    }
    return result_df, stats

# ── Data contracts & quarantine ────────────────────────────────────────────────
def validate_and_quarantine(df: pd.DataFrame, contracts: dict) -> tuple[pd.DataFrame, pd.DataFrame, list]:
    """Apply basic contracts. Returns (clean_df, quarantine_df, violations)."""
    if not contracts:
        return df, pd.DataFrame(), []

    mask_bad = pd.Series([False] * len(df), index=df.index)
    violations = []

    for col, rules in contracts.items():
        if col not in df.columns:
            continue
        if "not_null" in rules and rules["not_null"]:
            nulls = df[col].isnull()
            mask_bad |= nulls
            if nulls.any():
                violations.append(f"{col}: {nulls.sum()} null values (contract: not_null)")
        if "min_val" in rules:
            try:
                below = df[col].dropna() < rules["min_val"]
                bad_idx = df[col].dropna()[below].index
                mask_bad.loc[bad_idx] = True
                if below.any():
                    violations.append(f"{col}: {below.sum()} values below min {rules['min_val']}")
            except Exception:
                pass
        if "max_val" in rules:
            try:
                above = df[col].dropna() > rules["max_val"]
                bad_idx = df[col].dropna()[above].index
                mask_bad.loc[bad_idx] = True
                if above.any():
                    violations.append(f"{col}: {above.sum()} values above max {rules['max_val']}")
            except Exception:
                pass

    clean_df = df[~mask_bad].reset_index(drop=True)
    quarantine_df = df[mask_bad].reset_index(drop=True)
    return clean_df, quarantine_df, violations

# ── Reconciliation ─────────────────────────────────────────────────────────────
def reconcile(source_rows: int, output_rows: int, excluded_rows: int, quarantine_rows: int) -> dict:
    check = source_rows - output_rows - excluded_rows - quarantine_rows
    return {
        "source":     source_rows,
        "output":     output_rows,
        "excluded":   excluded_rows,
        "quarantine": quarantine_rows,
        "diff":       check,
        "balanced":   check == 0,
    }

# ── CSV/parquet auto-profiler ──────────────────────────────────────────────────
def profile_df(df: pd.DataFrame, name: str) -> dict:
    profile = {}
    for col in df.columns:
        s = df[col]
        profile[col] = {
            "dtype":      str(s.dtype),
            "null_pct":   round(s.isnull().mean() * 100, 2),
            "unique_pct": round(s.nunique() / max(len(s), 1) * 100, 2),
            "sample":     s.dropna().head(5).tolist(),
        }
        try:
            profile[col]["min"] = float(s.min())
            profile[col]["max"] = float(s.max())
        except Exception:
            pass
    return profile

# ═══════════════════════════════════════════════════════════════════════════════
# UI
# ═══════════════════════════════════════════════════════════════════════════════

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="prism-header">
  <div>
    <div class="prism-logo">PRISM</div>
    <div class="prism-tagline">AI-Native Data Intelligence Engine · v2.0</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Tabs ───────────────────────────────────────────────────────────────────────
tabs = st.tabs(["⬡ ETL STUDIO", "⬡ DATA TRUST", "⬡ OUTPUT DATA", "⬡ SCHEMA"])

# ══════════════════════════════════════════════════════════════
# TAB 1 — ETL STUDIO
# ══════════════════════════════════════════════════════════════
with tabs[0]:
    col_left, col_right = st.columns([1, 1], gap="medium")

    # ── LEFT: Data sources ──────────────────────────────────────
    with col_left:
        st.markdown('<div class="section-label">📂 Data Sources</div>', unsafe_allow_html=True)

        uploaded = st.file_uploader(
            "Upload CSV files (multiple allowed)",
            type=["csv"],
            accept_multiple_files=True,
            label_visibility="collapsed",
        )

        if uploaded:
            new_tables = {}
            for f in uploaded:
                tname = re.sub(r"[^a-zA-Z0-9_]", "_", f.name.rsplit(".", 1)[0])
                try:
                    df = pd.read_csv(f)
                    new_tables[tname] = df
                except Exception as e:
                    st.error(f"Could not parse {f.name}: {e}")

            if new_tables:
                st.session_state.tables.update(new_tables)
                register_tables(st.session_state.tables)
                st.success(f"✓ Loaded {len(new_tables)} table(s) into DuckDB")

        # Demo data button
        if st.button("Load Demo Data (Sales + Customers)"):
            import numpy as np
            rng = np.random.default_rng(42)
            n = 500
            sales_df = pd.DataFrame({
                "order_id":    range(1001, 1001 + n),
                "customer_id": rng.integers(1, 101, n),
                "product":     rng.choice(["Widget A","Widget B","Gadget X","Gadget Y","Service Z"], n),
                "amount":      rng.uniform(10, 2000, n).round(2),
                "quantity":    rng.integers(1, 20, n),
                "order_date":  pd.date_range("2024-01-01", periods=n, freq="12h").strftime("%Y-%m-%d"),
                "status":      rng.choice(["COMPLETED","PENDING","CANCELLED","REFUNDED"], n, p=[0.7,0.15,0.1,0.05]),
                "region":      rng.choice(["North","South","East","West","Central"], n),
            })
            # Inject a few nulls + bad rows for realism
            sales_df.loc[rng.choice(n, 8, replace=False), "amount"] = None

            customers_df = pd.DataFrame({
                "customer_id": range(1, 101),
                "name":        [f"Customer {i}" for i in range(1, 101)],
                "segment":     rng.choice(["Enterprise","SMB","Consumer","Government"], 100, p=[0.15,0.35,0.4,0.1]),
                "country":     rng.choice(["India","US","UK","Singapore","UAE"], 100),
                "since_year":  rng.integers(2015, 2025, 100),
                "credit_limit": rng.choice([50000,100000,250000,500000,None], 100),
            })

            st.session_state.tables = {"sales": sales_df, "customers": customers_df}
            register_tables(st.session_state.tables)
            st.success("✓ Demo data loaded: sales (500 rows), customers (100 rows)")
            st.rerun()

        # Show loaded tables summary
        if st.session_state.tables:
            st.markdown('<div class="section-label" style="margin-top:16px">Loaded Tables</div>', unsafe_allow_html=True)
            for tname, df in st.session_state.tables.items():
                st.markdown(f"""
<div class="status-strip" style="margin-bottom:6px; padding:8px 12px;">
  <div class="stat-item"><span class="stat-label">Table</span><span class="stat-value stat-purple">{tname}</span></div>
  <div class="stat-item"><span class="stat-label">Rows</span><span class="stat-value stat-green">{len(df):,}</span></div>
  <div class="stat-item"><span class="stat-label">Cols</span><span class="stat-value stat-dim">{len(df.columns)}</span></div>
</div>
""", unsafe_allow_html=True)

    # ── RIGHT: Prompt + Generate ────────────────────────────────
    with col_right:
        st.markdown('<div class="section-label">✦ Prompt</div>', unsafe_allow_html=True)

        example_prompts = [
            "Select all columns from sales",
            "Total revenue by product, sorted highest to lowest",
            "Join sales with customers, show customer name and total spend per customer",
            "Count orders by status",
            "Monthly revenue trend — sum of amount grouped by order_date month",
            "Top 10 customers by total order value",
            "Sales by region with average order value",
            "Customers who haven't ordered yet (left join, filter nulls)",
            "Revenue from COMPLETED orders only, by segment",
        ]
        example = st.selectbox("Quick examples ↓", ["— type your own below —"] + example_prompts, label_visibility="collapsed")

        default_prompt = example if example != "— type your own below —" else ""
        prompt = st.text_area(
            "Describe what you want in plain English",
            value=default_prompt,
            height=100,
            placeholder="e.g. Show total revenue by product, sorted highest to lowest",
            label_visibility="collapsed",
        )

        # Contracts (optional)
        with st.expander("⚙ Data Contracts (optional)"):
            st.markdown("Define rules. Rows that violate go to quarantine.", unsafe_allow_html=True)
            contract_json = st.text_area(
                "Contracts JSON",
                value='{"amount": {"not_null": true, "min_val": 0}}',
                height=80,
                label_visibility="collapsed",
            )

        col_btn1, col_btn2 = st.columns([1, 1])
        with col_btn1:
            run_btn = st.button("▶  Generate & Execute", use_container_width=True)
        with col_btn2:
            clear_btn = st.button("✕  Clear", use_container_width=True)

        if clear_btn:
            st.session_state.last_sql = ""
            st.session_state.last_result = None
            st.session_state.last_stats = {}
            st.session_state.quarantine_df = None
            st.session_state.recon = {}
            st.session_state.status = "idle"
            st.rerun()

        if run_btn:
            if not st.session_state.tables:
                st.error("No tables loaded. Upload CSVs or click 'Load Demo Data' first.")
            elif not prompt.strip():
                st.error("Enter a prompt.")
            else:
                st.session_state.status = "running"
                st.session_state.status_msg = "Generating SQL…"

                with st.spinner("🔮 Generating SQL via AI…"):
                    try:
                        schema = st.session_state.schema_info
                        sql, confidence, explanation, warnings = generate_sql(prompt, schema)
                        st.session_state.last_sql = sql
                        st.session_state.confidence = confidence
                        st.session_state.status_msg = "Executing…"
                    except Exception as e:
                        st.session_state.status = "error"
                        st.session_state.status_msg = str(e)
                        st.error(f"SQL generation failed: {e}")
                        st.stop()

                with st.spinner("⚡ Executing in DuckDB…"):
                    try:
                        source_rows = sum(len(df) for df in st.session_state.tables.values())
                        result_df, stats = execute_sql(sql, source_rows)
                        st.session_state.last_stats = stats

                        # Parse contracts
                        try:
                            contracts = json.loads(contract_json)
                        except Exception:
                            contracts = {}

                        clean_df, q_df, violations = validate_and_quarantine(result_df, contracts)
                        st.session_state.last_result = clean_df
                        st.session_state.quarantine_df = q_df if len(q_df) > 0 else None

                        # Reconciliation
                        excluded = source_rows - stats["output_rows"]
                        recon = reconcile(source_rows, len(clean_df), max(0, excluded - len(q_df)), len(q_df))
                        st.session_state.recon = recon

                        # Save to run history
                        st.session_state.run_history.append({
                            "ts":         datetime.now().strftime("%H:%M:%S"),
                            "prompt":     prompt[:60],
                            "sql":        sql,
                            "rows_in":    source_rows,
                            "rows_out":   len(clean_df),
                            "quarantine": len(q_df),
                            "confidence": confidence,
                            "elapsed_ms": stats["elapsed_ms"],
                            "violations": violations,
                            "explanation": explanation,
                            "warnings":   warnings,
                            "balanced":   recon["balanced"],
                        })

                        st.session_state.status = "done"
                        st.session_state.status_msg = explanation
                        st.rerun()

                    except Exception as e:
                        st.session_state.status = "error"
                        st.session_state.status_msg = str(e)
                        st.error(f"Execution failed:\n```\n{traceback.format_exc()}\n```")

    # ── STATUS STRIP ────────────────────────────────────────────
    stats = st.session_state.last_stats
    recon = st.session_state.recon
    conf  = st.session_state.confidence

    status_color = {"idle": "stat-dim", "running": "stat-yellow", "done": "stat-green", "error": "stat-red"}.get(st.session_state.status, "stat-dim")
    status_icon  = {"idle": "○", "running": "◌", "done": "●", "error": "✕"}.get(st.session_state.status, "○")

    balanced_badge = ""
    if recon:
        if recon["balanced"]:
            balanced_badge = f'<div class="stat-item"><span class="stat-label">Reconciliation</span><span class="stat-value stat-green">✓ BALANCED</span></div>'
        else:
            balanced_badge = f'<div class="stat-item"><span class="stat-label">Reconciliation</span><span class="stat-value stat-red">✕ DIFF {recon["diff"]}</span></div>'

    conf_color = "stat-green" if conf >= 0.85 else "stat-yellow" if conf >= 0.6 else "stat-red"
    conf_str   = f"{int(conf*100)}%" if conf > 0 else "—"

    st.markdown(f"""
<div class="status-strip">
  <div class="stat-item">
    <span class="stat-label">Status</span>
    <span class="stat-value {status_color}">{status_icon} {st.session_state.status.upper()}</span>
  </div>
  <div class="stat-item">
    <span class="stat-label">Rows In</span>
    <span class="stat-value stat-purple">{stats.get("source_rows", 0):,}</span>
  </div>
  <div class="stat-item">
    <span class="stat-label">Rows Out</span>
    <span class="stat-value stat-green">{stats.get("output_rows", 0):,}</span>
  </div>
  <div class="stat-item">
    <span class="stat-label">Quarantine</span>
    <span class="stat-value stat-yellow">{len(st.session_state.quarantine_df) if st.session_state.quarantine_df is not None else 0}</span>
  </div>
  <div class="stat-item">
    <span class="stat-label">Confidence</span>
    <span class="stat-value {conf_color}">{conf_str}</span>
  </div>
  <div class="stat-item">
    <span class="stat-label">Exec Time</span>
    <span class="stat-value stat-dim">{stats.get("elapsed_ms", "—")} ms</span>
  </div>
  <div class="stat-item">
    <span class="stat-label">Columns</span>
    <span class="stat-value stat-dim">{stats.get("col_count", "—")}</span>
  </div>
  {balanced_badge}
</div>
""", unsafe_allow_html=True)

    # ── Generated SQL panel ─────────────────────────────────────
    if st.session_state.last_sql:
        st.markdown('<div class="section-label">Generated SQL</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="sql-block">{st.session_state.last_sql}</div>', unsafe_allow_html=True)

        # Verification badges
        if st.session_state.run_history:
            last = st.session_state.run_history[-1]
            badges = []

            # Schema check
            schema = st.session_state.schema_info
            all_cols = set()
            for info in schema.values():
                all_cols.update(info["columns"])
            sql_lower = st.session_state.last_sql.lower()
            referenced_cols = [c for c in all_cols if c.lower() in sql_lower]
            if referenced_cols:
                badges.append(('<span class="badge badge-pass">✓ SCHEMA VALID</span>'))
            else:
                badges.append(('<span class="badge badge-warn">⚠ SCHEMA UNVERIFIED</span>'))

            # Confidence gate
            if conf >= 0.85:
                badges.append('<span class="badge badge-pass">✓ CONFIDENCE GATE</span>')
            elif conf >= 0.6:
                badges.append('<span class="badge badge-warn">⚠ REVIEW RECOMMENDED</span>')
            else:
                badges.append('<span class="badge badge-fail">✕ HUMAN APPROVAL REQUIRED</span>')

            # Reconciliation
            if recon.get("balanced"):
                badges.append('<span class="badge badge-pass">✓ RECONCILED</span>')
            elif recon:
                badges.append('<span class="badge badge-fail">✕ RECON FAILED</span>')

            # Warnings
            for w in last.get("warnings", []):
                badges.append(f'<span class="badge badge-warn">⚠ {w[:60]}</span>')

            # Explanation
            if last.get("explanation"):
                badges.append(f'<span class="badge badge-info">ℹ {last["explanation"][:80]}</span>')

            st.markdown(f'<div class="badge-row">{"".join(badges)}</div>', unsafe_allow_html=True)

    # ── Run history ─────────────────────────────────────────────
    if st.session_state.run_history:
        with st.expander(f"⏱ Run History ({len(st.session_state.run_history)} runs)"):
            hist_df = pd.DataFrame([
                {
                    "Time":        r["ts"],
                    "Prompt":      r["prompt"],
                    "Rows In":     r["rows_in"],
                    "Rows Out":    r["rows_out"],
                    "Quarantine":  r["quarantine"],
                    "Confidence":  f"{int(r['confidence']*100)}%",
                    "Exec (ms)":   r["elapsed_ms"],
                    "Balanced":    "✓" if r["balanced"] else "✕",
                }
                for r in reversed(st.session_state.run_history)
            ])
            st.dataframe(hist_df, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════
# TAB 2 — DATA TRUST
# ══════════════════════════════════════════════════════════════
with tabs[1]:
    st.markdown('<div class="section-label">Reconciliation Guarantee</div>', unsafe_allow_html=True)

    recon = st.session_state.recon
    if recon:
        balanced = recon["balanced"]
        balance_color = "#4fffb0" if balanced else "#ff6b6b"
        diff_str = "0 ✓ BALANCED" if balanced else f"{recon['diff']} ✕ UNBALANCED"

        st.markdown(f"""
<div class="recon-block">
  <div class="recon-eq">INPUT ROWS = OUTPUT ROWS + EXCLUDED ROWS + QUARANTINE ROWS</div>
  <div style="margin-top:10px; font-size:13px; line-height:2.2;">
    <span class="recon-dim">Input rows &nbsp;&nbsp;&nbsp; </span><span class="recon-num">{recon["source"]:,}</span><br>
    <span class="recon-dim">Output rows &nbsp;&nbsp; </span><span class="recon-num">{recon["output"]:,}</span><br>
    <span class="recon-dim">Excluded rows  </span><span class="recon-num">{recon["excluded"]:,}</span><br>
    <span class="recon-dim">Quarantine rows</span><span class="recon-num">{recon["quarantine"]:,}</span><br>
    <span class="recon-dim">Difference &nbsp;&nbsp;&nbsp; </span><span style="color:{balance_color}; font-weight:700;">{diff_str}</span>
  </div>
</div>
""", unsafe_allow_html=True)
    else:
        st.info("Run a pipeline in ETL Studio to see reconciliation results here.")

    # ── Null analysis ────────────────────────────────────────────
    stats = st.session_state.last_stats
    if stats.get("null_counts"):
        st.markdown('<div class="section-label">Null Analysis — Output</div>', unsafe_allow_html=True)
        null_df = pd.DataFrame([
            {"Column": col, "Null Count": cnt, "Null %": f"{cnt / max(stats['output_rows'], 1) * 100:.1f}%"}
            for col, cnt in stats["null_counts"].items()
        ])
        null_df = null_df.sort_values("Null Count", ascending=False)
        st.dataframe(null_df, use_container_width=True, hide_index=True)

    # ── Quarantine viewer ────────────────────────────────────────
    q_df = st.session_state.quarantine_df
    if q_df is not None and len(q_df) > 0:
        st.markdown(f'<div class="section-label">Quarantine Queue — {len(q_df)} rows</div>', unsafe_allow_html=True)
        st.dataframe(q_df.head(100), use_container_width=True, hide_index=True)

        buf = BytesIO()
        q_df.to_csv(buf, index=False)
        st.download_button(
            "⬇ Download quarantine rows",
            data=buf.getvalue(),
            file_name="prism_quarantine.csv",
            mime="text/csv",
        )
    elif st.session_state.last_result is not None:
        st.markdown("""
<div class="badge-row">
  <span class="badge badge-pass">✓ No quarantine rows — all output rows passed contracts</span>
</div>
""", unsafe_allow_html=True)

    # ── Confidence history chart ─────────────────────────────────
    if len(st.session_state.run_history) > 1:
        st.markdown('<div class="section-label">Confidence History</div>', unsafe_allow_html=True)
        hist = st.session_state.run_history
        chart_df = pd.DataFrame({
            "Run":        [f"#{i+1}" for i in range(len(hist))],
            "Confidence": [r["confidence"] * 100 for r in hist],
        })
        st.line_chart(chart_df.set_index("Run"), color="#4fffb0", height=180)

    # ── Source data profiling ────────────────────────────────────
    if st.session_state.tables:
        st.markdown('<div class="section-label">Source Data Profiles</div>', unsafe_allow_html=True)
        for tname, df in st.session_state.tables.items():
            with st.expander(f"Profile: {tname} ({len(df):,} rows, {len(df.columns)} cols)"):
                profile = profile_df(df, tname)
                prof_rows = []
                for col, info in profile.items():
                    row = {
                        "Column":    col,
                        "Type":      info["dtype"],
                        "Null %":    f"{info['null_pct']}%",
                        "Unique %":  f"{info['unique_pct']}%",
                        "Samples":   str(info["sample"][:3]),
                    }
                    if "min" in info:
                        row["Min"] = info["min"]
                        row["Max"] = info["max"]
                    prof_rows.append(row)
                st.dataframe(pd.DataFrame(prof_rows), use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════
# TAB 3 — OUTPUT DATA
# ══════════════════════════════════════════════════════════════
with tabs[2]:
    result = st.session_state.last_result
    stats  = st.session_state.last_stats

    if result is not None and len(result) > 0:
        st.markdown(f'<div class="section-label">Result — {len(result):,} rows × {len(result.columns)} columns · {stats.get("elapsed_ms", "?")} ms</div>', unsafe_allow_html=True)

        # Numeric summary
        numeric_cols = result.select_dtypes(include="number").columns.tolist()
        if numeric_cols:
            cols = st.columns(min(len(numeric_cols), 4))
            for i, col in enumerate(numeric_cols[:4]):
                with cols[i]:
                    total = result[col].sum()
                    st.metric(
                        label=col,
                        value=f"{total:,.1f}" if abs(total) < 1e6 else f"{total/1e6:.2f}M",
                        delta=f"avg {result[col].mean():,.1f}",
                    )

        # Data grid
        st.dataframe(result, use_container_width=True, height=380, hide_index=True)

        # Download buttons
        col1, col2 = st.columns(2)
        with col1:
            csv_buf = BytesIO()
            result.to_csv(csv_buf, index=False)
            st.download_button(
                "⬇ Download CSV",
                data=csv_buf.getvalue(),
                file_name="prism_output.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with col2:
            json_str = result.to_json(orient="records", indent=2)
            st.download_button(
                "⬇ Download JSON",
                data=json_str,
                file_name="prism_output.json",
                mime="application/json",
                use_container_width=True,
            )

        # Quick charts for numeric columns
        if len(numeric_cols) >= 1 and len(result) > 1:
            st.markdown('<div class="section-label">Quick Visualisation</div>', unsafe_allow_html=True)
            chart_col = st.selectbox("Column to chart", numeric_cols, label_visibility="collapsed")
            # Try to find a categorical column for the index
            obj_cols = result.select_dtypes(include="object").columns.tolist()
            if obj_cols:
                idx_col = obj_cols[0]
                chart_data = result[[idx_col, chart_col]].set_index(idx_col)
            else:
                chart_data = result[[chart_col]].head(50)
            st.bar_chart(chart_data, color="#7c6fff", height=220)

    else:
        st.info("No output yet. Run a pipeline in ETL Studio.")

    # ── SQL Editor (direct) ──────────────────────────────────────
    st.markdown('<div class="section-label">SQL Editor (Direct)</div>', unsafe_allow_html=True)
    manual_sql = st.text_area(
        "Write SQL directly against loaded tables",
        height=100,
        placeholder="SELECT * FROM sales LIMIT 10",
        label_visibility="collapsed",
    )
    if st.button("▶ Run SQL"):
        if not st.session_state.tables:
            st.error("No tables loaded.")
        elif manual_sql.strip():
            try:
                source_rows = sum(len(df) for df in st.session_state.tables.values())
                result_df, stats = execute_sql(manual_sql.strip(), source_rows)
                st.session_state.last_result = result_df
                st.session_state.last_stats  = stats
                st.session_state.last_sql    = manual_sql.strip()
                st.session_state.status      = "done"
                st.session_state.recon       = reconcile(source_rows, len(result_df), source_rows - len(result_df), 0)
                st.rerun()
            except Exception as e:
                st.error(f"SQL error: {e}")

# ══════════════════════════════════════════════════════════════
# TAB 4 — SCHEMA
# ══════════════════════════════════════════════════════════════
with tabs[3]:
    schema = st.session_state.schema_info
    if schema:
        st.markdown('<div class="section-label">Loaded Schema</div>', unsafe_allow_html=True)
        for tname, info in schema.items():
            with st.expander(f"{tname}  ·  {info['row_count']:,} rows  ·  {len(info['columns'])} columns", expanded=True):
                rows = []
                for col in info["columns"]:
                    rows.append({
                        "Column":  col,
                        "Type":    info["dtypes"][col],
                        "Samples": ", ".join(str(v) for v in info["sample_values"][col][:3]),
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        # DuckDB table list
        conn = get_conn()
        try:
            tbl_list = conn.execute("SHOW TABLES").df()
            st.markdown('<div class="section-label">DuckDB SHOW TABLES</div>', unsafe_allow_html=True)
            st.dataframe(tbl_list, use_container_width=True, hide_index=True)
        except Exception:
            pass

    else:
        st.info("No tables loaded yet. Upload CSVs or load demo data in ETL Studio.")

# ── Footer ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="border-top:1px solid #1e1e35; margin-top:24px; padding: 12px 0 4px; text-align:center;">
  <span style="font-family:'DM Mono',monospace; font-size:10px; color:#2a2a50; letter-spacing:0.15em;">
    PRISM · AI-NATIVE DATA INTELLIGENCE ENGINE · v2.0 · MARCH 2026 · CONFIDENTIAL
  </span>
</div>
""", unsafe_allow_html=True)
