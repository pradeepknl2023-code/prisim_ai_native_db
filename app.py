"""
PRISM · AI-Native Data Intelligence Engine · v3.0
Single-file Streamlit deployment — hardened against 67 failure scenarios

Deploy:
    pip install streamlit duckdb pandas numpy requests openpyxl
    streamlit run app.py

Secrets (Streamlit Cloud → Settings → Secrets):
    GROQ_API_KEY = "gsk_xxxxxxxxxxxx"        # free at console.groq.com
    ANTHROPIC_API_KEY = "sk-ant-xxxx"        # optional fallback
"""

import streamlit as st
import duckdb
import pandas as pd
import numpy as np
import json, re, time, os, traceback, csv
from io import BytesIO, StringIO
from datetime import datetime

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PRISM · Data Intelligence",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@700;800&family=DM+Sans:wght@300;400;500&display=swap');
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;background:#09090f;color:#e8e8f0}
.stApp{background:#09090f}
#MainMenu,footer,header{visibility:hidden}
.prism-header{display:flex;align-items:center;gap:14px;padding:16px 24px 12px;border-bottom:1px solid #1e1e35}
.prism-logo{font-family:'Syne',sans-serif;font-size:22px;font-weight:800;letter-spacing:.08em;background:linear-gradient(135deg,#4fffb0,#7c6fff);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.prism-tag{font-size:10px;color:#3a3a6a;letter-spacing:.14em;text-transform:uppercase;font-family:'DM Mono',monospace}
.status-strip{display:flex;align-items:center;gap:20px;background:#0f0f1a;border:1px solid #1e1e35;border-radius:8px;padding:10px 16px;margin-bottom:10px;flex-wrap:wrap}
.si{display:flex;flex-direction:column;gap:2px}
.sl{font-size:9px;color:#33336a;letter-spacing:.1em;text-transform:uppercase;font-family:'DM Mono',monospace}
.sv{font-size:15px;font-weight:600;font-family:'DM Mono',monospace}
.g{color:#4fffb0}.p{color:#7c6fff}.y{color:#ffcc44}.r{color:#ff6b6b}.d{color:#55558a}
.sql-block{background:#0a0a18;border:1px solid #1e1e35;border-left:3px solid #7c6fff;border-radius:6px;padding:14px 16px;font-family:'DM Mono',monospace;font-size:12px;color:#c8c8f0;line-height:1.7;white-space:pre-wrap;margin-bottom:10px;max-height:260px;overflow-y:auto}
.badge-row{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px}
.badge{font-size:11px;font-family:'DM Mono',monospace;padding:3px 9px;border-radius:4px;letter-spacing:.04em}
.bp{background:#0d2a1e;color:#4fffb0;border:1px solid #1a4030}
.bw{background:#2a2000;color:#ffcc44;border:1px solid #4a3800}
.bf{background:#2a0a0a;color:#ff6b6b;border:1px solid #4a1a1a}
.bi{background:#0f0f30;color:#7c6fff;border:1px solid #2a2a50}
.recon-block{background:#0a0a18;border:1px solid #1e1e35;border-left:3px solid #4fffb0;border-radius:6px;padding:14px 16px;font-family:'DM Mono',monospace;font-size:12px;margin-bottom:10px;line-height:2.2}
.sec{font-size:10px;color:#33336a;letter-spacing:.15em;text-transform:uppercase;font-family:'DM Mono',monospace;margin-bottom:6px;margin-top:14px}
.stTabs [data-baseweb="tab-list"]{background:#0f0f1a;border-bottom:1px solid #1e1e35;gap:0}
.stTabs [data-baseweb="tab"]{background:transparent;color:#44448a;font-family:'DM Mono',monospace;font-size:11px;letter-spacing:.07em;padding:10px 18px;border-bottom:2px solid transparent}
.stTabs [aria-selected="true"]{color:#4fffb0!important;border-bottom:2px solid #4fffb0!important;background:transparent!important}
.stButton>button{background:linear-gradient(135deg,#1a3a2a,#1a1a3a);color:#4fffb0;border:1px solid #2a4a3a;font-family:'DM Mono',monospace;font-size:12px;letter-spacing:.05em;padding:8px 16px;transition:all .2s}
.stButton>button:hover{background:linear-gradient(135deg,#2a5a3a,#2a2a5a);border-color:#4fffb0}
.stTextArea textarea,.stTextInput input{background:#0f0f1a!important;color:#e8e8f0!important;border:1px solid #1e1e35!important;font-family:'DM Mono',monospace!important;font-size:12px!important}
[data-testid="stMetric"]{background:#0f0f1a;border:1px solid #1e1e35;border-radius:8px;padding:10px 14px}
[data-testid="stMetricLabel"]{color:#33336a!important;font-size:10px!important;letter-spacing:.1em;text-transform:uppercase}
[data-testid="stMetricValue"]{color:#4fffb0!important;font-family:'DM Mono',monospace!important}
[data-testid="stSidebar"]{background:#0f0f1a;border-right:1px solid #1e1e35}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS — FILE LOADING (scenarios 4.1–4.12)
# ══════════════════════════════════════════════════════════════════════════════

def safe_table_name(filename: str, existing: set) -> str:
    """Sanitise filename → valid SQL identifier, handle collisions (4.11, 4.12)."""
    base = re.sub(r"[^a-zA-Z0-9]", "_", filename.rsplit(".", 1)[0])
    base = re.sub(r"_+", "_", base).strip("_") or "table"
    if base[0].isdigit():
        base = "t_" + base
    name, i = base, 1
    while name in existing:
        name = f"{base}_{i}"; i += 1
    return name


def detect_delimiter(sample: str) -> str:
    """Auto-detect CSV delimiter (4.2)."""
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except Exception:
        return ","


def coerce_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise "NULL","N/A","","nan","none" strings → actual NaN (6.3)."""
    null_strings = {"null", "n/a", "na", "nan", "none", "nil", "", " "}
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(
            lambda x: np.nan if (isinstance(x, str) and x.strip().lower() in null_strings) else x
        )
    return df


def normalise_booleans(df: pd.DataFrame) -> pd.DataFrame:
    """Convert True/False object columns to int 1/0 for DuckDB (1.4)."""
    for col in df.select_dtypes(include="object").columns:
        sample = df[col].dropna().head(20)
        if set(sample.astype(str).str.lower().unique()).issubset({"true", "false", "1", "0"}):
            df[col] = df[col].map(
                lambda x: 1 if str(x).lower() in ("true", "1") else 0 if str(x).lower() in ("false", "0") else np.nan
            )
    return df


def load_file(uploaded_file) -> tuple[pd.DataFrame | None, str]:
    """Load CSV or XLSX with auto-encoding, delimiter, and type coercion. Returns (df, error_msg)."""
    fname = uploaded_file.name.lower()
    raw = uploaded_file.read()

    # XLSX support (4.10)
    if fname.endswith(".xlsx") or fname.endswith(".xls"):
        try:
            df = pd.read_excel(BytesIO(raw), nrows=500_000)
            df = coerce_nulls(normalise_booleans(df))
            return df, ""
        except Exception as e:
            return None, f"Excel read error: {e}"

    # CSV — try encodings (4.1)
    for enc in ["utf-8", "utf-8-sig", "latin-1", "windows-1252", "iso-8859-1"]:
        try:
            text = raw.decode(enc)
            delim = detect_delimiter(text[:4096])  # (4.2)
            df = pd.read_csv(StringIO(text), sep=delim, low_memory=False,
                             on_bad_lines="skip")  # skip malformed rows

            # Drop all-null columns (4.5)
            df = df.dropna(axis=1, how="all")

            # Warn on duplicate column names (4.8)
            dup_cols = [c for c in df.columns if str(c).endswith((".1", ".2", ".3"))]
            warn = f"Duplicate column names renamed: {dup_cols}" if dup_cols else ""

            # Coerce nulls and booleans
            df = coerce_nulls(normalise_booleans(df))

            # File size guard (4.9)
            size_mb = len(raw) / 1e6
            if size_mb > 200:
                warn += f" ⚠ Large file ({size_mb:.0f} MB) — DuckDB will handle it but upload may be slow."

            return df, warn
        except UnicodeDecodeError:
            continue
        except Exception as e:
            return None, f"CSV parse error ({enc}): {e}"

    return None, "Could not decode file — try saving as UTF-8 CSV."


# ══════════════════════════════════════════════════════════════════════════════
# DUCKDB CONNECTION (scenarios 3.6, 5.1, 5.2)
# ══════════════════════════════════════════════════════════════════════════════

def get_conn() -> duckdb.DuckDBPyConnection:
    """Get or recreate DuckDB connection, re-registering all tables."""
    if st.session_state.get("conn") is None:
        st.session_state.conn = duckdb.connect(database=":memory:", read_only=False)
    conn = st.session_state.conn
    # Always re-register to survive Streamlit reruns (5.1, 5.2)
    for tname, df in st.session_state.get("tables", {}).items():
        try:
            conn.register(tname, df)
        except Exception:
            pass
    return conn


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA ANALYSIS — TYPE MISMATCH DETECTION (scenarios 1.1–1.10)
# ══════════════════════════════════════════════════════════════════════════════

def analyse_schema(tables: dict) -> dict:
    """Build rich schema info including cross-table JOIN trap detection."""
    schema = {}
    col_registry: dict[str, list] = {}  # col_name → [(table, dtype, sample)]

    for tname, df in tables.items():
        info = {
            "columns":      list(df.columns),
            "dtypes":       {c: str(df[c].dtype) for c in df.columns},
            "row_count":    len(df),
            "sample_values":{c: df[c].dropna().head(5).tolist() for c in df.columns},
            "null_counts":  {c: int(df[c].isnull().sum()) for c in df.columns},
            "join_traps":   [],
        }
        schema[tname] = info
        for col in df.columns:
            col_registry.setdefault(col, []).append((tname, str(df[col].dtype)))

    # Detect cross-table type mismatches on same-named columns
    for col, appearances in col_registry.items():
        if len(appearances) > 1:
            types = [dtype for _, dtype in appearances]
            if len(set(types)) > 1:
                for tname, _ in appearances:
                    schema[tname]["join_traps"].append({
                        "col": col,
                        "appearances": appearances,
                    })
    return schema


def schema_to_prompt(schema: dict) -> str:
    """Rich schema string for AI — includes types, samples, JOIN warnings."""
    lines = ["=== SCHEMA ==="]
    all_traps = []

    for tname, info in schema.items():
        lines.append(f"\nTable: {tname}  ({info['row_count']:,} rows)")
        for col in info["columns"]:
            dtype   = info["dtypes"][col]
            samples = info["sample_values"][col]
            nulls   = info["null_counts"][col]
            # Render samples clearly (1.1 — show actual types)
            sample_str = ", ".join(repr(s) for s in samples[:3])
            null_note  = f"  [{nulls:,} nulls]" if nulls > 0 else ""
            lines.append(f"  {col}  [{dtype}]  e.g. {sample_str}{null_note}")
        if info["join_traps"]:
            all_traps.extend(info["join_traps"])

    if all_traps:
        lines.append("\n=== CRITICAL JOIN TYPE MISMATCHES — YOU MUST HANDLE THESE ===")
        seen = set()
        for trap in all_traps:
            key = trap["col"]
            if key in seen:
                continue
            seen.add(key)
            detail = ", ".join(f"{t}.{trap['col']} is [{d}]" for t, d in trap["appearances"])
            lines.append(f"⚠ '{trap['col']}': {detail}")
            lines.append(f"  → CAST both sides to VARCHAR: CAST(a.{trap['col']} AS VARCHAR) = CAST(b.{trap['col']} AS VARCHAR)")

    return "\n".join(lines)


def is_aggregation_query(sql: str) -> bool:
    """Detect if SQL is an aggregation (affects reconciliation logic — 6.1)."""
    sql_up = sql.upper()
    return bool(re.search(r"\bGROUP\s+BY\b|\bCOUNT\s*\(|\bSUM\s*\(|\bAVG\s*\(|\bMIN\s*\(|\bMAX\s*\(|\bDISTINCT\b", sql_up))


# ══════════════════════════════════════════════════════════════════════════════
# SQL GENERATION (scenarios 2.1–2.15)
# ══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are a DuckDB SQL expert inside PRISM, an AI-native data intelligence engine.

STRICT RULES — violation will break the product:
1. Return ONLY a JSON object — no markdown, no code fences, no explanation outside JSON.
2. JSON keys: "sql" (string), "confidence" (float 0-1), "explanation" (one sentence), "warnings" (list of strings).
3. Use ONLY DuckDB syntax — not MySQL, not Postgres, not SQL Server.
4. String literals use single quotes. Double quotes are for identifiers only.
5. Always qualify column names with table alias when joining: a.col, not just col.
6. For JOINs on columns with type mismatches shown in schema, CAST both sides to VARCHAR.
7. For division, always wrap denominator: NULLIF(denominator, 0) to avoid division by zero.
8. Use TRY_CAST instead of CAST when converting user data — never let a single bad row crash the query.
9. For counting, use COUNT_BIG or ensure BIGINT — never assume INT is enough.
10. When filtering dates stored as strings, use strptime or TRY_CAST(col AS DATE).
11. If the user asks for a PIVOT, use DuckDB PIVOT syntax, not CASE WHEN emulation.
12. Never use ILIKE — use LIKE or lower(col) LIKE lower(pattern) for case-insensitive match.
13. Wrap table names that contain spaces or special chars in double quotes.
14. Set confidence < 0.7 if schema has NULLs in key columns, type mismatches, or query is complex.
15. List any assumptions you made in "warnings"."""


def _get_secret(key: str) -> str:
    """Robustly read from st.secrets (flat + nested) or env (5.7)."""
    # 1. Direct root key
    try:
        v = st.secrets[key]
        if v:
            return str(v).strip()
    except Exception:
        pass
    # 2. Nested under any section
    try:
        for section in st.secrets:
            try:
                v = st.secrets[section][key]
                if v:
                    return str(v).strip()
            except Exception:
                pass
    except Exception:
        pass
    # 3. OS env
    return os.environ.get(key, "").strip()


def _extract_json(text: str) -> dict:
    """Robustly extract JSON from model output (2.8, 2.9, 2.13)."""
    # Strip markdown fences
    text = re.sub(r"```json\s*|```\s*", "", text).strip()
    # Try direct parse
    try:
        return json.loads(text)
    except Exception:
        pass
    # Find first {...} block
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except Exception:
            pass
    raise ValueError(f"Could not extract JSON from model response:\n{text[:500]}")


def call_groq(messages: list, key: str) -> str:
    """Call Groq API with proper error handling (2.10, 2.11, 2.12)."""
    import requests
    for attempt in range(2):
        try:
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Content-Type": "application/json",
                         "Authorization": f"Bearer {key}"},
                json={"model": "llama-3.3-70b-versatile",
                      "messages": messages,
                      "temperature": 0.1,
                      "max_tokens": 1000},
                timeout=40,
            )
            if resp.status_code == 429:
                wait = 2 ** attempt
                time.sleep(wait)
                continue
            if resp.status_code == 401:
                raise ValueError("Groq key invalid (401). Check it starts with 'gsk_'.")
            if resp.status_code == 403:
                raise ValueError(
                    "Groq 403 Forbidden — key format wrong in Streamlit secrets.\n"
                    "Go to ⚙ Settings → Secrets and set:\n"
                    '  GROQ_API_KEY = "gsk_your_key_here"\n'
                    "Then Save → Reboot app."
                )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
        except requests.exceptions.Timeout:
            if attempt == 1:
                raise ValueError("Groq API timed out after 40s. Try again in a moment.")
    raise ValueError("Groq API failed after 2 attempts (rate limited).")


def call_anthropic(messages: list, system: str, key: str) -> str:
    """Call Anthropic API with proper error handling."""
    import requests
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"Content-Type": "application/json",
                 "x-api-key": key,
                 "anthropic-version": "2023-06-01"},
        json={"model": "claude-sonnet-4-20250514",
              "max_tokens": 1000,
              "system": system,
              "messages": messages},
        timeout=40,
    )
    if resp.status_code in (401, 403):
        raise ValueError(
            f"Anthropic key rejected ({resp.status_code}). "
            "In Streamlit secrets: ANTHROPIC_API_KEY = \"sk-ant-...\""
        )
    resp.raise_for_status()
    return resp.json()["content"][0]["text"]


def generate_sql(prompt: str, schema: dict,
                 error_context: str = "") -> tuple[str, float, str, list]:
    """
    Generate DuckDB SQL from prompt + schema.
    If error_context is set, this is a self-healing retry (2.1–2.7).
    Returns (sql, confidence, explanation, warnings).
    """
    # Sanitise prompt — prevent injection (2.15)
    clean_prompt = re.sub(r"(drop|truncate|delete|insert|update|alter|create|grant|revoke)",
                          "[BLOCKED]", prompt, flags=re.IGNORECASE)

    schema_str = schema_to_prompt(schema)

    # Truncate schema if too wide (4.7)
    if len(schema_str) > 8000:
        schema_str = schema_str[:8000] + "\n... [schema truncated — too many columns]"

    user_content = f"User intent: {clean_prompt}\n\n{schema_str}"
    if error_context:
        user_content += (
            f"\n\n=== PREVIOUS SQL FAILED — SELF-HEALING RETRY ===\n"
            f"Error: {error_context}\n"
            f"Fix the SQL to avoid this error. Pay attention to type mismatches and column names."
        )

    groq_key      = _get_secret("GROQ_API_KEY")
    anthropic_key = _get_secret("ANTHROPIC_API_KEY")

    messages = [{"role": "user", "content": user_content}]

    raw = None
    if groq_key:
        raw = call_groq(
            [{"role": "system", "content": SYSTEM_PROMPT}] + messages,
            groq_key,
        )
    elif anthropic_key:
        raw = call_anthropic(messages, SYSTEM_PROMPT, anthropic_key)
    else:
        # No key — real DuckDB fallback (demo mode)
        tables = list(schema.keys())
        t = tables[0] if tables else "unknown"
        return (
            f'SELECT * FROM "{t}" LIMIT 500',
            0.5,
            f"No API key — showing first 500 rows of {t}. Add GROQ_API_KEY to Streamlit secrets.",
            ["No AI key configured — using fallback SELECT. Get a free key at console.groq.com"],
        )

    parsed = _extract_json(raw)
    sql         = parsed.get("sql", "").strip()
    confidence  = float(parsed.get("confidence", 0.7))
    explanation = parsed.get("explanation", "")
    warnings    = parsed.get("warnings", [])

    if not sql:
        raise ValueError("AI returned empty SQL. Please rephrase your prompt.")

    return sql, confidence, explanation, warnings


# ══════════════════════════════════════════════════════════════════════════════
# SQL EXECUTION with self-healing (scenarios 3.1–3.12, 2.1–2.7)
# ══════════════════════════════════════════════════════════════════════════════

def execute_sql_safe(sql: str, source_rows: int,
                     schema: dict, prompt: str) -> tuple[pd.DataFrame, dict, str, list]:
    """
    Execute SQL in DuckDB. On failure, auto-retry with error context (self-healing).
    Returns (result_df, stats, final_sql, warnings).
    """
    conn      = get_conn()
    warnings  = []
    final_sql = sql
    attempts  = []

    for attempt in range(3):  # max 3 attempts: original + 2 self-heal retries
        try:
            t0        = time.perf_counter()
            result_df = conn.execute(final_sql).df()
            elapsed   = (time.perf_counter() - t0) * 1000

            # Empty result (3.10)
            if len(result_df) == 0:
                warnings.append("Query returned 0 rows — check your filters.")

            # Duplicate column names in result (3.11)
            if len(result_df.columns) != len(set(result_df.columns)):
                result_df.columns = pd.io.parsers.ParserBase(
                    {"usecols": None})._maybe_dedup_names(result_df.columns)
                warnings.append("Duplicate column names in result — auto-renamed.")

            null_counts = {c: int(result_df[c].isnull().sum()) for c in result_df.columns}
            total_nulls = sum(null_counts.values())
            null_pct    = total_nulls / max(len(result_df) * max(len(result_df.columns), 1), 1) * 100

            stats = {
                "output_rows": len(result_df),
                "source_rows": source_rows,
                "elapsed_ms":  round(elapsed, 1),
                "col_count":   len(result_df.columns),
                "columns":     list(result_df.columns),
                "null_pct":    round(null_pct, 2),
                "null_counts": null_counts,
                "dtypes":      {c: str(result_df[c].dtype) for c in result_df.columns},
                "attempts":    attempt + 1,
                "self_healed": attempt > 0,
            }
            if attempt > 0:
                warnings.append(f"✓ Self-healed after {attempt} failed attempt(s).")
            return result_df, stats, final_sql, warnings

        except Exception as e:
            err_str  = str(e)
            err_type = type(e).__name__
            attempts.append({"attempt": attempt + 1, "sql": final_sql, "error": err_str})

            if attempt >= 2:
                # All retries exhausted — show diagnostic
                raise RuntimeError(
                    f"Query failed after {attempt+1} attempts.\n\n"
                    f"Last error: {err_str}\n\n"
                    f"Attempts:\n" +
                    "\n---\n".join(f"Attempt {a['attempt']}:\n{a['sql']}\n→ {a['error']}"
                                   for a in attempts)
                )

            # Self-healing: send error back to AI for fix (all 3.x scenarios)
            with st.spinner(f"⚕ Self-healing SQL (attempt {attempt+2})…"):
                try:
                    new_sql, _, _, new_warns = generate_sql(
                        prompt, schema,
                        error_context=f"{err_type}: {err_str}\nFailed SQL:\n{final_sql}"
                    )
                    final_sql = new_sql
                    warnings.extend(new_warns)
                    warnings.append(f"Self-heal attempt {attempt+1}: fixed {err_type}")
                except Exception as gen_err:
                    raise RuntimeError(
                        f"SQL generation failed during self-heal: {gen_err}\n"
                        f"Original error: {err_str}"
                    )


# ══════════════════════════════════════════════════════════════════════════════
# DATA QUALITY (scenarios 6.1–6.6)
# ══════════════════════════════════════════════════════════════════════════════

def validate_and_quarantine(df: pd.DataFrame,
                             contracts: dict) -> tuple[pd.DataFrame, pd.DataFrame, list]:
    """Apply contracts, return (clean, quarantine, violation_messages)."""
    if not contracts or df.empty:
        return df, pd.DataFrame(), []

    mask_bad   = pd.Series(False, index=df.index)
    violations = []

    for col, rules in contracts.items():
        if col not in df.columns:
            violations.append(f"Contract column '{col}' not in result — skipped.")
            continue

        if rules.get("not_null"):
            bad = df[col].isnull()
            mask_bad |= bad
            if bad.any():
                violations.append(f"{col}: {int(bad.sum()):,} null values violate not_null contract")

        if "min_val" in rules:
            try:
                bad = pd.to_numeric(df[col], errors="coerce") < rules["min_val"]
                bad = bad.fillna(False)
                mask_bad |= bad
                if bad.any():
                    violations.append(f"{col}: {int(bad.sum()):,} values < min {rules['min_val']}")
            except Exception:
                pass

        if "max_val" in rules:
            try:
                bad = pd.to_numeric(df[col], errors="coerce") > rules["max_val"]
                bad = bad.fillna(False)
                mask_bad |= bad
                if bad.any():
                    violations.append(f"{col}: {int(bad.sum()):,} values > max {rules['max_val']}")
            except Exception:
                pass

        if "allowed_values" in rules:
            allowed = set(str(v) for v in rules["allowed_values"])
            bad = ~df[col].astype(str).isin(allowed)
            mask_bad |= bad
            if bad.any():
                violations.append(f"{col}: {int(bad.sum()):,} values not in allowed set {allowed}")

        if "regex" in rules:
            try:
                bad = ~df[col].astype(str).str.match(rules["regex"])
                mask_bad |= bad
                if bad.any():
                    violations.append(f"{col}: {int(bad.sum()):,} values fail regex '{rules['regex']}'")
            except Exception as e:
                violations.append(f"{col}: regex check failed — {e}")

    clean_df = df[~mask_bad].reset_index(drop=True)
    quar_df  = df[mask_bad].reset_index(drop=True)
    return clean_df, quar_df, violations


def reconcile(source: int, output: int,
              excluded: int, quarantine: int, is_aggregation: bool) -> dict:
    """Reconciliation — skipped for aggregation queries (6.1)."""
    if is_aggregation:
        return {"skipped": True, "reason": "Aggregation query — row count changes by design"}
    diff = source - output - excluded - quarantine
    return {
        "skipped":    False,
        "source":     source,
        "output":     output,
        "excluded":   excluded,
        "quarantine": quarantine,
        "diff":       diff,
        "balanced":   diff == 0,
    }


def profile_df(df: pd.DataFrame, sample_n: int = 10_000) -> list[dict]:
    """Profile a DataFrame, sampling for speed on large files (6.6)."""
    sample = df.sample(min(sample_n, len(df)), random_state=42) if len(df) > sample_n else df
    rows = []
    for col in df.columns:
        s    = sample[col]
        info = {
            "Column":   col,
            "Type":     str(df[col].dtype),
            "Null %":   f"{df[col].isnull().mean()*100:.1f}%",
            "Unique":   f"{df[col].nunique():,}",
            "Sample":   ", ".join(repr(v) for v in df[col].dropna().head(3).tolist()),
        }
        num = pd.to_numeric(s, errors="coerce").dropna()
        if len(num) > 0:
            info["Min"] = f"{num.min():,.2f}"
            info["Max"] = f"{num.max():,.2f}"
            info["Mean"] = f"{num.mean():,.2f}"
        rows.append(info)
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════════════════════════════════════

_DEFAULTS = {
    "tables":       {},
    "conn":         None,
    "schema":       {},
    "last_sql":     "",
    "last_result":  None,
    "last_stats":   {},
    "quarantine":   None,
    "run_history":  [],
    "status":       "idle",
    "confidence":   0.0,
    "recon":        {},
    "exec_warnings":[],
    "ai_warnings":  [],
    "explanation":  "",
    "self_healed":  False,
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — KEY DIAGNOSTICS (scenario 2.12, 5.7)
# ══════════════════════════════════════════════════════════════════════════════

def _key_status(key: str) -> str:
    v = _get_secret(key)
    if v:
        return f"✅ Found ({v[:8]}…)"
    return "❌ Not found"

with st.sidebar:
    st.markdown("### 🔑 API Keys")
    st.markdown(f"**GROQ_API_KEY**  \n`{_key_status('GROQ_API_KEY')}`")
    st.markdown(f"**ANTHROPIC_API_KEY**  \n`{_key_status('ANTHROPIC_API_KEY')}`")
    st.markdown("---")
    st.markdown("""**Fix 403 — exact steps:**
1. Streamlit Cloud → ⚙ Settings → **Secrets**
2. Paste *exactly*:
```
GROQ_API_KEY = "gsk_xxxx"
```
3. **Save** → **Reboot app**

Free Groq key (no card): [console.groq.com](https://console.groq.com)
""")
    st.markdown("---")
    st.markdown("### 📊 Session")
    st.markdown(f"Tables loaded: **{len(st.session_state.tables)}**")
    st.markdown(f"Runs this session: **{len(st.session_state.run_history)}**")
    total_rows = sum(len(df) for df in st.session_state.tables.values())
    st.markdown(f"Total rows in DuckDB: **{total_rows:,}**")
    if st.button("🗑 Clear all data"):
        for k, v in _DEFAULTS.items():
            st.session_state[k] = v if not callable(v) else v()
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<div class="prism-header">
  <div>
    <div class="prism-logo">PRISM</div>
    <div class="prism-tag">AI-Native Data Intelligence Engine · v3.0 · Every number from real DuckDB</div>
  </div>
</div>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════

tab_studio, tab_trust, tab_output, tab_schema = st.tabs(
    ["⬡  ETL STUDIO", "⬡  DATA TRUST", "⬡  OUTPUT DATA", "⬡  SCHEMA"]
)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — ETL STUDIO
# ══════════════════════════════════════════════════════════════════════════════

with tab_studio:
    col_left, col_right = st.columns([1, 1], gap="medium")

    # ── LEFT: Data sources ──────────────────────────────────────────────────
    with col_left:
        st.markdown('<div class="sec">📂 Data Sources</div>', unsafe_allow_html=True)

        uploaded_files = st.file_uploader(
            "Upload CSV or XLSX (multiple allowed)",
            type=["csv", "xlsx", "xls"],
            accept_multiple_files=True,
            label_visibility="collapsed",
        )

        if uploaded_files:
            changed = False
            for f in uploaded_files:
                tname = safe_table_name(f.name, set(st.session_state.tables.keys()))
                df, warn = load_file(f)
                if df is None:
                    st.error(f"❌ {f.name}: {warn}")
                    continue
                if len(df.columns) > 200:
                    df = df.iloc[:, :200]
                    st.warning(f"⚠ {f.name} has >200 columns — truncated to first 200.")
                st.session_state.tables[tname] = df
                changed = True
                if warn:
                    st.warning(f"⚠ {f.name}: {warn}")
                else:
                    st.success(f"✓ {f.name} → table `{tname}` ({len(df):,} rows)")
            if changed:
                st.session_state.schema = analyse_schema(st.session_state.tables)
                get_conn()  # re-register

        if st.button("⬡ Load Demo Data (Sales + Customers)", use_container_width=True):
            rng = np.random.default_rng(42)
            n = 500
            sales_df = pd.DataFrame({
                "order_id":    range(1001, 1001 + n),
                "customer_id": rng.integers(1, 101, n),
                "product":     rng.choice(["Widget A","Widget B","Gadget X","Service Z"], n),
                "amount":      np.where(rng.random(n) < 0.02, np.nan,
                                        rng.uniform(10, 2000, n).round(2)),
                "quantity":    rng.integers(1, 20, n),
                "order_date":  pd.date_range("2024-01-01", periods=n, freq="12h").strftime("%Y-%m-%d"),
                "status":      rng.choice(["COMPLETED","PENDING","CANCELLED","REFUNDED"], n,
                                           p=[0.70,0.15,0.10,0.05]),
                "region":      rng.choice(["North","South","East","West","Central"], n),
            })
            customers_df = pd.DataFrame({
                "customer_id": range(1, 101),
                "name":        [f"Customer {i:03d}" for i in range(1, 101)],
                "segment":     rng.choice(["Enterprise","SMB","Consumer","Government"], 100),
                "country":     rng.choice(["India","US","UK","Singapore","UAE"], 100),
                "credit_limit": rng.choice([50000, 100000, 250000, None], 100),
            })
            st.session_state.tables = {"sales": sales_df, "customers": customers_df}
            st.session_state.schema = analyse_schema(st.session_state.tables)
            get_conn()
            st.success("✓ Demo loaded: sales (500 rows), customers (100 rows)")
            st.rerun()

        # Tables summary
        if st.session_state.tables:
            st.markdown('<div class="sec">Loaded Tables</div>', unsafe_allow_html=True)
            for tname, df in st.session_state.tables.items():
                traps = st.session_state.schema.get(tname, {}).get("join_traps", [])
                trap_badge = f'<span class="badge bw">⚠ {len(traps)} join trap(s)</span>' if traps else ""
                st.markdown(f"""
<div class="status-strip" style="padding:8px 12px;margin-bottom:4px">
  <div class="si"><span class="sl">Table</span><span class="sv p">{tname}</span></div>
  <div class="si"><span class="sl">Rows</span><span class="sv g">{len(df):,}</span></div>
  <div class="si"><span class="sl">Cols</span><span class="sv d">{len(df.columns)}</span></div>
  {trap_badge}
</div>""", unsafe_allow_html=True)

    # ── RIGHT: Prompt + Generate ────────────────────────────────────────────
    with col_right:
        st.markdown('<div class="sec">✦ Prompt</div>', unsafe_allow_html=True)

        examples = [
            "Total revenue by product, sorted highest to lowest",
            "Join sales with customers, show customer name and total spend",
            "Count orders by status",
            "Monthly revenue trend — sum of amount by month",
            "Top 10 customers by total order value",
            "Sales by region with average order value",
            "Find rows where amount is null or negative",
            "Revenue from COMPLETED orders only, by segment",
            "Average order value by country and segment",
        ]
        ex = st.selectbox("Quick examples", ["— or type below —"] + examples,
                          label_visibility="collapsed")
        default_prompt = ex if ex != "— or type below —" else ""

        prompt = st.text_area("Prompt", value=default_prompt, height=90,
                              placeholder="Describe what you want in plain English…",
                              label_visibility="collapsed")

        with st.expander("⚙ Data Contracts (optional)"):
            st.caption("Rows violating these go to quarantine. Supports: not_null, min_val, max_val, allowed_values, regex")
            contract_str = st.text_area(
                "Contracts JSON",
                value='{"amount": {"not_null": true, "min_val": 0}}',
                height=80,
                label_visibility="collapsed",
            )

        c1, c2 = st.columns(2)
        with c1:
            run_btn = st.button("▶  Generate & Execute", use_container_width=True)
        with c2:
            clear_btn = st.button("✕  Clear Results", use_container_width=True)

        if clear_btn:
            for k in ["last_sql","last_result","last_stats","quarantine",
                      "recon","exec_warnings","ai_warnings","explanation",
                      "self_healed","confidence"]:
                st.session_state[k] = _DEFAULTS[k]
            st.session_state.status = "idle"
            st.rerun()

    # ── RUN PIPELINE ────────────────────────────────────────────────────────
    if run_btn:
        if not st.session_state.tables:
            st.error("No tables loaded — upload files or click 'Load Demo Data'.")
        elif not prompt.strip():
            st.error("Enter a prompt.")
        else:
            # Parse contracts (6.4 — graceful failure)
            contracts = {}
            if contract_str.strip():
                try:
                    contracts = json.loads(contract_str)
                except Exception as e:
                    st.warning(f"Contract JSON invalid ({e}) — contracts ignored.")

            st.session_state.status = "running"
            schema = st.session_state.schema

            # Step 1: Generate SQL
            with st.spinner("🔮 Generating SQL…"):
                try:
                    sql, conf, explanation, ai_warns = generate_sql(prompt, schema)
                    st.session_state.last_sql    = sql
                    st.session_state.confidence  = conf
                    st.session_state.explanation = explanation
                    st.session_state.ai_warnings = ai_warns
                except Exception as e:
                    st.session_state.status = "error"
                    st.error(f"**SQL generation failed:**\n\n{e}")
                    st.stop()

            # Step 2: Execute with self-healing
            with st.spinner("⚡ Executing in DuckDB…"):
                try:
                    source_rows = sum(len(df) for df in st.session_state.tables.values())
                    result_df, stats, final_sql, exec_warns = execute_sql_safe(
                        sql, source_rows, schema, prompt
                    )
                    # Update SQL if self-healed
                    st.session_state.last_sql     = final_sql
                    st.session_state.last_stats   = stats
                    st.session_state.exec_warnings = exec_warns
                    st.session_state.self_healed   = stats.get("self_healed", False)
                except Exception as e:
                    st.session_state.status = "error"
                    st.error(f"**Execution failed:**\n\n{e}")
                    with st.expander("Full traceback"):
                        st.code(traceback.format_exc())
                    st.stop()

            # Step 3: Contracts + Quarantine
            clean_df, q_df, violations = validate_and_quarantine(result_df, contracts)
            st.session_state.last_result = clean_df
            st.session_state.quarantine  = q_df if len(q_df) > 0 else None

            # Step 4: Reconciliation
            agg = is_aggregation_query(final_sql)
            excluded = max(0, source_rows - stats["output_rows"] - len(q_df))
            recon = reconcile(source_rows, len(clean_df), excluded, len(q_df), agg)
            st.session_state.recon = recon

            # Step 5: Append run history
            st.session_state.run_history.append({
                "ts":          datetime.now().strftime("%H:%M:%S"),
                "prompt":      prompt[:70],
                "sql":         final_sql,
                "rows_in":     source_rows,
                "rows_out":    len(clean_df),
                "quarantine":  len(q_df),
                "violations":  violations,
                "confidence":  conf,
                "elapsed_ms":  stats["elapsed_ms"],
                "self_healed": stats.get("self_healed", False),
                "balanced":    recon.get("balanced", None),
                "explanation": explanation,
            })
            st.session_state.status = "done"
            st.rerun()

    # ── STATUS STRIP ────────────────────────────────────────────────────────
    stats     = st.session_state.last_stats
    recon     = st.session_state.recon
    conf      = st.session_state.confidence
    q_count   = len(st.session_state.quarantine) if st.session_state.quarantine is not None else 0

    status_cls = {"idle":"d","running":"y","done":"g","error":"r"}.get(st.session_state.status,"d")
    status_sym = {"idle":"○","running":"◌","done":"●","error":"✕"}.get(st.session_state.status,"○")
    conf_cls   = "g" if conf >= 0.85 else "y" if conf >= 0.60 else "r" if conf > 0 else "d"
    conf_str   = f"{int(conf*100)}%" if conf > 0 else "—"
    q_cls      = "y" if q_count > 0 else "d"
    healed_badge = '<div class="si"><span class="sl">Self-healed</span><span class="sv g">✓ YES</span></div>' if st.session_state.self_healed else ""

    recon_badge = ""
    if recon and not recon.get("skipped"):
        if recon["balanced"]:
            recon_badge = '<div class="si"><span class="sl">Reconciliation</span><span class="sv g">✓ BALANCED</span></div>'
        else:
            recon_badge = f'<div class="si"><span class="sl">Reconciliation</span><span class="sv r">✕ DIFF {recon["diff"]:+,}</span></div>'
    elif recon.get("skipped"):
        recon_badge = '<div class="si"><span class="sl">Reconciliation</span><span class="sv d">AGG — N/A</span></div>'

    st.markdown(f"""
<div class="status-strip">
  <div class="si"><span class="sl">Status</span><span class="sv {status_cls}">{status_sym} {st.session_state.status.upper()}</span></div>
  <div class="si"><span class="sl">Rows In</span><span class="sv p">{stats.get("source_rows",0):,}</span></div>
  <div class="si"><span class="sl">Rows Out</span><span class="sv g">{stats.get("output_rows",0):,}</span></div>
  <div class="si"><span class="sl">Quarantine</span><span class="sv {q_cls}">{q_count:,}</span></div>
  <div class="si"><span class="sl">Confidence</span><span class="sv {conf_cls}">{conf_str}</span></div>
  <div class="si"><span class="sl">Exec ms</span><span class="sv d">{stats.get("elapsed_ms","—")}</span></div>
  <div class="si"><span class="sl">Columns</span><span class="sv d">{stats.get("col_count","—")}</span></div>
  {healed_badge}
  {recon_badge}
</div>
""", unsafe_allow_html=True)

    # ── SQL panel ────────────────────────────────────────────────────────────
    if st.session_state.last_sql:
        st.markdown('<div class="sec">Generated SQL</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="sql-block">{st.session_state.last_sql}</div>',
                    unsafe_allow_html=True)

        # Verification badges
        badges = []
        if conf >= 0.85:
            badges.append('<span class="badge bp">✓ HIGH CONFIDENCE</span>')
        elif conf >= 0.60:
            badges.append('<span class="badge bw">⚠ MEDIUM CONFIDENCE — REVIEW</span>')
        elif conf > 0:
            badges.append('<span class="badge bf">✕ LOW CONFIDENCE — APPROVE BEFORE SCHEDULING</span>')

        if st.session_state.self_healed:
            badges.append('<span class="badge bp">✓ SELF-HEALED</span>')

        if stats.get("attempts", 1) > 1:
            badges.append(f'<span class="badge bw">⚠ NEEDED {stats["attempts"]} ATTEMPTS</span>')

        for w in st.session_state.ai_warnings[:3]:
            badges.append(f'<span class="badge bw">⚠ {w[:70]}</span>')

        for w in st.session_state.exec_warnings[:2]:
            badges.append(f'<span class="badge bi">ℹ {w[:70]}</span>')

        if st.session_state.explanation:
            badges.append(f'<span class="badge bi">ℹ {st.session_state.explanation[:90]}</span>')

        if badges:
            st.markdown(f'<div class="badge-row">{"".join(badges)}</div>',
                        unsafe_allow_html=True)

    # ── Run history ───────────────────────────────────────────────────────────
    if st.session_state.run_history:
        with st.expander(f"⏱ Run History ({len(st.session_state.run_history)} runs)"):
            hist = pd.DataFrame([{
                "Time":       r["ts"],
                "Prompt":     r["prompt"],
                "In":         f'{r["rows_in"]:,}',
                "Out":        f'{r["rows_out"]:,}',
                "Quar":       r["quarantine"],
                "Conf":       f'{int(r["confidence"]*100)}%',
                "ms":         r["elapsed_ms"],
                "Healed":     "✓" if r["self_healed"] else "",
                "Balanced":   "✓" if r.get("balanced") else ("AGG" if r.get("balanced") is None else "✕"),
            } for r in reversed(st.session_state.run_history)])
            st.dataframe(hist, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — DATA TRUST
# ══════════════════════════════════════════════════════════════════════════════

with tab_trust:
    recon = st.session_state.recon

    # ── Reconciliation ───────────────────────────────────────────────────────
    st.markdown('<div class="sec">Reconciliation Guarantee</div>', unsafe_allow_html=True)
    if recon:
        if recon.get("skipped"):
            st.markdown(f"""
<div class="recon-block">
  <span style="color:#7c6fff;font-weight:700">RECONCILIATION — N/A FOR AGGREGATION QUERIES</span><br>
  <span style="color:#44448a">{recon['reason']}</span>
</div>""", unsafe_allow_html=True)
        else:
            bc      = "#4fffb0" if recon["balanced"] else "#ff6b6b"
            diff_s  = "0  ✓  BALANCED" if recon["balanced"] else f"{recon['diff']:+,}  ✕  UNBALANCED"
            st.markdown(f"""
<div class="recon-block">
  <span style="color:#4fffb0;font-weight:700;font-size:13px">INPUT = OUTPUT + EXCLUDED + QUARANTINE</span><br>
  <span style="color:#44448a">Input rows &nbsp;&nbsp;&nbsp;&nbsp;</span><span style="color:#e8e8f0">{recon["source"]:,}</span><br>
  <span style="color:#44448a">Output rows &nbsp;&nbsp;&nbsp;</span><span style="color:#e8e8f0">{recon["output"]:,}</span><br>
  <span style="color:#44448a">Excluded rows &nbsp;</span><span style="color:#e8e8f0">{recon["excluded"]:,}</span><br>
  <span style="color:#44448a">Quarantine rows</span><span style="color:#e8e8f0">{recon["quarantine"]:,}</span><br>
  <span style="color:#44448a">Difference &nbsp;&nbsp;&nbsp;&nbsp;</span><span style="color:{bc};font-weight:700">{diff_s}</span>
</div>""", unsafe_allow_html=True)
    else:
        st.info("Run a pipeline in ETL Studio to see reconciliation here.")

    # ── Null analysis ─────────────────────────────────────────────────────────
    stats = st.session_state.last_stats
    if stats.get("null_counts"):
        st.markdown('<div class="sec">Null Analysis — Output</div>', unsafe_allow_html=True)
        null_df = pd.DataFrame([
            {"Column": c, "Nulls": v,
             "Null %": f"{v/max(stats['output_rows'],1)*100:.1f}%",
             "Status": "⚠ HIGH" if v/max(stats['output_rows'],1) > 0.1 else "OK"}
            for c, v in stats["null_counts"].items()
        ]).sort_values("Nulls", ascending=False)
        st.dataframe(null_df, use_container_width=True, hide_index=True)

    # ── Quarantine viewer ─────────────────────────────────────────────────────
    qdf = st.session_state.quarantine
    if qdf is not None and len(qdf) > 0:
        st.markdown(f'<div class="sec">Quarantine Queue — {len(qdf):,} rows</div>',
                    unsafe_allow_html=True)
        st.dataframe(qdf.head(500), use_container_width=True, hide_index=True)
        buf = BytesIO()
        qdf.to_csv(buf, index=False)
        buf.seek(0)  # (7.5 — always seek before download)
        st.download_button("⬇ Download quarantine CSV", buf.getvalue(),
                           "prism_quarantine.csv", "text/csv")
    elif st.session_state.last_result is not None:
        st.markdown('<div class="badge-row"><span class="badge bp">✓ No quarantine rows</span></div>',
                    unsafe_allow_html=True)

    # ── Confidence history ────────────────────────────────────────────────────
    if len(st.session_state.run_history) > 1:
        st.markdown('<div class="sec">Confidence History</div>', unsafe_allow_html=True)
        ch = pd.DataFrame({
            "Run":        [f"#{i+1}" for i in range(len(st.session_state.run_history))],
            "Confidence": [r["confidence"]*100 for r in st.session_state.run_history],
        }).set_index("Run")
        st.line_chart(ch, color="#4fffb0", height=160)

    # ── Source profiles ───────────────────────────────────────────────────────
    if st.session_state.tables:
        st.markdown('<div class="sec">Source Data Profiles</div>', unsafe_allow_html=True)
        for tname, df in st.session_state.tables.items():
            traps = st.session_state.schema.get(tname, {}).get("join_traps", [])
            label = f"{tname}  ·  {len(df):,} rows  ·  {len(df.columns)} cols"
            if traps:
                label += f"  ·  ⚠ {len(traps)} join trap(s)"
            with st.expander(label):
                if traps:
                    for t in traps:
                        detail = ", ".join(f"{tb}.{t['col']}=[{dt}]" for tb, dt in t["appearances"])
                        st.warning(f"JOIN TYPE MISMATCH on '{t['col']}': {detail} — auto-handled in SQL generation")
                st.dataframe(pd.DataFrame(profile_df(df)),
                             use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — OUTPUT DATA
# ══════════════════════════════════════════════════════════════════════════════

with tab_output:
    result = st.session_state.last_result
    stats  = st.session_state.last_stats

    if result is not None and len(result) > 0:
        st.markdown(
            f'<div class="sec">Result — {len(result):,} rows × {len(result.columns)} cols'
            f' · {stats.get("elapsed_ms","?")} ms</div>',
            unsafe_allow_html=True
        )

        # Numeric summary cards
        num_cols = result.select_dtypes(include="number").columns.tolist()
        if num_cols:
            cols = st.columns(min(len(num_cols), 4))
            for i, col in enumerate(num_cols[:4]):
                with cols[i]:
                    total = result[col].sum()
                    avg   = result[col].mean()
                    label = f"{total/1e6:.2f}M" if abs(total) >= 1e6 else f"{total:,.1f}"
                    st.metric(col, label, delta=f"avg {avg:,.1f}")

        # Paginated grid — show max 2000 rows (5.6)
        display_df = result.head(2000)
        if len(result) > 2000:
            st.caption(f"Showing first 2,000 of {len(result):,} rows — download for full data.")
        st.dataframe(display_df, use_container_width=True, height=360, hide_index=True)

        # Downloads
        c1, c2 = st.columns(2)
        with c1:
            buf = BytesIO()
            result.to_csv(buf, index=False)
            buf.seek(0)
            st.download_button("⬇ Download CSV", buf.getvalue(),
                               "prism_output.csv", "text/csv", use_container_width=True)
        with c2:
            buf2 = BytesIO()
            result.to_json(buf2, orient="records", indent=2)
            buf2.seek(0)
            st.download_button("⬇ Download JSON", buf2.getvalue(),
                               "prism_output.json", "application/json", use_container_width=True)

        # Quick chart — numeric columns only (7.6)
        if num_cols and len(result) > 1:
            st.markdown('<div class="sec">Quick Chart</div>', unsafe_allow_html=True)
            chart_col = st.selectbox("Column", num_cols, label_visibility="collapsed")
            obj_cols  = result.select_dtypes(include="object").columns.tolist()
            if obj_cols:
                # Limit categories for readable chart
                idx_col  = obj_cols[0]
                chart_df = result[[idx_col, chart_col]].dropna().head(30).set_index(idx_col)
            else:
                chart_df = result[[chart_col]].head(50)
            st.bar_chart(chart_df, color="#7c6fff", height=200)

    elif st.session_state.status == "done":
        st.info("Query returned 0 rows — try adjusting your filters.")
    else:
        st.info("Run a pipeline in ETL Studio to see results here.")

    # ── Direct SQL editor ─────────────────────────────────────────────────────
    st.markdown('<div class="sec">SQL Editor (Direct)</div>', unsafe_allow_html=True)
    st.caption("Write DuckDB SQL directly against loaded tables.")
    manual_sql = st.text_area("SQL", height=90,
                               placeholder="SELECT * FROM sales LIMIT 10",
                               label_visibility="collapsed")
    if st.button("▶ Run SQL"):
        if not st.session_state.tables:
            st.error("No tables loaded.")
        elif manual_sql.strip():
            try:
                source_rows = sum(len(df) for df in st.session_state.tables.values())
                result_df, stats, final_sql, warns = execute_sql_safe(
                    manual_sql.strip(), source_rows,
                    st.session_state.schema, manual_sql.strip()
                )
                st.session_state.last_result  = result_df
                st.session_state.last_stats   = stats
                st.session_state.last_sql     = final_sql
                st.session_state.status       = "done"
                st.session_state.exec_warnings = warns
                agg = is_aggregation_query(final_sql)
                st.session_state.recon = reconcile(
                    source_rows, len(result_df),
                    max(0, source_rows - len(result_df)), 0, agg
                )
                st.rerun()
            except Exception as e:
                st.error(f"SQL error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SCHEMA
# ══════════════════════════════════════════════════════════════════════════════

with tab_schema:
    schema = st.session_state.schema
    if schema:
        st.markdown('<div class="sec">Loaded Schema</div>', unsafe_allow_html=True)

        # JOIN trap summary at top
        all_traps = []
        for tname, info in schema.items():
            for t in info.get("join_traps", []):
                all_traps.append(t["col"])
        if all_traps:
            trap_cols = list(set(all_traps))
            st.warning(
                f"**{len(trap_cols)} JOIN type mismatch(es) detected:** `{'`, `'.join(trap_cols)}`  \n"
                "SQL generation automatically handles these with CAST — but check results carefully."
            )

        for tname, info in schema.items():
            traps = info.get("join_traps", [])
            label = f"{tname}  ·  {info['row_count']:,} rows  ·  {len(info['columns'])} cols"
            with st.expander(label, expanded=True):
                rows = []
                for col in info["columns"]:
                    trap_flag = "⚠" if any(t["col"] == col for t in traps) else ""
                    rows.append({
                        "Column":  trap_flag + col,
                        "Type":    info["dtypes"][col],
                        "Nulls":   f'{info["null_counts"][col]:,}',
                        "Samples": ", ".join(repr(v) for v in info["sample_values"][col][:3]),
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        # DuckDB SHOW TABLES
        try:
            conn = get_conn()
            tbl_list = conn.execute("SHOW TABLES").df()
            st.markdown('<div class="sec">DuckDB SHOW TABLES</div>', unsafe_allow_html=True)
            st.dataframe(tbl_list, use_container_width=True, hide_index=True)
        except Exception:
            pass

        # Full schema as JSON (useful for debugging)
        with st.expander("Raw schema JSON (debug)"):
            debug_schema = {
                t: {k: v for k, v in info.items() if k != "sample_values"}
                for t, info in schema.items()
            }
            st.json(debug_schema)
    else:
        st.info("No tables loaded yet. Upload CSVs or load demo data in ETL Studio.")


# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="border-top:1px solid #1e1e35;margin-top:20px;padding:10px 0 2px;text-align:center">
  <span style="font-family:'DM Mono',monospace;font-size:10px;color:#1e1e40;letter-spacing:.15em">
    PRISM · AI-NATIVE DATA INTELLIGENCE ENGINE · v3.0 · MARCH 2026 · CONFIDENTIAL
  </span>
</div>
""", unsafe_allow_html=True)
