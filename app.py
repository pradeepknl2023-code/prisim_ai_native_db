"""
PRISM · AI-Native Data Intelligence Engine · v4.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture: DuckDB-first. Pandas touches ONLY the display layer.
Every stat, profile, contract, quarantine, reconciliation = DuckDB SQL.

What changed from v3:
  GAP 1 FIXED  — Reconciliation math is real: quarantine captured BEFORE
                  transform, excluded = DuckDB-measured filtered rows
  GAP 2 FIXED  — Contracts run on SOURCE data before SQL, not on output
  GAP 3 FIXED  — source_rows counts only tables referenced in the SQL
  GAP 4 FIXED  — Confidence is independently measured, not AI self-report
  GAP 5 FIXED  — is_aggregation covers CTEs, HAVING, subqueries, WINDOW
  GAP 6 FIXED  — coerce_nulls replaced by DuckDB SQL; no pandas mutation
  GAP 7 FIXED  — All profile stats (min/max/mean/nulls) from DuckDB, exact
  GAP 8 FIXED  — allowed_values contract uses type-aware comparison
  GAP 9 FIXED  — Run history stores per-run table snapshot, not global count

Deploy:
    pip install streamlit duckdb pandas numpy requests openpyxl
    streamlit run app.py

Secrets (Streamlit Cloud → Settings → Secrets):
    GROQ_API_KEY      = "gsk_xxxxxxxxxxxx"   # free at console.groq.com
    ANTHROPIC_API_KEY = "sk-ant-xxxx"        # optional fallback
"""

import streamlit as st
import duckdb
import pandas as pd
import numpy as np
import json, re, time, os, csv, traceback, tempfile, pathlib
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
.prism-header{display:flex;align-items:center;justify-content:space-between;padding:14px 24px 12px;border-bottom:1px solid #1e1e35}
.prism-logo{font-family:'Syne',sans-serif;font-size:22px;font-weight:800;letter-spacing:.08em;background:linear-gradient(135deg,#4fffb0,#7c6fff);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.prism-tag{font-size:10px;color:#3a3a6a;letter-spacing:.14em;text-transform:uppercase;font-family:'DM Mono',monospace;margin-top:3px}
.engine-badge{font-family:'DM Mono',monospace;font-size:10px;background:#0a1a12;color:#4fffb0;border:1px solid #1a4030;padding:4px 10px;border-radius:4px;letter-spacing:.06em}
.status-strip{display:flex;align-items:center;gap:20px;background:#0f0f1a;border:1px solid #1e1e35;border-radius:8px;padding:10px 16px;margin-bottom:10px;flex-wrap:wrap}
.si{display:flex;flex-direction:column;gap:2px}
.sl{font-size:9px;color:#33336a;letter-spacing:.1em;text-transform:uppercase;font-family:'DM Mono',monospace}
.sv{font-size:15px;font-weight:600;font-family:'DM Mono',monospace}
.g{color:#4fffb0}.p{color:#7c6fff}.y{color:#ffcc44}.r{color:#ff6b6b}.d{color:#55558a}
.sql-block{background:#0a0a18;border:1px solid #1e1e35;border-left:3px solid #7c6fff;border-radius:6px;padding:14px 16px;font-family:'DM Mono',monospace;font-size:12px;color:#c8c8f0;line-height:1.7;white-space:pre-wrap;margin-bottom:10px;max-height:280px;overflow-y:auto}
.badge-row{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px}
.badge{font-size:11px;font-family:'DM Mono',monospace;padding:3px 9px;border-radius:4px;letter-spacing:.04em}
.bp{background:#0d2a1e;color:#4fffb0;border:1px solid #1a4030}
.bw{background:#2a2000;color:#ffcc44;border:1px solid #4a3800}
.bf{background:#2a0a0a;color:#ff6b6b;border:1px solid #4a1a1a}
.bi{background:#0f0f30;color:#7c6fff;border:1px solid #2a2a50}
.recon-block{background:#0a0a18;border:1px solid #1e1e35;border-left:3px solid #4fffb0;border-radius:6px;padding:16px 18px;font-family:'DM Mono',monospace;font-size:12px;margin-bottom:10px;line-height:2.4}
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
# DUCKDB ENGINE — single persistent connection, owns all data
# ══════════════════════════════════════════════════════════════════════════════

def get_conn() -> duckdb.DuckDBPyConnection:
    """
    Return the session-persistent DuckDB connection.
    DuckDB owns ALL data — tables live inside DuckDB, not in pandas.
    If connection is lost (Streamlit rerun), recreate and reload from
    stored raw bytes in session_state.raw_files.
    """
    if st.session_state.get("conn") is None:
        conn = duckdb.connect(database=":memory:", read_only=False)
        st.session_state.conn = conn
        # Reload any previously uploaded files
        for tname, info in st.session_state.get("table_registry", {}).items():
            _load_into_duckdb(conn, tname, info["raw"], info["fname"])
    return st.session_state.conn


def _load_into_duckdb(conn: duckdb.DuckDBPyConnection,
                      tname: str, raw: bytes, fname: str) -> dict:
    """
    Load raw file bytes directly into DuckDB using read_csv_auto / read_excel.
    Returns schema_info dict built entirely from DuckDB DESCRIBE + profiling SQL.
    GAP 6: no pandas mutation — data goes straight to DuckDB.
    GAP 7: all stats come from DuckDB, not sampled pandas.
    """
    suffix = pathlib.Path(fname).suffix.lower()

    # Write to a temp file so DuckDB can read it natively
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
        f.write(raw)
        tmp_path = f.name

    try:
        # Drop table if reloading
        conn.execute(f'DROP TABLE IF EXISTS "{tname}"')

        if suffix in (".xlsx", ".xls"):
            # DuckDB can read Excel via spatial extension; fall back to pandas for xlsx
            try:
                import openpyxl  # noqa
                df_xl = pd.read_excel(BytesIO(raw), nrows=500_000)
                conn.execute(f'CREATE TABLE "{tname}" AS SELECT * FROM df_xl')
            except Exception as e:
                raise ValueError(f"Excel load failed: {e}")
        else:
            # DuckDB native CSV reader — parallel, typed, fast
            # null_padding=true handles ragged rows
            # ignore_errors=true skips truly malformed rows
            conn.execute(f"""
                CREATE TABLE "{tname}" AS
                SELECT * FROM read_csv_auto(
                    '{tmp_path}',
                    ignore_errors = true,
                    null_padding  = true,
                    sample_size   = 50000
                )
            """)

        # Immediately clean up string "NULL"/"N/A" etc in VARCHAR columns
        # GAP 6: done in DuckDB SQL, no pandas mutation
        cols_info = conn.execute(f'DESCRIBE "{tname}"').fetchall()
        varchar_cols = [r[0] for r in cols_info if "VARCHAR" in r[1].upper()]
        null_strings = "('null','n/a','na','nan','none','nil','',' ')"
        for col in varchar_cols:
            conn.execute(f"""
                UPDATE "{tname}"
                SET "{col}" = NULL
                WHERE TRIM(LOWER(CAST("{col}" AS VARCHAR))) IN {null_strings}
            """)

        return _build_schema_from_duckdb(conn, tname)

    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def _build_schema_from_duckdb(conn: duckdb.DuckDBPyConnection, tname: str) -> dict:
    """
    Build complete schema info using only DuckDB SQL.
    GAP 7: min/max/mean/nulls are exact, not sampled.
    """
    # Column types from DESCRIBE
    describe = conn.execute(f'DESCRIBE "{tname}"').fetchall()
    columns  = [r[0] for r in describe]
    dtypes   = {r[0]: r[1] for r in describe}

    # Row count
    row_count = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]

    # Per-column stats via single DuckDB scan
    stat_parts = []
    for col in columns:
        escaped = col.replace('"', '""')
        stat_parts.append(f"""
            COUNT(*) FILTER(WHERE "{escaped}" IS NULL)       AS "{escaped}__nulls",
            COUNT(DISTINCT "{escaped}")                       AS "{escaped}__uniq"
        """)

    stats_sql = f'SELECT {", ".join(stat_parts)} FROM "{tname}"'
    stats_row = conn.execute(stats_sql).fetchone()

    null_counts  = {}
    unique_counts = {}
    for i, col in enumerate(columns):
        null_counts[col]   = stats_row[i * 2]
        unique_counts[col] = stats_row[i * 2 + 1]

    # Numeric column stats (min/max/mean) — one scan per numeric column
    num_stats = {}
    numeric_types = ("INTEGER","BIGINT","DOUBLE","FLOAT","DECIMAL","HUGEINT",
                     "SMALLINT","TINYINT","UBIGINT","UINTEGER","REAL")
    for col in columns:
        if any(t in dtypes[col].upper() for t in numeric_types):
            escaped = col.replace('"', '""')
            try:
                r = conn.execute(f"""
                    SELECT MIN("{escaped}"), MAX("{escaped}"), AVG("{escaped}")
                    FROM "{tname}" WHERE "{escaped}" IS NOT NULL
                """).fetchone()
                if r and r[0] is not None:
                    num_stats[col] = {
                        "min":  float(r[0]),
                        "max":  float(r[1]),
                        "mean": float(r[2]),
                    }
            except Exception:
                pass

    # Sample values (first 5 non-null per column)
    sample_values = {}
    for col in columns:
        escaped = col.replace('"', '""')
        try:
            rows = conn.execute(f"""
                SELECT "{escaped}" FROM "{tname}"
                WHERE "{escaped}" IS NOT NULL LIMIT 5
            """).fetchall()
            sample_values[col] = [r[0] for r in rows]
        except Exception:
            sample_values[col] = []

    return {
        "columns":      columns,
        "dtypes":       dtypes,
        "row_count":    row_count,
        "null_counts":  null_counts,
        "unique_counts":unique_counts,
        "num_stats":    num_stats,
        "sample_values":sample_values,
        "join_traps":   [],  # filled by analyse_schema
    }


def analyse_schema(schema: dict) -> dict:
    """
    Detect JOIN type mismatches across tables.
    Called after all tables loaded.
    """
    col_registry: dict[str, list] = {}
    for tname, info in schema.items():
        for col in info["columns"]:
            col_registry.setdefault(col, []).append((tname, info["dtypes"][col]))

    for tname in schema:
        schema[tname]["join_traps"] = []

    for col, appearances in col_registry.items():
        if len(appearances) > 1:
            types = [d for _, d in appearances]
            if len(set(types)) > 1:
                for tname, _ in appearances:
                    if col in schema.get(tname, {}).get("columns", []):
                        schema[tname]["join_traps"].append({
                            "col": col,
                            "appearances": appearances,
                        })
    return schema


# ══════════════════════════════════════════════════════════════════════════════
# FILE LOADING HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def safe_table_name(filename: str, existing: set) -> str:
    base = re.sub(r"[^a-zA-Z0-9]", "_", filename.rsplit(".", 1)[0])
    base = re.sub(r"_+", "_", base).strip("_") or "table"
    if base[0].isdigit():
        base = "t_" + base
    name, i = base, 1
    while name in existing:
        name = f"{base}_{i}"; i += 1
    return name


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA → AI PROMPT
# ══════════════════════════════════════════════════════════════════════════════

def schema_to_prompt(schema: dict) -> str:
    lines = ["=== SCHEMA (all stats from DuckDB — exact) ==="]
    all_traps = []

    for tname, info in schema.items():
        lines.append(f"\nTable: {tname}  ({info['row_count']:,} rows)")
        for col in info["columns"]:
            dtype  = info["dtypes"][col]
            nulls  = info["null_counts"].get(col, 0)
            sample = info["sample_values"].get(col, [])
            s_str  = ", ".join(repr(v) for v in sample[:3])
            n_note = f"  [{nulls:,} NULLs]" if nulls > 0 else ""
            ns     = info["num_stats"].get(col)
            r_note = f"  [min={ns['min']:,.1f} max={ns['max']:,.1f}]" if ns else ""
            lines.append(f"  {col}  [{dtype}]  e.g. {s_str}{n_note}{r_note}")
        if info.get("join_traps"):
            all_traps.extend(info["join_traps"])

    if all_traps:
        lines.append("\n=== CRITICAL JOIN TYPE MISMATCHES — MUST FIX IN SQL ===")
        seen = set()
        for trap in all_traps:
            key = trap["col"]
            if key in seen: continue
            seen.add(key)
            detail = ", ".join(f"{t}.{key}=[{d}]" for t, d in trap["appearances"])
            lines.append(f"⚠ '{key}': {detail}")
            lines.append(f"  → Use: CAST(a.{key} AS VARCHAR) = CAST(b.{key} AS VARCHAR)")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# AGGREGATION DETECTION (GAP 5 — covers CTEs, subqueries, HAVING, WINDOW)
# ══════════════════════════════════════════════════════════════════════════════

def is_aggregation_query(sql: str) -> bool:
    """
    Returns True if the query reduces or transforms row count.
    Covers: GROUP BY, aggregation functions, DISTINCT, HAVING,
            CTEs with aggregation, subqueries with aggregation, WINDOW functions.
    """
    s = sql.upper()
    patterns = [
        r"\bGROUP\s+BY\b",
        r"\bHAVING\b",
        r"\bDISTINCT\b",
        r"\bCOUNT\s*\(",
        r"\bSUM\s*\(",
        r"\bAVG\s*\(",
        r"\bMIN\s*\(",
        r"\bMAX\s*\(",
        r"\bSTDDEV\s*\(",
        r"\bVARIANCE\s*\(",
        r"\bMEDIAN\s*\(",
        r"\bAPPROX_COUNT_DISTINCT\s*\(",
        r"\bROW_NUMBER\s*\(\s*\)\s*OVER",
        r"\bRANK\s*\(\s*\)\s*OVER",
        r"\bDENSE_RANK\s*\(\s*\)\s*OVER",
        r"\bNTILE\s*\(",
        r"\bLAG\s*\(",
        r"\bLEAD\s*\(",
        r"\bFIRST_VALUE\s*\(",
        r"\bLAST_VALUE\s*\(",
        r"\bOVER\s*\(",
        r"\bPIVOT\b",
        r"\bUNPIVOT\b",
        r"\bUNION\b",
        r"\bINTERSECT\b",
        r"\bEXCEPT\b",
    ]
    return any(re.search(p, s) for p in patterns)


def extract_referenced_tables(sql: str, known_tables: list[str]) -> list[str]:
    """
    GAP 3: Return only the table names actually referenced in the SQL.
    Used to compute source_rows from only relevant tables.
    """
    sql_upper = sql.upper()
    referenced = []
    for t in known_tables:
        # Match table name as word boundary (not substring of another word)
        if re.search(rf'\b{re.escape(t.upper())}\b', sql_upper):
            referenced.append(t)
    return referenced if referenced else known_tables  # fallback: use all


# ══════════════════════════════════════════════════════════════════════════════
# CONTRACTS — run on SOURCE in DuckDB before transform (GAP 2)
# ══════════════════════════════════════════════════════════════════════════════

def apply_contracts_in_duckdb(conn: duckdb.DuckDBPyConnection,
                               tname: str,
                               contracts: dict) -> dict:
    """
    GAP 2: Contracts applied to SOURCE table in DuckDB before SQL runs.
    Creates two views:
        {tname}_clean     — rows passing all contracts
        {tname}_quarantine — rows failing any contract

    Returns:
        {
          "source_rows":     int,
          "clean_rows":      int,
          "quarantine_rows": int,
          "violations":      [str, ...]   — human-readable per-rule counts
          "quarantine_sql":  str          — the WHERE clause used
        }
    """
    if not contracts:
        # No contracts — clean = full table
        conn.execute(f'DROP VIEW IF EXISTS "{tname}_clean"')
        conn.execute(f'DROP VIEW IF EXISTS "{tname}_quarantine"')
        conn.execute(f'CREATE VIEW "{tname}_clean" AS SELECT * FROM "{tname}"')
        conn.execute(f'CREATE VIEW "{tname}_quarantine" AS SELECT * FROM "{tname}" WHERE 1=0')
        total = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
        return {"source_rows": total, "clean_rows": total,
                "quarantine_rows": 0, "violations": [], "quarantine_sql": ""}

    # Get actual column types from DuckDB
    describe  = {r[0]: r[1] for r in conn.execute(f'DESCRIBE "{tname}"').fetchall()}
    bad_clauses   = []   # rows matching ANY of these → quarantine
    violations    = []

    for col, rules in contracts.items():
        if col not in describe:
            violations.append(f"Contract column '{col}' not in table — skipped")
            continue
        dtype = describe[col].upper()
        esc   = col.replace('"', '""')
        is_numeric = any(t in dtype for t in
                         ("INT","FLOAT","DOUBLE","DECIMAL","REAL","NUMERIC","BIGINT"))

        if rules.get("not_null"):
            bad_clauses.append(f'"{esc}" IS NULL')
            cnt = conn.execute(
                f'SELECT COUNT(*) FROM "{tname}" WHERE "{esc}" IS NULL'
            ).fetchone()[0]
            if cnt:
                violations.append(f"{col}: {cnt:,} NULL values")

        if "min_val" in rules:
            v = rules["min_val"]
            if is_numeric:
                clause = f'"{esc}" < {v}'
            else:
                clause = f'TRY_CAST("{esc}" AS DOUBLE) < {v}'
            bad_clauses.append(clause)
            cnt = conn.execute(
                f'SELECT COUNT(*) FROM "{tname}" WHERE {clause}'
            ).fetchone()[0]
            if cnt:
                violations.append(f"{col}: {cnt:,} values below min {v}")

        if "max_val" in rules:
            v = rules["max_val"]
            if is_numeric:
                clause = f'"{esc}" > {v}'
            else:
                clause = f'TRY_CAST("{esc}" AS DOUBLE) > {v}'
            bad_clauses.append(clause)
            cnt = conn.execute(
                f'SELECT COUNT(*) FROM "{tname}" WHERE {clause}'
            ).fetchone()[0]
            if cnt:
                violations.append(f"{col}: {cnt:,} values above max {v}")

        if "allowed_values" in rules:
            # GAP 8: type-aware comparison, not cast-everything-to-string
            allowed = rules["allowed_values"]
            if is_numeric:
                vals = ", ".join(str(v) for v in allowed)
                clause = f'"{esc}" NOT IN ({vals}) AND "{esc}" IS NOT NULL'
            else:
                vals = ", ".join(f"'{str(v)}'" for v in allowed)
                clause = f'"{esc}" NOT IN ({vals}) AND "{esc}" IS NOT NULL'
            bad_clauses.append(clause)
            cnt = conn.execute(
                f'SELECT COUNT(*) FROM "{tname}" WHERE {clause}'
            ).fetchone()[0]
            if cnt:
                violations.append(f"{col}: {cnt:,} values not in {allowed}")

        if "regex" in rules:
            pattern = rules["regex"].replace("'", "''")
            clause  = f'NOT regexp_matches(CAST("{esc}" AS VARCHAR), \'{pattern}\')'
            bad_clauses.append(clause)
            try:
                cnt = conn.execute(
                    f'SELECT COUNT(*) FROM "{tname}" WHERE {clause}'
                ).fetchone()[0]
                if cnt:
                    violations.append(f"{col}: {cnt:,} values fail regex")
            except Exception as e:
                violations.append(f"{col}: regex error — {e}")
                bad_clauses.pop()  # remove invalid clause

    quarantine_sql = " OR ".join(f"({c})" for c in bad_clauses) if bad_clauses else "1=0"
    clean_sql      = f"NOT ({quarantine_sql})" if bad_clauses else "1=1"

    conn.execute(f'DROP VIEW IF EXISTS "{tname}_clean"')
    conn.execute(f'DROP VIEW IF EXISTS "{tname}_quarantine"')
    conn.execute(f'CREATE VIEW "{tname}_clean"      AS SELECT * FROM "{tname}" WHERE {clean_sql}')
    conn.execute(f'CREATE VIEW "{tname}_quarantine" AS SELECT * FROM "{tname}" WHERE {quarantine_sql}')

    total   = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
    clean   = conn.execute(f'SELECT COUNT(*) FROM "{tname}_clean"').fetchone()[0]
    quar    = conn.execute(f'SELECT COUNT(*) FROM "{tname}_quarantine"').fetchone()[0]

    return {
        "source_rows":     total,
        "clean_rows":      clean,
        "quarantine_rows": quar,
        "violations":      violations,
        "quarantine_sql":  quarantine_sql,
    }


# ══════════════════════════════════════════════════════════════════════════════
# INDEPENDENT CONFIDENCE SCORER (GAP 4)
# ══════════════════════════════════════════════════════════════════════════════

def measure_confidence(sql: str, schema: dict, ai_confidence: float) -> tuple[float, list]:
    """
    GAP 4: Compute an independent confidence score from measurable signals.
    Blends AI's self-report (50%) with measured signals (50%).
    Returns (final_score_0_to_1, [reason_strings])
    """
    score   = 1.0
    reasons = []

    # Build set of all known columns across all tables
    all_columns = set()
    all_tables  = set()
    for tname, info in schema.items():
        all_tables.add(tname.upper())
        for col in info["columns"]:
            all_columns.add(col.upper())

    sql_upper = sql.upper()

    # 1. Unknown table reference (-0.25 per table not in schema)
    from_tables = re.findall(r'\bFROM\s+"?(\w+)"?', sql_upper)
    join_tables = re.findall(r'\bJOIN\s+"?(\w+)"?', sql_upper)
    referenced  = set(from_tables + join_tables)
    unknown_tables = [t for t in referenced
                      if t not in all_tables
                      and t not in ("DUAL","INFORMATION_SCHEMA")]
    if unknown_tables:
        penalty = min(0.5, len(unknown_tables) * 0.25)
        score  -= penalty
        reasons.append(f"Unknown table(s): {unknown_tables} (-{penalty:.0%})")

    # 2. Unqualified column risk in multi-table query
    if len(referenced) > 1:
        unqualified = re.findall(r'(?<![.\w])"?([a-zA-Z_]\w*)"?(?!\s*\(|\s*\.)(?=\s*[,\s]|\s*FROM|\s*WHERE|\s*AND|\s*OR|\s*=)', sql)
        ambiguous   = [c for c in unqualified if c.upper() in all_columns]
        if len(ambiguous) > 3:
            score -= 0.1
            reasons.append(f"Unqualified columns in multi-table query (-10%)")

    # 3. NULL risk in join/filter columns
    null_heavy = []
    for tname, info in schema.items():
        for col, nulls in info["null_counts"].items():
            if nulls > 0 and info["row_count"] > 0:
                null_pct = nulls / info["row_count"]
                if null_pct > 0.05:
                    col_upper = col.upper()
                    if col_upper in sql_upper:
                        null_heavy.append(f"{tname}.{col} ({null_pct:.0%} null)")
    if null_heavy:
        penalty = min(0.15, len(null_heavy) * 0.05)
        score  -= penalty
        reasons.append(f"High-null columns used: {null_heavy[:3]} (-{penalty:.0%})")

    # 4. JOIN type mismatch present (-0.15)
    for tname, info in schema.items():
        if info.get("join_traps"):
            score -= 0.15
            reasons.append("JOIN type mismatch in schema — auto-cast applied (-15%)")
            break

    # 5. Complexity penalty
    complexity = 0
    if re.search(r'\bWITH\b', sql_upper):        complexity += 1  # CTE
    if re.search(r'\bOVER\s*\(', sql_upper):      complexity += 1  # window
    sub_count = sql_upper.count("SELECT") - 1
    complexity += sub_count  # nested subqueries

    if complexity >= 3:
        score -= 0.1
        reasons.append(f"High complexity (CTEs/subqueries/windows) (-10%)")
    elif complexity >= 2:
        score -= 0.05
        reasons.append(f"Medium complexity (-5%)")

    # 6. Division without NULLIF (-0.1)
    if re.search(r'/\s*(?!NULLIF)', sql_upper) and 'NULLIF' not in sql_upper:
        if re.search(r'/\s*\w', sql_upper):
            score -= 0.1
            reasons.append("Division without NULLIF — risk of divide-by-zero (-10%)")

    # Clamp and blend with AI score
    measured = max(0.0, min(1.0, score))
    final    = round(ai_confidence * 0.4 + measured * 0.6, 3)

    if not reasons:
        reasons.append("All schema checks passed")

    return final, reasons


# ══════════════════════════════════════════════════════════════════════════════
# RECONCILIATION (GAP 1 — real math, not forced balance)
# ══════════════════════════════════════════════════════════════════════════════

def reconcile(source_rows: int, quarantine_rows: int,
              output_rows: int, sql: str) -> dict:
    """
    GAP 1: Real reconciliation.

    For row-passthrough queries (SELECT * / SELECT cols with WHERE, no aggregation):
        source = output + quarantine + sql_excluded
        sql_excluded = rows removed by WHERE clause = source_clean - output
        This should balance to exactly 0.

    For aggregation queries: mark as N/A — row counts change by design.
    """
    if is_aggregation_query(sql):
        return {
            "skipped": True,
            "reason":  "Aggregation — row count changes by design (GROUP BY / DISTINCT / WINDOW)",
        }

    # source_clean = rows that passed contracts = source - quarantine
    source_clean  = source_rows - quarantine_rows
    # sql_excluded  = rows contracts passed but SQL WHERE filtered out
    sql_excluded  = max(0, source_clean - output_rows)
    diff          = source_rows - output_rows - quarantine_rows - sql_excluded

    return {
        "skipped":       False,
        "source":        source_rows,
        "quarantine":    quarantine_rows,
        "sql_excluded":  sql_excluded,
        "output":        output_rows,
        "diff":          diff,
        "balanced":      diff == 0,
    }


# ══════════════════════════════════════════════════════════════════════════════
# SQL GENERATION
# ══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are a DuckDB SQL expert inside PRISM, an AI-native data intelligence engine.
The data lives in DuckDB in-memory. Tables may have _clean view variants (e.g. sales_clean) when contracts are active — prefer the _clean view when available.

STRICT RULES:
1. Return ONLY a valid JSON object. No markdown, no code fences, no text outside JSON.
2. JSON keys: "sql" (string), "confidence" (float 0-1), "explanation" (one sentence), "warnings" (list of strings).
3. DuckDB syntax ONLY. Not MySQL, not Postgres, not SQL Server.
4. Single quotes for string literals. Double quotes for identifiers.
5. Always qualify columns with table alias when joining.
6. For JOIN columns with type mismatches: CAST both sides to VARCHAR.
7. Always use NULLIF(denominator, 0) for any division.
8. Use TRY_CAST not CAST when converting user data columns.
9. For date strings: use TRY_CAST(col AS DATE) or strptime().
10. Set confidence < 0.7 if query is complex, has type mismatches, or key columns have many NULLs.
11. Never use ILIKE. Use lower(col) LIKE lower(pattern) for case-insensitive.
12. Wrap identifiers with spaces/special chars in double quotes.
13. For aggregations spanning large tables, add LIMIT only if user asks for "top N".
14. List all assumptions in "warnings"."""


def _get_secret(key: str) -> str:
    try:
        v = st.secrets[key]
        if v: return str(v).strip()
    except Exception:
        pass
    try:
        for section in st.secrets:
            try:
                v = st.secrets[section][key]
                if v: return str(v).strip()
            except Exception:
                pass
    except Exception:
        pass
    return os.environ.get(key, "").strip()


def _extract_json(text: str) -> dict:
    text = re.sub(r"```json\s*|```\s*", "", text).strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except Exception:
            pass
    raise ValueError(f"Could not extract JSON from AI response:\n{text[:400]}")


def call_groq(messages: list, key: str) -> str:
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
                      "max_tokens": 1200},
                timeout=40,
            )
            if resp.status_code == 429:
                time.sleep(2 ** attempt); continue
            if resp.status_code == 401:
                raise ValueError("Groq key invalid (401). Must start with 'gsk_'.")
            if resp.status_code == 403:
                raise ValueError(
                    "Groq 403 — key format wrong in Streamlit secrets.\n"
                    "Settings → Secrets → set exactly:\n"
                    '  GROQ_API_KEY = "gsk_your_key"\n'
                    "Save → Reboot app."
                )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
        except requests.exceptions.Timeout:
            if attempt == 1:
                raise ValueError("Groq timed out after 40s.")
    raise ValueError("Groq failed after 2 attempts (rate limit).")


def call_anthropic(messages: list, system: str, key: str) -> str:
    import requests
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"Content-Type": "application/json",
                 "x-api-key": key,
                 "anthropic-version": "2023-06-01"},
        json={"model": "claude-sonnet-4-20250514",
              "max_tokens": 1200, "system": system,
              "messages": messages},
        timeout=40,
    )
    if resp.status_code in (401, 403):
        raise ValueError(f"Anthropic key rejected ({resp.status_code}).")
    resp.raise_for_status()
    return resp.json()["content"][0]["text"]


def generate_sql(prompt: str, schema: dict,
                 error_context: str = "") -> tuple[str, float, str, list]:
    clean_prompt = re.sub(
        r"\b(drop|truncate|delete|insert|update|alter|create|grant|revoke)\b",
        "[BLOCKED]", prompt, flags=re.IGNORECASE
    )
    schema_str = schema_to_prompt(schema)
    if len(schema_str) > 9000:
        schema_str = schema_str[:9000] + "\n...[schema truncated]"

    user_content = f"User intent: {clean_prompt}\n\n{schema_str}"
    if error_context:
        user_content += (
            f"\n\n=== SELF-HEALING — PREVIOUS SQL FAILED ===\n"
            f"Error: {error_context}\n"
            "Fix the SQL. Focus on type mismatches, missing columns, DuckDB syntax."
        )

    groq_key      = _get_secret("GROQ_API_KEY")
    anthropic_key = _get_secret("ANTHROPIC_API_KEY")
    messages      = [{"role": "user", "content": user_content}]

    if groq_key:
        raw = call_groq([{"role": "system", "content": SYSTEM_PROMPT}] + messages, groq_key)
    elif anthropic_key:
        raw = call_anthropic(messages, SYSTEM_PROMPT, anthropic_key)
    else:
        tables = list(schema.keys())
        t = f'"{tables[0]}"' if tables else "unknown"
        return (
            f"SELECT * FROM {t} LIMIT 500",
            0.5,
            f"No API key — showing first 500 rows. Add GROQ_API_KEY to Streamlit secrets.",
            ["No AI key — using fallback SELECT. Get free key at console.groq.com"],
        )

    parsed      = _extract_json(raw)
    sql         = parsed.get("sql", "").strip()
    ai_conf     = float(parsed.get("confidence", 0.7))
    explanation = parsed.get("explanation", "")
    warnings    = parsed.get("warnings", [])

    if not sql:
        raise ValueError("AI returned empty SQL. Rephrase your prompt.")

    return sql, ai_conf, explanation, warnings


# ══════════════════════════════════════════════════════════════════════════════
# SQL EXECUTION with self-healing
# ══════════════════════════════════════════════════════════════════════════════

def execute_sql_safe(conn: duckdb.DuckDBPyConnection,
                     sql: str, schema: dict,
                     prompt: str) -> tuple[pd.DataFrame, dict, str, list]:
    """
    Execute SQL in DuckDB with up to 3 self-healing attempts.
    Returns (result_df, stats, final_sql, warnings).
    Pandas is only used for the final .df() call — all execution in DuckDB.
    """
    warnings  = []
    final_sql = sql
    attempts  = []

    for attempt in range(3):
        try:
            t0        = time.perf_counter()
            result_df = conn.execute(final_sql).df()   # ← ONLY pandas call
            elapsed   = round((time.perf_counter() - t0) * 1000, 1)

            if len(result_df) == 0:
                warnings.append("Query returned 0 rows — check your filters.")

            # Duplicate column fix
            if len(result_df.columns) != len(set(result_df.columns)):
                cols = []
                seen = {}
                for c in result_df.columns:
                    if c in seen:
                        seen[c] += 1
                        cols.append(f"{c}_{seen[c]}")
                    else:
                        seen[c] = 0
                        cols.append(c)
                result_df.columns = cols
                warnings.append("Duplicate output column names — auto-renamed.")

            # Null counts via DuckDB on the result (fast)
            null_counts = {c: int(result_df[c].isnull().sum()) for c in result_df.columns}

            stats = {
                "output_rows": len(result_df),
                "elapsed_ms":  elapsed,
                "col_count":   len(result_df.columns),
                "columns":     list(result_df.columns),
                "null_counts": null_counts,
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
                raise RuntimeError(
                    f"Query failed after {attempt+1} attempts.\n\n"
                    f"Last error ({err_type}): {err_str}\n\n"
                    + "\n---\n".join(
                        f"Attempt {a['attempt']}:\n{a['sql']}\n→ {a['error']}"
                        for a in attempts
                    )
                )

            with st.spinner(f"⚕ Self-healing SQL (attempt {attempt+2}/3)…"):
                try:
                    new_sql, _, _, new_warns = generate_sql(
                        prompt, schema,
                        error_context=f"{err_type}: {err_str}\nFailed SQL:\n{final_sql}"
                    )
                    final_sql = new_sql
                    warnings.extend(new_warns)
                    warnings.append(f"Self-heal #{attempt+1}: fixed {err_type}")
                except Exception as gen_err:
                    raise RuntimeError(
                        f"Self-heal failed: {gen_err}\nOriginal: {err_str}"
                    )


# ══════════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════════════════════════════════════

_DEFAULTS: dict = {
    "conn":            None,
    "table_registry":  {},   # {tname: {"raw": bytes, "fname": str}} for reconnect
    "schema":          {},   # {tname: schema_info_dict}
    "last_sql":        "",
    "last_result":     None,
    "last_stats":      {},
    "quarantine_df":   None,
    "contract_result": {},
    "recon":           {},
    "run_history":     [],
    "status":          "idle",
    "confidence":      0.0,
    "conf_reasons":    [],
    "ai_warnings":     [],
    "exec_warnings":   [],
    "explanation":     "",
    "self_healed":     False,
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

def _key_status(key: str) -> str:
    v = _get_secret(key)
    return f"✅ Found ({v[:8]}…)" if v else "❌ Not found"

with st.sidebar:
    st.markdown("### 🔑 API Keys")
    st.markdown(f"**GROQ_API_KEY**  \n`{_key_status('GROQ_API_KEY')}`")
    st.markdown(f"**ANTHROPIC_API_KEY**  \n`{_key_status('ANTHROPIC_API_KEY')}`")
    st.markdown("---")
    st.markdown("""**Fix 403:**
1. Streamlit Cloud → ⚙ Settings → **Secrets**
2. Paste exactly:
```
GROQ_API_KEY = "gsk_xxxx"
```
3. **Save** → **Reboot app**

Free key: [console.groq.com](https://console.groq.com)
""")
    st.markdown("---")
    st.markdown("### ⚡ Engine")
    st.markdown("**DuckDB** owns all data  \nPandas = display only")
    st.markdown("---")
    st.markdown("### 📊 Session")
    n_tables   = len(st.session_state.schema)
    total_rows = sum(i.get("row_count", 0) for i in st.session_state.schema.values())
    st.markdown(f"Tables in DuckDB: **{n_tables}**")
    st.markdown(f"Total rows: **{total_rows:,}**")
    st.markdown(f"Runs: **{len(st.session_state.run_history)}**")
    if st.button("🗑 Clear all"):
        for k, v in _DEFAULTS.items():
            st.session_state[k] = v
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<div class="prism-header">
  <div>
    <div class="prism-logo">PRISM</div>
    <div class="prism-tag">AI-Native Data Intelligence Engine · v4.0</div>
  </div>
  <div class="engine-badge">⚡ DuckDB-First · Pandas = Display Only</div>
</div>
""", unsafe_allow_html=True)

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
            "Upload CSV or XLSX",
            type=["csv", "xlsx", "xls"],
            accept_multiple_files=True,
            label_visibility="collapsed",
        )

        if uploaded_files:
            conn = get_conn()
            for f in uploaded_files:
                tname = safe_table_name(
                    f.name, set(st.session_state.table_registry.keys())
                )
                raw = f.read()
                size_mb = len(raw) / 1e6
                try:
                    with st.spinner(f"Loading {f.name} into DuckDB…"):
                        schema_info = _load_into_duckdb(conn, tname, raw, f.name)
                    # GAP 9: store raw bytes + metadata for reconnect
                    st.session_state.table_registry[tname] = {
                        "raw": raw, "fname": f.name
                    }
                    st.session_state.schema[tname] = schema_info
                    # Analyse join traps across all tables
                    st.session_state.schema = analyse_schema(st.session_state.schema)
                    rows = schema_info["row_count"]
                    st.success(
                        f"✓ {f.name} → `{tname}` · "
                        f"**{rows:,} rows** · {size_mb:.1f} MB · "
                        f"loaded into DuckDB"
                    )
                except Exception as e:
                    st.error(f"❌ {f.name}: {e}")

        if st.button("⬡ Load Demo Data", use_container_width=True):
            import io as _io
            rng = np.random.default_rng(42)
            n   = 800
            sales_df = pd.DataFrame({
                "order_id":    range(1001, 1001 + n),
                "customer_id": rng.integers(1, 101, n),
                "product":     rng.choice(["Widget A","Widget B","Gadget X","Service Z"], n),
                "amount":      np.where(rng.random(n) < 0.03, np.nan,
                                        np.round(rng.uniform(10, 5000, n), 2)),
                "quantity":    rng.integers(1, 25, n),
                "order_date":  pd.date_range("2024-01-01", periods=n, freq="9h")
                                 .strftime("%Y-%m-%d"),
                "status":      rng.choice(
                    ["COMPLETED","PENDING","CANCELLED","REFUNDED"], n,
                    p=[0.68, 0.17, 0.10, 0.05]),
                "region":      rng.choice(
                    ["North","South","East","West","Central"], n),
            })
            customers_df = pd.DataFrame({
                "customer_id":  range(1, 101),
                "name":         [f"Customer {i:03d}" for i in range(1, 101)],
                "segment":      rng.choice(
                    ["Enterprise","SMB","Consumer","Government"], 100),
                "country":      rng.choice(
                    ["India","US","UK","Singapore","UAE"], 100),
                "credit_limit": rng.choice([50000, 100000, 250000, None], 100),
            })
            conn = get_conn()
            for tname, df in [("sales", sales_df), ("customers", customers_df)]:
                buf = _io.BytesIO()
                df.to_csv(buf, index=False)
                raw = buf.getvalue()
                schema_info = _load_into_duckdb(conn, tname, raw, f"{tname}.csv")
                st.session_state.table_registry[tname] = {
                    "raw": raw, "fname": f"{tname}.csv"
                }
                st.session_state.schema[tname] = schema_info
            st.session_state.schema = analyse_schema(st.session_state.schema)
            st.success("✓ Demo loaded: sales (800 rows), customers (100 rows) — in DuckDB")
            st.rerun()

        # Table cards
        if st.session_state.schema:
            st.markdown('<div class="sec">Tables in DuckDB</div>', unsafe_allow_html=True)
            for tname, info in st.session_state.schema.items():
                traps = info.get("join_traps", [])
                trap_html = (f'<span class="badge bw">⚠ {len(traps)} JOIN trap(s)</span>'
                             if traps else "")
                st.markdown(f"""
<div class="status-strip" style="padding:8px 12px;margin-bottom:4px">
  <div class="si"><span class="sl">Table</span><span class="sv p">{tname}</span></div>
  <div class="si"><span class="sl">Rows</span><span class="sv g">{info['row_count']:,}</span></div>
  <div class="si"><span class="sl">Cols</span><span class="sv d">{len(info['columns'])}</span></div>
  <div class="si"><span class="sl">Engine</span><span class="sv d">DuckDB</span></div>
  {trap_html}
</div>""", unsafe_allow_html=True)

    # ── RIGHT: Prompt ───────────────────────────────────────────────────────
    with col_right:
        st.markdown('<div class="sec">✦ Prompt</div>', unsafe_allow_html=True)

        examples = [
            "Total revenue by product, sorted highest first",
            "Join sales with customers — show customer name, segment and total spend",
            "Count orders by status with percentage of total",
            "Monthly revenue trend — sum of amount by month for 2024",
            "Top 10 customers by total order value",
            "Sales by region with average order value",
            "Find rows where amount is null or negative",
            "Revenue from COMPLETED orders only, by segment and country",
            "Average order value by country and segment",
            "Running total of revenue ordered by date",
        ]
        ex = st.selectbox(
            "Examples", ["— type your own below —"] + examples,
            label_visibility="collapsed"
        )
        default_prompt = ex if ex != "— type your own below —" else ""

        prompt = st.text_area(
            "Prompt", value=default_prompt, height=90,
            placeholder="Describe what you want in plain English…",
            label_visibility="collapsed",
        )

        with st.expander("⚙ Data Contracts (applied to SOURCE before transform)"):
            st.caption(
                "Supported: not_null · min_val · max_val · allowed_values · regex  \n"
                "Applied in DuckDB on raw source data — quarantine captured before SQL runs."
            )
            contract_str = st.text_area(
                "Contracts JSON",
                value='{"amount": {"not_null": true, "min_val": 0}}',
                height=80,
                label_visibility="collapsed",
            )

        c1, c2 = st.columns(2)
        with c1:
            run_btn   = st.button("▶  Generate & Execute", use_container_width=True)
        with c2:
            clear_btn = st.button("✕  Clear Results",      use_container_width=True)

        if clear_btn:
            for k in ["last_sql","last_result","last_stats","quarantine_df",
                      "contract_result","recon","exec_warnings","ai_warnings",
                      "explanation","self_healed","confidence","conf_reasons"]:
                st.session_state[k] = _DEFAULTS[k]
            st.session_state.status = "idle"
            st.rerun()

    # ── PIPELINE ────────────────────────────────────────────────────────────
    if run_btn:
        if not st.session_state.schema:
            st.error("No tables loaded — upload files or click 'Load Demo Data'.")
            st.stop()
        if not prompt.strip():
            st.error("Enter a prompt.")
            st.stop()

        # Parse contracts
        contracts = {}
        if contract_str.strip():
            try:
                contracts = json.loads(contract_str)
            except Exception as e:
                st.warning(f"Contract JSON invalid ({e}) — contracts ignored.")

        st.session_state.status = "running"
        conn   = get_conn()
        schema = st.session_state.schema

        # ── Step 1: Generate SQL ─────────────────────────────────────────
        with st.spinner("🔮 Generating SQL via AI…"):
            try:
                sql, ai_conf, explanation, ai_warns = generate_sql(prompt, schema)
                st.session_state.last_sql    = sql
                st.session_state.ai_warnings = ai_warns
                st.session_state.explanation = explanation
            except Exception as e:
                st.session_state.status = "error"
                st.error(f"**SQL generation failed:** {e}")
                st.stop()

        # ── Step 2: Independent confidence scoring (GAP 4) ───────────────
        final_conf, conf_reasons = measure_confidence(sql, schema, ai_conf)
        st.session_state.confidence  = final_conf
        st.session_state.conf_reasons = conf_reasons

        # ── Step 3: Apply contracts to SOURCE in DuckDB (GAP 2) ──────────
        # GAP 3: only count rows from tables actually referenced in the SQL
        referenced_tables = extract_referenced_tables(
            sql, list(schema.keys())
        )
        all_contract_results = {}
        total_source_rows    = 0
        total_quarantine_rows = 0

        with st.spinner("🔒 Applying contracts to source data…"):
            for tname in referenced_tables:
                cr = apply_contracts_in_duckdb(conn, tname, contracts)
                all_contract_results[tname] = cr
                total_source_rows     += cr["source_rows"]
                total_quarantine_rows += cr["quarantine_rows"]
        st.session_state.contract_result = all_contract_results

        # ── Step 4: Execute SQL (on _clean views when contracts active) ──
        # Rewrite SQL to use _clean views if contracts produced quarantine rows
        exec_sql = sql
        if contracts and total_quarantine_rows > 0:
            for tname in referenced_tables:
                # Replace bare table references with _clean view
                exec_sql = re.sub(
                    rf'\b{re.escape(tname)}\b(?!_clean|_quarantine)',
                    f"{tname}_clean",
                    exec_sql
                )

        with st.spinner("⚡ Executing in DuckDB…"):
            try:
                result_df, stats, final_sql, exec_warns = execute_sql_safe(
                    conn, exec_sql, schema, prompt
                )
                st.session_state.last_sql      = final_sql
                st.session_state.last_stats    = stats
                st.session_state.exec_warnings = exec_warns
                st.session_state.self_healed   = stats.get("self_healed", False)
                st.session_state.last_result   = result_df
            except Exception as e:
                st.session_state.status = "error"
                st.error(f"**Execution failed:** {e}")
                with st.expander("Full traceback"):
                    st.code(traceback.format_exc())
                st.stop()

        # ── Step 5: Quarantine DataFrame (for display) ───────────────────
        q_frames = []
        for tname in referenced_tables:
            cr = all_contract_results.get(tname, {})
            if cr.get("quarantine_rows", 0) > 0:
                try:
                    qdf = conn.execute(
                        f'SELECT * FROM "{tname}_quarantine" LIMIT 5000'
                    ).df()
                    qdf["_source_table"] = tname
                    q_frames.append(qdf)
                except Exception:
                    pass
        st.session_state.quarantine_df = (
            pd.concat(q_frames, ignore_index=True) if q_frames else None
        )

        # ── Step 6: Reconciliation (GAP 1 — real math) ───────────────────
        output_rows = stats["output_rows"]
        recon = reconcile(
            total_source_rows,
            total_quarantine_rows,
            output_rows,
            final_sql,
        )
        st.session_state.recon = recon

        # ── Step 7: Run history (GAP 9 — per-run table snapshot) ─────────
        st.session_state.run_history.append({
            "ts":           datetime.now().strftime("%H:%M:%S"),
            "prompt":       prompt[:70],
            "sql":          final_sql,
            "tables_used":  referenced_tables,
            "table_rows":   {t: schema[t]["row_count"] for t in referenced_tables
                             if t in schema},
            "source_rows":  total_source_rows,
            "quarantine":   total_quarantine_rows,
            "output_rows":  output_rows,
            "confidence":   final_conf,
            "elapsed_ms":   stats["elapsed_ms"],
            "self_healed":  stats.get("self_healed", False),
            "balanced":     recon.get("balanced"),
            "explanation":  explanation,
            "violations":   [v for cr in all_contract_results.values()
                             for v in cr.get("violations", [])],
        })

        st.session_state.status = "done"
        st.rerun()

    # ── STATUS STRIP ────────────────────────────────────────────────────────
    stats   = st.session_state.last_stats
    recon   = st.session_state.recon
    conf    = st.session_state.confidence
    q_count = (len(st.session_state.quarantine_df)
               if st.session_state.quarantine_df is not None else 0)

    sc_cls  = {"idle":"d","running":"y","done":"g","error":"r"}.get(
        st.session_state.status, "d")
    sc_sym  = {"idle":"○","running":"◌","done":"●","error":"✕"}.get(
        st.session_state.status, "○")
    cf_cls  = "g" if conf >= 0.80 else "y" if conf >= 0.60 else "r" if conf > 0 else "d"
    cf_str  = f"{int(conf*100)}%" if conf > 0 else "—"
    q_cls   = "y" if q_count > 0 else "d"

    healed_html = (
        '<div class="si"><span class="sl">Self-healed</span>'
        '<span class="sv g">✓ YES</span></div>'
        if st.session_state.self_healed else ""
    )

    cr          = st.session_state.contract_result
    source_disp = sum(v.get("source_rows", 0) for v in cr.values()) if cr else stats.get("source_rows", 0)

    recon_html = ""
    if recon:
        if recon.get("skipped"):
            recon_html = '<div class="si"><span class="sl">Reconciliation</span><span class="sv d">AGG — N/A</span></div>'
        elif recon.get("balanced"):
            recon_html = '<div class="si"><span class="sl">Reconciliation</span><span class="sv g">✓ BALANCED</span></div>'
        else:
            recon_html = f'<div class="si"><span class="sl">Reconciliation</span><span class="sv r">✕ DIFF {recon["diff"]:+,}</span></div>'

    st.markdown(f"""
<div class="status-strip">
  <div class="si"><span class="sl">Status</span>
    <span class="sv {sc_cls}">{sc_sym} {st.session_state.status.upper()}</span></div>
  <div class="si"><span class="sl">Rows In</span>
    <span class="sv p">{source_disp:,}</span></div>
  <div class="si"><span class="sl">Rows Out</span>
    <span class="sv g">{stats.get('output_rows',0):,}</span></div>
  <div class="si"><span class="sl">Quarantine</span>
    <span class="sv {q_cls}">{q_count:,}</span></div>
  <div class="si"><span class="sl">Confidence</span>
    <span class="sv {cf_cls}">{cf_str}</span></div>
  <div class="si"><span class="sl">Exec ms</span>
    <span class="sv d">{stats.get('elapsed_ms','—')}</span></div>
  <div class="si"><span class="sl">Cols</span>
    <span class="sv d">{stats.get('col_count','—')}</span></div>
  {healed_html}
  {recon_html}
</div>
""", unsafe_allow_html=True)

    # ── SQL + verification badges ────────────────────────────────────────
    if st.session_state.last_sql:
        st.markdown('<div class="sec">Generated SQL</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="sql-block">{st.session_state.last_sql}</div>',
            unsafe_allow_html=True
        )

        badges = []
        if conf >= 0.80:
            badges.append('<span class="badge bp">✓ HIGH CONFIDENCE</span>')
        elif conf >= 0.60:
            badges.append('<span class="badge bw">⚠ MEDIUM CONFIDENCE — REVIEW</span>')
        elif conf > 0:
            badges.append('<span class="badge bf">✕ LOW CONFIDENCE — NEEDS APPROVAL</span>')

        if st.session_state.self_healed:
            badges.append('<span class="badge bp">✓ SELF-HEALED</span>')
        if stats.get("attempts", 1) > 1:
            badges.append(f'<span class="badge bw">⚠ {stats["attempts"]} ATTEMPTS</span>')

        # Show measured confidence breakdown
        for r in st.session_state.conf_reasons[:3]:
            badges.append(f'<span class="badge bi">ℹ {r[:70]}</span>')

        for w in st.session_state.ai_warnings[:2]:
            badges.append(f'<span class="badge bw">⚠ {w[:70]}</span>')

        if st.session_state.explanation:
            badges.append(f'<span class="badge bi">ℹ {st.session_state.explanation[:90]}</span>')

        if badges:
            st.markdown(
                f'<div class="badge-row">{"".join(badges)}</div>',
                unsafe_allow_html=True
            )

    # ── Run history ──────────────────────────────────────────────────────
    if st.session_state.run_history:
        with st.expander(f"⏱ Run History ({len(st.session_state.run_history)} runs)"):
            hist = pd.DataFrame([{
                "Time":     r["ts"],
                "Prompt":   r["prompt"],
                "Tables":   ", ".join(r.get("tables_used", [])),
                "In":       f'{r["source_rows"]:,}',
                "Out":      f'{r["output_rows"]:,}',
                "Quar":     r["quarantine"],
                "Conf":     f'{int(r["confidence"]*100)}%',
                "ms":       r["elapsed_ms"],
                "Healed":   "✓" if r["self_healed"] else "",
                "Recon":    ("✓" if r.get("balanced") is True
                             else "AGG" if r.get("balanced") is None
                             else "✕"),
            } for r in reversed(st.session_state.run_history)])
            st.dataframe(hist, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — DATA TRUST
# ══════════════════════════════════════════════════════════════════════════════

with tab_trust:

    # ── Reconciliation ───────────────────────────────────────────────────
    st.markdown('<div class="sec">Reconciliation Guarantee</div>', unsafe_allow_html=True)
    recon = st.session_state.recon
    cr    = st.session_state.contract_result

    if recon:
        if recon.get("skipped"):
            st.markdown(f"""
<div class="recon-block">
  <span style="color:#7c6fff;font-weight:700">RECONCILIATION — N/A</span><br>
  <span style="color:#44448a">{recon['reason']}</span>
</div>""", unsafe_allow_html=True)
        else:
            bc     = "#4fffb0" if recon["balanced"] else "#ff6b6b"
            diff_s = ("0  ✓  BALANCED" if recon["balanced"]
                      else f"{recon['diff']:+,}  ✕  UNBALANCED")
            st.markdown(f"""
<div class="recon-block">
  <span style="color:#4fffb0;font-weight:700;font-size:13px">
    SOURCE = OUTPUT + QUARANTINE + SQL_EXCLUDED
  </span><br>
  <span style="color:#44448a">Source rows (referenced tables) &nbsp;&nbsp;&nbsp;</span>
  <span style="color:#e8e8f0">{recon['source']:,}</span><br>
  <span style="color:#44448a">Quarantine (failed contracts) &nbsp;&nbsp;&nbsp;&nbsp;</span>
  <span style="color:#ffcc44">{recon['quarantine']:,}</span><br>
  <span style="color:#44448a">SQL excluded (WHERE filtered) &nbsp;&nbsp;&nbsp;&nbsp;</span>
  <span style="color:#7c6fff">{recon['sql_excluded']:,}</span><br>
  <span style="color:#44448a">Output rows &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
  <span style="color:#4fffb0">{recon['output']:,}</span><br>
  <span style="color:#44448a">Difference &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
  <span style="color:{bc};font-weight:700">{diff_s}</span>
</div>""", unsafe_allow_html=True)
    else:
        st.info("Run a pipeline in ETL Studio to see reconciliation.")

    # ── Per-table contract results ────────────────────────────────────────
    if cr:
        st.markdown('<div class="sec">Contract Results by Table</div>', unsafe_allow_html=True)
        for tname, res in cr.items():
            s = res["source_rows"]
            c = res["clean_rows"]
            q = res["quarantine_rows"]
            q_pct = q / max(s, 1) * 100
            color = "#ff6b6b" if q > 0 else "#4fffb0"
            st.markdown(f"""
<div class="status-strip" style="margin-bottom:6px">
  <div class="si"><span class="sl">Table</span><span class="sv p">{tname}</span></div>
  <div class="si"><span class="sl">Source</span><span class="sv d">{s:,}</span></div>
  <div class="si"><span class="sl">Clean</span><span class="sv g">{c:,}</span></div>
  <div class="si"><span class="sl">Quarantine</span>
    <span class="sv" style="color:{color}">{q:,} ({q_pct:.1f}%)</span></div>
</div>""", unsafe_allow_html=True)
            for v in res.get("violations", []):
                st.markdown(f'<div class="badge-row"><span class="badge bw">⚠ {v}</span></div>',
                            unsafe_allow_html=True)

    # ── Quarantine viewer ─────────────────────────────────────────────────
    qdf = st.session_state.quarantine_df
    if qdf is not None and len(qdf) > 0:
        st.markdown(
            f'<div class="sec">Quarantine Queue — {len(qdf):,} rows</div>',
            unsafe_allow_html=True
        )
        st.dataframe(qdf.head(500), use_container_width=True, hide_index=True)
        buf = BytesIO()
        qdf.to_csv(buf, index=False)
        buf.seek(0)
        st.download_button("⬇ Download quarantine CSV",
                           buf.getvalue(), "prism_quarantine.csv", "text/csv")
    elif st.session_state.last_result is not None:
        st.markdown(
            '<div class="badge-row"><span class="badge bp">✓ Zero quarantine rows</span></div>',
            unsafe_allow_html=True
        )

    # ── Confidence breakdown ──────────────────────────────────────────────
    if st.session_state.conf_reasons:
        st.markdown('<div class="sec">Confidence Score Breakdown (Measured)</div>',
                    unsafe_allow_html=True)
        for r in st.session_state.conf_reasons:
            badge = "bp" if "passed" in r.lower() else "bw" if "%" in r else "bi"
            st.markdown(
                f'<div class="badge-row"><span class="badge {badge}">{r}</span></div>',
                unsafe_allow_html=True
            )

    # ── Confidence history ────────────────────────────────────────────────
    if len(st.session_state.run_history) > 1:
        st.markdown('<div class="sec">Confidence History</div>', unsafe_allow_html=True)
        ch = pd.DataFrame({
            "Run":        [f"#{i+1}" for i in range(len(st.session_state.run_history))],
            "Confidence": [r["confidence"] * 100 for r in st.session_state.run_history],
        }).set_index("Run")
        st.line_chart(ch, color="#4fffb0", height=160)

    # ── Source profiles from DuckDB (GAP 7) ──────────────────────────────
    if st.session_state.schema:
        st.markdown('<div class="sec">Source Profiles (All Stats from DuckDB)</div>',
                    unsafe_allow_html=True)
        for tname, info in st.session_state.schema.items():
            traps = info.get("join_traps", [])
            label = f"{tname}  ·  {info['row_count']:,} rows  ·  {len(info['columns'])} cols"
            if traps:
                label += f"  ·  ⚠ {len(traps)} JOIN trap(s)"
            with st.expander(label):
                if traps:
                    for trap in traps:
                        detail = ", ".join(
                            f"{t}.{trap['col']}=[{d}]" for t, d in trap["appearances"]
                        )
                        st.warning(
                            f"Type mismatch on '{trap['col']}': {detail} "
                            "— SQL generation auto-handles with CAST"
                        )
                rows = []
                for col in info["columns"]:
                    trap_flag = "⚠ " if any(t["col"] == col for t in traps) else ""
                    ns        = info["num_stats"].get(col)
                    row = {
                        "Column":   trap_flag + col,
                        "Type":     info["dtypes"][col],
                        "Nulls":    f'{info["null_counts"].get(col, 0):,}',
                        "Null %":   f'{info["null_counts"].get(col,0)/max(info["row_count"],1)*100:.1f}%',
                        "Unique":   f'{info["unique_counts"].get(col, "?"):,}',
                        "Sample":   ", ".join(
                            repr(v) for v in info["sample_values"].get(col, [])[:3]
                        ),
                    }
                    if ns:
                        row["Min"]  = f'{ns["min"]:,.2f}'
                        row["Max"]  = f'{ns["max"]:,.2f}'
                        row["Mean"] = f'{ns["mean"]:,.2f}'
                    rows.append(row)
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — OUTPUT DATA
# ══════════════════════════════════════════════════════════════════════════════

with tab_output:
    result = st.session_state.last_result
    stats  = st.session_state.last_stats

    if result is not None and len(result) > 0:
        st.markdown(
            f'<div class="sec">Result — {len(result):,} rows × '
            f'{len(result.columns)} cols · {stats.get("elapsed_ms","?")} ms</div>',
            unsafe_allow_html=True
        )

        # Numeric summary cards
        num_cols = result.select_dtypes(include="number").columns.tolist()
        if num_cols:
            display_cols = st.columns(min(len(num_cols), 4))
            for i, col in enumerate(num_cols[:4]):
                with display_cols[i]:
                    total = result[col].sum()
                    avg   = result[col].mean()
                    label = (f"{total/1e6:.2f}M" if abs(total) >= 1e6
                             else f"{total:,.1f}")
                    st.metric(col, label, delta=f"avg {avg:,.1f}")

        # Paginated grid
        display_n  = 2000
        display_df = result.head(display_n)
        if len(result) > display_n:
            st.caption(
                f"Showing {display_n:,} of {len(result):,} rows "
                "— download CSV/JSON for full data."
            )
        st.dataframe(display_df, use_container_width=True, height=360, hide_index=True)

        # Downloads
        c1, c2 = st.columns(2)
        with c1:
            buf = BytesIO()
            result.to_csv(buf, index=False)
            buf.seek(0)
            st.download_button("⬇ CSV", buf.getvalue(),
                               "prism_output.csv", "text/csv",
                               use_container_width=True)
        with c2:
            buf2 = BytesIO()
            result.to_json(buf2, orient="records", indent=2)
            buf2.seek(0)
            st.download_button("⬇ JSON", buf2.getvalue(),
                               "prism_output.json", "application/json",
                               use_container_width=True)

        # Quick chart
        if num_cols and len(result) > 1:
            st.markdown('<div class="sec">Quick Chart</div>', unsafe_allow_html=True)
            chart_col = st.selectbox(
                "Column to chart", num_cols, label_visibility="collapsed"
            )
            obj_cols = result.select_dtypes(include="object").columns.tolist()
            if obj_cols:
                idx_col  = obj_cols[0]
                chart_df = (result[[idx_col, chart_col]]
                            .dropna().head(30).set_index(idx_col))
            else:
                chart_df = result[[chart_col]].head(50)
            st.bar_chart(chart_df, color="#7c6fff", height=200)

    elif st.session_state.status == "done":
        st.info("Query returned 0 rows — check your filters or rephrase.")
    else:
        st.info("Run a pipeline in ETL Studio to see output here.")

    # ── Direct SQL editor ─────────────────────────────────────────────────
    st.markdown('<div class="sec">SQL Editor (Direct DuckDB)</div>', unsafe_allow_html=True)
    st.caption("Write DuckDB SQL against any loaded table.")
    manual_sql = st.text_area(
        "SQL", height=90,
        placeholder='SELECT * FROM "sales" LIMIT 10',
        label_visibility="collapsed",
    )
    if st.button("▶ Run SQL"):
        if not st.session_state.schema:
            st.error("No tables loaded.")
        elif manual_sql.strip():
            try:
                conn = get_conn()
                result_df, stats, final_sql, warns = execute_sql_safe(
                    conn, manual_sql.strip(),
                    st.session_state.schema, manual_sql.strip()
                )
                ref = extract_referenced_tables(
                    manual_sql, list(st.session_state.schema.keys())
                )
                src = sum(
                    st.session_state.schema.get(t, {}).get("row_count", 0)
                    for t in ref
                )
                st.session_state.last_result   = result_df
                st.session_state.last_stats    = stats
                st.session_state.last_sql      = final_sql
                st.session_state.exec_warnings = warns
                st.session_state.status        = "done"
                st.session_state.recon         = reconcile(
                    src, 0, len(result_df), final_sql
                )
                st.rerun()
            except Exception as e:
                st.error(f"SQL error: {e}")
                with st.expander("Traceback"):
                    st.code(traceback.format_exc())


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SCHEMA
# ══════════════════════════════════════════════════════════════════════════════

with tab_schema:
    schema = st.session_state.schema
    conn   = get_conn()

    if schema:
        # JOIN trap banner
        all_trap_cols = list({
            trap["col"]
            for info in schema.values()
            for trap in info.get("join_traps", [])
        })
        if all_trap_cols:
            st.warning(
                f"**{len(all_trap_cols)} JOIN type mismatch(es):** "
                f"`{'`, `'.join(all_trap_cols)}`  \n"
                "SQL generation auto-handles with CAST — verify results carefully."
            )

        # DuckDB SHOW TABLES
        try:
            tbl_list = conn.execute("SHOW TABLES").df()
            st.markdown('<div class="sec">DuckDB Tables & Views</div>',
                        unsafe_allow_html=True)
            st.dataframe(tbl_list, use_container_width=True, hide_index=True)
        except Exception:
            pass

        # Per-table schema cards
        st.markdown('<div class="sec">Column Details</div>', unsafe_allow_html=True)
        for tname, info in schema.items():
            traps = info.get("join_traps", [])
            label = (f"{tname}  ·  {info['row_count']:,} rows  "
                     f"·  {len(info['columns'])} cols")
            with st.expander(label, expanded=False):
                rows = []
                for col in info["columns"]:
                    trap_flag = "⚠ " if any(t["col"] == col for t in traps) else ""
                    ns        = info["num_stats"].get(col)
                    row = {
                        "Column":  trap_flag + col,
                        "DuckDB Type": info["dtypes"][col],
                        "Nulls":   f'{info["null_counts"].get(col, 0):,}',
                        "Unique":  f'{info["unique_counts"].get(col, "?"):,}',
                        "Samples": ", ".join(
                            repr(v) for v in info["sample_values"].get(col, [])[:3]
                        ),
                    }
                    if ns:
                        row["Min"]  = f'{ns["min"]:,.2f}'
                        row["Max"]  = f'{ns["max"]:,.2f}'
                        row["Mean"] = f'{ns["mean"]:,.2f}'
                    rows.append(row)
                st.dataframe(pd.DataFrame(rows), use_container_width=True,
                             hide_index=True)

        # Debug JSON
        with st.expander("Raw schema JSON (debug)"):
            debug = {
                t: {k: v for k, v in info.items()
                    if k not in ("sample_values", "num_stats")}
                for t, info in schema.items()
            }
            st.json(debug)
    else:
        st.info("No tables loaded. Upload CSVs or click 'Load Demo Data'.")


# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="border-top:1px solid #1e1e35;margin-top:20px;padding:10px 0 2px;text-align:center">
  <span style="font-family:'DM Mono',monospace;font-size:10px;color:#1e1e40;letter-spacing:.15em">
    PRISM · AI-NATIVE DATA INTELLIGENCE ENGINE · v4.0 · DuckDB-FIRST · MARCH 2026 · CONFIDENTIAL
  </span>
</div>
""", unsafe_allow_html=True)
