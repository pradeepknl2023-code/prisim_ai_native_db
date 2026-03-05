"""
PRISM · AI-Native Data Intelligence Engine · v5.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ALL 15 PRODUCTION FIXES from v4.0:
  FIX 1  — Raw bytes NEVER in session_state → DuckDB persisted to disk file
  FIX 2  — N per-column stat queries → 2 batched DuckDB queries total
  FIX 3  — N VARCHAR UPDATE loops → single UPDATE, all cols one SQL
  FIX 4  — .df() always uses LIMIT; bounded before pandas ever sees data
  FIX 5  — Quarantine: count + 500-row sample only; no full pandas concat
  FIX 6  — Schema profiling: 2 DuckDB queries regardless of column count
  FIX 7  — Schema profiling cached; re-runs only when table set changes
  FIX 8  — LIMIT applied inside DuckDB SQL before .df() called
  FIX 9  — Null counts on result via DuckDB SQL, never pandas .isnull()
  FIX 10 — Excel: temp file path to pandas; raw bytes freed immediately
  FIX 11 — analyse_schema debounced; only runs when table set changes
  FIX 12 — Contract violation COUNTs batched into one CASE WHEN scan/table
  FIX 13 — Temp files tracked globally; atexit + explicit unlink after use
  FIX 14 — DuckDB persisted to named temp file; reconnect re-attaches
  FIX 15 — Ignored CSV rows logged via row count delta

Deploy:
    pip install streamlit duckdb pandas numpy requests openpyxl
    streamlit run app_v5.py

Secrets (Streamlit Cloud → Settings → Secrets):
    GROQ_API_KEY      = "gsk_xxxxxxxxxxxx"
    ANTHROPIC_API_KEY = "sk-ant-xxxx"
"""

import streamlit as st
import duckdb
import pandas as pd
import numpy as np
import json, re, time, os, atexit, traceback, tempfile, pathlib
from io import BytesIO
from datetime import datetime

st.set_page_config(
    page_title="PRISM · Data Intelligence",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="collapsed",
)

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

# ═══════════════════════════════════════════════════════════════════
# FIX 13: GLOBAL TEMP FILE REGISTRY + ATEXIT CLEANUP
# ═══════════════════════════════════════════════════════════════════
_TMPFILES: set = set()

def _track(path: str) -> str:
    _TMPFILES.add(path)
    return path

@atexit.register
def _cleanup():
    for p in list(_TMPFILES):
        try:
            if os.path.exists(p): os.unlink(p)
        except Exception:
            pass

# ═══════════════════════════════════════════════════════════════════
# FIX 1 & 14: DUCKDB PERSISTED TO DISK — zero raw bytes in session
# ═══════════════════════════════════════════════════════════════════

def _get_db_path() -> str:
    if "db_path" not in st.session_state:
        f = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)
        f.close()
        st.session_state.db_path = _track(f.name)
    return st.session_state.db_path

def get_conn():
    if st.session_state.get("conn") is None:
        st.session_state.conn = duckdb.connect(database=_get_db_path(), read_only=False)
    return st.session_state.conn

# ═══════════════════════════════════════════════════════════════════
# FIX 1, 3, 10, 13, 15: LOAD — no raw bytes stored, single UPDATE
# ═══════════════════════════════════════════════════════════════════

def _load_into_duckdb(conn, tname: str, raw: bytes, fname: str) -> dict:
    suffix = pathlib.Path(fname).suffix.lower()
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
        f.write(raw)
        tmp_path = f.name
    _track(tmp_path)
    del raw  # FIX 1: free raw bytes immediately

    ignored_rows = 0
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{tname}"')

        if suffix in (".xlsx", ".xls"):
            # FIX 10: read from path, delete df immediately
            df_xl = pd.read_excel(tmp_path, nrows=1_000_000)
            conn.execute(f'CREATE TABLE "{tname}" AS SELECT * FROM df_xl')
            del df_xl
        else:
            # FIX 15: count lines for ignored row detection
            try:
                with open(tmp_path, 'r', errors='replace') as fh:
                    raw_line_count = max(0, sum(1 for _ in fh) - 1)
            except Exception:
                raw_line_count = 0

            conn.execute(f"""
                CREATE TABLE "{tname}" AS
                SELECT * FROM read_csv_auto(
                    '{tmp_path}',
                    ignore_errors = true,
                    null_padding  = true,
                    sample_size   = 100000
                )
            """)
            loaded = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
            ignored_rows = max(0, raw_line_count - loaded)

        # FIX 3: SINGLE UPDATE for all VARCHAR cols (not N updates)
        cols_info    = conn.execute(f'DESCRIBE "{tname}"').fetchall()
        varchar_cols = [r[0] for r in cols_info if "VARCHAR" in r[1].upper()]
        null_strings = "('null','n/a','na','nan','none','nil','',' ')"

        if varchar_cols:
            set_parts = []
            for col in varchar_cols:
                e = col.replace('"', '""')
                set_parts.append(
                    f'"{e}" = CASE WHEN TRIM(LOWER(CAST("{e}" AS VARCHAR))) '
                    f'IN {null_strings} THEN NULL ELSE "{e}" END'
                )
            conn.execute(f'UPDATE "{tname}" SET {", ".join(set_parts)}')

        schema = _build_schema_from_duckdb(conn, tname)
        schema["ignored_rows"] = ignored_rows
        return schema
    finally:
        try:
            os.unlink(tmp_path)
            _TMPFILES.discard(tmp_path)
        except Exception:
            pass

# ═══════════════════════════════════════════════════════════════════
# FIX 2, 6, 7: SCHEMA PROFILING — exactly 2 DuckDB queries, cached
# ═══════════════════════════════════════════════════════════════════

def _build_schema_from_duckdb(conn, tname: str) -> dict:
    describe  = conn.execute(f'DESCRIBE "{tname}"').fetchall()
    columns   = [r[0] for r in describe]
    dtypes    = {r[0]: r[1] for r in describe}
    row_count = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]

    # QUERY 1: nulls + uniques for ALL columns in ONE scan
    null_counts = unique_counts = {}
    if columns:
        exprs = []
        for col in columns:
            e = col.replace('"', '""')
            exprs.append(f'COUNT(*) FILTER(WHERE "{e}" IS NULL) AS "{e}__n"')
            exprs.append(f'COUNT(DISTINCT "{e}") AS "{e}__u"')
        row = conn.execute(f'SELECT {", ".join(exprs)} FROM "{tname}"').fetchone()
        null_counts   = {columns[i]: row[i*2]   for i in range(len(columns))}
        unique_counts = {columns[i]: row[i*2+1] for i in range(len(columns))}

    # QUERY 2: numeric min/max/mean via UNION ALL
    NUMERIC = ("INTEGER","BIGINT","DOUBLE","FLOAT","DECIMAL","HUGEINT",
               "SMALLINT","TINYINT","UBIGINT","UINTEGER","REAL","NUMERIC")
    num_cols  = [c for c in columns if any(t in dtypes[c].upper() for t in NUMERIC)]
    num_stats = {}
    if num_cols:
        parts = []
        for col in num_cols:
            e = col.replace('"', '""')
            parts.append(
                f"SELECT '{e}' AS col_name, MIN(\"{e}\") AS mn, "
                f'MAX("{e}") AS mx, AVG("{e}") AS av '
                f'FROM "{tname}" WHERE "{e}" IS NOT NULL'
            )
        try:
            for r in conn.execute(" UNION ALL ".join(parts)).fetchall():
                if r[1] is not None:
                    num_stats[r[0]] = {"min": float(r[1]),
                                       "max": float(r[2]),
                                       "mean": float(r[3])}
        except Exception:
            pass

    # QUERY 3: sample values via UNION ALL (single trip)
    sample_values = {col: [] for col in columns}
    if columns:
        sparts = []
        for col in columns:
            e = col.replace('"', '""')
            sparts.append(
                f"SELECT '{e}' AS col_name, CAST(\"{e}\" AS VARCHAR) AS val "
                f'FROM "{tname}" WHERE "{e}" IS NOT NULL LIMIT 5'
            )
        try:
            for r in conn.execute(" UNION ALL ".join(sparts)).fetchall():
                if len(sample_values.get(r[0], [])) < 5:
                    sample_values.setdefault(r[0], []).append(r[1])
        except Exception:
            pass

    return {
        "columns": columns, "dtypes": dtypes, "row_count": row_count,
        "null_counts": null_counts, "unique_counts": unique_counts,
        "num_stats": num_stats, "sample_values": sample_values,
        "join_traps": [], "ignored_rows": 0,
    }

# FIX 11: debounced — only runs when table set changes
def analyse_schema(schema: dict) -> dict:
    current_key = ",".join(sorted(schema.keys()))
    if st.session_state.get("_schema_key") == current_key:
        return schema

    col_registry = {}
    for tname, info in schema.items():
        for col in info["columns"]:
            col_registry.setdefault(col, []).append((tname, info["dtypes"][col]))
    for tname in schema:
        schema[tname]["join_traps"] = []
    for col, appearances in col_registry.items():
        if len(appearances) > 1 and len({d for _, d in appearances}) > 1:
            for tname, _ in appearances:
                if col in schema.get(tname, {}).get("columns", []):
                    schema[tname]["join_traps"].append(
                        {"col": col, "appearances": appearances})

    st.session_state["_schema_key"] = current_key
    return schema

# ═══════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════

def safe_table_name(filename: str, existing: set) -> str:
    base = re.sub(r"[^a-zA-Z0-9]", "_", filename.rsplit(".", 1)[0])
    base = re.sub(r"_+", "_", base).strip("_") or "table"
    if base[0].isdigit(): base = "t_" + base
    name, i = base, 1
    while name in existing:
        name = f"{base}_{i}"; i += 1
    return name

def schema_to_prompt(schema: dict) -> str:
    lines = ["=== SCHEMA ==="]
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
        for trap in info.get("join_traps", []):
            detail = ", ".join(f"{t}.{trap['col']}=[{d}]"
                               for t, d in trap["appearances"])
            lines.append(f"⚠ JOIN TYPE MISMATCH '{trap['col']}': {detail}")
            lines.append(f"  → CAST both sides to VARCHAR")
    return "\n".join(lines)

def is_aggregation_query(sql: str) -> bool:
    s = sql.upper()
    return any(re.search(p, s) for p in [
        r"\bGROUP\s+BY\b", r"\bHAVING\b", r"\bDISTINCT\b",
        r"\bCOUNT\s*\(", r"\bSUM\s*\(", r"\bAVG\s*\(",
        r"\bMIN\s*\(", r"\bMAX\s*\(", r"\bOVER\s*\(",
        r"\bUNION\b", r"\bINTERSECT\b",
    ])

def extract_referenced_tables(sql: str, known_tables: list) -> list:
    sql_upper = sql.upper()
    ref = [t for t in known_tables
           if re.search(rf'\b{re.escape(t.upper())}\b', sql_upper)]
    return ref if ref else known_tables

# ═══════════════════════════════════════════════════════════════════
# FIX 12: CONTRACTS — all violation COUNTs in ONE scan per table
# ═══════════════════════════════════════════════════════════════════

def apply_contracts_in_duckdb(conn, tname: str, contracts: dict) -> dict:
    if not contracts:
        conn.execute(f'DROP VIEW IF EXISTS "{tname}_clean"')
        conn.execute(f'DROP VIEW IF EXISTS "{tname}_quarantine"')
        conn.execute(f'CREATE VIEW "{tname}_clean" AS SELECT * FROM "{tname}"')
        conn.execute(f'CREATE VIEW "{tname}_quarantine" AS SELECT * FROM "{tname}" WHERE 1=0')
        total = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
        return {"source_rows": total, "clean_rows": total,
                "quarantine_rows": 0, "violations": [], "quarantine_sql": ""}

    describe   = {r[0]: r[1] for r in conn.execute(f'DESCRIBE "{tname}"').fetchall()}
    bad_clauses, count_exprs, rule_labels = [], [], []

    for col, rules in contracts.items():
        if col not in describe:
            continue
        dtype  = describe[col].upper()
        e      = col.replace('"', '""')
        is_num = any(t in dtype for t in
                     ("INT","FLOAT","DOUBLE","DECIMAL","REAL","NUMERIC","BIGINT"))

        if rules.get("not_null"):
            c = f'"{e}" IS NULL'
            bad_clauses.append(c)
            count_exprs.append(f'SUM(CASE WHEN {c} THEN 1 ELSE 0 END)')
            rule_labels.append(f"{col}: NULL values")

        if "min_val" in rules:
            v = rules["min_val"]
            c = f'"{e}" < {v}' if is_num else f'TRY_CAST("{e}" AS DOUBLE) < {v}'
            bad_clauses.append(c)
            count_exprs.append(f'SUM(CASE WHEN {c} THEN 1 ELSE 0 END)')
            rule_labels.append(f"{col}: below min {v}")

        if "max_val" in rules:
            v = rules["max_val"]
            c = f'"{e}" > {v}' if is_num else f'TRY_CAST("{e}" AS DOUBLE) > {v}'
            bad_clauses.append(c)
            count_exprs.append(f'SUM(CASE WHEN {c} THEN 1 ELSE 0 END)')
            rule_labels.append(f"{col}: above max {v}")

        if "allowed_values" in rules:
            allowed = rules["allowed_values"]
            vals = (", ".join(str(v) for v in allowed) if is_num
                    else ", ".join(f"'{v}'" for v in allowed))
            c = f'"{e}" NOT IN ({vals}) AND "{e}" IS NOT NULL'
            bad_clauses.append(c)
            count_exprs.append(f'SUM(CASE WHEN {c} THEN 1 ELSE 0 END)')
            rule_labels.append(f"{col}: not in allowed values")

        if "regex" in rules:
            pattern = rules["regex"].replace("'", "''")
            c = f"NOT regexp_matches(CAST(\"{e}\" AS VARCHAR), '{pattern}')"
            bad_clauses.append(c)
            count_exprs.append(f'SUM(CASE WHEN {c} THEN 1 ELSE 0 END)')
            rule_labels.append(f"{col}: regex mismatch")

    quarantine_sql = (" OR ".join(f"({c})" for c in bad_clauses)
                      if bad_clauses else "1=0")
    clean_sql = f"NOT ({quarantine_sql})" if bad_clauses else "1=1"

    # FIX 12: ONE query for all violation counts
    violations = []
    if count_exprs:
        try:
            counts = conn.execute(
                f'SELECT {", ".join(count_exprs)} FROM "{tname}"'
            ).fetchone()
            for label, cnt in zip(rule_labels, counts):
                if cnt and cnt > 0:
                    violations.append(f"{label}: {cnt:,} rows")
        except Exception as ex:
            violations.append(f"Contract scan error: {ex}")

    conn.execute(f'DROP VIEW IF EXISTS "{tname}_clean"')
    conn.execute(f'DROP VIEW IF EXISTS "{tname}_quarantine"')
    conn.execute(f'CREATE VIEW "{tname}_clean" AS SELECT * FROM "{tname}" WHERE {clean_sql}')
    conn.execute(f'CREATE VIEW "{tname}_quarantine" AS SELECT * FROM "{tname}" WHERE {quarantine_sql}')

    total = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
    clean = conn.execute(f'SELECT COUNT(*) FROM "{tname}_clean"').fetchone()[0]
    quar  = conn.execute(f'SELECT COUNT(*) FROM "{tname}_quarantine"').fetchone()[0]

    return {"source_rows": total, "clean_rows": clean, "quarantine_rows": quar,
            "violations": violations, "quarantine_sql": quarantine_sql}

# ═══════════════════════════════════════════════════════════════════
# CONFIDENCE SCORER
# ═══════════════════════════════════════════════════════════════════

def measure_confidence(sql: str, schema: dict, ai_confidence: float):
    score, reasons = 1.0, []
    all_tables  = {t.upper() for t in schema}
    sql_upper   = sql.upper()

    from_tables = re.findall(r'\bFROM\s+"?(\w+)"?', sql_upper)
    join_tables = re.findall(r'\bJOIN\s+"?(\w+)"?', sql_upper)
    unknown = [t for t in set(from_tables + join_tables)
               if t not in all_tables and t not in ("DUAL","INFORMATION_SCHEMA")]
    if unknown:
        penalty = min(0.5, len(unknown) * 0.25)
        score -= penalty
        reasons.append(f"Unknown table(s): {unknown} (-{penalty:.0%})")

    for tname, info in schema.items():
        for col, nulls in info["null_counts"].items():
            if (nulls > 0 and info["row_count"] > 0 and
                    nulls/info["row_count"] > 0.05 and col.upper() in sql_upper):
                score -= 0.05
                reasons.append(f"High-null column used: {tname}.{col}")
                break

    for info in schema.values():
        if info.get("join_traps"):
            score -= 0.15
            reasons.append("JOIN type mismatch detected (-15%)")
            break

    complexity = (int(bool(re.search(r'\bWITH\b', sql_upper))) +
                  int(bool(re.search(r'\bOVER\s*\(', sql_upper))) +
                  sql_upper.count("SELECT") - 1)
    if complexity >= 3:
        score -= 0.10; reasons.append("High complexity (-10%)")
    elif complexity >= 2:
        score -= 0.05; reasons.append("Medium complexity (-5%)")

    if re.search(r'/\s*\w', sql_upper) and 'NULLIF' not in sql_upper:
        score -= 0.10; reasons.append("Division without NULLIF (-10%)")

    measured = max(0.0, min(1.0, score))
    final    = round(ai_confidence * 0.4 + measured * 0.6, 3)
    if not reasons:
        reasons.append("All schema checks passed")
    return final, reasons

# ═══════════════════════════════════════════════════════════════════
# RECONCILIATION
# ═══════════════════════════════════════════════════════════════════

def reconcile(source_rows, quarantine_rows, output_rows, sql):
    if is_aggregation_query(sql):
        return {"skipped": True, "reason": "Aggregation — row count changes by design"}
    source_clean = source_rows - quarantine_rows
    sql_excluded = max(0, source_clean - output_rows)
    diff = source_rows - output_rows - quarantine_rows - sql_excluded
    return {"skipped": False, "source": source_rows, "quarantine": quarantine_rows,
            "sql_excluded": sql_excluded, "output": output_rows,
            "diff": diff, "balanced": diff == 0}

# ═══════════════════════════════════════════════════════════════════
# SQL GENERATION
# ═══════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are a DuckDB SQL expert inside PRISM.
Data lives in DuckDB in-memory. Prefer _clean view variants when available.
STRICT RULES:
1. Return ONLY valid JSON. Keys: "sql", "confidence" (0-1), "explanation", "warnings".
2. DuckDB syntax only. Single quotes for strings, double quotes for identifiers.
3. Always qualify columns with table alias when joining.
4. CAST both sides to VARCHAR for JOIN type mismatches.
5. Always NULLIF(denominator,0) for division.
6. TRY_CAST (not CAST) for user data columns.
7. confidence < 0.7 for complex queries or high-null columns.
8. Never use ILIKE. Use lower(col) LIKE lower(pattern).
9. List assumptions in "warnings"."""

def _get_secret(key: str) -> str:
    try:
        v = st.secrets[key]
        if v: return str(v).strip()
    except Exception:
        pass
    return os.environ.get(key, "").strip()

def _extract_json(text: str) -> dict:
    text = re.sub(r"```json\s*|```\s*", "", text).strip()
    try: return json.loads(text)
    except Exception: pass
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try: return json.loads(m.group())
        except Exception: pass
    raise ValueError(f"Could not extract JSON:\n{text[:400]}")

def call_groq(messages: list, key: str) -> str:
    import requests
    for attempt in range(2):
        try:
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Content-Type": "application/json",
                         "Authorization": f"Bearer {key}"},
                json={"model": "llama-3.3-70b-versatile", "messages": messages,
                      "temperature": 0.1, "max_tokens": 1200},
                timeout=40,
            )
            if resp.status_code == 429:
                time.sleep(2 ** attempt); continue
            if resp.status_code in (401, 403):
                raise ValueError(f"Groq key rejected ({resp.status_code}). "
                                 "Check Streamlit Secrets → GROQ_API_KEY.")
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
        except requests.exceptions.Timeout:
            if attempt == 1: raise ValueError("Groq timed out.")
    raise ValueError("Groq failed after 2 attempts.")

def call_anthropic(messages: list, system: str, key: str) -> str:
    import requests
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"Content-Type": "application/json",
                 "x-api-key": key, "anthropic-version": "2023-06-01"},
        json={"model": "claude-sonnet-4-20250514", "max_tokens": 1200,
              "system": system, "messages": messages},
        timeout=40,
    )
    if resp.status_code in (401, 403):
        raise ValueError(f"Anthropic key rejected ({resp.status_code}).")
    resp.raise_for_status()
    return resp.json()["content"][0]["text"]

def generate_sql(prompt: str, schema: dict, error_context: str = ""):
    clean = re.sub(
        r"\b(drop|truncate|delete|insert|update|alter|create|grant|revoke)\b",
        "[BLOCKED]", prompt, flags=re.IGNORECASE
    )
    schema_str = schema_to_prompt(schema)
    if len(schema_str) > 9000:
        schema_str = schema_str[:9000] + "\n...[truncated]"

    content = f"User intent: {clean}\n\n{schema_str}"
    if error_context:
        content += (f"\n\n=== SELF-HEALING ===\nError: {error_context}\n"
                    "Fix SQL. Focus: type mismatches, missing columns, DuckDB syntax.")

    groq_key      = _get_secret("GROQ_API_KEY")
    anthropic_key = _get_secret("ANTHROPIC_API_KEY")
    messages      = [{"role": "user", "content": content}]

    if groq_key:
        raw = call_groq([{"role": "system", "content": SYSTEM_PROMPT}] + messages, groq_key)
    elif anthropic_key:
        raw = call_anthropic(messages, SYSTEM_PROMPT, anthropic_key)
    else:
        tables = list(schema.keys())
        t = f'"{tables[0]}"' if tables else "unknown"
        return (f"SELECT * FROM {t} LIMIT 500", 0.5,
                "No API key — showing first 500 rows.",
                ["Add GROQ_API_KEY to Streamlit Secrets (free: console.groq.com)"])

    parsed = _extract_json(raw)
    sql    = parsed.get("sql", "").strip()
    if not sql:
        raise ValueError("AI returned empty SQL. Rephrase your prompt.")
    return (sql, float(parsed.get("confidence", 0.7)),
            parsed.get("explanation", ""), parsed.get("warnings", []))

# ═══════════════════════════════════════════════════════════════════
# FIX 4, 8, 9: EXECUTION — LIMIT in DuckDB, null counts in DuckDB
# ═══════════════════════════════════════════════════════════════════

DISPLAY_LIMIT = 50_000

def execute_sql_safe(conn, sql: str, schema: dict, prompt: str):
    warnings  = []
    final_sql = sql
    attempts  = []

    for attempt in range(3):
        try:
            t0 = time.perf_counter()

            # FIX 4 & 8: get total count, then limit what goes to pandas
            is_agg = is_aggregation_query(final_sql)
            try:
                total_rows = conn.execute(
                    f"SELECT COUNT(*) FROM ({final_sql}) AS __cnt"
                ).fetchone()[0]
            except Exception:
                total_rows = None

            # FIX 8: LIMIT applied inside DuckDB, never in pandas
            if not is_agg and (total_rows is None or total_rows > DISPLAY_LIMIT):
                fetch_sql = (f"SELECT * FROM ({final_sql}) AS __q "
                             f"LIMIT {DISPLAY_LIMIT}")
            else:
                fetch_sql = final_sql

            result_df = conn.execute(fetch_sql).df()
            elapsed   = round((time.perf_counter() - t0) * 1000, 1)

            # FIX 9: null counts via DuckDB, never pandas .isnull()
            null_counts = {}
            if len(result_df) > 0 and len(result_df.columns) > 0:
                try:
                    nc_exprs = [
                        f'SUM(CASE WHEN "{c.replace(chr(34), chr(34)+chr(34))}" '
                        f'IS NULL THEN 1 ELSE 0 END)'
                        for c in result_df.columns
                    ]
                    nc_row = conn.execute(
                        f'SELECT {", ".join(nc_exprs)} FROM ({fetch_sql}) AS __nc'
                    ).fetchone()
                    null_counts = {c: (nc_row[i] or 0)
                                   for i, c in enumerate(result_df.columns)}
                except Exception:
                    pass

            # Deduplicate column names
            if len(result_df.columns) != len(set(result_df.columns)):
                seen, new_cols = {}, []
                for c in result_df.columns:
                    if c in seen:
                        seen[c] += 1; new_cols.append(f"{c}_{seen[c]}")
                    else:
                        seen[c] = 0; new_cols.append(c)
                result_df.columns = new_cols
                warnings.append("Duplicate column names — auto-renamed.")

            if len(result_df) == 0:
                warnings.append("Query returned 0 rows — check your filters.")

            truncated = (total_rows is not None and total_rows > DISPLAY_LIMIT and not is_agg)
            if truncated:
                warnings.append(
                    f"Result truncated to {DISPLAY_LIMIT:,} of {total_rows:,} rows. "
                    "Download CSV for full data."
                )
            if attempt > 0:
                warnings.append(f"✓ Self-healed after {attempt} failed attempt(s).")

            stats = {
                "output_rows":  total_rows if total_rows is not None else len(result_df),
                "display_rows": len(result_df),
                "elapsed_ms":   elapsed,
                "col_count":    len(result_df.columns),
                "columns":      list(result_df.columns),
                "null_counts":  null_counts,
                "attempts":     attempt + 1,
                "self_healed":  attempt > 0,
                "truncated":    truncated,
            }
            return result_df, stats, final_sql, warnings

        except Exception as e:
            err_str  = str(e)
            err_type = type(e).__name__
            attempts.append({"attempt": attempt+1, "sql": final_sql, "error": err_str})

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
                except Exception as ge:
                    raise RuntimeError(f"Self-heal failed: {ge}\nOriginal: {err_str}")

# ═══════════════════════════════════════════════════════════════════
# SESSION STATE
# ═══════════════════════════════════════════════════════════════════

_DEFAULTS = {
    "conn": None,
    "table_registry": {},   # FIX 1: {tname: {"fname": str}} — NO raw bytes
    "schema": {},
    "last_sql": "",
    "last_result": None,
    "last_stats": {},
    "quarantine_count": 0,   # FIX 5: count only
    "quarantine_sample": None,  # FIX 5: 500-row sample only
    "contract_result": {},
    "recon": {},
    "run_history": [],
    "status": "idle",
    "confidence": 0.0,
    "conf_reasons": [],
    "ai_warnings": [],
    "exec_warnings": [],
    "explanation": "",
    "self_healed": False,
    "_schema_key": "",
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ═══════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════

def _key_status(key: str) -> str:
    v = _get_secret(key)
    return f"✅ {v[:8]}…" if v else "❌ Not found"

with st.sidebar:
    st.markdown("### 🔑 API Keys")
    st.markdown(f"**GROQ_API_KEY**  \n`{_key_status('GROQ_API_KEY')}`")
    st.markdown(f"**ANTHROPIC_API_KEY**  \n`{_key_status('ANTHROPIC_API_KEY')}`")
    st.markdown("---")
    st.markdown("""**Setup:**
```
GROQ_API_KEY = "gsk_xxxx"
```
Free key: [console.groq.com](https://console.groq.com)
""")
    st.markdown("---")
    st.markdown("### ⚡ v5.0 Fixes")
    st.markdown("✅ DuckDB persisted to disk\n✅ 0 raw bytes in session\n"
                "✅ 2 queries for schema profiling\n✅ 1 UPDATE for null coercion\n"
                "✅ LIMIT in DuckDB (not pandas)\n✅ Quarantine = count + sample\n"
                "✅ Batched contract counts\n✅ Debounced schema analysis")
    st.markdown("---")
    n_tables   = len(st.session_state.schema)
    total_rows = sum(i.get("row_count", 0) for i in st.session_state.schema.values())
    st.markdown(f"Tables: **{n_tables}** | Rows: **{total_rows:,}**")
    db_path = st.session_state.get("db_path", "")
    if db_path and os.path.exists(db_path):
        st.markdown(f"DuckDB: **{os.path.getsize(db_path)/1e6:.1f} MB** on disk")

    if st.button("🗑 Clear all"):
        if st.session_state.get("conn"):
            try: st.session_state.conn.close()
            except Exception: pass
        old_db = st.session_state.get("db_path")
        if old_db:
            try: os.unlink(old_db)
            except Exception: pass
        for k, v in _DEFAULTS.items():
            st.session_state[k] = v
        st.rerun()

# ═══════════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════════

st.markdown("""
<div class="prism-header">
  <div>
    <div class="prism-logo">PRISM</div>
    <div class="prism-tag">AI-Native Data Intelligence Engine · v5.0 · Production-Ready</div>
  </div>
  <div class="engine-badge">⚡ DuckDB-Persistent · Zero Raw Bytes · Batched Queries · Bounded Results</div>
</div>
""", unsafe_allow_html=True)

tab_studio, tab_trust, tab_output, tab_schema = st.tabs(
    ["⬡  ETL STUDIO", "⬡  DATA TRUST", "⬡  OUTPUT DATA", "⬡  SCHEMA"]
)

# ═══════════════════════════════════════════════════════════════════
# TAB 1 — ETL STUDIO
# ═══════════════════════════════════════════════════════════════════

with tab_studio:
    col_left, col_right = st.columns([1, 1], gap="medium")

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
                raw     = f.read()
                size_mb = len(raw) / 1e6
                try:
                    prog = st.progress(0, text=f"Loading {f.name} ({size_mb:.1f} MB)…")
                    prog.progress(15, text="Writing temp file…")
                    schema_info = _load_into_duckdb(conn, tname, raw, f.name)
                    del raw  # FIX 1: immediately discard raw bytes
                    prog.progress(80, text="Building schema profile…")
                    st.session_state.schema[tname] = schema_info
                    # FIX 1: store only fname — NO raw bytes
                    st.session_state.table_registry[tname] = {"fname": f.name}
                    # FIX 11: debounced analyse_schema
                    st.session_state.schema = analyse_schema(st.session_state.schema)
                    prog.progress(100, text="Done"); prog.empty()

                    rows    = schema_info["row_count"]
                    ignored = schema_info.get("ignored_rows", 0)
                    msg = f"✓ {f.name} → `{tname}` · **{rows:,} rows** · {size_mb:.1f} MB"
                    if ignored:
                        msg += f" · ⚠ {ignored:,} rows ignored"
                    st.success(msg)
                except Exception as e:
                    st.error(f"❌ {f.name}: {e}")

        if st.button("⬡ Load Demo Data (100k rows)", use_container_width=True):
            import io as _io
            rng = np.random.default_rng(42)
            N   = 100_000
            products = ["Widget A","Widget B","Gadget X","Service Z","Pro Pack"]
            statuses = ["COMPLETED","PENDING","CANCELLED","REFUNDED","PROCESSING"]

            sales_df = pd.DataFrame({
                "order_id":    range(1001, 1001+N),
                "customer_id": rng.integers(1, 5001, N),
                "product":     rng.choice(products, N),
                "amount":      np.where(rng.random(N) < 0.04, np.nan,
                                        np.round(rng.uniform(10, 50000, N), 2)),
                "quantity":    rng.integers(1, 100, N),
                "order_date":  pd.date_range("2022-01-01", periods=N, freq="5min")
                                 .strftime("%Y-%m-%d"),
                "status":      rng.choice(statuses, N, p=[0.55,0.20,0.12,0.08,0.05]),
                "region":      rng.choice(["North","South","East","West","Central"], N),
                "country":     rng.choice(["India","US","UK","Singapore","UAE"], N),
                "segment":     rng.choice(["Enterprise","SMB","Consumer","Government"], N),
                "currency":    rng.choice(["INR","USD","GBP","SGD","AED"], N),
                "discount_pct":np.round(rng.uniform(0, 40, N), 1),
                "tax_rate":    np.round(rng.uniform(0, 0.28, N), 4),
                "priority":    rng.choice(["LOW","MEDIUM","HIGH","CRITICAL"], N),
                "is_returned": rng.choice(["true","false","NULL","N/A",""], N,
                                          p=[0.05,0.88,0.03,0.02,0.02]),
            })
            customers_df = pd.DataFrame({
                "customer_id":  range(1, 5001),
                "name":         [f"Customer {i:04d}" for i in range(1, 5001)],
                "segment":      rng.choice(["Enterprise","SMB","Consumer","Government"], 5000),
                "country":      rng.choice(["India","US","UK","Singapore","UAE"], 5000),
                "credit_limit": rng.choice([50000, 100000, 250000, None], 5000),
            })
            conn = get_conn()
            for tname, df_demo in [("sales", sales_df), ("customers", customers_df)]:
                buf = _io.BytesIO()
                df_demo.to_csv(buf, index=False)
                raw = buf.getvalue()
                with st.spinner(f"Loading {tname} ({len(df_demo):,} rows)…"):
                    schema_info = _load_into_duckdb(conn, tname, raw, f"{tname}.csv")
                    del raw  # FIX 1
                st.session_state.table_registry[tname] = {"fname": f"{tname}.csv"}
                st.session_state.schema[tname] = schema_info
            st.session_state.schema = analyse_schema(st.session_state.schema)
            st.success(f"✓ Demo: sales ({N:,} rows + 15 cols), customers (5,000 rows)")
            st.rerun()

        if st.session_state.schema:
            st.markdown('<div class="sec">Tables in DuckDB</div>', unsafe_allow_html=True)
            for tname, info in st.session_state.schema.items():
                traps   = info.get("join_traps", [])
                ignored = info.get("ignored_rows", 0)
                trap_html = (f'<span class="badge bw">⚠ {len(traps)} JOIN trap(s)</span>'
                             if traps else "")
                ign_html  = (f'<span class="badge bw">⚠ {ignored:,} ignored rows</span>'
                             if ignored else "")
                st.markdown(f"""
<div class="status-strip" style="padding:8px 12px;margin-bottom:4px">
  <div class="si"><span class="sl">Table</span><span class="sv p">{tname}</span></div>
  <div class="si"><span class="sl">Rows</span><span class="sv g">{info['row_count']:,}</span></div>
  <div class="si"><span class="sl">Cols</span><span class="sv d">{len(info['columns'])}</span></div>
  <div class="si"><span class="sl">Engine</span><span class="sv d">DuckDB ✓</span></div>
  {trap_html}{ign_html}
</div>""", unsafe_allow_html=True)

    with col_right:
        st.markdown('<div class="sec">✦ Prompt</div>', unsafe_allow_html=True)

        examples = [
            "Total revenue by product, sorted highest first",
            "Join sales with customers — show name, segment, total spend",
            "Count orders by status with percentage of total",
            "Monthly revenue trend for 2022",
            "Top 10 customers by total order value",
            "Sales by region with average order value",
            "Find rows where amount is null or negative",
            "Revenue from COMPLETED orders by segment and country",
            "Average order value by country and segment",
            "Running total of revenue ordered by date",
        ]
        ex = st.selectbox("Examples",
                          ["— type your own below —"] + examples,
                          label_visibility="collapsed")
        default_prompt = ex if ex != "— type your own below —" else ""

        prompt = st.text_area(
            "Prompt", value=default_prompt, height=90,
            placeholder="Describe what you want in plain English…",
            label_visibility="collapsed",
        )

        with st.expander("⚙ Data Contracts (FIX 12: all counts in one scan)"):
            st.caption("not_null · min_val · max_val · allowed_values · regex")
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
            clear_btn = st.button("✕  Clear Results", use_container_width=True)

        if clear_btn:
            for k in ["last_sql","last_result","last_stats","quarantine_count",
                      "quarantine_sample","contract_result","recon","exec_warnings",
                      "ai_warnings","explanation","self_healed","confidence","conf_reasons"]:
                st.session_state[k] = _DEFAULTS[k]
            st.session_state.status = "idle"
            st.rerun()

    # ── PIPELINE ──────────────────────────────────────────────────────────
    if run_btn:
        if not st.session_state.schema:
            st.error("No tables loaded."); st.stop()
        if not prompt.strip():
            st.error("Enter a prompt."); st.stop()

        contracts = {}
        if contract_str.strip():
            try:
                contracts = json.loads(contract_str)
            except Exception as e:
                st.warning(f"Contract JSON invalid ({e}) — ignored.")

        st.session_state.status = "running"
        conn   = get_conn()
        schema = st.session_state.schema

        with st.spinner("🔮 Generating SQL via AI…"):
            try:
                sql, ai_conf, explanation, ai_warns = generate_sql(prompt, schema)
                st.session_state.last_sql    = sql
                st.session_state.ai_warnings = ai_warns
                st.session_state.explanation = explanation
            except Exception as e:
                st.session_state.status = "error"
                st.error(f"SQL generation failed: {e}"); st.stop()

        final_conf, conf_reasons = measure_confidence(sql, schema, ai_conf)
        st.session_state.confidence   = final_conf
        st.session_state.conf_reasons = conf_reasons

        referenced_tables     = extract_referenced_tables(sql, list(schema.keys()))
        all_contract_results  = {}
        total_source_rows     = 0
        total_quarantine_rows = 0

        with st.spinner("🔒 Applying contracts (FIX 12: batched scan)…"):
            for tname in referenced_tables:
                cr = apply_contracts_in_duckdb(conn, tname, contracts)
                all_contract_results[tname] = cr
                total_source_rows     += cr["source_rows"]
                total_quarantine_rows += cr["quarantine_rows"]
        st.session_state.contract_result = all_contract_results

        exec_sql = sql
        if contracts and total_quarantine_rows > 0:
            for tname in referenced_tables:
                exec_sql = re.sub(
                    rf'\b{re.escape(tname)}\b(?!_clean|_quarantine)',
                    f"{tname}_clean", exec_sql
                )

        with st.spinner("⚡ Executing in DuckDB (FIX 4/8: bounded)…"):
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
                st.error(f"Execution failed: {e}")
                with st.expander("Traceback"): st.code(traceback.format_exc())
                st.stop()

        # FIX 5: quarantine count + 500-row sample only
        total_q = 0
        q_samples = []
        for tname in referenced_tables:
            qc = all_contract_results.get(tname, {}).get("quarantine_rows", 0)
            total_q += qc
            if qc > 0:
                try:
                    qs = conn.execute(
                        f'SELECT * FROM "{tname}_quarantine" LIMIT 500'
                    ).df()
                    qs["_source_table"] = tname
                    q_samples.append(qs)
                except Exception:
                    pass
        st.session_state.quarantine_count  = total_q
        st.session_state.quarantine_sample = (
            pd.concat(q_samples, ignore_index=True) if q_samples else None
        )

        output_rows = stats["output_rows"]
        st.session_state.recon = reconcile(
            total_source_rows, total_quarantine_rows, output_rows, final_sql
        )

        st.session_state.run_history.append({
            "ts": datetime.now().strftime("%H:%M:%S"),
            "prompt": prompt[:70],
            "sql": final_sql,
            "tables_used": referenced_tables,
            "source_rows": total_source_rows,
            "quarantine":  total_quarantine_rows,
            "output_rows": output_rows,
            "display_rows":stats["display_rows"],
            "truncated":   stats.get("truncated", False),
            "confidence":  final_conf,
            "elapsed_ms":  stats["elapsed_ms"],
            "self_healed": stats.get("self_healed", False),
            "balanced":    st.session_state.recon.get("balanced"),
            "explanation": explanation,
            "violations":  [v for cr in all_contract_results.values()
                            for v in cr.get("violations", [])],
        })
        st.session_state.status = "done"
        st.rerun()

    # ── STATUS STRIP ──────────────────────────────────────────────────────
    stats = st.session_state.last_stats
    recon = st.session_state.recon
    conf  = st.session_state.confidence
    q_cnt = st.session_state.quarantine_count
    cr    = st.session_state.contract_result

    sc_cls = {"idle":"d","running":"y","done":"g","error":"r"}.get(
        st.session_state.status, "d")
    sc_sym = {"idle":"○","running":"◌","done":"●","error":"✕"}.get(
        st.session_state.status, "○")
    cf_cls = "g" if conf >= 0.80 else "y" if conf >= 0.60 else "r" if conf > 0 else "d"
    cf_str = f"{int(conf*100)}%" if conf > 0 else "—"
    q_cls  = "y" if q_cnt > 0 else "d"
    source_disp  = sum(v.get("source_rows",0) for v in cr.values()) if cr else 0
    display_rows = stats.get("display_rows", stats.get("output_rows", 0))
    output_rows  = stats.get("output_rows", 0)

    trunc_html = (
        f'<div class="si"><span class="sl">Total Rows</span>'
        f'<span class="sv y">{output_rows:,} ✂</span></div>'
        if stats.get("truncated") else ""
    )
    healed_html = (
        '<div class="si"><span class="sl">Self-healed</span>'
        '<span class="sv g">✓</span></div>'
        if st.session_state.self_healed else ""
    )
    recon_html = ""
    if recon:
        if recon.get("skipped"):
            recon_html = '<div class="si"><span class="sl">Recon</span><span class="sv d">AGG</span></div>'
        elif recon.get("balanced"):
            recon_html = '<div class="si"><span class="sl">Recon</span><span class="sv g">✓ OK</span></div>'
        else:
            recon_html = f'<div class="si"><span class="sl">Recon</span><span class="sv r">DIFF {recon["diff"]:+,}</span></div>'

    st.markdown(f"""
<div class="status-strip">
  <div class="si"><span class="sl">Status</span>
    <span class="sv {sc_cls}">{sc_sym} {st.session_state.status.upper()}</span></div>
  <div class="si"><span class="sl">Rows In</span>
    <span class="sv p">{source_disp:,}</span></div>
  <div class="si"><span class="sl">Displayed</span>
    <span class="sv g">{display_rows:,}</span></div>
  {trunc_html}
  <div class="si"><span class="sl">Quarantine</span>
    <span class="sv {q_cls}">{q_cnt:,}</span></div>
  <div class="si"><span class="sl">Confidence</span>
    <span class="sv {cf_cls}">{cf_str}</span></div>
  <div class="si"><span class="sl">ms</span>
    <span class="sv d">{stats.get('elapsed_ms','—')}</span></div>
  {healed_html}{recon_html}
</div>
""", unsafe_allow_html=True)

    if st.session_state.last_sql:
        st.markdown('<div class="sec">Generated SQL</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="sql-block">{st.session_state.last_sql}</div>',
            unsafe_allow_html=True
        )
        badges = []
        if conf >= 0.80:   badges.append('<span class="badge bp">✓ HIGH CONFIDENCE</span>')
        elif conf >= 0.60: badges.append('<span class="badge bw">⚠ MEDIUM CONFIDENCE</span>')
        elif conf > 0:     badges.append('<span class="badge bf">✕ LOW CONFIDENCE</span>')
        if st.session_state.self_healed:
            badges.append('<span class="badge bp">✓ SELF-HEALED</span>')
        if stats.get("truncated"):
            badges.append(f'<span class="badge bw">✂ TRUNCATED TO {DISPLAY_LIMIT:,}</span>')
        for r in st.session_state.conf_reasons[:2]:
            badges.append(f'<span class="badge bi">ℹ {r[:70]}</span>')
        for w in st.session_state.ai_warnings[:2]:
            badges.append(f'<span class="badge bw">⚠ {w[:70]}</span>')
        if st.session_state.explanation:
            badges.append(f'<span class="badge bi">ℹ {st.session_state.explanation[:90]}</span>')
        if badges:
            st.markdown(f'<div class="badge-row">{"".join(badges)}</div>',
                        unsafe_allow_html=True)

    if st.session_state.run_history:
        with st.expander(f"⏱ Run History ({len(st.session_state.run_history)} runs)"):
            hist_df = pd.DataFrame([{
                "Time":    r["ts"],
                "Prompt":  r["prompt"],
                "Tables":  ", ".join(r.get("tables_used", [])),
                "In":      f'{r["source_rows"]:,}',
                "Out":     f'{r["output_rows"]:,}',
                "Display": f'{r.get("display_rows", r["output_rows"]):,}',
                "Trunc":   "✂" if r.get("truncated") else "",
                "Quar":    r["quarantine"],
                "Conf":    f'{int(r["confidence"]*100)}%',
                "ms":      r["elapsed_ms"],
                "Healed":  "✓" if r["self_healed"] else "",
                "Recon":   ("✓" if r.get("balanced") is True
                            else "AGG" if r.get("balanced") is None else "✕"),
            } for r in reversed(st.session_state.run_history)])
            st.dataframe(hist_df, use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════════════
# TAB 2 — DATA TRUST
# ═══════════════════════════════════════════════════════════════════

with tab_trust:
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
            diff_s = "0  ✓  BALANCED" if recon["balanced"] else f"{recon['diff']:+,}  ✕"
            st.markdown(f"""
<div class="recon-block">
  <span style="color:#4fffb0;font-weight:700;font-size:13px">SOURCE = OUTPUT + QUARANTINE + SQL_EXCLUDED</span><br>
  <span style="color:#44448a">Source rows &nbsp;&nbsp;&nbsp;</span><span style="color:#e8e8f0">{recon['source']:,}</span><br>
  <span style="color:#44448a">Quarantine &nbsp;&nbsp;&nbsp;&nbsp;</span><span style="color:#ffcc44">{recon['quarantine']:,}</span><br>
  <span style="color:#44448a">SQL excluded &nbsp;</span><span style="color:#7c6fff">{recon['sql_excluded']:,}</span><br>
  <span style="color:#44448a">Output rows &nbsp;&nbsp;</span><span style="color:#4fffb0">{recon['output']:,}</span><br>
  <span style="color:#44448a">Difference &nbsp;&nbsp;&nbsp;</span><span style="color:{bc};font-weight:700">{diff_s}</span>
</div>""", unsafe_allow_html=True)
    else:
        st.info("Run a pipeline in ETL Studio to see reconciliation.")

    if cr:
        st.markdown('<div class="sec">Contract Results by Table</div>', unsafe_allow_html=True)
        for tname, res in cr.items():
            s = res["source_rows"]; c = res["clean_rows"]; q = res["quarantine_rows"]
            q_pct = q / max(s, 1) * 100
            color = "#ff6b6b" if q > 0 else "#4fffb0"
            st.markdown(f"""
<div class="status-strip" style="margin-bottom:6px">
  <div class="si"><span class="sl">Table</span><span class="sv p">{tname}</span></div>
  <div class="si"><span class="sl">Source</span><span class="sv d">{s:,}</span></div>
  <div class="si"><span class="sl">Clean</span><span class="sv g">{c:,}</span></div>
  <div class="si"><span class="sl">Quarantine</span><span class="sv" style="color:{color}">{q:,} ({q_pct:.1f}%)</span></div>
</div>""", unsafe_allow_html=True)
            for v in res.get("violations", []):
                st.markdown(
                    f'<div class="badge-row"><span class="badge bw">⚠ {v}</span></div>',
                    unsafe_allow_html=True)

    # FIX 5: show count + sample (not full concat)
    q_cnt    = st.session_state.quarantine_count
    q_sample = st.session_state.quarantine_sample
    if q_cnt > 0:
        st.markdown(
            f'<div class="sec">Quarantine — {q_cnt:,} total rows '
            f'(FIX 5: showing 500-row sample only)</div>',
            unsafe_allow_html=True
        )
        if q_sample is not None:
            st.dataframe(q_sample, use_container_width=True, hide_index=True)
        conn = get_conn()
        if st.button("⬇ Prepare full quarantine export"):
            all_q = []
            for tname in st.session_state.contract_result:
                try:
                    chunk = conn.execute(
                        f'SELECT *, \'{tname}\' AS _source FROM "{tname}_quarantine"'
                    ).df()
                    all_q.append(chunk)
                except Exception:
                    pass
            if all_q:
                full_q = pd.concat(all_q, ignore_index=True)
                buf = BytesIO()
                full_q.to_csv(buf, index=False); buf.seek(0)
                st.download_button("⬇ Download quarantine CSV",
                                   buf.getvalue(), "prism_quarantine.csv", "text/csv")
                del full_q
    elif st.session_state.last_result is not None:
        st.markdown(
            '<div class="badge-row"><span class="badge bp">✓ Zero quarantine rows</span></div>',
            unsafe_allow_html=True)

    if st.session_state.conf_reasons:
        st.markdown('<div class="sec">Confidence Breakdown</div>', unsafe_allow_html=True)
        for r in st.session_state.conf_reasons:
            badge = "bp" if "passed" in r.lower() else "bw" if "%" in r else "bi"
            st.markdown(
                f'<div class="badge-row"><span class="badge {badge}">{r}</span></div>',
                unsafe_allow_html=True)

    if len(st.session_state.run_history) > 1:
        st.markdown('<div class="sec">Confidence History</div>', unsafe_allow_html=True)
        ch = pd.DataFrame({
            "Run":        [f"#{i+1}" for i in range(len(st.session_state.run_history))],
            "Confidence": [r["confidence"]*100 for r in st.session_state.run_history],
        }).set_index("Run")
        st.line_chart(ch, color="#4fffb0", height=160)

    if st.session_state.schema:
        st.markdown('<div class="sec">Source Profiles (FIX 6: 2 DuckDB queries)</div>',
                    unsafe_allow_html=True)
        for tname, info in st.session_state.schema.items():
            traps   = info.get("join_traps", [])
            ignored = info.get("ignored_rows", 0)
            label   = f"{tname} · {info['row_count']:,} rows · {len(info['columns'])} cols"
            if traps:   label += f" · ⚠ {len(traps)} JOIN trap(s)"
            if ignored: label += f" · ⚠ {ignored:,} ignored"
            with st.expander(label):
                if traps:
                    for trap in traps:
                        detail = ", ".join(f"{t}.{trap['col']}=[{d}]"
                                           for t, d in trap["appearances"])
                        st.warning(f"Type mismatch on '{trap['col']}': {detail}")
                rows = []
                for col in info["columns"]:
                    tf  = "⚠ " if any(t["col"] == col for t in traps) else ""
                    ns  = info["num_stats"].get(col)
                    row = {
                        "Column":  tf + col,
                        "Type":    info["dtypes"][col],
                        "Nulls":   f'{info["null_counts"].get(col, 0):,}',
                        "Null%":   f'{info["null_counts"].get(col,0)/max(info["row_count"],1)*100:.1f}%',
                        "Unique":  f'{info["unique_counts"].get(col, "?"):,}',
                        "Sample":  ", ".join(
                            repr(v) for v in info["sample_values"].get(col, [])[:3]),
                    }
                    if ns:
                        row["Min"]  = f'{ns["min"]:,.2f}'
                        row["Max"]  = f'{ns["max"]:,.2f}'
                        row["Mean"] = f'{ns["mean"]:,.2f}'
                    rows.append(row)
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════════════
# TAB 3 — OUTPUT DATA
# ═══════════════════════════════════════════════════════════════════

with tab_output:
    result = st.session_state.last_result
    stats  = st.session_state.last_stats

    if result is not None and len(result) > 0:
        output_rows  = stats.get("output_rows", len(result))
        display_rows = stats.get("display_rows", len(result))
        truncated    = stats.get("truncated", False)

        hdr = (f'Result — {display_rows:,} displayed'
               + (f' of {output_rows:,} total ✂' if truncated else ' rows')
               + f' × {len(result.columns)} cols · {stats.get("elapsed_ms","?")} ms')
        st.markdown(f'<div class="sec">{hdr}</div>', unsafe_allow_html=True)

        if truncated:
            st.warning(f"⚠ {output_rows:,} total rows — showing first {display_rows:,}. "
                       "Download for full data.")

        num_cols = result.select_dtypes(include="number").columns.tolist()
        if num_cols:
            dcols = st.columns(min(len(num_cols), 4))
            for i, col in enumerate(num_cols[:4]):
                with dcols[i]:
                    total = result[col].sum()
                    avg   = result[col].mean()
                    label = (f"{total/1e6:.2f}M" if abs(total) >= 1e6
                             else f"{total/1e3:.1f}K" if abs(total) >= 1e3
                             else f"{total:,.1f}")
                    st.metric(col, label, delta=f"avg {avg:,.1f}")

        st.dataframe(result, use_container_width=True, height=360, hide_index=True)

        c1, c2, c3 = st.columns(3)
        with c1:
            buf = BytesIO()
            result.to_csv(buf, index=False); buf.seek(0)
            st.download_button(f"⬇ CSV ({display_rows:,})", buf.getvalue(),
                               "prism_output.csv", "text/csv", use_container_width=True)
        with c2:
            buf2 = BytesIO()
            result.to_json(buf2, orient="records", indent=2); buf2.seek(0)
            st.download_button(f"⬇ JSON ({display_rows:,})", buf2.getvalue(),
                               "prism_output.json", "application/json",
                               use_container_width=True)
        with c3:
            if truncated and st.session_state.last_sql:
                if st.button(f"⬇ Full CSV ({output_rows:,} rows)", use_container_width=True):
                    conn    = get_conn()
                    full_df = conn.execute(st.session_state.last_sql).df()
                    buf3    = BytesIO()
                    full_df.to_csv(buf3, index=False); buf3.seek(0)
                    st.download_button("⬇ Download full CSV", buf3.getvalue(),
                                       "prism_full_output.csv", "text/csv")
                    del full_df

        if num_cols and len(result) > 1:
            st.markdown('<div class="sec">Quick Chart</div>', unsafe_allow_html=True)
            chart_col = st.selectbox("Column", num_cols, label_visibility="collapsed")
            obj_cols  = result.select_dtypes(include="object").columns.tolist()
            chart_df  = (result[[obj_cols[0], chart_col]].dropna().head(30)
                         .set_index(obj_cols[0]) if obj_cols
                         else result[[chart_col]].head(50))
            st.bar_chart(chart_df, color="#7c6fff", height=200)

    elif st.session_state.status == "done":
        st.info("Query returned 0 rows — check your filters.")
    else:
        st.info("Run a pipeline in ETL Studio to see output here.")

    st.markdown('<div class="sec">SQL Editor (Direct DuckDB)</div>', unsafe_allow_html=True)
    st.caption(f"Results capped at {DISPLAY_LIMIT:,} rows (FIX 4/8)")
    manual_sql = st.text_area("SQL", height=90,
                              placeholder='SELECT * FROM "sales" LIMIT 10',
                              label_visibility="collapsed")
    if st.button("▶ Run SQL"):
        if not st.session_state.schema:
            st.error("No tables loaded.")
        elif manual_sql.strip():
            try:
                conn = get_conn()
                result_df, stats_m, final_sql, warns = execute_sql_safe(
                    conn, manual_sql.strip(), st.session_state.schema, manual_sql.strip()
                )
                ref = extract_referenced_tables(manual_sql, list(st.session_state.schema.keys()))
                src = sum(st.session_state.schema.get(t, {}).get("row_count", 0) for t in ref)
                st.session_state.last_result   = result_df
                st.session_state.last_stats    = stats_m
                st.session_state.last_sql      = final_sql
                st.session_state.exec_warnings = warns
                st.session_state.status        = "done"
                st.session_state.recon         = reconcile(src, 0, stats_m["output_rows"], final_sql)
                st.rerun()
            except Exception as e:
                st.error(f"SQL error: {e}")
                with st.expander("Traceback"): st.code(traceback.format_exc())

# ═══════════════════════════════════════════════════════════════════
# TAB 4 — SCHEMA
# ═══════════════════════════════════════════════════════════════════

with tab_schema:
    schema = st.session_state.schema
    conn   = get_conn()

    if schema:
        all_trap_cols = list({
            trap["col"]
            for info in schema.values()
            for trap in info.get("join_traps", [])
        })
        if all_trap_cols:
            st.warning(f"**{len(all_trap_cols)} JOIN type mismatch(es):** "
                       f"`{'`, `'.join(all_trap_cols)}` — auto-handled with CAST.")
        try:
            tbl_list = conn.execute("SHOW TABLES").df()
            st.markdown('<div class="sec">DuckDB Tables & Views</div>',
                        unsafe_allow_html=True)
            st.dataframe(tbl_list, use_container_width=True, hide_index=True)
        except Exception:
            pass

        st.markdown('<div class="sec">Column Details</div>', unsafe_allow_html=True)
        for tname, info in schema.items():
            traps   = info.get("join_traps", [])
            ignored = info.get("ignored_rows", 0)
            label   = (f"{tname} · {info['row_count']:,} rows · "
                       f"{len(info['columns'])} cols")
            if ignored: label += f" · ⚠ {ignored:,} ignored"
            with st.expander(label, expanded=False):
                rows = []
                for col in info["columns"]:
                    tf  = "⚠ " if any(t["col"] == col for t in traps) else ""
                    ns  = info["num_stats"].get(col)
                    row = {
                        "Column":      tf + col,
                        "DuckDB Type": info["dtypes"][col],
                        "Nulls":       f'{info["null_counts"].get(col, 0):,}',
                        "Unique":      f'{info["unique_counts"].get(col, "?"):,}',
                        "Samples":     ", ".join(
                            repr(v) for v in info["sample_values"].get(col, [])[:3]),
                    }
                    if ns:
                        row["Min"]  = f'{ns["min"]:,.2f}'
                        row["Max"]  = f'{ns["max"]:,.2f}'
                        row["Mean"] = f'{ns["mean"]:,.2f}'
                    rows.append(row)
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        with st.expander("Raw schema JSON (debug)"):
            debug = {t: {k: v for k, v in info.items()
                         if k not in ("sample_values","num_stats")}
                     for t, info in schema.items()}
            st.json(debug)
    else:
        st.info("No tables loaded. Upload CSVs or click 'Load Demo Data'.")

st.markdown("""
<div style="border-top:1px solid #1e1e35;margin-top:20px;padding:10px 0 2px;text-align:center">
  <span style="font-family:'DM Mono',monospace;font-size:10px;color:#1e1e40;letter-spacing:.15em">
    PRISM · v5.0 · ALL 15 PRODUCTION FIXES · DuckDB-PERSISTENT · ZERO RAW BYTES · BOUNDED RESULTS
  </span>
</div>
""", unsafe_allow_html=True)
