"""
Campaign Intelligence Dashboard
Handles Instamart/Swiggy Ads CSV exports with smart column detection,
full KPI calculations, learning phase logic, and actionable insights.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import io
import hashlib
import json
import psycopg2
from psycopg2 import pool as pg_pool

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Campaign Intelligence Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  /* ── Typography & base ── */
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

  /* ── Global background: soft warm white ── */
  .stApp                    { background: #f7f8fc; }
  section[data-testid="stSidebar"] { background: #ffffff; border-right: 1px solid #e4e7ef; }

  /* ── Header ── */
  .dash-header {
    background: linear-gradient(135deg, #1a73e8, #0d47a1);
    padding: 1.5rem 2rem;
    border-radius: 14px;
    margin-bottom: 1.4rem;
    text-align: center;
    box-shadow: 0 4px 18px rgba(26,115,232,0.18);
  }
  .dash-header h1 { color: #fff; font-size: 1.9rem; font-weight: 700; margin: 0; letter-spacing: -0.4px; }
  .dash-header p  { color: #cfe3ff; margin: 0.25rem 0 0; font-size: 0.88rem; }

  /* ── KPI cards ── */
  .kpi-card {
    background: #ffffff;
    border-radius: 12px;
    padding: 1.1rem 1.4rem;
    border: 1px solid #e4e7ef;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    position: relative;
    overflow: hidden;
  }
  .kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0;
    width: 4px; height: 100%;
    border-radius: 4px 0 0 4px;
  }
  .kpi-card.green::before  { background: #2e7d32; }
  .kpi-card.blue::before   { background: #1565c0; }
  .kpi-card.orange::before { background: #e65100; }
  .kpi-card.red::before    { background: #c62828; }
  .kpi-card.purple::before { background: #6a1b9a; }
  .kpi-label { font-size: 0.72rem; color: #6b7280; text-transform: uppercase; letter-spacing: 0.06em; font-weight: 600; }
  .kpi-value { font-size: 1.7rem; font-weight: 700; color: #111827; line-height: 1.2; margin-top: 0.15rem; }
  .kpi-sub   { font-size: 0.76rem; color: #9ca3af; margin-top: 0.2rem; }

  /* ── Alert boxes ── */
  .alert-red    { background:#fff5f5; border-left:4px solid #e53e3e; padding:0.75rem 1rem; border-radius:8px; color:#742a2a; margin:0.35rem 0; }
  .alert-yellow { background:#fffbeb; border-left:4px solid #d97706; padding:0.75rem 1rem; border-radius:8px; color:#78350f; margin:0.35rem 0; }
  .alert-green  { background:#f0fdf4; border-left:4px solid #16a34a; padding:0.75rem 1rem; border-radius:8px; color:#14532d; margin:0.35rem 0; }
  .alert-blue   { background:#eff6ff; border-left:4px solid #2563eb; padding:0.75rem 1rem; border-radius:8px; color:#1e3a8a; margin:0.35rem 0; }

  /* ── Learning badge ── */
  .badge-learning { background:#dbeafe; color:#1e40af; padding:2px 9px; border-radius:20px; font-size:0.71rem; font-weight:700; }
  .badge-stable   { background:#dcfce7; color:#166534; padding:2px 9px; border-radius:20px; font-size:0.71rem; font-weight:700; }

  /* ── Section title ── */
  .section-title {
    font-size: 1.08rem; font-weight: 700; color: #1e293b;
    border-bottom: 2px solid #1a73e8;
    padding-bottom: 0.35rem; margin: 1.1rem 0 0.75rem;
    letter-spacing: -0.2px;
  }

  /* ── Action pill ── */
  .action-pill {
    display: inline-block;
    background: #1a73e8; color: #fff;
    border-radius: 20px; padding: 3px 11px;
    font-size: 0.76rem; font-weight: 600;
    margin: 2px;
  }

  /* ── Streamlit component overrides ── */
  .stDataFrame               { border-radius: 10px; border: 1px solid #e4e7ef; }
  div[data-testid="stMetric"]{ background: #ffffff; border-radius: 10px;
                               padding: 0.6rem 1rem; border: 1px solid #e4e7ef;
                               box-shadow: 0 1px 4px rgba(0,0,0,0.05); }
  .stTabs [data-baseweb="tab-list"] { background: #eef2ff; border-radius: 10px; padding: 3px; }
  .stTabs [data-baseweb="tab"]      { border-radius: 8px; font-weight: 500; color: #374151; }
  .stTabs [aria-selected="true"]    { background: #ffffff !important; color: #1a73e8 !important;
                                      font-weight: 700; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }
  div[data-testid="stExpander"]     { background: #ffffff; border: 1px solid #e4e7ef;
                                      border-radius: 10px; }
  .stRadio > label                  { color: #374151 !important; font-weight: 500; }
  .stSlider [data-testid="stWidgetLabel"] { color: #374151; font-weight: 500; }
  code { background: #eef2ff; color: #1e40af; border-radius: 4px; padding: 1px 5px; font-size: 0.82rem; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# POSTGRESQL DATA STORE
# Persists all campaign data to Neon PostgreSQL across sessions.
# Falls back to session-state if DB is unavailable.
# ─────────────────────────────────────────────────────────────────────────────

# Columns that are deduplication keys per file type
DEDUP_KEYS = {
    "granular":     ["date", "campaign_id", "keyword", "city", "placement"],
    "date":         ["date", "campaign_id"],
    "summary":      ["campaign_id"],
    "city":         ["campaign_id", "city"],
    "placement":    ["campaign_id", "placement"],
    "product":      ["campaign_id", "product"],
    "search_query": ["date", "campaign_id", "search_query"],
    "unknown":      ["date", "campaign"],
}

# All numeric metric columns stored in DB
METRIC_COLS = [
    "spend", "revenue", "clicks", "impressions", "orders",
    "budget", "a2c", "ecpm", "ecpc", "direct_gmv7", "direct_roi7",
]

# All dimension / text columns stored in DB
DIM_COLS = [
    "date", "campaign_id", "campaign", "status", "keyword", "city",
    "placement", "product", "search_query", "brand", "start_date",
    "match_type", "bidding", "phase",
]


def _write_cockroach_cert() -> str | None:
    """
    If COCKROACH_CERT secret exists (base64-encoded root.crt),
    decode it and write to /tmp/cockroach-root.crt.
    Returns the file path, or None if secret not found.
    """
    cert_b64 = st.secrets.get("COCKROACH_CERT", "")
    if not cert_b64:
        return None
    import base64, re
    cert_b64_clean = re.sub(r"\s+", "", cert_b64.strip())
    cert_bytes = base64.b64decode(cert_b64_clean)
    cert_path  = "/tmp/cockroach-root.crt"
    with open(cert_path, "wb") as f:
        f.write(cert_bytes)
    return cert_path


def _db_url_changed() -> bool:
    """
    Detect if DATABASE_URL has changed since the pool was created.
    If yes, clear the cache so a fresh pool is built with the new URL.
    """
    current_url = st.secrets.get("DATABASE_URL", "")
    cached_url  = st.session_state.get("_cached_db_url", "")
    if current_url != cached_url:
        st.session_state["_cached_db_url"] = current_url
        _get_db_pool.clear()   # force @st.cache_resource to rebuild
        return True
    return False


@st.cache_resource(show_spinner=False)
def _get_db_pool():
    """
    Create a connection pool. Supports:
      • Neon PostgreSQL  (sslmode=require  — no cert needed)
      • CockroachDB      (sslmode=verify-full + cert written from secret)

    SSL resolution order for CockroachDB:
      1. COCKROACH_CERT secret present → decode, write to /tmp, use verify-full
      2. No cert secret               → fall back to sslmode=require (still encrypted)
    """
    try:
        conn_str = st.secrets.get("DATABASE_URL", "")
        if not conn_str:
            return None

        if "cockroachlabs.cloud" in conn_str:
            import re

            # Remove any existing ssl params so we can set them cleanly
            conn_str = re.sub(r"[?&]sslmode=[^&]*",     "", conn_str)
            conn_str = re.sub(r"[?&]sslrootcert=[^&]*", "", conn_str)

            cert_path = _write_cockroach_cert()

            if cert_path:
                # Full cert verification — most secure
                sep = "&" if "?" in conn_str else "?"
                conn_str = f"{conn_str}{sep}sslmode=verify-full&sslrootcert={cert_path}"
            else:
                # Encrypted but no cert verification — still safe for transit
                sep = "&" if "?" in conn_str else "?"
                conn_str = f"{conn_str}{sep}sslmode=require"

        p = pg_pool.SimpleConnectionPool(
            1, 5, conn_str,
            application_name="campaign_dashboard",
            connect_timeout=15,
        )
        return p

    except Exception as e:
        st.warning(f"⚠️ DB pool init failed: {e}")
        return None


def _get_conn():
    pool = _get_db_pool()
    if pool is None:
        return None
    try:
        return pool.getconn()
    except Exception:
        return None


def _release(conn):
    pool = _get_db_pool()
    if pool and conn:
        try:
            pool.putconn(conn)
        except Exception:
            pass


def _init_schema():
    """Create tables and indexes. Compatible with both PostgreSQL (Neon) and CockroachDB.
    Stores success/failure in session state so the UI can show DB health status.
    """
    conn = _get_conn()
    if conn is None:
        st.session_state["_schema_status"] = "no_connection"
        return

    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS campaign_data (
                id              BIGSERIAL PRIMARY KEY,
                file_type       TEXT      NOT NULL,
                row_hash        TEXT      NOT NULL,
                date            DATE,
                campaign_id     TEXT,
                campaign        TEXT,
                status          TEXT,
                keyword         TEXT,
                city            TEXT,
                placement       TEXT,
                product         TEXT,
                search_query    TEXT,
                brand           TEXT,
                start_date      DATE,
                match_type      TEXT,
                bidding         TEXT,
                phase           TEXT,
                spend           DECIMAL(14,4),
                revenue         DECIMAL(14,4),
                clicks          BIGINT,
                impressions     BIGINT,
                orders          BIGINT,
                budget          DECIMAL(14,4),
                a2c             BIGINT,
                ecpm            DECIMAL(10,4),
                ecpc            DECIMAL(10,4),
                direct_gmv7     DECIMAL(14,4),
                direct_roi7     DECIMAL(10,4),
                uploaded_at     TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (file_type, row_hash)
            );
        """)
        conn.commit()

        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_cd_date     ON campaign_data (date);",
            "CREATE INDEX IF NOT EXISTS idx_cd_filetype ON campaign_data (file_type);",
        ]:
            try:
                cur.execute(idx_sql)
                conn.commit()
            except Exception:
                conn.rollback()

        # Verify table is actually queryable
        cur.execute("SELECT COUNT(*) FROM campaign_data;")
        row_count = cur.fetchone()[0]
        st.session_state["_schema_status"] = "ok"
        st.session_state["_db_row_count"]  = int(row_count)

    except Exception as e:
        conn.rollback()
        st.session_state["_schema_status"] = f"error: {e}"
    finally:
        _release(conn)


def _row_hash(row: pd.Series, key_cols: list[str]) -> str:
    """SHA-1 of the dedup key values — used to detect duplicates fast."""
    parts = "|".join(str(row.get(c, "")) for c in key_cols)
    return hashlib.sha1(parts.encode()).hexdigest()[:16]


class DataStore:
    """
    PostgreSQL-backed persistent store.

    Write path:  upsert(ftype, df)
        - Computes a row_hash for each row using DEDUP_KEYS[ftype]
        - Fetches existing hashes from DB for that file_type
        - Inserts only genuinely new rows (no duplicates)

    Read path:   get(ftype, date_from, date_to)
        - Queries DB with optional date filter
        - Returns a pandas DataFrame

    Fallback:    if DB is unreachable, uses st.session_state transparently
    """

    _SS_KEY = "_ds_fallback"   # session-state fallback store

    # ── session-state fallback helpers ───────────────────────────────────────
    @classmethod
    def _ss(cls) -> dict:
        if cls._SS_KEY not in st.session_state:
            st.session_state[cls._SS_KEY] = {}
        return st.session_state[cls._SS_KEY]

    @classmethod
    def _db_ok(cls) -> bool:
        return _get_db_pool() is not None

    # ── public API ───────────────────────────────────────────────────────────

    @classmethod
    def upsert(cls, ftype: str, new_df: pd.DataFrame) -> tuple[int, int]:
        """Insert new rows, skip duplicates. Returns (added, skipped)."""
        if new_df is None or new_df.empty:
            return 0, 0

        key_cols = DEDUP_KEYS.get(ftype, ["date", "campaign"])
        # Only use keys that actually exist in the dataframe
        key_cols = [c for c in key_cols if c in new_df.columns] or ["campaign"]

        # Compute hash for every row
        new_df = new_df.copy()
        new_df["_hash"] = new_df.apply(lambda r: _row_hash(r, key_cols), axis=1)

        if cls._db_ok():
            return cls._upsert_db(ftype, new_df)
        else:
            return cls._upsert_ss(ftype, new_df, key_cols)

    @classmethod
    def get(cls, ftype: str,
            date_from=None, date_to=None) -> pd.DataFrame | None:
        """Fetch data for ftype with optional date filter."""
        if cls._db_ok():
            return cls._get_db(ftype, date_from, date_to)
        else:
            return cls._get_ss(ftype, date_from, date_to)

    @classmethod
    def all_types(cls) -> list[str]:
        if cls._db_ok():
            conn = _get_conn()
            if conn is None:
                return []
            try:
                cur = conn.cursor()
                cur.execute("SELECT DISTINCT file_type FROM campaign_data ORDER BY file_type;")
                return [r[0] for r in cur.fetchall()]
            except Exception:
                return []
            finally:
                _release(conn)
        else:
            return list(cls._ss().keys())

    @classmethod
    def date_range(cls) -> tuple:
        """Return (min_date, max_date) across all stored data."""
        if cls._db_ok():
            conn = _get_conn()
            if conn is None:
                return None, None
            try:
                cur = conn.cursor()
                cur.execute("SELECT MIN(date), MAX(date) FROM campaign_data WHERE date IS NOT NULL;")
                row = cur.fetchone()
                return (pd.Timestamp(row[0]) if row[0] else None,
                        pd.Timestamp(row[1]) if row[1] else None)
            except Exception:
                return None, None
            finally:
                _release(conn)
        else:
            mins, maxs = [], []
            for df in cls._ss().values():
                if "date" in df.columns and df["date"].notna().any():
                    mins.append(df["date"].min())
                    maxs.append(df["date"].max())
            return (min(mins) if mins else None, max(maxs) if maxs else None)

    @classmethod
    def total_rows(cls) -> int:
        if cls._db_ok():
            conn = _get_conn()
            if conn is None:
                return 0
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM campaign_data;")
                return cur.fetchone()[0]
            except Exception:
                return 0
            finally:
                _release(conn)
        else:
            return sum(len(df) for df in cls._ss().values())

    @classmethod
    def summary(cls) -> pd.DataFrame:
        if cls._db_ok():
            conn = _get_conn()
            if conn is None:
                return pd.DataFrame()
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT file_type,
                           COUNT(*)          AS rows,
                           MIN(date)::TEXT   AS date_from,
                           MAX(date)::TEXT   AS date_to,
                           MIN(uploaded_at)::DATE::TEXT AS first_upload,
                           MAX(uploaded_at)::DATE::TEXT AS last_upload
                    FROM campaign_data
                    GROUP BY file_type
                    ORDER BY file_type;
                """)
                cols = ["Type","Rows","Date From","Date To","First Upload","Last Upload"]
                return pd.DataFrame(cur.fetchall(), columns=cols)
            except Exception as e:
                return pd.DataFrame([{"Error": str(e)}])
            finally:
                _release(conn)
        else:
            rows = []
            for ftype, df in cls._ss().items():
                d_min = d_max = "—"
                if "date" in df.columns and df["date"].notna().any():
                    d_min = str(df["date"].min().date())
                    d_max = str(df["date"].max().date())
                rows.append({"Type": ftype, "Rows": len(df),
                             "Date From": d_min, "Date To": d_max})
            return pd.DataFrame(rows)

    @classmethod
    def clear(cls):
        if cls._db_ok():
            conn = _get_conn()
            if conn:
                try:
                    conn.cursor().execute("DELETE FROM campaign_data;")
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    st.error(f"Clear failed: {e}")
                finally:
                    _release(conn)
        st.session_state[cls._SS_KEY] = {}

    # ── private DB methods ────────────────────────────────────────────────────

    @classmethod
    def _upsert_db(cls, ftype: str, new_df: pd.DataFrame) -> tuple[int, int]:
        conn = _get_conn()
        if conn is None:
            return cls._upsert_ss(ftype, new_df,
                                  [c for c in DEDUP_KEYS.get(ftype, ["campaign"])
                                   if c in new_df.columns])
        try:
            cur = conn.cursor()

            # ── Always query the ACTUAL connected DB for existing hashes ──────
            # Never use session-state cache here — if DB changed (e.g. migrated
            # from Neon to CockroachDB), session state hashes are stale/wrong.
            cur.execute(
                "SELECT row_hash FROM campaign_data WHERE file_type = %s;",
                (ftype,)
            )
            existing_hashes = {r[0] for r in cur.fetchall()}

            # Filter to only genuinely new rows vs THIS database
            mask_new  = ~new_df["_hash"].isin(existing_hashes)
            to_insert = new_df[mask_new]
            added     = len(to_insert)
            skipped   = len(new_df) - added

            if added == 0:
                return 0, skipped

            # 3. Build insert rows
            def _safe(val, cast=None):
                try:
                    if val is None: return None
                    if isinstance(val, float) and pd.isna(val): return None
                    if isinstance(val, str) and val.strip() == "": return None
                    if cast == "int":   return int(float(val))
                    if cast == "float": return float(val)
                    if cast == "date":  return pd.Timestamp(val).date()
                    return str(val)[:512]
                except Exception:
                    return None

            # ── Fast bulk insert using pandas + SQLAlchemy ───────────────────
            # iterrows() + mogrify is too slow for large files (38k rows).
            # Instead: build a clean DataFrame of only new rows and use
            # pandas to_sql with method='multi' for fast bulk inserts.

            DB_COLS = [
                "file_type","row_hash","date","campaign_id","campaign","status",
                "keyword","city","placement","product","search_query","brand",
                "start_date","match_type","bidding","phase",
                "spend","revenue","clicks","impressions","orders","budget",
                "a2c","ecpm","ecpc","direct_gmv7","direct_roi7",
            ]

            # Build insert DataFrame with correct column names
            insert_df = to_insert.copy()
            insert_df["file_type"] = ftype
            insert_df["row_hash"]  = insert_df["_hash"]

            # Keep only DB columns that exist; fill missing with None
            for col in DB_COLS:
                if col not in insert_df.columns:
                    insert_df[col] = None

            insert_df = insert_df[DB_COLS].copy()

            # Coerce date columns
            for dc in ["date", "start_date"]:
                if dc in insert_df.columns:
                    insert_df[dc] = pd.to_datetime(insert_df[dc], errors="coerce").dt.date

            # Coerce numeric columns to proper Python types
            int_cols   = ["clicks","impressions","orders","a2c"]
            float_cols = ["spend","revenue","budget","ecpm","ecpc","direct_gmv7","direct_roi7"]
            for c in int_cols:
                insert_df[c] = pd.to_numeric(insert_df[c], errors="coerce").where(
                    insert_df[c].notna(), other=None)
            for c in float_cols:
                insert_df[c] = pd.to_numeric(insert_df[c], errors="coerce").where(
                    insert_df[c].notna(), other=None)

            # Truncate long text fields
            for c in ["keyword","search_query","campaign","product","city","placement"]:
                if c in insert_df.columns:
                    insert_df[c] = insert_df[c].astype(str).str[:512].where(
                        insert_df[c].notna(), other=None)

            # Use SQLAlchemy for fast bulk insert (avoids mogrify loop)
            try:
                from sqlalchemy import create_engine, text
                import re as _re

                raw_url = st.secrets.get("DATABASE_URL","")
                # Convert psycopg2 URL to SQLAlchemy format
                sa_url = raw_url.replace("postgresql://","postgresql+psycopg2://")

                # Handle CockroachDB SSL cert for SQLAlchemy
                cert_path = _write_cockroach_cert()
                if cert_path and "sslrootcert" not in sa_url:
                    sep = "&" if "?" in sa_url else "?"
                    sa_url = f"{sa_url}{sep}sslrootcert={cert_path}"
                # Remove verify-full for sqlalchemy — use require instead
                sa_url = _re.sub(r"sslmode=verify-full","sslmode=require", sa_url)
                sa_url = _re.sub(r"channel_binding=[^&]*","", sa_url)
                sa_url = _re.sub(r"&&","&", sa_url).rstrip("&?")

                engine = create_engine(sa_url, connect_args={"connect_timeout": 30})

                BATCH = 500
                committed = 0
                for i in range(0, len(insert_df), BATCH):
                    chunk = insert_df.iloc[i:i+BATCH]
                    chunk.to_sql(
                        "campaign_data",
                        engine,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )
                    committed += len(chunk)

                engine.dispose()

            except Exception as sa_err:
                # SQLAlchemy failed — fall back to psycopg2 executemany in small batches
                st.warning(f"SQLAlchemy insert failed ({sa_err}), using fallback…")
                insert_sql = """INSERT INTO campaign_data
                    (file_type,row_hash,date,campaign_id,campaign,status,
                     keyword,city,placement,product,search_query,brand,
                     start_date,match_type,bidding,phase,
                     spend,revenue,clicks,impressions,orders,budget,
                     a2c,ecpm,ecpc,direct_gmv7,direct_roi7)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (file_type,row_hash) DO NOTHING;"""
                rows = [tuple(r) for r in insert_df.itertuples(index=False, name=None)]
                BATCH = 100
                for i in range(0, len(rows), BATCH):
                    cur.executemany(insert_sql, rows[i:i+BATCH])
                    conn.commit()

            return added, skipped

        except Exception as e:
            conn.rollback()
            # Show full error — don't hide it silently
            st.error(f"❌ Database insert failed: {e}")
            # Fall back to session state so data is still usable this session
            return cls._upsert_ss(ftype, new_df,
                                  [c for c in DEDUP_KEYS.get(ftype, ["campaign"])
                                   if c in new_df.columns])
        finally:
            _release(conn)

    @classmethod
    def _get_db(cls, ftype: str, date_from=None, date_to=None) -> pd.DataFrame | None:
        conn = _get_conn()
        if conn is None:
            return cls._get_ss(ftype, date_from, date_to)
        try:
            cur = conn.cursor()
            params: list = [ftype]
            where = "WHERE file_type = %s"
            if date_from:
                where += " AND date >= %s";  params.append(date_from)
            if date_to:
                where += " AND date <= %s";  params.append(date_to)

            query = f"""
                SELECT date, campaign_id, campaign, status, keyword, city,
                       placement, product, search_query, brand, start_date,
                       match_type, bidding, phase,
                       spend, revenue, clicks, impressions, orders, budget,
                       a2c, ecpm, ecpc, direct_gmv7, direct_roi7
                FROM campaign_data {where}
                ORDER BY date NULLS LAST, campaign;
            """
            cur.execute(query, params)
            cols = [desc[0] for desc in cur.description]
            df   = pd.DataFrame(cur.fetchall(), columns=cols)
            if df.empty:
                return None
            # Re-apply types — psycopg2 returns NUMERIC as Python Decimal
            for c in ["date", "start_date"]:
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors="coerce")
            # Force ALL metric columns to float64 (Decimal → float)
            for c in ["spend","revenue","clicks","impressions","orders",
                      "budget","a2c","ecpm","ecpc","direct_gmv7","direct_roi7"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(float)
            return df
        except Exception as e:
            st.warning(f"DB read warning: {e}")
            return cls._get_ss(ftype, date_from, date_to)
        finally:
            _release(conn)

    # ── private session-state fallback methods ────────────────────────────────

    @classmethod
    def _upsert_ss(cls, ftype: str, new_df: pd.DataFrame, key_cols: list) -> tuple[int, int]:
        ss = cls._ss()
        if ftype not in ss:
            ss[ftype] = new_df.drop(columns=["_hash"], errors="ignore").copy()
            return len(new_df), 0
        existing = ss[ftype]
        existing_hashes = set(new_df["_hash"]) if "_hash" not in existing.columns else set(existing.get("_hash",[]))
        # Recompute existing hashes if not cached
        if "_hash" not in existing.columns:
            kc = [c for c in key_cols if c in existing.columns] or ["campaign"]
            existing["_hash"] = existing.apply(lambda r: _row_hash(r, kc), axis=1)
        existing_hashes = set(existing["_hash"])
        mask_new = ~new_df["_hash"].isin(existing_hashes)
        added    = int(mask_new.sum())
        skipped  = int((~mask_new).sum())
        if added > 0:
            ss[ftype] = pd.concat(
                [existing, new_df[mask_new].drop(columns=["_hash"], errors="ignore")],
                ignore_index=True
            )
        return added, skipped

    @classmethod
    def _get_ss(cls, ftype: str, date_from=None, date_to=None) -> pd.DataFrame | None:
        df = cls._ss().get(ftype)
        if df is None or df.empty:
            return None
        if date_from and "date" in df.columns:
            df = df[df["date"] >= pd.Timestamp(date_from)]
        if date_to and "date" in df.columns:
            df = df[df["date"] <= pd.Timestamp(date_to)]
        return df if not df.empty else None

    @classmethod
    def db_status(cls) -> str:
        """Return a human-readable DB connection status string."""
        if cls._db_ok():
            raw_url = st.secrets.get("DATABASE_URL", "")
            if "cockroachlabs" in raw_url:
                return "🟢 CockroachDB"
            elif "neon.tech" in raw_url:
                return "🟢 PostgreSQL (Neon)"
            return "🟢 PostgreSQL"
        return "🟡 Session state (DB unavailable)"


# ── Initialise schema on every cold start ─────────────────────────────────────
_init_schema()


# ─────────────────────────────────────────────────────────────────────────────
# NUMBER FORMATTING HELPER
# ─────────────────────────────────────────────────────────────────────────────

# Columns that show 0 decimal places (integer counts)
_ZERO_DP_COLS = {"clicks", "impressions", "orders", "a2c", "budget"}

# Everything numeric that isn't a count gets 1 decimal place
# (ROAS, CTR%, CVR%, CAC, AOV, spend, revenue, ecpm, ecpc, etc.)


def round_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a display-ready copy of df:
    - Forces all object/Decimal columns that contain numbers to float first
      (psycopg2 returns NUMERIC as Python Decimal which is dtype=object)
    - Integer-count columns → 0 dp, displayed as Int64
    - All other numeric columns → 1 dp
    """
    out = df.copy()
    for col in out.columns:
        series = out[col]

        # ── Step 1: coerce Decimal / mixed-object columns to float ───────────
        if series.dtype == object:
            try:
                converted = pd.to_numeric(series, errors="coerce")
                # Only replace if at least some values converted successfully
                if converted.notna().sum() > 0 and series.notna().sum() > 0:
                    if converted.notna().sum() / series.notna().sum() > 0.5:
                        out[col] = converted
                        series = out[col]
            except Exception:
                pass

        # ── Step 2: round based on column name ──────────────────────────────
        if not pd.api.types.is_numeric_dtype(out[col]):
            continue  # skip text columns

        if col in _ZERO_DP_COLS:
            out[col] = out[col].round(0).astype("Int64", errors="ignore")
        else:
            out[col] = out[col].round(1)

    return out


def _compute_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Force metric cols to float, then compute and round ROAS/CTR%/CVR%/CAC to 1dp.
    Safe to call on any aggregated DataFrame.
    """
    # Coerce metrics (handles Decimal from PostgreSQL)
    for c in ["spend","revenue","clicks","impressions","orders"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(float)

    if "spend" in df.columns and "revenue" in df.columns:
        df["ROAS"] = np.where(df["spend"] > 0,
                              (df["revenue"] / df["spend"]).round(1), 0.0)
    if "clicks" in df.columns and "impressions" in df.columns:
        df["CTR%"] = np.where(df["impressions"] > 0,
                              (df["clicks"] / df["impressions"] * 100).round(1), 0.0)
    if "orders" in df.columns and "clicks" in df.columns:
        df["CVR%"] = np.where(df["clicks"] > 0,
                              (df["orders"] / df["clicks"] * 100).round(1), 0.0)
    if "spend" in df.columns and "orders" in df.columns:
        df["CAC"]  = np.where(df["orders"] > 0,
                              (df["spend"] / df["orders"]).round(1), np.nan)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS – column name maps for auto-detection
# ─────────────────────────────────────────────────────────────────────────────
COL_MAP = {
    "date":         ["METRICS_DATE", "DATE", "date", "Day", "REPORT_DATE"],
    "campaign_id":  ["CAMPAIGN_ID"],
    "campaign":     ["CAMPAIGN_NAME", "Campaign Name", "campaign_name"],
    "status":       ["CAMPAIGN_STATUS"],
    "spend":        ["TOTAL_BUDGET_BURNT", "Spend", "spend", "Cost", "cost", "SPEND"],
    "revenue":      ["TOTAL_GMV", "GMV", "Revenue", "revenue", "REVENUE"],
    "clicks":       ["TOTAL_CLICKS", "Clicks", "clicks", "CLICKS"],
    "impressions":  ["TOTAL_IMPRESSIONS", "Impressions", "impressions"],
    "orders":       ["TOTAL_CONVERSIONS", "Conversions", "Orders", "orders"],
    "roas":         ["TOTAL_ROI", "ROI", "ROAS", "roas"],
    "ctr":          ["TOTAL_CTR", "CTR", "ctr"],
    "a2c":          ["TOTAL_A2C", "A2C"],
    "budget":       ["TOTAL_BUDGET", "Budget", "budget"],
    "keyword":      ["KEYWORD", "Keyword", "keyword"],
    "city":         ["CITY", "City", "city"],
    "placement":    ["AD_PROPERTY", "Placement", "placement"],
    "product":      ["PRODUCT_NAME", "Product Name"],
    "brand":        ["BRAND_NAME"],
    "start_date":   ["CAMPAIGN_START_DATE"],
    "match_type":   ["MATCH_TYPE"],
    "search_query": ["SEARCH_QUERY"],
    "bidding":      ["BIDDING_TYPE"],
    "ecpm":         ["eCPM"],
    "ecpc":         ["eCPC"],
    "direct_gmv7":  ["TOTAL_DIRECT_GMV_7_DAYS"],
    "direct_roi7":  ["TOTAL_DIRECT_ROI_7_DAYS"],
}


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING & PARSING
# ─────────────────────────────────────────────────────────────────────────────

def find_header_row(raw_df: pd.DataFrame) -> int:
    """
    Find the row index containing real column headers, skipping Instamart-style
    metadata rows (From Date, To Date, Ads Type, Campaign Name or ID, blank row).

    Strategy: the true header row in IM_* exports is always the FIRST row where
    ≥ 5 values are ALL_CAPS_WITH_UNDERSCORE tokens (e.g. METRICS_DATE,
    CAMPAIGN_ID, TOTAL_GMV …).  Metadata rows like 'Campaign Name or ID'
    / 'All Campaigns' never satisfy that strict criterion.
    """
    for i, row in raw_df.iterrows():
        vals = [str(v).strip() for v in row.dropna().values if str(v).strip()]
        if not vals:
            continue
        # Strict ALL_CAPS_UNDERSCORE check — must have at least 5 such tokens
        # This matches METRICS_DATE, CAMPAIGN_ID, TOTAL_GMV etc. but NOT
        # mixed-case metadata like 'Campaign Name or ID'
        strict_caps = sum(
            1 for v in vals
            if v == v.upper()               # entirely uppercase
            and v.replace("_", "").isalpha()# only letters + underscores
            and "_" in v                    # must contain underscore (rules out dates like '01/03/2026')
            and len(v) > 3
        )
        if strict_caps >= 5:
            return i

        # Fallback: row has many keyword-bearing column-name tokens AND
        # none of its values look like a date or a long prose string
        kw_count = sum(1 for v in vals if any(kw in v.upper() for kw in
                       ["METRICS_DATE","CAMPAIGN_ID","CAMPAIGN_NAME",
                        "TOTAL_IMPRESSIONS","TOTAL_CLICKS","TOTAL_GMV",
                        "TOTAL_BUDGET","TOTAL_ROI","BRAND_NAME"]))
        if kw_count >= 3:
            return i

    return 0


def load_csv(file_obj) -> pd.DataFrame | None:
    """Load a CSV uploaded via Streamlit, auto-skip metadata header rows."""
    try:
        raw = pd.read_csv(file_obj, header=None, low_memory=False)
        hrow = find_header_row(raw)
        df = pd.read_csv(io.BytesIO(file_obj.getvalue()), header=hrow, low_memory=False)
        # Drop rows that are all NaN
        df.dropna(how="all", inplace=True)
        # Drop fully-null columns
        df.dropna(axis=1, how="all", inplace=True)
        return df
    except Exception as e:
        st.error(f"Error loading {getattr(file_obj,'name','file')}: {e}")
        return None


def resolve(df: pd.DataFrame, key: str):
    """Return the first matching column name for a semantic key, else None."""
    for candidate in COL_MAP.get(key, []):
        if candidate in df.columns:
            return candidate
    return None


def to_num(series: pd.Series) -> pd.Series:
    """Strip currency symbols / percentage signs and coerce to float."""
    if series.dtype == object:
        series = series.astype(str).str.replace(r"[₹,%\s]", "", regex=True)
    return pd.to_numeric(series, errors="coerce").fillna(0)


def detect_file_type(filename: str) -> str:
    """Classify upload by filename."""
    name = filename.upper()
    if "GRANULAR" in name:    return "granular"
    if "DATE" in name:        return "date"
    if "CITY" in name:        return "city"
    if "PLACEMENT" in name:   return "placement"
    if "PRODUCT" in name:     return "product"
    if "SEARCH_QUERY" in name:return "search_query"
    if "SUMMARY" in name:     return "summary"
    return "unknown"


def standardize(df: pd.DataFrame) -> pd.DataFrame:
    """Rename detected columns to standard names and compute KPIs."""
    mapping = {}
    for std_key, candidates in COL_MAP.items():
        for c in candidates:
            if c in df.columns:
                mapping[c] = std_key
                break
    df = df.rename(columns=mapping)

    # Numeric coercion
    for col in ["spend","revenue","clicks","impressions","orders","roas","budget",
                "a2c","ecpm","ecpc","direct_gmv7","direct_roi7"]:
        if col in df.columns:
            df[col] = to_num(df[col])

    # CTR: strip % and convert to fraction if > 1
    if "ctr" in df.columns:
        df["ctr"] = to_num(df["ctr"])
        if df["ctr"].max() > 1:
            df["ctr"] = df["ctr"] / 100

    # Compute derived KPIs
    if "revenue" in df.columns and "spend" in df.columns:
        df["ROAS"] = np.where(df["spend"] > 0, df["revenue"] / df["spend"], 0)
    elif "roas" in df.columns:
        df["ROAS"] = df["roas"]
    else:
        df["ROAS"] = 0

    if "spend" in df.columns and "orders" in df.columns:
        df["CAC"] = np.where(df["orders"] > 0, df["spend"] / df["orders"], np.nan)

    if "clicks" in df.columns and "impressions" in df.columns:
        df["CTR_calc"] = np.where(df["impressions"] > 0,
                                  df["clicks"] / df["impressions"] * 100, 0)

    if "orders" in df.columns and "clicks" in df.columns:
        df["CVR"] = np.where(df["clicks"] > 0, df["orders"] / df["clicks"] * 100, 0)

    if "revenue" in df.columns and "orders" in df.columns:
        df["AOV"] = np.where(df["orders"] > 0, df["revenue"] / df["orders"], 0)

    # Parse dates
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Campaign start date for learning-phase logic
    if "start_date" in df.columns:
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# CAMPAIGN INTELLIGENCE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def classify_campaign(name: str) -> str:
    """Rule-based campaign type classifier."""
    if pd.isna(name):
        return "Unknown"
    n = str(name).lower()
    brand_terms     = ["brand","our","own","official","branded"]
    competitor_terms = ["competitor","comp","vs","rival","against"]
    generic_terms   = ["generic","search","category","broad","keywords","instamart"]
    if any(t in n for t in competitor_terms): return "Competitor"
    if any(t in n for t in brand_terms):      return "Brand"
    if any(t in n for t in generic_terms):    return "Generic"
    return "Generic"   # default to generic


def keyword_bucket(keyword) -> str:
    """Assign keyword to a strategy bucket."""
    if pd.isna(keyword):
        return "Unknown"
    k = str(keyword).lower()
    if any(t in k for t in ["brand","official","our","company"]): return "Brand"
    if any(t in k for t in ["vs","competitor","rival","compare"]): return "Competitor"
    if any(t in k for t in ["buy","order","cheap","best","top","price"]): return "Purchase Intent"
    if any(t in k for t in ["what is","how to","review","guide"]): return "Informational"
    return "Generic"


def learning_phase_flag(df: pd.DataFrame) -> pd.DataFrame:
    """Tag each campaign as Learning or Stable based on spend + age."""
    MIN_DAYS  = 7
    MIN_SPEND = 500   # ₹

    if "campaign" not in df.columns:
        return df

    today = pd.Timestamp.today()

    # Per-campaign aggregates
    grp_cols = ["campaign"]
    if "campaign_id" in df.columns:
        grp_cols = ["campaign_id","campaign"]

    agg = {col: "sum" for col in ["spend","clicks","impressions","orders"] if col in df.columns}
    if not agg:
        df["phase"] = "Unknown"
        return df

    camp_agg = df.groupby(grp_cols).agg(agg).reset_index()

    if "start_date" in df.columns:
        start_map = df.groupby(grp_cols[0])["start_date"].min().reset_index()
        camp_agg  = camp_agg.merge(start_map, on=grp_cols[0], how="left")
        camp_agg["age_days"] = (today - camp_agg["start_date"]).dt.days.fillna(0)
    else:
        camp_agg["age_days"] = MIN_DAYS + 1   # assume mature if no start date

    camp_agg["phase"] = np.where(
        (camp_agg.get("age_days", MIN_DAYS + 1) < MIN_DAYS) |
        (camp_agg.get("spend", MIN_SPEND + 1) < MIN_SPEND),
        "Learning", "Stable"
    )

    phase_map = dict(zip(camp_agg[grp_cols[0]], camp_agg["phase"]))
    df["phase"] = df[grp_cols[0]].map(phase_map).fillna("Stable")
    return df


def budget_suggestion(row) -> dict:
    """Return a budget action suggestion for a campaign row."""
    roas    = row.get("ROAS", 0) or 0
    cac     = row.get("CAC",  np.nan)
    spend   = row.get("spend", 0) or 0
    phase   = row.get("phase", "Stable")

    if phase == "Learning":
        return {"action": "⏳ Wait", "detail": "Needs more data (Learning phase)",
                "pct_change": 0, "color": "blue"}

    if roas >= 3:
        return {"action": "🚀 Scale Up", "detail": f"ROAS {roas:.1f}x — increase budget 20-30%",
                "pct_change": 25, "color": "green"}
    if roas >= 1.5:
        return {"action": "📈 Increase", "detail": f"ROAS {roas:.1f}x — increase budget 10%",
                "pct_change": 10, "color": "green"}
    if 0.8 <= roas < 1.5:
        return {"action": "👀 Monitor", "detail": f"ROAS {roas:.1f}x — optimise bids first",
                "pct_change": 0, "color": "orange"}
    if 0 < roas < 0.8:
        return {"action": "✂️ Reduce", "detail": f"ROAS {roas:.1f}x — cut budget 20%",
                "pct_change": -20, "color": "red"}
    return {"action": "⏸️ Pause", "detail": "No revenue — consider pausing",
            "pct_change": -100, "color": "red"}


# ─────────────────────────────────────────────────────────────────────────────
# AGGREGATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def safe_sum(df, col):
    return df[col].sum() if col in df.columns else 0

def safe_mean(df, col):
    return df[col].mean() if col in df.columns else 0


def campaign_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-campaign metrics for the summary table."""
    if "campaign" not in df.columns:
        return pd.DataFrame()

    grp = ["campaign"]
    if "campaign_id" in df.columns:
        grp = ["campaign_id","campaign"]

    # Force metric cols to numeric first (handles Decimal from PostgreSQL)
    for c in ["spend","revenue","clicks","impressions","orders"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    agg_dict = {}
    for c in ["spend","revenue","clicks","impressions","orders"]:
        if c in df.columns:
            agg_dict[c] = "sum"
    if "phase" in df.columns:
        agg_dict["phase"] = "first"
    if "start_date" in df.columns:
        agg_dict["start_date"] = "min"
    if "status" in df.columns:
        agg_dict["status"] = "first"

    tbl = df.groupby(grp).agg(agg_dict).reset_index()

    # Re-compute KPIs and round immediately to 1 dp
    if "spend" in tbl.columns and "revenue" in tbl.columns:
        tbl["ROAS"] = np.where(tbl["spend"] > 0,
                               (tbl["revenue"] / tbl["spend"]).round(1), 0.0)
    else:
        tbl["ROAS"] = 0.0

    if "clicks" in tbl.columns and "impressions" in tbl.columns:
        tbl["CTR%"] = np.where(tbl["impressions"] > 0,
                               (tbl["clicks"] / tbl["impressions"] * 100).round(1), 0.0)
    else:
        tbl["CTR%"] = 0.0

    if "orders" in tbl.columns and "clicks" in tbl.columns:
        tbl["CVR%"] = np.where(tbl["clicks"] > 0,
                               (tbl["orders"] / tbl["clicks"] * 100).round(1), 0.0)
    else:
        tbl["CVR%"] = 0.0

    if "spend" in tbl.columns and "orders" in tbl.columns:
        tbl["CAC"] = np.where(tbl["orders"] > 0,
                              (tbl["spend"] / tbl["orders"]).round(1), np.nan)
    else:
        tbl["CAC"] = np.nan

    # Round raw metric cols too
    for c in ["spend","revenue"]:
        if c in tbl.columns:
            tbl[c] = tbl[c].round(1)
    for c in ["clicks","impressions","orders"]:
        if c in tbl.columns:
            tbl[c] = tbl[c].round(0).astype("Int64", errors="ignore")

    if "campaign" in tbl.columns:
        tbl["Type"] = tbl["campaign"].apply(classify_campaign)

    # Budget suggestion
    tbl["Suggestion"] = tbl.apply(lambda r: budget_suggestion(r)["action"], axis=1)
    tbl["Advice"]     = tbl.apply(lambda r: budget_suggestion(r)["detail"], axis=1)

    return tbl


# ─────────────────────────────────────────────────────────────────────────────
# CHART BUILDERS
# ─────────────────────────────────────────────────────────────────────────────
PLOTLY_TEMPLATE = "plotly_white"
CHART_HEIGHT    = 320


def line_chart(df, x, y_cols, title, y_label="Value"):
    fig = go.Figure()
    colors = ["#1d4ed8","#15803d","#b45309","#be185d","#6d28d9"]
    for i, col in enumerate(y_cols):
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df[x], y=df[col], mode="lines+markers",
                name=col, line=dict(color=colors[i % len(colors)], width=2.5),
                marker=dict(size=5)
            ))
    fig.update_layout(template=PLOTLY_TEMPLATE, title=title,
                      height=CHART_HEIGHT, xaxis_title=x, yaxis_title=y_label,
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    return fig


def bar_chart(df, x, y, title, color=None, color_scale=None):
    fig = px.bar(df, x=x, y=y, title=title, template=PLOTLY_TEMPLATE,
                 color=color, color_continuous_scale=color_scale or "Blues",
                 height=CHART_HEIGHT)
    fig.update_layout(showlegend=False)
    return fig


def scatter_chart(df, x, y, size=None, color=None, text=None, title=""):
    fig = px.scatter(df, x=x, y=y, size=size, color=color, text=text,
                     title=title, template=PLOTLY_TEMPLATE, height=CHART_HEIGHT)
    fig.update_traces(textposition="top center")
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# ALERT GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def generate_alerts(df: pd.DataFrame, roas_thresh=1.0, cac_thresh=500) -> list[dict]:
    """Produce a list of alert dicts from the summary table."""
    alerts = []
    if df.empty:
        return alerts

    # Low ROAS
    if "ROAS" in df.columns:
        low_roas = df[(df["ROAS"] > 0) & (df["ROAS"] < roas_thresh)]
        for _, row in low_roas.iterrows():
            alerts.append({"level":"red",
                           "msg":f"⚠️ Low ROAS: <b>{row.get('campaign','?')}</b> — ROAS {row['ROAS']:.1f}x (target ≥ {roas_thresh}x)",
                           "action":"Reduce bids or pause under-performers"})

    # High CAC
    if "CAC" in df.columns:
        high_cac = df[df["CAC"] > cac_thresh]
        for _, row in high_cac.iterrows():
            alerts.append({"level":"yellow",
                           "msg":f"🔴 High CAC: <b>{row.get('campaign','?')}</b> — ₹{row['CAC']:.0f}/order",
                           "action":"Review targeting, bids or product price"})

    # Scale-up opportunities
    if "ROAS" in df.columns and "spend" in df.columns:
        stars = df[df["ROAS"] >= 3]
        for _, row in stars.iterrows():
            alerts.append({"level":"green",
                           "msg":f"🌟 Star performer: <b>{row.get('campaign','?')}</b> — ROAS {row['ROAS']:.1f}x",
                           "action":"Increase budget 20-30%"})

    # Zero spend
    if "spend" in df.columns:
        no_spend = df[df["spend"] == 0]
        for _, row in no_spend.iterrows():
            alerts.append({"level":"blue",
                           "msg":f"ℹ️ Zero spend: <b>{row.get('campaign','?')}</b>",
                           "action":"Check campaign status / budget"})

    return alerts


# ─────────────────────────────────────────────────────────────────────────────
# FILE SLOT DEFINITIONS  – one entry per distinct report type
# ─────────────────────────────────────────────────────────────────────────────
FILE_SLOTS = [
    {
        "key":       "granular",
        "label":     "📊 Granular Report",
        "filename":  "IM_GRANULAR_*",
        "help":      "Keyword × City × Day breakdown. Most detailed – required for keyword & city tabs.",
        "required":  True,
        # Which report frequencies need this file
        "frequency": ["Daily", "Weekly", "Monthly"],
    },
    {
        "key":       "date",
        "label":     "📅 Campaign × Date Report",
        "filename":  "IM_CAMPAIGN_X_DATE_*",
        "help":      "Daily campaign-level metrics. Powers the Trends tab.",
        "required":  True,
        "frequency": ["Daily", "Weekly", "Monthly"],
    },
    {
        "key":       "summary",
        "label":     "📋 Summary Report",
        "filename":  "IM_SUMMARY_*",
        "help":      "Overall campaign totals. Good for weekly/monthly reviews.",
        "required":  False,
        "frequency": ["Weekly", "Monthly"],
    },
    {
        "key":       "city",
        "label":     "🏙️ Campaign × City Report",
        "filename":  "IM_CAMPAIGN_X_CITY_*",
        "help":      "City-level spend & GMV breakdown.",
        "required":  False,
        "frequency": ["Daily", "Weekly", "Monthly"],
    },
    {
        "key":       "placement",
        "label":     "📍 Campaign × Placement Report",
        "filename":  "IM_CAMPAIGN_X_PLACEMENT_*",
        "help":      "Ad-placement / keyword performance.",
        "required":  False,
        "frequency": ["Daily", "Weekly", "Monthly"],
    },
    {
        "key":       "product",
        "label":     "📦 Campaign × Product Report",
        "filename":  "IM_CAMPAIGN_X_PRODUCT_*",
        "help":      "Product-level GMV & ROAS.",
        "required":  False,
        "frequency": ["Weekly", "Monthly"],
    },
    {
        "key":       "search_query",
        "label":     "🔍 Search Query Report",
        "filename":  "IM_CAMPAIGN_X_SEARCH_QUERY_*",
        "help":      "User search terms — for keyword mining & negatives.",
        "required":  False,
        "frequency": ["Daily", "Weekly", "Monthly"],   # ← available in all frequencies
    },
]

# Frequency → which KPI views make most sense
FREQUENCY_CONFIG = {
    "Daily": {
        "icon":        "🗓️",
        "focus":       ["Spend","Revenue","CTR","ROAS"],
        "description": "Track day-over-day spend pacing and ROAS. Catch budget exhaustion early.",
        "required_files": ["granular","date"],
        "optional_files": ["city","placement","search_query"],   # ← added search_query
    },
    "Weekly": {
        "icon":        "📆",
        "focus":       ["ROAS","CAC","CVR","Budget trends"],
        "description": "Review weekly ROAS & CAC trends. Adjust bids and budgets for the coming week.",
        "required_files": ["granular","date","summary"],
        "optional_files": ["city","placement","search_query"],
    },
    "Monthly": {
        "icon":        "🗃️",
        "focus":       ["Growth","Contribution","AOV","Channel mix"],
        "description": "Monthly growth analysis, product contribution, and channel efficiency review.",
        "required_files": ["granular","date","summary"],
        "optional_files": ["city","placement","product","search_query"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # ── DB URL change detection (runs once per session, no rerun) ─────────────
    _db_url_changed()

    # ── Sidebar ──────────────────────────────────────────────────────────────
    with st.sidebar:

        # ── STEP 0: Report Frequency ─────────────────────────────────────
        st.markdown("## 🔁 Report Frequency")
        frequency = st.radio(
            "How often are you reviewing?",
            list(FREQUENCY_CONFIG.keys()),
            format_func=lambda f: f"{FREQUENCY_CONFIG[f]['icon']} {f}",
            index=0,
            help="Controls which files are required/optional and which KPIs are highlighted."
        )
        fc = FREQUENCY_CONFIG[frequency]
        st.markdown(
            f'<div class="alert-blue" style="margin:0.4rem 0 0.8rem;">'
            f'<b>{fc["icon"]} {frequency} focus:</b> {", ".join(fc["focus"])}<br>'
            f'<small>{fc["description"]}</small></div>',
            unsafe_allow_html=True
        )

        st.markdown("---")

        # ── STEP 1: Individual file uploaders ────────────────────────────
        st.markdown("## 📁 Upload Reports")

        # Collect uploaded file objects keyed by slot
        slot_files: dict[str, object] = {}

        for slot in FILE_SLOTS:
            # Only show slots relevant to the chosen frequency
            if frequency not in slot["frequency"]:
                continue

            is_required = slot["key"] in fc["required_files"]
            is_optional = slot["key"] in fc["optional_files"]

            if not (is_required or is_optional):
                continue  # skip if not applicable at all

            badge = "🔴 Required" if is_required else "🟡 Optional"
            label = f"{slot['label']}  ·  {badge}"

            f = st.file_uploader(
                label,
                type=["csv","xlsx"],
                key=f"upload_{slot['key']}",
                help=f"Expected filename pattern: {slot['filename']}\n\n{slot['help']}",
            )
            slot_files[slot["key"]] = f

        # ── Upload status checklist ──────────────────────────────────────
        st.markdown("---")
        st.markdown("### ✅ Upload Status")

        all_required_ok = True
        for slot in FILE_SLOTS:
            if frequency not in slot["frequency"]:
                continue
            is_required = slot["key"] in fc["required_files"]
            is_optional = slot["key"] in fc["optional_files"]
            if not (is_required or is_optional):
                continue

            uploaded_file = slot_files.get(slot["key"])
            if uploaded_file:
                icon = "✅"
                note = f"<span style='color:#66bb6a'>{uploaded_file.name[:28]}</span>"
            elif is_required:
                icon = "❌"
                note = "<span style='color:#ef5350'>Missing — required</span>"
                all_required_ok = False
            else:
                icon = "⬜"
                note = "<span style='color:#888'>Not uploaded (optional)</span>"

            st.markdown(
                f"{icon} **{slot['label']}**<br>"
                f"<small style='margin-left:1.4rem'>{note}</small>",
                unsafe_allow_html=True
            )

        if not all_required_ok:
            st.markdown(
                '<div class="alert-yellow" style="margin-top:0.6rem">'
                '⚠️ Upload all <b>required</b> files to enable full analysis.</div>',
                unsafe_allow_html=True
            )

        st.markdown("---")

        # ── STEP 2: Settings ──────────────────────────────────────────────
        st.markdown("## ⚙️ Thresholds")
        roas_thresh = st.slider("Min ROAS target", 0.5, 5.0, 1.5, 0.1)
        cac_thresh  = st.slider("Max acceptable CAC (₹)", 50, 2000, 300, 50)

        st.markdown("---")

        # ── STEP 3: Database summary + date filter ────────────────────────
        st.markdown("## 🗄️ Data Store")

        # ── Live connection diagnostic ────────────────────────────────────
        db_status_str = DataStore.db_status()
        status_color  = "alert-green" if "PostgreSQL" in db_status_str or "CockroachDB" in db_status_str else "alert-yellow"

        # Show which host we're actually connected to
        raw_url = st.secrets.get("DATABASE_URL", "")
        if raw_url:
            try:
                from urllib.parse import urlparse
                parsed   = urlparse(raw_url)
                db_host  = parsed.hostname or "unknown"
                db_name  = parsed.path.lstrip("/") or "unknown"
                db_label = f"{db_host[:45]}/{db_name}"
            except Exception:
                db_label = "configured"
        else:
            db_label = "not configured"

        st.markdown(
            f'<div class="{status_color}" style="margin-bottom:0.5rem">'
            f'<b>Storage:</b> {db_status_str}<br>'
            f'<small style="word-break:break-all">🔌 {db_label}</small>'
            f'</div>',
            unsafe_allow_html=True
        )

        total_rows = DataStore.total_rows()
        db_min, db_max = DataStore.date_range()

        # Show schema health
        schema_status = st.session_state.get("_schema_status", "unknown")
        if schema_status == "ok":
            st.markdown(
                f'<div class="alert-blue">'
                f'<b>📦 {total_rows:,} rows stored</b><br>'
                f'<small>Date range: {db_min.date() if db_min else "—"} → {db_max.date() if db_max else "—"}</small>'
                f'</div>', unsafe_allow_html=True
            )
        elif schema_status == "no_connection":
            st.markdown(
                '<div class="alert-red"><b>❌ Cannot connect to database</b><br>'
                '<small>Check DATABASE_URL secret and COCKROACH_CERT.</small></div>',
                unsafe_allow_html=True
            )
        elif schema_status.startswith("error:"):
            st.markdown(
                f'<div class="alert-red"><b>❌ Schema error</b><br>'
                f'<small>{schema_status}</small></div>',
                unsafe_allow_html=True
            )
        else:
            st.caption("Checking database…")

        if total_rows > 0:
            with st.expander("📋 Store contents", expanded=False):
                st.dataframe(DataStore.summary(), use_container_width=True, hide_index=True)
            if st.button("🗑️ Clear all stored data", use_container_width=True):
                DataStore.clear()
                for k in list(st.session_state.keys()):
                    if k.startswith("_ds_") or k in ("_schema_status","_db_row_count"):
                        del st.session_state[k]
                st.rerun()
        elif schema_status == "ok":
            st.markdown(
                '<div class="alert-yellow">'
                '⚠️ <b>No data in this database yet.</b><br>'
                '<small>Upload your CSV files above. '
                'If you just switched databases, upload the same files again — '
                'they will insert fresh into the new DB.</small>'
                '</div>', unsafe_allow_html=True
            )

        st.markdown("---")
        st.markdown("## 📅 Date Filter")

        # Build filter options based on what's actually in the store
        date_filter = st.radio(
            "Period",
            ["All time", "Last 7 days", "Last 30 days", "Custom"],
            index=0
        )
        custom_start = custom_end = None
        if date_filter == "Custom":
            if db_min and db_max:
                custom_start = st.date_input("From", value=db_min.date(),
                                             min_value=db_min.date(), max_value=db_max.date())
                custom_end   = st.date_input("To",   value=db_max.date(),
                                             min_value=db_min.date(), max_value=db_max.date())
            else:
                custom_start = st.date_input("From")
                custom_end   = st.date_input("To")

    # ── Header (rendered after sidebar so sidebar vars are available) ─────────
    st.markdown("""
    <div class="dash-header">
      <h1>📊 Campaign Intelligence Dashboard</h1>
      <p>Upload your Instamart / Swiggy Ads CSV exports to unlock deep performance insights</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Collect all uploaded files ────────────────────────────────────────────
    uploaded_pairs: list[tuple] = []
    for key, fobj in slot_files.items():
        if fobj is not None:
            uploaded_pairs.append((fobj, key))

    # ── Upsert newly uploaded files into DataStore ────────────────────────────
    if uploaded_pairs:
        upsert_log = []
        newly_added = 0
        progress_bar = st.progress(0, text="Starting upload…")
        total_files  = len(uploaded_pairs)

        for idx, (fobj, ftype) in enumerate(uploaded_pairs):
            pct  = int(idx / total_files * 100)
            progress_bar.progress(pct, text=f"Processing {fobj.name[:40]} ({idx+1}/{total_files})…")

            raw = load_csv(fobj)
            if raw is None:
                continue
            std = standardize(raw)
            std = learning_phase_flag(std)

            progress_bar.progress(pct + int(1/total_files*50),
                                  text=f"Saving {fobj.name[:40]} to database…")

            added, skipped = DataStore.upsert(ftype, std)
            newly_added += added
            upsert_log.append({
                "File": fobj.name, "Type": ftype,
                "New rows added": added, "Duplicate rows skipped": skipped
            })

        progress_bar.progress(100, text="✅ Done!")
        import time; time.sleep(0.5)
        progress_bar.empty()

        if upsert_log:
            all_skipped = all(r["New rows added"] == 0 for r in upsert_log)
            with st.expander("📥 Upload result", expanded=True):
                st.dataframe(pd.DataFrame(upsert_log), use_container_width=True, hide_index=True)
                if all_skipped:
                    db_rows = DataStore.total_rows()
                    if db_rows == 0:
                        st.markdown(
                            '<div class="alert-red">'
                            '<b>⚠️ All rows skipped but database appears empty!</b><br>'
                            'This usually means you recently switched databases. '
                            'Try clicking <b>🗑️ Clear all stored data</b> in the sidebar and re-uploading.'
                            '</div>', unsafe_allow_html=True
                        )
                    else:
                        st.markdown(
                            f'<div class="alert-blue">'
                            f'ℹ️ All rows already exist in the database ({db_rows:,} rows stored). '
                            f'No duplicates inserted — this is correct behaviour.'
                            f'</div>', unsafe_allow_html=True
                        )

        # ── Rerun after inserting new rows so dashboard loads fresh ───────────
        # Only rerun if DB insert succeeded (data is in DB, not just session state)
        if newly_added > 0 and DataStore._db_ok():
            st.rerun()

    # ── Show onboarding if DB still empty and nothing uploaded ────────────────
    has_stored = DataStore.total_rows() > 0
    if not has_stored:
        st.markdown(f"### {fc['icon']} {frequency} Report — Getting Started")
        req_slots = [s for s in FILE_SLOTS if s["key"] in fc["required_files"] and frequency in s["frequency"]]
        opt_slots = [s for s in FILE_SLOTS if s["key"] in fc["optional_files"] and frequency in s["frequency"]]
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### 🔴 Required files")
            for s in req_slots:
                st.markdown(f"""
                <div class="alert-red" style="margin:0.3rem 0">
                  <b>{s['label']}</b><br>
                  <code style='font-size:0.75rem'>{s['filename']}</code><br>
                  <small>{s['help']}</small>
                </div>""", unsafe_allow_html=True)
        with c2:
            st.markdown("#### 🟡 Optional files")
            for s in opt_slots:
                st.markdown(f"""
                <div class="alert-yellow" style="margin:0.3rem 0">
                  <b>{s['label']}</b><br>
                  <code style='font-size:0.75rem'>{s['filename']}</code><br>
                  <small>{s['help']}</small>
                </div>""", unsafe_allow_html=True)
        st.markdown("---")
        st.info("👈 Use the sidebar uploaders to add your files. Each report type has its own uploader so you can clearly see what's missing.")
        return

    # ── Resolve date bounds for DB query ──────────────────────────────────────
    today = pd.Timestamp.today()
    q_date_from = q_date_to = None

    if date_filter == "Last 7 days":
        q_date_from = (today - timedelta(days=7)).date()
    elif date_filter == "Last 30 days":
        q_date_from = (today - timedelta(days=30)).date()
    elif date_filter == "Custom" and custom_start and custom_end:
        q_date_from = custom_start
        q_date_to   = custom_end

    # ── Read datasets from DataStore (DB filters applied server-side) ─────────
    datasets: dict[str, pd.DataFrame] = {}
    for ftype in DataStore.all_types():
        df = DataStore.get(ftype, date_from=q_date_from, date_to=q_date_to)
        if df is not None and not df.empty:
            # Re-apply learning phase flag after load (phase not stored for sessions fallback)
            if "phase" not in df.columns or df["phase"].isna().all():
                df = learning_phase_flag(df)
            datasets[ftype] = df

    if not datasets:
        st.error("No data found for the selected date range. Try 'All time' or upload more files.")
        return

    # ── Choose primary dataset ────────────────────────────────────────────────
    primary_key = next((k for k in ["granular","date","summary","city","placement","product","search_query"]
                        if k in datasets), list(datasets.keys())[0])
    primary = datasets[primary_key]

    if primary.empty:
        st.warning("No data in selected date range. Try 'All time' or a different period.")
        return

    # ── Frequency context banner ──────────────────────────────────────────────
    missing_req = [s["label"] for s in FILE_SLOTS
                   if s["key"] in fc["required_files"]
                   and s["key"] not in datasets
                   and frequency in s["frequency"]]
    missing_opt = [s["label"] for s in FILE_SLOTS
                   if s["key"] in fc["optional_files"]
                   and s["key"] not in datasets
                   and frequency in s["frequency"]]

    banner_cols = st.columns([3,2])
    with banner_cols[0]:
        loaded_types = ", ".join(f"`{k}`" for k in datasets.keys())
        st.markdown(
            f'<div class="alert-green">'
            f'<b>{fc["icon"]} {frequency} Review Mode</b> — '
            f'Loaded: {loaded_types}<br>'
            f'<small>KPI focus: {" · ".join(fc["focus"])}</small>'
            f'</div>', unsafe_allow_html=True
        )
    with banner_cols[1]:
        if missing_req:
            st.markdown(
                f'<div class="alert-red">'
                f'<b>Missing required files:</b><br>'
                + "<br>".join(f"❌ {m}" for m in missing_req) +
                f'</div>', unsafe_allow_html=True
            )
        elif missing_opt:
            st.markdown(
                f'<div class="alert-yellow">'
                f'<b>Optional files not uploaded:</b><br>'
                + "<br>".join(f"⬜ {m}" for m in missing_opt) +
                f'</div>', unsafe_allow_html=True
            )
        else:
            st.markdown(
                '<div class="alert-green"><b>✅ All recommended files loaded!</b></div>',
                unsafe_allow_html=True
            )

    # ── KPI CARDS ─────────────────────────────────────────────────────────────
    st.markdown('<div class="section-title">📌 Key Performance Indicators</div>', unsafe_allow_html=True)

    total_spend  = safe_sum(primary,"spend")
    total_rev    = safe_sum(primary,"revenue")
    total_clicks = safe_sum(primary,"clicks")
    total_impr   = safe_sum(primary,"impressions")
    total_orders = safe_sum(primary,"orders")

    overall_roas = total_rev / total_spend if total_spend > 0 else 0
    overall_ctr  = (total_clicks / total_impr * 100) if total_impr > 0 else \
                   (primary["CTR_calc"].mean() if "CTR_calc" in primary.columns else 0)
    overall_cvr  = (total_orders / total_clicks * 100) if total_clicks > 0 else 0
    overall_cac  = (total_spend / total_orders) if total_orders > 0 else 0
    overall_aov  = (total_rev / total_orders) if total_orders > 0 else 0

    def kpi_card(label, value, sub, color):
        return f"""
        <div class="kpi-card {color}">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value">{value}</div>
          <div class="kpi-sub">{sub}</div>
        </div>"""

    cols = st.columns(5)
    cards = [
        ("ROAS",       f"{overall_roas:.1f}×", f"Revenue/Spend · Target ≥{roas_thresh}×",
         "green" if overall_roas >= roas_thresh else "red"),
        ("Total Spend",f"₹{total_spend:,.0f}", f"Across {primary.get('campaign',primary.get('campaign_id',pd.Series())).nunique() if 'campaign' in primary else '—'} campaigns","blue"),
        ("CTR",        f"{overall_ctr:.1f}%",  "Clicks / Impressions","orange"),
        ("CVR",        f"{overall_cvr:.1f}%",  "Orders / Clicks","purple"),
        ("CAC",        f"₹{overall_cac:,.0f}"  if overall_cac else "—",
         f"Target ≤ ₹{cac_thresh}","red" if overall_cac > cac_thresh else "green"),
    ]
    for col, (label, val, sub, color) in zip(cols, cards):
        col.markdown(kpi_card(label, val, sub, color), unsafe_allow_html=True)

    # Secondary row
    cols2 = st.columns(5)
    secondary = [
        ("Revenue",     f"₹{total_rev:,.0f}",    "Total GMV generated","green"),
        ("Clicks",      f"{total_clicks:,}",      "Ad clicks","blue"),
        ("Impressions", f"{total_impr:,}",         "Ad views","blue"),
        ("Orders",      f"{total_orders:,}",       "Conversions","green"),
        ("AOV",         f"₹{overall_aov:,.0f}" if overall_aov else "—",
         "Avg. order value","orange"),
    ]
    st.markdown("<br>", unsafe_allow_html=True)
    for col, (label, val, sub, color) in zip(cols2, secondary):
        col.markdown(kpi_card(label, val, sub, color), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── ALERTS ────────────────────────────────────────────────────────────────
    camp_tbl = campaign_summary_table(primary)

    alerts = generate_alerts(camp_tbl, roas_thresh, cac_thresh)
    if alerts:
        st.markdown('<div class="section-title">🚨 Alerts & Opportunities</div>', unsafe_allow_html=True)
        col_a, col_b = st.columns(2)
        for i, a in enumerate(alerts[:10]):
            with (col_a if i % 2 == 0 else col_b):
                st.markdown(
                    f'<div class="alert-{a["level"]}">{a["msg"]}<br>'
                    f'<small><i>→ {a["action"]}</i></small></div>',
                    unsafe_allow_html=True)

    # ── TABS ──────────────────────────────────────────────────────────────────
    tab_labels = ["📈 Trends","🎯 Campaigns","💰 Budget","🔑 Keywords","🏙️ Cities","📦 Products","🔍 Queries","📁 Raw Data"]
    tabs = st.tabs(tab_labels)

    # ── TAB 1: TRENDS ─────────────────────────────────────────────────────────
    with tabs[0]:
        st.markdown('<div class="section-title">Daily Performance Trends</div>', unsafe_allow_html=True)

        date_src = datasets.get("date", datasets.get("granular"))
        if date_src is not None and "date" in date_src.columns:
            # Coerce metrics to float (handles Decimal from PostgreSQL)
            for _c in ["spend","revenue","clicks","impressions","orders"]:
                if _c in date_src.columns:
                    date_src[_c] = pd.to_numeric(date_src[_c], errors="coerce").fillna(0).astype(float)
            daily = date_src.groupby("date").agg(
                spend=("spend","sum") if "spend" in date_src.columns else ("spend","first"),
                revenue=("revenue","sum") if "revenue" in date_src.columns else ("revenue","first"),
                clicks=("clicks","sum") if "clicks" in date_src.columns else ("clicks","first"),
                impressions=("impressions","sum") if "impressions" in date_src.columns else ("impressions","first"),
                orders=("orders","sum") if "orders" in date_src.columns else ("orders","first"),
            ).reset_index()
            # Recompute daily KPIs rounded to 1dp
            daily["ROAS"] = np.where(daily["spend"] > 0,
                                     (daily["revenue"] / daily["spend"]).round(1), 0.0)
            daily["CTR%"] = np.where(daily["impressions"] > 0,
                                     (daily["clicks"] / daily["impressions"] * 100).round(1), 0.0)

            c1, c2 = st.columns(2)
            with c1:
                if daily["spend"].sum() > 0 or daily["revenue"].sum() > 0:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=daily["date"], y=daily["spend"],
                                            name="Spend", fill="tozeroy",
                                            line=dict(color="#dc2626",width=2),
                                            fillcolor="rgba(220,38,38,0.08)"))
                    fig.add_trace(go.Scatter(x=daily["date"], y=daily["revenue"],
                                            name="Revenue", fill="tozeroy",
                                            line=dict(color="#16a34a",width=2),
                                            fillcolor="rgba(22,163,74,0.08)"))
                    fig.update_layout(template=PLOTLY_TEMPLATE, title="Spend vs Revenue (Daily)",
                                      height=CHART_HEIGHT, yaxis_title="₹")
                    st.plotly_chart(fig, use_container_width=True)

            with c2:
                if daily["ROAS"].sum() > 0:
                    fig2 = go.Figure()
                    fig2.add_trace(go.Scatter(x=daily["date"], y=daily["ROAS"],
                                             mode="lines+markers", name="ROAS",
                                             line=dict(color="#1d4ed8",width=2.5),
                                             marker=dict(size=6)))
                    fig2.add_hline(y=roas_thresh, line_dash="dash", line_color="#d97706",
                                  annotation_text=f"Target {roas_thresh}×")
                    fig2.add_hline(y=1.0, line_dash="dot", line_color="#dc2626",
                                  annotation_text="Break-even")
                    fig2.update_layout(template=PLOTLY_TEMPLATE, title="Daily ROAS",
                                       height=CHART_HEIGHT, yaxis_title="ROAS (×)")
                    st.plotly_chart(fig2, use_container_width=True)

            c3, c4 = st.columns(2)
            with c3:
                if daily["CTR%"].sum() > 0:
                    st.plotly_chart(
                        line_chart(daily, "date", ["CTR%"], "CTR % Trend", "CTR %"),
                        use_container_width=True)
            with c4:
                if daily["orders"].sum() > 0:
                    st.plotly_chart(
                        bar_chart(daily, "date", "orders", "Daily Orders"),
                        use_container_width=True)
        else:
            st.info("No date-level data available. Upload IM_CAMPAIGN_X_DATE or IM_GRANULAR files.")

    # ── TAB 2: CAMPAIGNS ──────────────────────────────────────────────────────
    with tabs[1]:
        st.markdown('<div class="section-title">Campaign Performance</div>', unsafe_allow_html=True)

        if not camp_tbl.empty:
            # Metrics summary
            n_stable   = (camp_tbl.get("phase","") == "Stable").sum()
            n_learning = (camp_tbl.get("phase","") == "Learning").sum()
            n_types    = camp_tbl.get("Type", pd.Series()).value_counts()

            mc1,mc2,mc3,mc4 = st.columns(4)
            mc1.metric("Total Campaigns", len(camp_tbl))
            mc2.metric("Stable",  n_stable)
            mc3.metric("Learning",n_learning)
            mc4.metric("Active",  len(camp_tbl[camp_tbl.get("spend",pd.Series(0)) > 0]) if "spend" in camp_tbl.columns else "—")

            # Table with colour-coded suggestions
            def colour_suggestion(val):
                if "Scale" in str(val) or "Increase" in str(val): return "color: #15803d; font-weight:700"
                if "Pause" in str(val) or "Reduce" in str(val):   return "color: #b91c1c; font-weight:700"
                if "Wait"  in str(val):                            return "color: #1d4ed8"
                return "color: #b45309; font-weight:600"

            display_cols = [c for c in ["campaign","Type","phase","spend","revenue","ROAS","CTR%","CVR%","CAC","orders","Suggestion","Advice"]
                           if c in camp_tbl.columns]
            display_df   = round_df(camp_tbl[display_cols])
            styled = display_df.style
            if "Suggestion" in display_cols:
                styled = styled.map(colour_suggestion, subset=["Suggestion"])
            st.dataframe(styled, use_container_width=True, hide_index=True)

            # ROAS scatter
            if "ROAS" in camp_tbl.columns and "spend" in camp_tbl.columns:
                fig_s = scatter_chart(
                    camp_tbl, x="spend", y="ROAS",
                    size="revenue" if "revenue" in camp_tbl.columns else None,
                    color="ROAS", text="campaign" if "campaign" in camp_tbl.columns else None,
                    title="Campaign Bubble: Spend vs ROAS (size = Revenue)"
                )
                fig_s.add_hline(y=roas_thresh, line_dash="dash", line_color="#d97706",
                               annotation_text=f"ROAS Target {roas_thresh}×")
                st.plotly_chart(fig_s, use_container_width=True)
        else:
            st.info("Campaign-level aggregation requires a 'campaign' column.")

    # ── TAB 3: BUDGET SUGGESTIONS ─────────────────────────────────────────────
    with tabs[2]:
        st.markdown('<div class="section-title">Budget Recommendations</div>', unsafe_allow_html=True)

        if not camp_tbl.empty and "ROAS" in camp_tbl.columns and "spend" in camp_tbl.columns:
            for _, row in camp_tbl.sort_values("ROAS", ascending=False).iterrows():
                sug = budget_suggestion(row)
                spend = row.get("spend", 0)
                new_spend = spend * (1 + sug["pct_change"]/100) if sug["pct_change"] != -100 else 0
                phase_badge = (f'<span class="badge-learning">🔵 Learning</span>'
                               if row.get("phase") == "Learning"
                               else f'<span class="badge-stable">🟢 Stable</span>')
                roas_str = f"{row.get('ROAS',0):.1f}×" if "ROAS" in row else "—"
                action_col = {"green":"#15803d","red":"#b91c1c","orange":"#b45309","blue":"#1d4ed8"}.get(sug["color"],"#374151")
                st.markdown(f"""
                <div style="background:#ffffff;border-radius:10px;padding:0.9rem 1.2rem;
                            border:1px solid #e4e7ef;border-left:4px solid {action_col};
                            margin:0.5rem 0;box-shadow:0 1px 4px rgba(0,0,0,0.06);">
                  <b style="color:#111827">{row.get('campaign','Campaign')}</b>
                  &nbsp;{phase_badge}&nbsp;
                  <span style="color:#6b7280;font-size:0.82rem">ROAS {roas_str} | Spend ₹{spend:,.0f}</span><br>
                  <span style="color:{action_col};font-weight:700;font-size:1.05rem">{sug['action']}</span>
                  &nbsp;&nbsp;<span style="color:#374151;font-size:0.85rem">{sug['detail']}</span>
                  {f'<br><span style="color:#6b7280;font-size:0.8rem">→ Suggested budget: ₹{new_spend:,.0f}</span>' if new_spend != spend else ''}
                </div>""", unsafe_allow_html=True)
        else:
            st.info("Upload data with spend & revenue columns to see budget recommendations.")

    # ── TAB 4: KEYWORDS ──────────────────────────────────────────────────────
    with tabs[3]:
        st.markdown('<div class="section-title">Keyword Intelligence</div>', unsafe_allow_html=True)

        kw_src = datasets.get("granular", datasets.get("placement"))
        if kw_src is not None and "keyword" in kw_src.columns:
            kw_grp_cols = ["keyword"]
            if "match_type" in kw_src.columns:
                kw_grp_cols.append("match_type")

            kw_agg_dict = {c:"sum" for c in ["spend","revenue","clicks","impressions","orders"] if c in kw_src.columns}
            kw = kw_src.groupby(kw_grp_cols).agg(kw_agg_dict).reset_index()
            kw = _compute_kpis(kw)
            kw["Bucket"] = kw["keyword"].apply(keyword_bucket)
            kw["Status"] = kw["ROAS"].apply(
                lambda r: "🟢 Scale" if r >= roas_thresh else ("🟡 Monitor" if r >= 0.8 else "🔴 Pause"))

            # Bucket distribution
            c1, c2 = st.columns(2)
            with c1:
                bucket_counts = kw["Bucket"].value_counts().reset_index()
                bucket_counts.columns = ["Bucket","Count"]
                fig_b = px.pie(bucket_counts, values="Count", names="Bucket",
                               title="Keyword Bucket Distribution",
                               template=PLOTLY_TEMPLATE, hole=0.4)
                st.plotly_chart(fig_b, use_container_width=True)
            with c2:
                top_kw = kw.nlargest(15, "spend") if "spend" in kw.columns else kw.head(15)
                if "ROAS" in top_kw.columns:
                    st.plotly_chart(
                        bar_chart(top_kw, "keyword", "ROAS", "Top Keywords by ROAS", "ROAS"),
                        use_container_width=True)

            # Table
            st.dataframe(round_df(kw.sort_values("ROAS", ascending=False).head(100)),
                        use_container_width=True, hide_index=True)
        else:
            st.info("No keyword data found. Upload IM_GRANULAR or IM_CAMPAIGN_X_PLACEMENT files.")

    # ── TAB 5: CITIES ────────────────────────────────────────────────────────
    with tabs[4]:
        st.markdown('<div class="section-title">City-Level Performance</div>', unsafe_allow_html=True)

        city_src = datasets.get("city", datasets.get("granular"))
        if city_src is not None and "city" in city_src.columns:
            city_agg_dict = {c:"sum" for c in ["spend","revenue","clicks","impressions","orders"] if c in city_src.columns}
            cities = city_src.groupby("city").agg(city_agg_dict).reset_index()
            cities = _compute_kpis(cities)
            cities = cities.sort_values("revenue" if "revenue" in cities.columns else "spend", ascending=False)

            c1,c2 = st.columns(2)
            with c1:
                top_cities = cities.head(20)
                fig_city = px.bar(top_cities, x="revenue" if "revenue" in top_cities.columns else "spend",
                                 y="city", orientation="h",
                                 color="ROAS", color_continuous_scale="RdYlGn",
                                 title="Top Cities by Revenue", template=PLOTLY_TEMPLATE,
                                 height=CHART_HEIGHT)
                st.plotly_chart(fig_city, use_container_width=True)
            with c2:
                if "spend" in cities.columns and "revenue" in cities.columns:
                    fig_sc = scatter_chart(cities.head(30), x="spend", y="revenue",
                                          color="ROAS", text="city",
                                          title="City Spend vs Revenue")
                    st.plotly_chart(fig_sc, use_container_width=True)

            st.dataframe(round_df(cities.head(50)), use_container_width=True, hide_index=True)
        else:
            st.info("No city data found. Upload IM_CAMPAIGN_X_CITY or IM_GRANULAR files.")

    # ── TAB 6: PRODUCTS ──────────────────────────────────────────────────────
    with tabs[5]:
        st.markdown('<div class="section-title">Product Performance</div>', unsafe_allow_html=True)

        prod_src = datasets.get("product", datasets.get("granular"))
        if prod_src is not None and "product" in prod_src.columns:
            prod_agg = {c:"sum" for c in ["spend","revenue","clicks","orders"] if c in prod_src.columns}
            prods = prod_src.groupby("product").agg(prod_agg).reset_index()
            prods = _compute_kpis(prods)

            c1,c2 = st.columns(2)
            with c1:
                top20 = prods.nlargest(20,"revenue" if "revenue" in prods.columns else "spend")
                st.plotly_chart(
                    bar_chart(top20,"product","revenue" if "revenue" in top20.columns else "spend",
                             "Top Products by Revenue","ROAS","RdYlGn"),
                    use_container_width=True)
            with c2:
                if "ROAS" in prods.columns:
                    st.plotly_chart(
                        bar_chart(prods.nlargest(20,"ROAS"),"product","ROAS","Top Products by ROAS"),
                        use_container_width=True)

            st.dataframe(round_df(prods.sort_values("ROAS",ascending=False)), use_container_width=True, hide_index=True)
        else:
            st.info("No product data found. Upload IM_CAMPAIGN_X_PRODUCT or IM_GRANULAR files.")

    # ── TAB 7: SEARCH QUERIES ─────────────────────────────────────────────────
    with tabs[6]:
        st.markdown('<div class="section-title">Search Query Analysis</div>', unsafe_allow_html=True)

        sq_src = datasets.get("search_query")
        if sq_src is not None and "search_query" in sq_src.columns:
            sq_agg = {c:"sum" for c in ["spend","revenue","clicks","impressions","orders"] if c in sq_src.columns}
            sq = sq_src.groupby("search_query").agg(sq_agg).reset_index()
            sq = _compute_kpis(sq)
            sq["intent"] = sq["search_query"].apply(keyword_bucket)

            c1,c2 = st.columns(2)
            with c1:
                intent_counts = sq["intent"].value_counts().reset_index()
                intent_counts.columns = ["Intent","Count"]
                fig_i = px.pie(intent_counts, values="Count", names="Intent",
                               title="Search Intent Distribution",
                               template=PLOTLY_TEMPLATE, hole=0.4)
                st.plotly_chart(fig_i, use_container_width=True)
            with c2:
                top_sq = sq.nlargest(15,"clicks" if "clicks" in sq.columns else "revenue")
                if not top_sq.empty and "clicks" in top_sq.columns:
                    st.plotly_chart(
                        bar_chart(top_sq,"search_query","clicks","Top Search Queries by Clicks"),
                        use_container_width=True)

            # High-intent, no conversion
            if "orders" in sq.columns and "clicks" in sq.columns:
                missed = sq[(sq["clicks"] >= 10) & (sq["orders"] == 0)].nlargest(20,"clicks")
                if not missed.empty:
                    st.markdown("##### ⚠️ High-Click, Zero-Conversion Queries (review landing / bid)")
                    st.dataframe(round_df(missed[["search_query","clicks","spend"]]),
                                use_container_width=True, hide_index=True)

            st.dataframe(round_df(sq.sort_values("ROAS",ascending=False).head(200)),
                        use_container_width=True, hide_index=True)
        else:
            st.info("No search query data found. Upload IM_CAMPAIGN_X_SEARCH_QUERY file.")

    # ── TAB 8: RAW DATA ──────────────────────────────────────────────────────
    with tabs[7]:
        st.markdown('<div class="section-title">Raw Data Explorer</div>', unsafe_allow_html=True)

        src_choice = st.selectbox("Select dataset to explore", list(datasets.keys()))
        raw_view   = datasets[src_choice]

        col_filter = st.multiselect("Select columns", raw_view.columns.tolist(),
                                   default=raw_view.columns[:12].tolist())
        st.dataframe(round_df(raw_view[col_filter].head(500)), use_container_width=True, hide_index=True)

        # Download
        csv_bytes = raw_view.to_csv(index=False).encode()
        st.download_button("⬇️ Download as CSV", csv_bytes,
                          file_name=f"{src_choice}_standardized.csv", mime="text/csv")

    # ── FOOTER ───────────────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '<div style="text-align:center;color:#9ca3af;font-size:0.78rem;padding:1rem 0;">'
        '📊 Campaign Intelligence Dashboard · Built for Instamart Ads · '
        'Data is processed locally in your browser session'
        '</div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
