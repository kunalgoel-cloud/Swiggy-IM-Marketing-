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

  /* ── Header ── */
  .dash-header {
    background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
    padding: 1.6rem 2rem;
    border-radius: 12px;
    margin-bottom: 1.5rem;
    text-align: center;
  }
  .dash-header h1 {
    color: #fff;
    font-size: 2rem;
    font-weight: 700;
    margin: 0;
    letter-spacing: -0.5px;
  }
  .dash-header p { color: #90caf9; margin: 0.3rem 0 0; font-size: 0.9rem; }

  /* ── KPI cards ── */
  .kpi-card {
    background: #1e1e2e;
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    border: 1px solid #2a2a40;
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
  .kpi-card.green::before  { background: #4caf50; }
  .kpi-card.blue::before   { background: #2196f3; }
  .kpi-card.orange::before { background: #ff9800; }
  .kpi-card.red::before    { background: #f44336; }
  .kpi-card.purple::before { background: #9c27b0; }
  .kpi-label { font-size: 0.75rem; color: #888; text-transform: uppercase; letter-spacing: 0.05em; }
  .kpi-value { font-size: 1.8rem; font-weight: 700; color: #fff; line-height: 1.2; }
  .kpi-sub   { font-size: 0.78rem; color: #aaa; margin-top: 0.2rem; }

  /* ── Alert boxes ── */
  .alert-red    { background:#3b1a1a; border-left:4px solid #f44336; padding:0.8rem 1rem; border-radius:8px; color:#ffcdd2; margin:0.4rem 0; }
  .alert-yellow { background:#3b2e00; border-left:4px solid #ffc107; padding:0.8rem 1rem; border-radius:8px; color:#fff8e1; margin:0.4rem 0; }
  .alert-green  { background:#0d2b0d; border-left:4px solid #4caf50; padding:0.8rem 1rem; border-radius:8px; color:#c8e6c9; margin:0.4rem 0; }
  .alert-blue   { background:#0d1f3b; border-left:4px solid #2196f3; padding:0.8rem 1rem; border-radius:8px; color:#bbdefb; margin:0.4rem 0; }

  /* ── Learning badge ── */
  .badge-learning { background:#1a237e; color:#90caf9; padding:2px 8px; border-radius:20px; font-size:0.72rem; font-weight:600; }
  .badge-stable   { background:#1b5e20; color:#a5d6a7; padding:2px 8px; border-radius:20px; font-size:0.72rem; font-weight:600; }

  /* ── Section title ── */
  .section-title {
    font-size: 1.15rem; font-weight: 600; color: #e0e0e0;
    border-bottom: 2px solid #2196f3;
    padding-bottom: 0.4rem; margin: 1.2rem 0 0.8rem;
  }

  /* ── Action pill ── */
  .action-pill {
    display: inline-block;
    background: #1565c0; color: #fff;
    border-radius: 20px; padding: 4px 12px;
    font-size: 0.78rem; font-weight: 600;
    margin: 2px;
  }

  /* ── Streamlit overrides ── */
  .stApp { background: #12121f; }
  .stDataFrame { border-radius: 8px; }
  div[data-testid="stMetric"] { background: #1e1e2e; border-radius:8px; padding:0.6rem 1rem; }
</style>
""", unsafe_allow_html=True)


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
    """Find the row index that contains actual column headers (skip metadata rows)."""
    for i, row in raw_df.iterrows():
        vals = [str(v) for v in row.dropna().values]
        if not vals:
            continue
        # Look for typical field-name patterns: ALL_CAPS_WITH_UNDERSCORE or mixed
        caps_like = sum(1 for v in vals if v.replace("_", "").isalpha() and v == v.upper() and len(v) > 2)
        if caps_like >= 3:
            return i
        # Also accept mixed-case names that look like column headers
        header_like = sum(1 for v in vals if any(kw in v.upper() for kw in
                          ["DATE","CAMPAIGN","IMPRESSION","CLICK","SPEND","GMV","ROI","BUDGET","BRAND"]))
        if header_like >= 2:
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
        return {"action": "🚀 Scale Up", "detail": f"ROAS {roas:.2f}x — increase budget 20-30%",
                "pct_change": 25, "color": "green"}
    if roas >= 1.5:
        return {"action": "📈 Increase", "detail": f"ROAS {roas:.2f}x — increase budget 10%",
                "pct_change": 10, "color": "green"}
    if 0.8 <= roas < 1.5:
        return {"action": "👀 Monitor", "detail": f"ROAS {roas:.2f}x — optimise bids first",
                "pct_change": 0, "color": "orange"}
    if 0 < roas < 0.8:
        return {"action": "✂️ Reduce", "detail": f"ROAS {roas:.2f}x — cut budget 20%",
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

    # Re-compute KPIs on aggregated data
    tbl["ROAS"] = np.where(tbl.get("spend",pd.Series(0)) > 0,
                           tbl.get("revenue", 0) / tbl["spend"], 0) if "spend" in tbl.columns and "revenue" in tbl.columns else 0
    tbl["CTR%"] = np.where(tbl.get("impressions",pd.Series(0)) > 0,
                           tbl.get("clicks",0) / tbl["impressions"] * 100, 0) if "clicks" in tbl.columns and "impressions" in tbl.columns else 0
    tbl["CVR%"] = np.where(tbl.get("clicks",pd.Series(0)) > 0,
                           tbl.get("orders",0) / tbl["clicks"] * 100, 0) if "orders" in tbl.columns and "clicks" in tbl.columns else 0
    tbl["CAC"]  = np.where(tbl.get("orders",pd.Series(0)) > 0,
                           tbl.get("spend",0) / tbl["orders"], np.nan) if "spend" in tbl.columns and "orders" in tbl.columns else np.nan

    if "campaign" in tbl.columns:
        tbl["Type"] = tbl["campaign"].apply(classify_campaign)

    # Budget suggestion
    tbl["Suggestion"] = tbl.apply(lambda r: budget_suggestion(r)["action"], axis=1)
    tbl["Advice"]     = tbl.apply(lambda r: budget_suggestion(r)["detail"], axis=1)

    return tbl


# ─────────────────────────────────────────────────────────────────────────────
# CHART BUILDERS
# ─────────────────────────────────────────────────────────────────────────────
PLOTLY_TEMPLATE = "plotly_dark"
CHART_HEIGHT    = 320


def line_chart(df, x, y_cols, title, y_label="Value"):
    fig = go.Figure()
    colors = ["#2196f3","#4caf50","#ff9800","#e91e63","#9c27b0"]
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
                           "msg":f"⚠️ Low ROAS: <b>{row.get('campaign','?')}</b> — ROAS {row['ROAS']:.2f}x (target ≥ {roas_thresh}x)",
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
                           "msg":f"🌟 Star performer: <b>{row.get('campaign','?')}</b> — ROAS {row['ROAS']:.2f}x",
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
        "frequency": ["Weekly", "Monthly"],
    },
]

# Frequency → which KPI views make most sense
FREQUENCY_CONFIG = {
    "Daily": {
        "icon":        "🗓️",
        "focus":       ["Spend","Revenue","CTR","ROAS"],
        "description": "Track day-over-day spend pacing and ROAS. Catch budget exhaustion early.",
        "required_files": ["granular","date"],
        "optional_files": ["city","placement"],
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
    # ── Header ──────────────────────────────────────────────────────────────
    st.markdown("""
    <div class="dash-header">
      <h1>📊 Campaign Intelligence Dashboard</h1>
      <p>Upload your Instamart / Swiggy Ads CSV exports to unlock deep performance insights</p>
    </div>
    """, unsafe_allow_html=True)

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
        st.markdown("## 📅 Date Filter")
        date_filter = st.radio("Period", ["All time","Last 7 days","Last 30 days","Custom"], index=0)
        custom_start = custom_end = None
        if date_filter == "Custom":
            custom_start = st.date_input("From")
            custom_end   = st.date_input("To")

    # ── Collect all uploaded files ────────────────────────────────────────────
    # Build a flat list of (file_obj, detected_key) from named slots + auto-detect
    uploaded_pairs: list[tuple] = []
    for key, fobj in slot_files.items():
        if fobj is not None:
            uploaded_pairs.append((fobj, key))

    # ── No files yet — show onboarding ───────────────────────────────────────
    if not uploaded_pairs:
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

    # ── Load and standardize uploaded files ──────────────────────────────────
    datasets: dict[str, pd.DataFrame] = {}
    file_log = []

    with st.spinner("Loading and standardizing data …"):
        for fobj, ftype in uploaded_pairs:
            raw = load_csv(fobj)
            if raw is None:
                continue
            std = standardize(raw)
            std = learning_phase_flag(std)
            if ftype in datasets:
                datasets[ftype] = pd.concat([datasets[ftype], std], ignore_index=True)
            else:
                datasets[ftype] = std
            file_log.append({"File": fobj.name, "Type": ftype,
                             "Rows": len(std), "Cols": len(std.columns)})

    if not datasets:
        st.error("No valid data could be extracted. Please check your files.")
        return

    # ── Choose primary dataset ────────────────────────────────────────────────
    # Preference order: granular > date > summary > others
    primary_key = next((k for k in ["granular","date","summary","city","placement","product","search_query"]
                        if k in datasets), list(datasets.keys())[0])
    primary = datasets[primary_key]

    # ── Date filtering ────────────────────────────────────────────────────────
    if "date" in primary.columns and primary["date"].notna().any():
        min_d = primary["date"].min()
        max_d = primary["date"].max()
        today = pd.Timestamp.today()

        if date_filter == "Last 7 days":
            mask = primary["date"] >= today - timedelta(days=7)
        elif date_filter == "Last 30 days":
            mask = primary["date"] >= today - timedelta(days=30)
        elif date_filter == "Custom" and custom_start and custom_end:
            mask = (primary["date"] >= pd.Timestamp(custom_start)) & \
                   (primary["date"] <= pd.Timestamp(custom_end))
        else:
            mask = pd.Series(True, index=primary.index)

        primary = primary[mask]

    if primary.empty:
        st.warning("No data in selected date range. Try 'All time'.")
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

    # ── Uploaded file summary ─────────────────────────────────────────────────
    with st.expander("📋 Uploaded files detail", expanded=False):
        st.dataframe(pd.DataFrame(file_log), use_container_width=True, hide_index=True)

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
        ("ROAS",       f"{overall_roas:.2f}×", f"Revenue/Spend · Target ≥{roas_thresh}×",
         "green" if overall_roas >= roas_thresh else "red"),
        ("Total Spend",f"₹{total_spend:,.0f}", f"Across {primary.get('campaign',primary.get('campaign_id',pd.Series())).nunique() if 'campaign' in primary else '—'} campaigns","blue"),
        ("CTR",        f"{overall_ctr:.2f}%",  "Clicks / Impressions","orange"),
        ("CVR",        f"{overall_cvr:.2f}%",  "Orders / Clicks","purple"),
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
            daily = date_src.groupby("date").agg(
                spend=("spend","sum") if "spend" in date_src.columns else ("spend","first"),
                revenue=("revenue","sum") if "revenue" in date_src.columns else ("revenue","first"),
                clicks=("clicks","sum") if "clicks" in date_src.columns else ("clicks","first"),
                impressions=("impressions","sum") if "impressions" in date_src.columns else ("impressions","first"),
                orders=("orders","sum") if "orders" in date_src.columns else ("orders","first"),
            ).reset_index()
            # Recompute daily ROAS
            daily["ROAS"] = np.where(daily["spend"] > 0, daily["revenue"] / daily["spend"], 0)
            daily["CTR%"] = np.where(daily["impressions"] > 0,
                                     daily["clicks"] / daily["impressions"] * 100, 0)

            c1, c2 = st.columns(2)
            with c1:
                if daily["spend"].sum() > 0 or daily["revenue"].sum() > 0:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=daily["date"], y=daily["spend"],
                                            name="Spend", fill="tozeroy",
                                            line=dict(color="#ef5350",width=2)))
                    fig.add_trace(go.Scatter(x=daily["date"], y=daily["revenue"],
                                            name="Revenue", fill="tozeroy",
                                            line=dict(color="#66bb6a",width=2)))
                    fig.update_layout(template=PLOTLY_TEMPLATE, title="Spend vs Revenue (Daily)",
                                      height=CHART_HEIGHT, yaxis_title="₹")
                    st.plotly_chart(fig, use_container_width=True)

            with c2:
                if daily["ROAS"].sum() > 0:
                    fig2 = go.Figure()
                    fig2.add_trace(go.Scatter(x=daily["date"], y=daily["ROAS"],
                                             mode="lines+markers", name="ROAS",
                                             line=dict(color="#42a5f5",width=2.5),
                                             marker=dict(size=6)))
                    fig2.add_hline(y=roas_thresh, line_dash="dash", line_color="#ffc107",
                                  annotation_text=f"Target {roas_thresh}×")
                    fig2.add_hline(y=1.0, line_dash="dot", line_color="#ef5350",
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
                if "Scale" in str(val) or "Increase" in str(val): return "color: #66bb6a; font-weight:600"
                if "Pause" in str(val) or "Reduce" in str(val):   return "color: #ef5350; font-weight:600"
                if "Wait"  in str(val):                            return "color: #42a5f5"
                return "color: #ffa726"

            display_cols = [c for c in ["campaign","Type","phase","spend","revenue","ROAS","CTR%","CVR%","CAC","orders","Suggestion","Advice"]
                           if c in camp_tbl.columns]
            st.dataframe(
                camp_tbl[display_cols].style.applymap(colour_suggestion, subset=["Suggestion"] if "Suggestion" in display_cols else []),
                use_container_width=True, hide_index=True
            )

            # ROAS scatter
            if "ROAS" in camp_tbl.columns and "spend" in camp_tbl.columns:
                fig_s = scatter_chart(
                    camp_tbl, x="spend", y="ROAS",
                    size="revenue" if "revenue" in camp_tbl.columns else None,
                    color="ROAS", text="campaign" if "campaign" in camp_tbl.columns else None,
                    title="Campaign Bubble: Spend vs ROAS (size = Revenue)"
                )
                fig_s.add_hline(y=roas_thresh, line_dash="dash", line_color="#ffc107",
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
                roas_str = f"{row.get('ROAS',0):.2f}×" if "ROAS" in row else "—"
                action_col = {"green":"#66bb6a","red":"#ef5350","orange":"#ffa726","blue":"#42a5f5"}.get(sug["color"],"#fff")
                st.markdown(f"""
                <div style="background:#1e1e2e;border-radius:10px;padding:0.9rem 1.2rem;
                            border-left:4px solid {action_col};margin:0.5rem 0;">
                  <b style="color:#e0e0e0">{row.get('campaign','Campaign')}</b>
                  &nbsp;{phase_badge}&nbsp;
                  <span style="color:#aaa;font-size:0.82rem">ROAS {roas_str} | Spend ₹{spend:,.0f}</span><br>
                  <span style="color:{action_col};font-weight:700;font-size:1.05rem">{sug['action']}</span>
                  &nbsp;&nbsp;<span style="color:#bbb;font-size:0.85rem">{sug['detail']}</span>
                  {f'<br><span style="color:#aaa;font-size:0.8rem">→ Suggested budget: ₹{new_spend:,.0f}</span>' if new_spend != spend else ''}
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

            kw["ROAS"] = np.where(kw.get("spend",pd.Series(0)) > 0,
                                  kw.get("revenue",0) / kw["spend"], 0) if "spend" in kw.columns and "revenue" in kw.columns else 0
            kw["CTR%"] = np.where(kw.get("impressions",pd.Series(0)) > 0,
                                  kw.get("clicks",0) / kw["impressions"] * 100, 0) if "clicks" in kw.columns and "impressions" in kw.columns else 0
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
            st.dataframe(kw.sort_values("ROAS", ascending=False).head(100),
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
            cities["ROAS"] = np.where(cities.get("spend",pd.Series(0)) > 0,
                                      cities.get("revenue",0) / cities["spend"], 0) if "spend" in cities.columns and "revenue" in cities.columns else 0
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

            st.dataframe(cities.head(50), use_container_width=True, hide_index=True)
        else:
            st.info("No city data found. Upload IM_CAMPAIGN_X_CITY or IM_GRANULAR files.")

    # ── TAB 6: PRODUCTS ──────────────────────────────────────────────────────
    with tabs[5]:
        st.markdown('<div class="section-title">Product Performance</div>', unsafe_allow_html=True)

        prod_src = datasets.get("product", datasets.get("granular"))
        if prod_src is not None and "product" in prod_src.columns:
            prod_agg = {c:"sum" for c in ["spend","revenue","clicks","orders"] if c in prod_src.columns}
            prods = prod_src.groupby("product").agg(prod_agg).reset_index()
            prods["ROAS"] = np.where(prods.get("spend",pd.Series(0)) > 0,
                                     prods.get("revenue",0) / prods["spend"], 0) if "spend" in prods.columns and "revenue" in prods.columns else 0

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

            st.dataframe(prods.sort_values("ROAS",ascending=False), use_container_width=True, hide_index=True)
        else:
            st.info("No product data found. Upload IM_CAMPAIGN_X_PRODUCT or IM_GRANULAR files.")

    # ── TAB 7: SEARCH QUERIES ─────────────────────────────────────────────────
    with tabs[6]:
        st.markdown('<div class="section-title">Search Query Analysis</div>', unsafe_allow_html=True)

        sq_src = datasets.get("search_query")
        if sq_src is not None and "search_query" in sq_src.columns:
            sq_agg = {c:"sum" for c in ["spend","revenue","clicks","impressions","orders"] if c in sq_src.columns}
            sq = sq_src.groupby("search_query").agg(sq_agg).reset_index()
            sq["ROAS"] = np.where(sq.get("spend",pd.Series(0)) > 0,
                                  sq.get("revenue",0) / sq["spend"], 0) if "spend" in sq.columns and "revenue" in sq.columns else 0
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
                    st.dataframe(missed[["search_query","clicks","spend"]],
                                use_container_width=True, hide_index=True)

            st.dataframe(sq.sort_values("ROAS",ascending=False).head(200),
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
        st.dataframe(raw_view[col_filter].head(500), use_container_width=True, hide_index=True)

        # Download
        csv_bytes = raw_view.to_csv(index=False).encode()
        st.download_button("⬇️ Download as CSV", csv_bytes,
                          file_name=f"{src_choice}_standardized.csv", mime="text/csv")

    # ── FOOTER ───────────────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '<div style="text-align:center;color:#555;font-size:0.78rem;">'
        '📊 Campaign Intelligence Dashboard · Built for Instamart Ads · '
        'Data is processed locally in your browser session'
        '</div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
