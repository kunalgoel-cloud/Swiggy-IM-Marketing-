import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from io import BytesIO
import hashlib

# Page configuration
st.set_page_config(
    page_title="Campaign Optimization Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .trend-positive { color: #4caf50; font-weight: bold; }
    .trend-negative { color: #f44336; font-weight: bold; }
    .trend-neutral { color: #ff9800; font-weight: bold; }
    .data-age { 
        background-color: #e3f2fd; 
        padding: 0.5rem; 
        border-radius: 0.3rem; 
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# HISTORICAL DATA MANAGEMENT
# ============================================================================

class HistoryManager:
    """Manages historical campaign data storage and retrieval"""
    
    def __init__(self, storage_dir="campaign_history"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        self.index_file = self.storage_dir / "index.json"
        self._load_index()
    
    def _load_index(self):
        """Load the index of all stored data"""
        if self.index_file.exists():
            with open(self.index_file, 'r') as f:
                self.index = json.load(f)
        else:
            self.index = {"uploads": []}
    
    def _save_index(self):
        """Save the index"""
        with open(self.index_file, 'w') as f:
            json.dump(self.index, f, indent=2)
    
    def _get_file_hash(self, df):
        """Generate hash of dataframe to detect duplicates"""
        return hashlib.md5(pd.util.hash_pandas_object(df).values).hexdigest()
    
    def save_data(self, df, filename, user_label=None):
        """Save campaign data with metadata"""
        timestamp = datetime.now().isoformat()
        file_hash = self._get_file_hash(df)
        
        # Check for duplicates
        for upload in self.index["uploads"]:
            if upload.get("file_hash") == file_hash:
                st.warning("⚠️ This data was already uploaded on " + upload["timestamp"][:10])
                return upload["id"]
        
        # Generate unique ID
        upload_id = f"upload_{len(self.index['uploads']) + 1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Save data
        data_file = self.storage_dir / f"{upload_id}.parquet"
        df.to_parquet(data_file)
        
        # Calculate metrics for quick reference
        metrics = {
            'total_spend': float(df['TOTAL_BUDGET_BURNT'].sum()),
            'total_gmv': float(df['TOTAL_GMV'].sum()),
            'avg_roi': float(df['TOTAL_ROI'].mean()),
            'total_conversions': float(df['TOTAL_CONVERSIONS'].sum()),
            'avg_ctr': float(df['TOTAL_CTR'].mean()),
            'row_count': len(df)
        }
        
        # Add to index
        upload_entry = {
            'id': upload_id,
            'timestamp': timestamp,
            'filename': filename,
            'user_label': user_label or f"Upload {len(self.index['uploads']) + 1}",
            'metrics': metrics,
            'file_hash': file_hash
        }
        
        self.index["uploads"].append(upload_entry)
        self._save_index()
        
        return upload_id
    
    def get_all_uploads(self):
        """Get list of all uploads with metadata"""
        return self.index["uploads"]
    
    def get_data(self, upload_id):
        """Load data for specific upload"""
        data_file = self.storage_dir / f"{upload_id}.parquet"
        if data_file.exists():
            return pd.read_parquet(data_file)
        return None
    
    def get_historical_data(self, limit=None):
        """Get all historical data sorted by date"""
        uploads = sorted(self.index["uploads"], key=lambda x: x['timestamp'])
        if limit:
            uploads = uploads[-limit:]
        
        historical_data = []
        for upload in uploads:
            df = self.get_data(upload['id'])
            if df is not None:
                historical_data.append({
                    'date': upload['timestamp'][:10],
                    'label': upload['user_label'],
                    'data': df,
                    'metrics': upload['metrics']
                })
        
        return historical_data
    
    def get_trend_data(self):
        """Get metrics over time for trend analysis"""
        uploads = sorted(self.index["uploads"], key=lambda x: x['timestamp'])
        
        trend_data = {
            'dates': [],
            'labels': [],
            'spend': [],
            'gmv': [],
            'roi': [],
            'conversions': [],
            'ctr': []
        }
        
        for upload in uploads:
            trend_data['dates'].append(upload['timestamp'][:10])
            trend_data['labels'].append(upload['user_label'])
            trend_data['spend'].append(upload['metrics']['total_spend'])
            trend_data['gmv'].append(upload['metrics']['total_gmv'])
            trend_data['roi'].append(upload['metrics']['avg_roi'])
            trend_data['conversions'].append(upload['metrics']['total_conversions'])
            trend_data['ctr'].append(upload['metrics']['avg_ctr'] * 100)
        
        return pd.DataFrame(trend_data)
    
    def delete_upload(self, upload_id):
        """Delete a specific upload"""
        # Remove file
        data_file = self.storage_dir / f"{upload_id}.parquet"
        if data_file.exists():
            data_file.unlink()
        
        # Remove from index
        self.index["uploads"] = [u for u in self.index["uploads"] if u['id'] != upload_id]
        self._save_index()
    
    def get_campaign_maturity(self, df):
        """Determine if campaigns are new or mature based on data"""
        # Check if we have METRICS_DATE column
        if 'METRICS_DATE' in df.columns:
            dates = pd.to_datetime(df['METRICS_DATE'], errors='coerce')
            date_range = (dates.max() - dates.min()).days if len(dates) > 0 else 0
        else:
            date_range = 0
        
        # Check total impressions and conversions
        total_impressions = df['TOTAL_IMPRESSIONS'].sum()
        total_conversions = df['TOTAL_CONVERSIONS'].sum()
        
        if date_range < 3 or total_impressions < 1000 or total_conversions < 10:
            return "NEW", "⚠️ New campaigns - wait 3-7 days for sufficient data"
        elif date_range < 7 or total_conversions < 30:
            return "EARLY", "🟡 Early stage - proceed with caution"
        else:
            return "MATURE", "✅ Sufficient data for optimization"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

@st.cache_data
def load_campaign_data(file):
    """Load and process campaign data"""
    try:
        df = pd.read_csv(file, skiprows=6)
        
        # Clean and convert data types
        df['TOTAL_ROI'] = pd.to_numeric(df['TOTAL_ROI'], errors='coerce')
        df['TOTAL_CTR'] = df['TOTAL_CTR'].str.rstrip('%').astype('float') / 100.0
        df['A2C_RATE'] = df['A2C_RATE'].str.rstrip('%').astype('float') / 100.0
        df['TOTAL_BUDGET_BURNT'] = pd.to_numeric(df['TOTAL_BUDGET_BURNT'], errors='coerce')
        df['TOTAL_CLICKS'] = pd.to_numeric(df['TOTAL_CLICKS'], errors='coerce')
        df['TOTAL_IMPRESSIONS'] = pd.to_numeric(df['TOTAL_IMPRESSIONS'], errors='coerce')
        df['TOTAL_GMV'] = pd.to_numeric(df['TOTAL_GMV'], errors='coerce')
        df['TOTAL_CONVERSIONS'] = pd.to_numeric(df['TOTAL_CONVERSIONS'], errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

def calculate_metrics(df):
    """Calculate key performance metrics"""
    total_conversions = df['TOTAL_CONVERSIONS'].sum()
    return {
        'total_spend': df['TOTAL_BUDGET_BURNT'].sum(),
        'total_gmv': df['TOTAL_GMV'].sum(),
        'total_conversions': total_conversions,
        'avg_roi': df['TOTAL_ROI'].mean(),
        'avg_ctr': df['TOTAL_CTR'].mean(),
        'avg_a2c': df['A2C_RATE'].mean(),
        'cost_per_conversion': df['TOTAL_BUDGET_BURNT'].sum() / total_conversions if total_conversions > 0 else 0
    }

def analyze_keywords(df, min_roi=0.5, min_ctr=0.02):
    """Analyze keyword performance"""
    keyword_perf = df.groupby('KEYWORD').agg({
        'TOTAL_IMPRESSIONS': 'sum',
        'TOTAL_CLICKS': 'sum',
        'TOTAL_BUDGET_BURNT': 'sum',
        'TOTAL_CONVERSIONS': 'sum',
        'TOTAL_GMV': 'sum',
        'TOTAL_ROI': 'mean',
        'TOTAL_CTR': 'mean',
        'A2C_RATE': 'mean'
    }).reset_index()
    
    keyword_perf['CTR'] = keyword_perf['TOTAL_CLICKS'] / keyword_perf['TOTAL_IMPRESSIONS']
    keyword_perf['Conversion_Rate'] = keyword_perf['TOTAL_CONVERSIONS'] / keyword_perf['TOTAL_CLICKS']
    
    keyword_perf['Status'] = keyword_perf.apply(lambda x: 
        '🟢 Scale Up' if x['TOTAL_ROI'] > 2.0 and x['TOTAL_CONVERSIONS'] > 0
        else ('🔴 Pause' if x['TOTAL_ROI'] < min_roi or x['CTR'] < min_ctr
        else '🟡 Monitor'), axis=1)
    
    return keyword_perf.sort_values('TOTAL_ROI', ascending=False)

def compare_periods(current_metrics, previous_metrics):
    """Compare current vs previous period metrics"""
    if not previous_metrics:
        return None
    
    comparison = {}
    for key in current_metrics:
        if key in previous_metrics and previous_metrics[key] != 0:
            change = ((current_metrics[key] - previous_metrics[key]) / previous_metrics[key]) * 100
            comparison[key] = {
                'current': current_metrics[key],
                'previous': previous_metrics[key],
                'change_pct': change,
                'change_abs': current_metrics[key] - previous_metrics[key]
            }
    
    return comparison

# ============================================================================
# INITIALIZE SESSION STATE
# ============================================================================

if 'history_manager' not in st.session_state:
    st.session_state.history_manager = HistoryManager()

if 'current_data' not in st.session_state:
    st.session_state.current_data = None

# ============================================================================
# MAIN APP
# ============================================================================

st.markdown('<div class="main-header">📊 Campaign Optimization Dashboard with Trend Analysis</div>', unsafe_allow_html=True)
st.markdown("---")

# Sidebar
with st.sidebar:
    st.header("📁 Data Upload")
    
    uploaded_file = st.file_uploader(
        "Upload Campaign Data (CSV)",
        type=['csv'],
        help="Upload IM_GRANULAR_*.csv file"
    )
    
    if uploaded_file:
        user_label = st.text_input(
            "Label this upload (optional)",
            placeholder=f"e.g., Week 1, March 13, After optimization",
            help="Give this data a memorable name for tracking"
        )
        
        if st.button("📊 Analyze & Save to History", type="primary"):
            with st.spinner("Loading and saving data..."):
                df = load_campaign_data(uploaded_file)
                if df is not None:
                    # Save to history
                    upload_id = st.session_state.history_manager.save_data(
                        df, 
                        uploaded_file.name,
                        user_label
                    )
                    st.session_state.current_data = df
                    st.session_state.current_upload_id = upload_id
                    st.success("✅ Data saved to history!")
                    st.rerun()
    
    st.markdown("---")
    st.header("📜 History")
    
    uploads = st.session_state.history_manager.get_all_uploads()
    
    if uploads:
        st.write(f"**Total uploads:** {len(uploads)}")
        
        # Show recent uploads
        for upload in sorted(uploads, key=lambda x: x['timestamp'], reverse=True)[:5]:
            with st.expander(f"📅 {upload['user_label']}", expanded=False):
                st.write(f"**Date:** {upload['timestamp'][:10]}")
                st.write(f"**ROI:** {upload['metrics']['avg_roi']:.2f}x")
                st.write(f"**Spend:** ₹{upload['metrics']['total_spend']:,.0f}")
                
                if st.button(f"Load", key=f"load_{upload['id']}"):
                    st.session_state.current_data = st.session_state.history_manager.get_data(upload['id'])
                    st.session_state.current_upload_id = upload['id']
                    st.rerun()
    else:
        st.info("No historical data yet. Upload your first file above!")
    
    st.markdown("---")
    st.header("⚙️ Settings")
    
    min_roi = st.slider("Minimum ROI Threshold", 0.0, 2.0, 0.5, 0.1)
    min_ctr = st.slider("Minimum CTR (%)", 0.0, 10.0, 2.0, 0.5) / 100
    
    # Campaign maturity override
    st.markdown("---")
    st.subheader("🕐 Data Maturity")
    force_mature = st.checkbox(
        "Override - treat as mature data",
        help="Check this if you know your campaigns have sufficient data"
    )

# ============================================================================
# MAIN CONTENT
# ============================================================================

if st.session_state.current_data is not None:
    df = st.session_state.current_data
    metrics = calculate_metrics(df)
    
    # Check campaign maturity
    if not force_mature:
        maturity_level, maturity_msg = st.session_state.history_manager.get_campaign_maturity(df)
    else:
        maturity_level = "MATURE"
        maturity_msg = "✅ Treated as mature (override enabled)"
    
    # Show maturity warning
    if maturity_level == "NEW":
        st.warning(f"""
        ### ⚠️ NEW CAMPAIGNS DETECTED
        
        {maturity_msg}
        
        **Recommendation:** Wait 3-7 days before making major changes. Use this dashboard to:
        - Monitor daily performance trends
        - Identify early signals (good or bad)
        - Watch for obvious issues (0% CTR, technical problems)
        
        **DO NOT:** Make aggressive optimizations yet - let the data mature!
        """)
    elif maturity_level == "EARLY":
        st.info(f"""
        ### 🟡 EARLY STAGE CAMPAIGNS
        
        {maturity_msg}
        
        **Recommendation:** You can make cautious optimizations:
        - Pause obvious non-performers (0% CTR, technical issues)
        - Don't scale up yet - wait for more data
        - Continue monitoring daily
        """)
    
    # ========================================================================
    # TREND ANALYSIS (if historical data exists)
    # ========================================================================
    
    trend_df = st.session_state.history_manager.get_trend_data()
    
    if len(trend_df) > 1:
        st.header("📈 Performance Trends")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # ROI Trend
            fig_roi = go.Figure()
            fig_roi.add_trace(go.Scatter(
                x=trend_df['dates'],
                y=trend_df['roi'],
                mode='lines+markers',
                name='ROI',
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=10)
            ))
            fig_roi.add_hline(y=2.0, line_dash="dash", line_color="green", 
                             annotation_text="Target: 2.0x")
            fig_roi.add_hline(y=1.0, line_dash="dash", line_color="orange", 
                             annotation_text="Break-even: 1.0x")
            fig_roi.update_layout(
                title="ROI Trend Over Time",
                xaxis_title="Date",
                yaxis_title="ROI (x)",
                height=300
            )
            st.plotly_chart(fig_roi, use_container_width=True)
        
        with col2:
            # CTR Trend
            fig_ctr = go.Figure()
            fig_ctr.add_trace(go.Scatter(
                x=trend_df['dates'],
                y=trend_df['ctr'],
                mode='lines+markers',
                name='CTR',
                line=dict(color='#ff7f0e', width=3),
                marker=dict(size=10)
            ))
            fig_ctr.add_hline(y=5.0, line_dash="dash", line_color="green", 
                             annotation_text="Target: 5%")
            fig_ctr.update_layout(
                title="CTR Trend Over Time",
                xaxis_title="Date",
                yaxis_title="CTR (%)",
                height=300
            )
            st.plotly_chart(fig_ctr, use_container_width=True)
        
        # Spend vs GMV Trend
        fig_spend = go.Figure()
        fig_spend.add_trace(go.Bar(
            x=trend_df['dates'],
            y=trend_df['spend'],
            name='Spend',
            marker_color='#ff6b6b'
        ))
        fig_spend.add_trace(go.Bar(
            x=trend_df['dates'],
            y=trend_df['gmv'],
            name='GMV',
            marker_color='#4caf50'
        ))
        fig_spend.update_layout(
            title="Spend vs GMV Over Time",
            xaxis_title="Date",
            yaxis_title="Amount (₹)",
            barmode='group',
            height=350
        )
        st.plotly_chart(fig_spend, use_container_width=True)
        
        # Period Comparison
        if len(trend_df) >= 2:
            st.subheader("📊 Period-over-Period Comparison")
            
            current_idx = len(trend_df) - 1
            previous_idx = len(trend_df) - 2
            
            current_metrics = trend_df.iloc[current_idx]
            previous_metrics = trend_df.iloc[previous_idx]
            
            col1, col2, col3, col4 = st.columns(4)
            
            def format_change(current, previous, is_percentage=False):
                change = ((current - previous) / previous * 100) if previous != 0 else 0
                arrow = "↑" if change > 0 else "↓" if change < 0 else "→"
                color = "trend-positive" if change > 0 else "trend-negative" if change < 0 else "trend-neutral"
                return f'<span class="{color}">{arrow} {abs(change):.1f}%</span>'
            
            with col1:
                st.metric(
                    "ROI Change",
                    f"{current_metrics['roi']:.2f}x",
                    delta=f"{current_metrics['roi'] - previous_metrics['roi']:.2f}x",
                    delta_color="normal"
                )
            
            with col2:
                st.metric(
                    "Spend Change",
                    f"₹{current_metrics['spend']:,.0f}",
                    delta=f"₹{current_metrics['spend'] - previous_metrics['spend']:,.0f}",
                    delta_color="inverse"
                )
            
            with col3:
                st.metric(
                    "GMV Change",
                    f"₹{current_metrics['gmv']:,.0f}",
                    delta=f"₹{current_metrics['gmv'] - previous_metrics['gmv']:,.0f}",
                    delta_color="normal"
                )
            
            with col4:
                st.metric(
                    "CTR Change",
                    f"{current_metrics['ctr']:.2f}%",
                    delta=f"{current_metrics['ctr'] - previous_metrics['ctr']:.2f}%",
                    delta_color="normal"
                )
        
        st.markdown("---")
    
    # ========================================================================
    # CURRENT SNAPSHOT
    # ========================================================================
    
    st.header("📸 Current Snapshot")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Spend", f"₹{metrics['total_spend']:,.0f}")
    
    with col2:
        st.metric(
            "Average ROI",
            f"{metrics['avg_roi']:.2f}x",
            delta=f"{((metrics['avg_roi'] - 2.0) / 2.0 * 100):.0f}% vs target",
            delta_color="normal" if metrics['avg_roi'] >= 2.0 else "inverse"
        )
    
    with col3:
        st.metric("Total GMV", f"₹{metrics['total_gmv']:,.0f}")
    
    with col4:
        st.metric(
            "Cost/Conv",
            f"₹{metrics['cost_per_conversion']:.2f}",
            delta=f"{((50 - metrics['cost_per_conversion']) / 50 * 100):.0f}% vs target",
            delta_color="normal" if metrics['cost_per_conversion'] <= 50 else "inverse"
        )
    
    # ========================================================================
    # RECOMMENDATIONS (adjusted based on maturity)
    # ========================================================================
    
    st.header("🎯 Recommendations")
    
    keyword_analysis = analyze_keywords(df, min_roi, min_ctr)
    
    if maturity_level == "NEW":
        st.warning("""
        ### ⏳ Too Early for Major Changes
        
        Your campaigns are too new for aggressive optimization. Here's what to watch:
        """)
        
        # Show monitoring metrics only
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("✅ Early Positive Signals")
            good_signs = keyword_analysis[keyword_analysis['TOTAL_ROI'] > 1.0].head(5)
            if len(good_signs) > 0:
                for _, row in good_signs.iterrows():
                    st.write(f"✓ **{row['KEYWORD']}** - ROI: {row['TOTAL_ROI']:.2f}x")
            else:
                st.info("Keep monitoring - wait for more data")
        
        with col2:
            st.subheader("⚠️ Watch These Closely")
            concerns = keyword_analysis[
                (keyword_analysis['TOTAL_IMPRESSIONS'] > 100) & 
                (keyword_analysis['TOTAL_CLICKS'] == 0)
            ]
            if len(concerns) > 0:
                for _, row in concerns.head(5).iterrows():
                    st.write(f"⚠️ **{row['KEYWORD']}** - {row['TOTAL_IMPRESSIONS']:.0f} impr, 0 clicks")
            else:
                st.success("No obvious issues detected")
    
    elif maturity_level == "EARLY":
        st.info("""
        ### 🟡 Cautious Optimization Phase
        
        You have some data, but proceed carefully:
        """)
        
        # Only show obvious pause candidates
        obvious_failures = keyword_analysis[
            (keyword_analysis['TOTAL_IMPRESSIONS'] > 500) & 
            ((keyword_analysis['TOTAL_CLICKS'] == 0) | (keyword_analysis['TOTAL_ROI'] < 0.2))
        ]
        
        if len(obvious_failures) > 0:
            st.subheader("❌ Consider Pausing (Obvious Issues)")
            st.dataframe(
                obvious_failures[['KEYWORD', 'TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'TOTAL_ROI', 'TOTAL_BUDGET_BURNT']].head(10),
                use_container_width=True
            )
            st.caption(f"Potential savings: ₹{obvious_failures['TOTAL_BUDGET_BURNT'].sum():,.2f}/day")
        
        st.subheader("📊 Top Performers (Monitor, Don't Scale Yet)")
        top_performers = keyword_analysis[keyword_analysis['TOTAL_ROI'] > 2.0].head(5)
        if len(top_performers) > 0:
            st.dataframe(
                top_performers[['KEYWORD', 'TOTAL_ROI', 'TOTAL_CONVERSIONS', 'TOTAL_BUDGET_BURNT']],
                use_container_width=True
            )
    
    else:  # MATURE
        st.success("""
        ### ✅ Ready for Full Optimization
        
        You have sufficient data for confident decisions:
        """)
        
        # Full recommendations
        pause_keywords = keyword_analysis[keyword_analysis['Status'] == '🔴 Pause']
        scale_keywords = keyword_analysis[keyword_analysis['Status'] == '🟢 Scale Up']
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader(f"🔴 Pause {len(pause_keywords)} Keywords")
            if len(pause_keywords) > 0:
                st.dataframe(
                    pause_keywords[['KEYWORD', 'TOTAL_ROI', 'CTR', 'TOTAL_BUDGET_BURNT']].head(10),
                    use_container_width=True
                )
                st.caption(f"**Potential savings:** ₹{pause_keywords['TOTAL_BUDGET_BURNT'].sum():,.2f}/day")
        
        with col2:
            st.subheader(f"🟢 Scale {len(scale_keywords)} Keywords")
            if len(scale_keywords) > 0:
                st.dataframe(
                    scale_keywords[['KEYWORD', 'TOTAL_ROI', 'TOTAL_CONVERSIONS', 'TOTAL_GMV']].head(10),
                    use_container_width=True
                )
                st.caption(f"**Avg ROI:** {scale_keywords['TOTAL_ROI'].mean():.2f}x")
    
    # ========================================================================
    # DETAILED ANALYSIS TABS
    # ========================================================================
    
    st.markdown("---")
    
    tab1, tab2, tab3 = st.tabs(["🔑 All Keywords", "📊 Charts", "📥 Export"])
    
    with tab1:
        st.subheader("Complete Keyword Analysis")
        
        status_filter = st.multiselect(
            "Filter by Status",
            options=['🟢 Scale Up', '🟡 Monitor', '🔴 Pause'],
            default=['🟢 Scale Up', '🟡 Monitor', '🔴 Pause']
        )
        
        filtered = keyword_analysis[keyword_analysis['Status'].isin(status_filter)]
        
        st.dataframe(
            filtered[['KEYWORD', 'Status', 'TOTAL_ROI', 'CTR', 'TOTAL_CONVERSIONS', 
                     'TOTAL_BUDGET_BURNT', 'TOTAL_GMV']].style.format({
                'TOTAL_ROI': '{:.2f}x',
                'CTR': '{:.2%}',
                'TOTAL_BUDGET_BURNT': '₹{:,.2f}',
                'TOTAL_GMV': '₹{:,.2f}',
                'TOTAL_CONVERSIONS': '{:.0f}'
            }),
            use_container_width=True,
            height=400
        )
    
    with tab2:
        st.subheader("Performance Distribution")
        
        fig = px.scatter(
            keyword_analysis,
            x='TOTAL_BUDGET_BURNT',
            y='TOTAL_ROI',
            size='TOTAL_CONVERSIONS',
            color='Status',
            hover_data=['KEYWORD'],
            title='Keyword Performance Matrix',
            color_discrete_map={
                '🟢 Scale Up': 'green',
                '🟡 Monitor': 'orange',
                '🔴 Pause': 'red'
            }
        )
        fig.add_hline(y=min_roi, line_dash="dash", annotation_text=f"Min ROI: {min_roi}x")
        st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("Export Reports")
        
        # Generate report
        report = f"""
CAMPAIGN PERFORMANCE REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Data Maturity: {maturity_level}
{'='*80}

CURRENT METRICS
---------------
Total Spend: ₹{metrics['total_spend']:,.2f}
Total GMV: ₹{metrics['total_gmv']:,.2f}
ROI: {metrics['avg_roi']:.2f}x
Conversions: {metrics['total_conversions']:.0f}
CTR: {metrics['avg_ctr']*100:.2f}%

HISTORICAL UPLOADS: {len(uploads)}
"""
        
        if len(trend_df) > 1:
            report += f"\nTREND SUMMARY\n-------------\n"
            report += f"First Upload: {trend_df.iloc[0]['dates']} - ROI: {trend_df.iloc[0]['roi']:.2f}x\n"
            report += f"Latest Upload: {trend_df.iloc[-1]['dates']} - ROI: {trend_df.iloc[-1]['roi']:.2f}x\n"
            report += f"Change: {((trend_df.iloc[-1]['roi'] - trend_df.iloc[0]['roi']) / trend_df.iloc[0]['roi'] * 100):.1f}%\n"
        
        st.download_button(
            "📄 Download Report",
            report,
            f"campaign_report_{datetime.now().strftime('%Y%m%d')}.txt",
            "text/plain"
        )
        
        # Excel export
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            keyword_analysis.to_excel(writer, sheet_name='Keywords', index=False)
            if len(trend_df) > 0:
                trend_df.to_excel(writer, sheet_name='Trends', index=False)
        
        output.seek(0)
        st.download_button(
            "📊 Download Excel",
            output,
            f"campaign_analysis_{datetime.now().strftime('%Y%m%d')}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

else:
    st.info("""
    ### 👋 Welcome to Campaign Optimization Dashboard
    
    **New Feature: Automatic Historical Tracking!** 📈
    
    This dashboard now:
    - ✅ Automatically saves every upload
    - ✅ Tracks performance trends over time
    - ✅ Compares period-over-period
    - ✅ Warns about new campaigns (wait for data!)
    - ✅ Shows trend lines and improvement
    
    **Get Started:**
    1. Upload your first CSV file
    2. Give it a label (e.g., "Week 1", "Before optimization")
    3. The system will track all future uploads automatically
    4. Come back daily/weekly to see trends!
    
    **For New Campaigns:**
    - The system will detect if your data is too new
    - You'll see warnings about waiting 3-7 days
    - Recommendations will be conservative until data matures
    
    👈 Upload your data in the sidebar to begin!
    """)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 1rem;'>
    Campaign Optimization Dashboard v2.0 with Historical Tracking | 
    Data is automatically saved and tracked over time
</div>
""", unsafe_allow_html=True)
