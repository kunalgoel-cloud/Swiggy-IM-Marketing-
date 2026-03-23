import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from io import BytesIO
import hashlib

# PostgreSQL imports
import psycopg2
from psycopg2.extras import Json
from psycopg2.pool import SimpleConnectionPool
import pickle


# Page configuration
st.set_page_config(
    page_title="Campaign Intelligence Dashboard",
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
        background: linear-gradient(90deg, #1f77b4, #2ecc71);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-positive { color: #4caf50; font-weight: bold; }
    .metric-negative { color: #f44336; font-weight: bold; }
    .metric-neutral { color: #ff9800; font-weight: bold; }
    .insight-box {
        background-color: #e3f2fd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    .alert-critical {
        background-color: #ffebee;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #f44336;
    }
    .alert-success {
        background-color: #e8f5e9;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #4caf50;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# ENHANCED HISTORY MANAGER - NOW HANDLES 3 FILES
# ============================================================================


# ============================================================================
# DATABASE MANAGER - NEON POSTGRESQL
# This replaces the old HistoryManager with persistent database storage
# ============================================================================

class DatabaseManager:
    """Manages all database operations with Neon PostgreSQL"""
    
    def __init__(self, connection_string=None):
        """Initialize database connection"""
        if connection_string is None:
            try:
                connection_string = st.secrets.get("DATABASE_URL")
            except:
                try:
                    import os
                    connection_string = os.environ.get("DATABASE_URL")
                except:
                    connection_string = None
        
        self.connection_string = connection_string
        self.pool = None
        
        if self.connection_string:
            try:
                self.pool = SimpleConnectionPool(1, 10, self.connection_string)
                self._init_database()
            except Exception as e:
                st.error(f"Database connection failed: {str(e)}")
    
    def _get_connection(self):
        """Get connection from pool"""
        if self.pool:
            return self.pool.getconn()
        return None
    
    def _return_connection(self, conn):
        """Return connection to pool"""
        if self.pool and conn:
            self.pool.putconn(conn)
    
    def _init_database(self):
        """Initialize database schema"""
        conn = self._get_connection()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS campaign_uploads (
                    upload_id VARCHAR(255) PRIMARY KEY,
                    upload_date TIMESTAMP NOT NULL,
                    user_label VARCHAR(255),
                    has_placement BOOLEAN DEFAULT FALSE,
                    has_search BOOLEAN DEFAULT FALSE,
                    file_hash VARCHAR(64),
                    metrics JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS campaign_granular (
                    id SERIAL PRIMARY KEY,
                    upload_id VARCHAR(255) REFERENCES campaign_uploads(upload_id) ON DELETE CASCADE,
                    data BYTEA,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS campaign_placement (
                    id SERIAL PRIMARY KEY,
                    upload_id VARCHAR(255) REFERENCES campaign_uploads(upload_id) ON DELETE CASCADE,
                    data BYTEA,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS campaign_search (
                    id SERIAL PRIMARY KEY,
                    upload_id VARCHAR(255) REFERENCES campaign_uploads(upload_id) ON DELETE CASCADE,
                    data BYTEA,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_upload_date ON campaign_uploads(upload_date DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_upload_id_granular ON campaign_granular(upload_id)")
            
            conn.commit()
            cursor.close()
        except Exception as e:
            conn.rollback()
            st.error(f"Database initialization error: {str(e)}")
        finally:
            self._return_connection(conn)
    
    def save_upload(self, granular_df, placement_df=None, search_df=None, user_label=None):
        """Save upload to database"""
        conn = self._get_connection()
        if not conn:
            st.error("No database connection available")
            return None
        
        try:
            cursor = conn.cursor()
            timestamp = datetime.now()
            upload_id = f"upload_{timestamp.strftime('%Y%m%d_%H%M%S')}"
            file_hash = hashlib.md5(pd.util.hash_pandas_object(granular_df).values).hexdigest()
            
            cursor.execute("SELECT upload_id, user_label, upload_date FROM campaign_uploads WHERE file_hash = %s", (file_hash,))
            duplicate = cursor.fetchone()
            if duplicate:
                st.warning(f"⚠️ Data already uploaded on {duplicate[2].strftime('%Y-%m-%d')} as '{duplicate[1]}'")
                self._return_connection(conn)
                return duplicate[0]
            
            metrics = self._calculate_comprehensive_metrics(granular_df, placement_df, search_df)
            
            cursor.execute("""
                INSERT INTO campaign_uploads 
                (upload_id, upload_date, user_label, has_placement, has_search, file_hash, metrics)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (upload_id, timestamp, user_label or f"Upload {timestamp.strftime('%Y-%m-%d')}",
                  placement_df is not None, search_df is not None, file_hash, Json(metrics)))
            
            cursor.execute("INSERT INTO campaign_granular (upload_id, data) VALUES (%s, %s)",
                          (upload_id, psycopg2.Binary(pickle.dumps(granular_df))))
            
            if placement_df is not None:
                cursor.execute("INSERT INTO campaign_placement (upload_id, data) VALUES (%s, %s)",
                              (upload_id, psycopg2.Binary(pickle.dumps(placement_df))))
            
            if search_df is not None:
                cursor.execute("INSERT INTO campaign_search (upload_id, data) VALUES (%s, %s)",
                              (upload_id, psycopg2.Binary(pickle.dumps(search_df))))
            
            conn.commit()
            cursor.close()
            return upload_id
        except Exception as e:
            conn.rollback()
            st.error(f"Error saving to database: {str(e)}")
            return None
        finally:
            self._return_connection(conn)
    
    def _calculate_comprehensive_metrics(self, granular_df, placement_df, search_df):
        """Calculate metrics across all files"""
        metrics = {'granular': self._calc_metrics(granular_df)}
        
        if placement_df is not None and len(placement_df) > 0:
            metrics['placement'] = {
                'unique_keywords': int(placement_df['KEYWORD'].nunique()),
                'keyword_count': len(placement_df),
                'top_keyword_roi': float(placement_df.groupby('KEYWORD')['TOTAL_ROI'].mean().max()),
            }
        
        if search_df is not None and 'SEARCH_QUERY' in search_df.columns:
            metrics['search'] = {
                'unique_queries': int(search_df['SEARCH_QUERY'].nunique()),
                'query_count': len(search_df),
            }
        
        if 'CITY' in granular_df.columns:
            city_metrics = granular_df.groupby('CITY').agg({'TOTAL_ROI': 'mean'})
            if len(city_metrics) > 0:
                metrics['cities'] = {
                    'unique_cities': int(granular_df['CITY'].nunique()),
                    'top_city': str(city_metrics['TOTAL_ROI'].idxmax()),
                    'top_city_roi': float(city_metrics['TOTAL_ROI'].max()),
                }
        
        if 'PRODUCT_NAME' in granular_df.columns:
            product_metrics = granular_df.groupby('PRODUCT_NAME').agg({'TOTAL_ROI': 'mean'})
            if len(product_metrics) > 0:
                metrics['products'] = {
                    'unique_products': int(granular_df['PRODUCT_NAME'].nunique()),
                    'top_product': str(product_metrics['TOTAL_ROI'].idxmax()),
                    'top_product_roi': float(product_metrics['TOTAL_ROI'].max()),
                }
        
        return metrics
    
    def _calc_metrics(self, df):
        """Calculate basic metrics from dataframe"""
        total_conversions = df['TOTAL_CONVERSIONS'].sum()
        return {
            'total_spend': float(df['TOTAL_BUDGET_BURNT'].sum()),
            'total_gmv': float(df['TOTAL_GMV'].sum()),
            'avg_roi': float(df['TOTAL_ROI'].mean()),
            'total_conversions': float(total_conversions),
            'avg_ctr': float(df['TOTAL_CTR'].mean()),
            'cost_per_conversion': float(df['TOTAL_BUDGET_BURNT'].sum() / total_conversions) if total_conversions > 0 else 0,
            'row_count': len(df)
        }
    
    def get_all_uploads(self, limit=None):
        """Get all uploads metadata"""
        conn = self._get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            query = "SELECT upload_id, upload_date, user_label, has_placement, has_search, metrics FROM campaign_uploads ORDER BY upload_date DESC"
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            uploads = []
            for row in rows:
                uploads.append({
                    'id': row[0],
                    'timestamp': row[1].isoformat(),
                    'user_label': row[2],
                    'has_placement': row[3],
                    'has_search': row[4],
                    'metrics': row[5]
                })
            
            cursor.close()
            return uploads
        except Exception as e:
            st.error(f"Error fetching uploads: {str(e)}")
            return []
        finally:
            self._return_connection(conn)
    
    def get_upload_data(self, upload_id):
        """Get all data for a specific upload"""
        conn = self._get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            data = {}
            
            cursor.execute("SELECT data FROM campaign_granular WHERE upload_id = %s", (upload_id,))
            row = cursor.fetchone()
            if row:
                data['granular'] = pickle.loads(bytes(row[0]))
            
            cursor.execute("SELECT data FROM campaign_placement WHERE upload_id = %s", (upload_id,))
            row = cursor.fetchone()
            if row:
                data['placement'] = pickle.loads(bytes(row[0]))
            
            cursor.execute("SELECT data FROM campaign_search WHERE upload_id = %s", (upload_id,))
            row = cursor.fetchone()
            if row:
                data['search'] = pickle.loads(bytes(row[0]))
            
            cursor.close()
            return data
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            return {}
        finally:
            self._return_connection(conn)
    
    def get_trend_data(self):
        """Get comprehensive trend data across all uploads"""
        conn = self._get_connection()
        if not conn:
            return pd.DataFrame()
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT upload_date, user_label, metrics FROM campaign_uploads ORDER BY upload_date ASC")
            rows = cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            trend_data = {
                'dates': [], 'labels': [], 'spend': [], 'gmv': [], 'roi': [],
                'conversions': [], 'ctr': [], 'cost_per_conv': [],
                'unique_cities': [], 'unique_keywords': [], 'top_city_roi': [], 'top_keyword_roi': []
            }
            
            for row in rows:
                metrics = row[2]
                if not metrics or 'granular' not in metrics:
                    continue
                
                m = metrics['granular']
                trend_data['dates'].append(row[0].strftime('%Y-%m-%d'))
                trend_data['labels'].append(row[1])
                trend_data['spend'].append(m.get('total_spend', 0))
                trend_data['gmv'].append(m.get('total_gmv', 0))
                trend_data['roi'].append(m.get('avg_roi', 0))
                trend_data['conversions'].append(m.get('total_conversions', 0))
                trend_data['ctr'].append(m.get('avg_ctr', 0) * 100)
                trend_data['cost_per_conv'].append(m.get('cost_per_conversion', 0))
                
                trend_data['unique_cities'].append(metrics.get('cities', {}).get('unique_cities', 0))
                trend_data['top_city_roi'].append(metrics.get('cities', {}).get('top_city_roi', 0))
                trend_data['unique_keywords'].append(metrics.get('placement', {}).get('unique_keywords', 0))
                trend_data['top_keyword_roi'].append(metrics.get('placement', {}).get('top_keyword_roi', 0))
            
            cursor.close()
            return pd.DataFrame(trend_data)
        except Exception as e:
            st.error(f"Error getting trend data: {str(e)}")
            return pd.DataFrame()
        finally:
            self._return_connection(conn)
    
    def get_campaign_maturity(self, df):
        """Determine campaign maturity"""
        if 'METRICS_DATE' in df.columns:
            dates = pd.to_datetime(df['METRICS_DATE'], errors='coerce')
            date_range = (dates.max() - dates.min()).days if len(dates) > 0 else 0
        else:
            date_range = 0
        
        total_impressions = df['TOTAL_IMPRESSIONS'].sum()
        total_conversions = df['TOTAL_CONVERSIONS'].sum()
        
        if date_range < 3 or total_impressions < 1000 or total_conversions < 10:
            return "NEW", "⚠️ New campaigns - wait 3-7 days"
        elif date_range < 7 or total_conversions < 30:
            return "EARLY", "🟡 Early stage - proceed with caution"
        else:
            return "MATURE", "✅ Sufficient data for optimization"

# ============================================================================
# DATA LOADING & PROCESSING
# ============================================================================

@st.cache_data
def load_csv_file(file):
    """Load and process a single CSV file"""
    try:
        df = pd.read_csv(file, skiprows=6)
        
        # Common cleaning for all file types
        if 'TOTAL_ROI' in df.columns:
            df['TOTAL_ROI'] = pd.to_numeric(df['TOTAL_ROI'], errors='coerce')
        if 'TOTAL_CTR' in df.columns:
            df['TOTAL_CTR'] = df['TOTAL_CTR'].str.rstrip('%').astype('float') / 100.0
        if 'A2C_RATE' in df.columns:
            df['A2C_RATE'] = df['A2C_RATE'].str.rstrip('%').astype('float') / 100.0
        if 'TOTAL_BUDGET_BURNT' in df.columns:
            df['TOTAL_BUDGET_BURNT'] = pd.to_numeric(df['TOTAL_BUDGET_BURNT'], errors='coerce')
        if 'TOTAL_CLICKS' in df.columns:
            df['TOTAL_CLICKS'] = pd.to_numeric(df['TOTAL_CLICKS'], errors='coerce')
        if 'TOTAL_IMPRESSIONS' in df.columns:
            df['TOTAL_IMPRESSIONS'] = pd.to_numeric(df['TOTAL_IMPRESSIONS'], errors='coerce')
        if 'TOTAL_GMV' in df.columns:
            df['TOTAL_GMV'] = pd.to_numeric(df['TOTAL_GMV'], errors='coerce')
        if 'TOTAL_CONVERSIONS' in df.columns:
            df['TOTAL_CONVERSIONS'] = pd.to_numeric(df['TOTAL_CONVERSIONS'], errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        return None

def analyze_keywords(df, min_roi=0.5, min_ctr=0.02):
    """Analyze keyword performance from granular or placement data"""
    keyword_perf = df.groupby('KEYWORD').agg({
        'TOTAL_IMPRESSIONS': 'sum',
        'TOTAL_CLICKS': 'sum',
        'TOTAL_BUDGET_BURNT': 'sum',
        'TOTAL_CONVERSIONS': 'sum',
        'TOTAL_GMV': 'sum',
        'TOTAL_ROI': 'mean',
        'TOTAL_CTR': 'mean',
    }).reset_index()
    
    keyword_perf['CTR'] = keyword_perf['TOTAL_CLICKS'] / keyword_perf['TOTAL_IMPRESSIONS']
    keyword_perf['Conversion_Rate'] = keyword_perf['TOTAL_CONVERSIONS'] / keyword_perf['TOTAL_CLICKS']
    
    keyword_perf['Status'] = keyword_perf.apply(lambda x: 
        '🟢 Scale Up' if x['TOTAL_ROI'] > 2.0 and x['TOTAL_CONVERSIONS'] > 0
        else ('🔴 Pause' if x['TOTAL_ROI'] < min_roi or x['CTR'] < min_ctr
        else '🟡 Monitor'), axis=1)
    
    return keyword_perf.sort_values('TOTAL_ROI', ascending=False)

def analyze_cities(df, min_roi=0.5):
    """Analyze city performance"""
    if 'CITY' not in df.columns:
        return pd.DataFrame()
    
    city_perf = df.groupby('CITY').agg({
        'TOTAL_IMPRESSIONS': 'sum',
        'TOTAL_CLICKS': 'sum',
        'TOTAL_BUDGET_BURNT': 'sum',
        'TOTAL_CONVERSIONS': 'sum',
        'TOTAL_GMV': 'sum',
        'TOTAL_ROI': 'mean'
    }).reset_index()
    
    city_perf['Status'] = city_perf['TOTAL_ROI'].apply(
        lambda x: '🟢 Expand' if x > 2.0 else ('🔴 Reduce' if x < min_roi else '🟡 Monitor')
    )
    
    return city_perf.sort_values('TOTAL_ROI', ascending=False)

def analyze_products(df):
    """Analyze product performance"""
    if 'PRODUCT_NAME' not in df.columns:
        return pd.DataFrame()
    
    product_perf = df.groupby('PRODUCT_NAME').agg({
        'TOTAL_IMPRESSIONS': 'sum',
        'TOTAL_CLICKS': 'sum',
        'TOTAL_BUDGET_BURNT': 'sum',
        'TOTAL_CONVERSIONS': 'sum',
        'TOTAL_GMV': 'sum',
        'TOTAL_ROI': 'mean',
        'TOTAL_CTR': 'mean'
    }).reset_index()
    
    product_perf['Recommendation'] = product_perf['TOTAL_ROI'].apply(
        lambda x: '🚀 Scale Up' if x > 3.0 else ('⚠️ Reduce' if x < 1.0 else '➡️ Maintain')
    )
    
    return product_perf.sort_values('TOTAL_ROI', ascending=False)

def analyze_search_queries(df, min_roi=0.5):
    """Analyze search query performance"""
    if df is None or 'SEARCH_QUERY' not in df.columns:
        return pd.DataFrame()
    
    query_perf = df.groupby('SEARCH_QUERY').agg({
        'TOTAL_IMPRESSIONS': 'sum',
        'TOTAL_CLICKS': 'sum',
        'TOTAL_BUDGET_BURNT': 'sum',
        'TOTAL_CONVERSIONS': 'sum',
        'TOTAL_GMV': 'sum',
        'TOTAL_ROI': 'mean',
    }).reset_index()
    
    query_perf['CTR'] = query_perf['TOTAL_CLICKS'] / query_perf['TOTAL_IMPRESSIONS']
    
    query_perf['Status'] = query_perf.apply(lambda x: 
        '🟢 Add as Keyword' if x['TOTAL_ROI'] > 2.0 and x['TOTAL_CONVERSIONS'] > 0
        else ('🔴 Negative Keyword' if x['TOTAL_ROI'] < min_roi
        else '🟡 Monitor'), axis=1)
    
    return query_perf.sort_values('TOTAL_ROI', ascending=False)

def generate_combined_insights(granular_df, placement_df, search_df, keyword_analysis, city_analysis, product_analysis, search_analysis):
    """Generate comprehensive insights combining all data sources"""
    insights = []
    
    # Top keyword insight
    if len(keyword_analysis) > 0:
        top_kw = keyword_analysis.iloc[0]
        insights.append({
            'type': '🏆 Top Performer',
            'title': f"Keyword: {top_kw['KEYWORD']}",
            'metric': f"ROI: {top_kw['TOTAL_ROI']:.2f}x",
            'detail': f"Generated ₹{top_kw['TOTAL_GMV']:,.0f} from ₹{top_kw['TOTAL_BUDGET_BURNT']:,.0f} spend",
            'action': '💡 Action: Increase budget by 200-300%'
        })
    
    # Worst keyword
    worst_kw = keyword_analysis[keyword_analysis['Status'] == '🔴 Pause']
    if len(worst_kw) > 0:
        total_waste = worst_kw['TOTAL_BUDGET_BURNT'].sum()
        insights.append({
            'type': '⚠️ Budget Waste',
            'title': f"{len(worst_kw)} underperforming keywords",
            'metric': f"Wasting ₹{total_waste:,.0f}/day",
            'detail': f"Average ROI: {worst_kw['TOTAL_ROI'].mean():.2f}x",
            'action': '💡 Action: Pause immediately to save budget'
        })
    
    # Top city insight
    if len(city_analysis) > 0:
        top_city = city_analysis.iloc[0]
        insights.append({
            'type': '📍 Geographic Winner',
            'title': f"City: {top_city['CITY']}",
            'metric': f"ROI: {top_city['TOTAL_ROI']:.2f}x",
            'detail': f"Only ₹{top_city['TOTAL_BUDGET_BURNT']:,.0f} spent - huge opportunity!",
            'action': '💡 Action: Expand targeting in this city'
        })
    
    # Product insight
    if len(product_analysis) > 0:
        top_product = product_analysis.iloc[0]
        insights.append({
            'type': '📦 Product Champion',
            'title': f"{top_product['PRODUCT_NAME']}",
            'metric': f"ROI: {top_product['TOTAL_ROI']:.2f}x",
            'detail': f"{top_product['TOTAL_CONVERSIONS']:.0f} conversions, ₹{top_product['TOTAL_GMV']:,.0f} GMV",
            'action': '💡 Action: Create dedicated campaign for this product'
        })
    
    # Search query opportunity
    if len(search_analysis) > 0:
        new_opportunities = search_analysis[search_analysis['Status'] == '🟢 Add as Keyword']
        if len(new_opportunities) > 0:
            insights.append({
                'type': '🔍 New Opportunity',
                'title': f"{len(new_opportunities)} high-performing search queries",
                'metric': f"Not yet added as keywords!",
                'detail': f"Top query ROI: {new_opportunities.iloc[0]['TOTAL_ROI']:.2f}x",
                'action': '💡 Action: Add these as exact match keywords'
            })
    
    # Geographic concentration
    if len(city_analysis) > 5:
        top_5_cities = city_analysis.head(5)
        top_5_spend = top_5_cities['TOTAL_BUDGET_BURNT'].sum()
        total_spend = city_analysis['TOTAL_BUDGET_BURNT'].sum()
        concentration = (top_5_spend / total_spend) * 100
        
        if concentration > 70:
            insights.append({
                'type': '🎯 Focus Insight',
                'title': "Budget concentrated in top 5 cities",
                'metric': f"{concentration:.0f}% of total spend",
                'detail': f"Generating {(top_5_cities['TOTAL_GMV'].sum() / city_analysis['TOTAL_GMV'].sum() * 100):.0f}% of GMV",
                'action': '💡 Action: This concentration is good - double down on winners'
            })
    
    return insights

# ============================================================================
# INITIALIZE SESSION STATE
# ============================================================================

if 'db_manager' not in st.session_state:
    st.session_state.db_manager = DatabaseManager()

if 'current_data' not in st.session_state:
    st.session_state.current_data = {}

# ============================================================================
# SIDEBAR
# ============================================================================

st.markdown('<div class="main-header">📊 Campaign Intelligence Dashboard</div>', unsafe_allow_html=True)

# Check database connection
if not st.session_state.db_manager.connection_string:
    st.error("""
    ### ⚠️ Database Not Configured
    
    Please configure your Neon PostgreSQL connection:
    
    1. Go to Streamlit Cloud Settings → Secrets
    2. Add your database URL:
    
    ```toml
    DATABASE_URL = "postgresql://user:password@host.neon.tech/dbname?sslmode=require"
    ```
    """)
    st.stop()

st.markdown("### *Complete 3-File Analysis with Historical Tracking*")
st.markdown("---")

with st.sidebar:
    st.header("📁 Upload Campaign Data")
    
    st.markdown("**Upload all 3 files for complete analysis:**")
    
    # File uploaders
    granular_file = st.file_uploader(
        "1️⃣ Granular File (Required)",
        type=['csv'],
        help="IM_GRANULAR_*.csv - Most detailed data"
    )
    
    placement_file = st.file_uploader(
        "2️⃣ Placement File (Recommended)",
        type=['csv'],
        help="IM_CAMPAIGN_X_PLACEMENT_*.csv - Keyword analysis"
    )
    
    search_file = st.file_uploader(
        "3️⃣ Search Query File (Optional)",
        type=['csv'],
        help="IM_CAMPAIGN_X_SEARCH_QUERY_*.csv - User search behavior"
    )
    
    # Upload label
    user_label = st.text_input(
        "📝 Label this upload",
        placeholder="e.g., Day 1, Week 1, After optimization",
        help="Give this data snapshot a memorable name"
    )
    
    # Upload button
    if granular_file:
        if st.button("📊 Analyze & Save All Files", type="primary", use_container_width=True):
            with st.spinner("Loading and analyzing all files..."):
                # Load granular
                granular_df = load_csv_file(granular_file)
                
                # Load placement if provided
                placement_df = None
                if placement_file:
                    placement_df = load_csv_file(placement_file)
                
                # Load search if provided
                search_df = None
                if search_file:
                    search_df = load_csv_file(search_file)
                
                if granular_df is not None:
                    # Save to history
                    upload_id = st.session_state.db_manager.save_upload(
                        granular_df,
                        placement_df,
                        search_df,
                        user_label
                    )
                    
                    # Set current data
                    st.session_state.current_data = {
                        'granular': granular_df,
                        'placement': placement_df,
                        'search': search_df,
                        'upload_id': upload_id
                    }
                    
                    st.success("✅ All files saved to history!")
                    st.rerun()
    else:
        st.info("👆 Upload Granular file to begin")
    
    st.markdown("---")
    st.header("📜 Upload History")
    
    uploads = st.session_state.db_manager.get_all_uploads()
    
    if uploads:
        st.metric("Total Uploads", len(uploads))
        
        for upload in uploads[:5]:
            with st.expander(f"📅 {upload['user_label']}", expanded=False):
                st.write(f"**Date:** {upload['timestamp'][:10]}")
                
                # Safely access metrics with error handling
                try:
                    if 'metrics' in upload and 'granular' in upload['metrics']:
                        m = upload['metrics']['granular']
                        st.write(f"**ROI:** {m['avg_roi']:.2f}x")
                        st.write(f"**Spend:** ₹{m['total_spend']:,.0f}")
                    elif 'metrics' in upload:
                        # Old format compatibility
                        m = upload['metrics']
                        st.write(f"**ROI:** {m.get('avg_roi', 0):.2f}x")
                        st.write(f"**Spend:** ₹{m.get('total_spend', 0):,.0f}")
                    else:
                        st.caption("Metrics not available")
                except (KeyError, TypeError) as e:
                    st.caption("Metrics unavailable for this upload")
                
                # Show what files were included
                files_included = ["Granular"]
                if upload.get('has_placement'):
                    files_included.append("Placement")
                if upload.get('has_search'):
                    files_included.append("Search")
                st.caption(f"Files: {', '.join(files_included)}")
                
                if st.button(f"Load", key=f"load_{upload['id']}"):
                    data = st.session_state.db_manager.get_upload_data(upload['id'])
                    st.session_state.current_data = data
                    st.session_state.current_data['upload_id'] = upload['id']
                    st.rerun()
    else:
        st.info("No history yet. Upload your first set of files!")
    
    st.markdown("---")
    st.header("⚙️ Analysis Settings")
    
    min_roi = st.slider("Minimum ROI Threshold", 0.0, 2.0, 0.5, 0.1)
    min_ctr = st.slider("Minimum CTR (%)", 0.0, 10.0, 2.0, 0.5) / 100
    
    st.markdown("---")
    st.subheader("🕐 Campaign Maturity")
    force_mature = st.checkbox(
        "Override - treat as mature",
        help="Skip maturity detection"
    )

# ============================================================================
# MAIN CONTENT
# ============================================================================

if 'granular' in st.session_state.current_data and st.session_state.current_data['granular'] is not None:
    
    granular_df = st.session_state.current_data['granular']
    placement_df = st.session_state.current_data.get('placement')
    search_df = st.session_state.current_data.get('search')
    
    # Check maturity
    if not force_mature:
        maturity_level, maturity_msg = st.session_state.db_manager.get_campaign_maturity(granular_df)
    else:
        maturity_level = "MATURE"
        maturity_msg = "✅ Treated as mature (override enabled)"
    
    # Perform all analyses
    keyword_analysis = analyze_keywords(granular_df, min_roi, min_ctr)
    city_analysis = analyze_cities(granular_df, min_roi)
    product_analysis = analyze_products(granular_df)
    search_analysis = analyze_search_queries(search_df, min_roi)
    
    # Generate insights
    insights = generate_combined_insights(
        granular_df, placement_df, search_df,
        keyword_analysis, city_analysis, product_analysis, search_analysis
    )
    
    # Show maturity warning
    if maturity_level == "NEW":
        st.error(f"""
        ### ⚠️ NEW CAMPAIGNS DETECTED
        {maturity_msg}
        
        **Current Status:** Monitoring mode only
        - Upload daily for the next 3-7 days
        - Watch trends form
        - Don't make major changes yet
        """)
    elif maturity_level == "EARLY":
        st.warning(f"""
        ### 🟡 EARLY STAGE CAMPAIGNS
        {maturity_msg}
        
        **Current Status:** Cautious optimization allowed
        - Pause obvious failures only
        - Monitor top performers (don't scale yet)
        - Continue daily uploads
        """)
    
    # ========================================================================
    # TREND ANALYSIS
    # ========================================================================
    
    trend_df = st.session_state.db_manager.get_trend_data()
    
    if len(trend_df) > 1:
        st.header("📈 Performance Trends Over Time")
        
        # Main metrics trends
        col1, col2, col3 = st.columns(3)
        
        with col1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=trend_df['dates'], y=trend_df['roi'],
                mode='lines+markers', name='ROI',
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=10)
            ))
            fig.add_hline(y=2.0, line_dash="dash", line_color="green", annotation_text="Target")
            fig.add_hline(y=1.0, line_dash="dash", line_color="orange", annotation_text="Break-even")
            fig.update_layout(title="ROI Trend", height=250, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=trend_df['dates'], y=trend_df['ctr'],
                mode='lines+markers', name='CTR',
                line=dict(color='#ff7f0e', width=3),
                marker=dict(size=10)
            ))
            fig.add_hline(y=5.0, line_dash="dash", line_color="green", annotation_text="Target")
            fig.update_layout(title="CTR Trend (%)", height=250, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col3:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=trend_df['dates'], y=trend_df['cost_per_conv'],
                mode='lines+markers', name='Cost/Conv',
                line=dict(color='#d62728', width=3),
                marker=dict(size=10)
            ))
            fig.add_hline(y=50, line_dash="dash", line_color="green", annotation_text="Target")
            fig.update_layout(title="Cost per Conversion (₹)", height=250, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        # Spend vs GMV trend
        fig = go.Figure()
        fig.add_trace(go.Bar(x=trend_df['dates'], y=trend_df['spend'], name='Spend', marker_color='#ff6b6b'))
        fig.add_trace(go.Bar(x=trend_df['dates'], y=trend_df['gmv'], name='GMV', marker_color='#4caf50'))
        fig.update_layout(
            title="💰 Spend vs GMV Over Time",
            xaxis_title="Date",
            yaxis_title="Amount (₹)",
            barmode='group',
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Geographic & Keyword trends
        col1, col2 = st.columns(2)
        
        with col1:
            if trend_df['top_city_roi'].sum() > 0:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=trend_df['dates'], y=trend_df['top_city_roi'],
                    mode='lines+markers', name='Top City ROI',
                    line=dict(color='#2ecc71', width=3),
                    marker=dict(size=10),
                    fill='tozeroy'
                ))
                fig.update_layout(title="🏆 Best City ROI Over Time", height=250)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if trend_df['top_keyword_roi'].sum() > 0:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=trend_df['dates'], y=trend_df['top_keyword_roi'],
                    mode='lines+markers', name='Top Keyword ROI',
                    line=dict(color='#9b59b6', width=3),
                    marker=dict(size=10),
                    fill='tozeroy'
                ))
                fig.update_layout(title="🔑 Best Keyword ROI Over Time", height=250)
                st.plotly_chart(fig, use_container_width=True)
        
        # Period comparison
        if len(trend_df) >= 2:
            st.subheader("📊 Latest vs Previous Period")
            
            current = trend_df.iloc[-1]
            previous = trend_df.iloc[-2]
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                delta = current['roi'] - previous['roi']
                st.metric("ROI", f"{current['roi']:.2f}x", f"{delta:+.2f}x")
            
            with col2:
                delta = current['spend'] - previous['spend']
                st.metric("Spend", f"₹{current['spend']:,.0f}", f"{delta:+,.0f}")
            
            with col3:
                delta = current['gmv'] - previous['gmv']
                st.metric("GMV", f"₹{current['gmv']:,.0f}", f"{delta:+,.0f}")
            
            with col4:
                delta = current['conversions'] - previous['conversions']
                st.metric("Conversions", f"{current['conversions']:.0f}", f"{delta:+.0f}")
            
            with col5:
                delta = current['ctr'] - previous['ctr']
                st.metric("CTR", f"{current['ctr']:.2f}%", f"{delta:+.2f}%")
        
        st.markdown("---")
    
    # ========================================================================
    # COMBINED INSIGHTS
    # ========================================================================
    
    st.header("💡 Key Insights from All Data Sources")
    
    col1, col2 = st.columns(2)
    
    for i, insight in enumerate(insights):
        with col1 if i % 2 == 0 else col2:
            st.markdown(f"""
            <div class="insight-box">
                <strong>{insight['type']}</strong><br>
                <span style="font-size: 1.1rem;">{insight['title']}</span><br>
                <span class="metric-positive">{insight['metric']}</span><br>
                <small>{insight['detail']}</small><br>
                <em>{insight['action']}</em>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ========================================================================
    # DETAILED ANALYSIS TABS
    # ========================================================================
    
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🔑 Keywords",
        "📍 Cities", 
        "📦 Products",
        "🔍 Search Queries",
        "📊 Charts",
        "📥 Export"
    ])
    
    # TAB 1: KEYWORDS
    with tab1:
        st.subheader("🔑 Keyword Performance Analysis")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Keywords", len(keyword_analysis))
        with col2:
            scale_count = len(keyword_analysis[keyword_analysis['Status'] == '🟢 Scale Up'])
            st.metric("Scale Up", scale_count, delta="Winners")
        with col3:
            pause_count = len(keyword_analysis[keyword_analysis['Status'] == '🔴 Pause'])
            st.metric("To Pause", pause_count, delta="Losers", delta_color="inverse")
        
        # Filter
        status_filter = st.multiselect(
            "Filter by Status",
            options=['🟢 Scale Up', '🟡 Monitor', '🔴 Pause'],
            default=['🟢 Scale Up', '🟡 Monitor', '🔴 Pause']
        )
        
        filtered_kw = keyword_analysis[keyword_analysis['Status'].isin(status_filter)]
        
        st.dataframe(
            filtered_kw[['KEYWORD', 'Status', 'TOTAL_ROI', 'CTR', 'TOTAL_CONVERSIONS', 
                        'TOTAL_BUDGET_BURNT', 'TOTAL_GMV']].style.format({
                'TOTAL_ROI': '{:.2f}x',
                'CTR': '{:.2%}',
                'TOTAL_BUDGET_BURNT': '₹{:,.0f}',
                'TOTAL_GMV': '₹{:,.0f}',
                'TOTAL_CONVERSIONS': '{:.0f}'
            }),
            use_container_width=True,
            height=400
        )
        
        # Quick actions
        if maturity_level == "MATURE":
            st.markdown("### 🎯 Quick Actions")
            
            col1, col2 = st.columns(2)
            
            with col1:
                pause_kws = keyword_analysis[keyword_analysis['Status'] == '🔴 Pause']
                if len(pause_kws) > 0:
                    st.markdown(f"""
                    <div class="alert-critical">
                        <strong>⛔ Pause {len(pause_kws)} Keywords</strong><br>
                        Save ₹{pause_kws['TOTAL_BUDGET_BURNT'].sum():,.0f}/day<br>
                        <small>Keywords: {', '.join(pause_kws['KEYWORD'].head(5).tolist())}</small>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col2:
                scale_kws = keyword_analysis[keyword_analysis['Status'] == '🟢 Scale Up']
                if len(scale_kws) > 0:
                    st.markdown(f"""
                    <div class="alert-success">
                        <strong>🚀 Scale {len(scale_kws)} Keywords</strong><br>
                        Avg ROI: {scale_kws['TOTAL_ROI'].mean():.2f}x<br>
                        <small>Keywords: {', '.join(scale_kws['KEYWORD'].head(5).tolist())}</small>
                    </div>
                    """, unsafe_allow_html=True)
    
    # TAB 2: CITIES
    with tab2:
        st.subheader("📍 Geographic Performance")
        
        if len(city_analysis) > 0:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Cities", len(city_analysis))
            with col2:
                expand = len(city_analysis[city_analysis['Status'] == '🟢 Expand'])
                st.metric("Expand", expand)
            with col3:
                reduce = len(city_analysis[city_analysis['Status'] == '🔴 Reduce'])
                st.metric("Reduce", reduce, delta_color="inverse")
            
            # Top & Bottom cities
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 🏆 Top 10 Cities")
                st.dataframe(
                    city_analysis.head(10)[['CITY', 'TOTAL_ROI', 'TOTAL_BUDGET_BURNT', 'TOTAL_GMV', 'Status']],
                    use_container_width=True
                )
            
            with col2:
                st.markdown("#### ⚠️ Bottom 10 Cities")
                st.dataframe(
                    city_analysis.tail(10)[['CITY', 'TOTAL_ROI', 'TOTAL_BUDGET_BURNT', 'TOTAL_GMV', 'Status']],
                    use_container_width=True
                )
            
            # City ROI chart
            fig = px.bar(
                city_analysis.head(20),
                x='TOTAL_ROI',
                y='CITY',
                orientation='h',
                title='Top 20 Cities by ROI',
                color='TOTAL_ROI',
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("City data not available in this dataset")
    
    # TAB 3: PRODUCTS
    with tab3:
        st.subheader("📦 Product Performance")
        
        if len(product_analysis) > 0:
            st.dataframe(
                product_analysis.style.format({
                    'TOTAL_ROI': '{:.2f}x',
                    'TOTAL_CTR': '{:.2%}',
                    'TOTAL_BUDGET_BURNT': '₹{:,.0f}',
                    'TOTAL_GMV': '₹{:,.0f}',
                    'TOTAL_CONVERSIONS': '{:.0f}'
                }),
                use_container_width=True
            )
            
            # Product comparison
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name='Budget',
                x=product_analysis['PRODUCT_NAME'],
                y=product_analysis['TOTAL_BUDGET_BURNT'],
                marker_color='#ff6b6b'
            ))
            fig.add_trace(go.Bar(
                name='GMV',
                x=product_analysis['PRODUCT_NAME'],
                y=product_analysis['TOTAL_GMV'],
                marker_color='#4caf50'
            ))
            fig.add_trace(go.Scatter(
                name='ROI',
                x=product_analysis['PRODUCT_NAME'],
                y=product_analysis['TOTAL_ROI'],
                yaxis='y2',
                mode='lines+markers',
                marker=dict(size=12, color='red')
            ))
            fig.update_layout(
                title='Product Performance Overview',
                yaxis=dict(title='Amount (₹)'),
                yaxis2=dict(title='ROI (x)', overlaying='y', side='right'),
                barmode='group',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Product data not available")
    
    # TAB 4: SEARCH QUERIES
    with tab4:
        st.subheader("🔍 Search Query Analysis")
        
        if len(search_analysis) > 0:
            st.markdown("""
            **What this shows:** Actual terms users searched before clicking your ads.
            Use this to discover new keyword opportunities!
            """)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Queries", len(search_analysis))
            with col2:
                add_kw = len(search_analysis[search_analysis['Status'] == '🟢 Add as Keyword'])
                st.metric("Add as Keywords", add_kw)
            with col3:
                neg_kw = len(search_analysis[search_analysis['Status'] == '🔴 Negative Keyword'])
                st.metric("Negative Keywords", neg_kw, delta_color="inverse")
            
            # Show opportunities
            st.markdown("#### 🟢 High-Performing Queries (Add as Keywords)")
            opportunities = search_analysis[search_analysis['Status'] == '🟢 Add as Keyword'].head(20)
            if len(opportunities) > 0:
                st.dataframe(
                    opportunities[['SEARCH_QUERY', 'TOTAL_ROI', 'CTR', 'TOTAL_CONVERSIONS', 'TOTAL_GMV']],
                    use_container_width=True
                )
            else:
                st.info("No new keyword opportunities found")
            
            # Show negative keywords
            st.markdown("#### 🔴 Poor-Performing Queries (Add as Negative)")
            negatives = search_analysis[search_analysis['Status'] == '🔴 Negative Keyword'].head(20)
            if len(negatives) > 0:
                st.dataframe(
                    negatives[['SEARCH_QUERY', 'TOTAL_ROI', 'TOTAL_BUDGET_BURNT', 'TOTAL_IMPRESSIONS']],
                    use_container_width=True
                )
        else:
            st.info("Search query file not uploaded. Upload to see user search behavior!")
    
    # TAB 5: CHARTS
    with tab5:
        st.subheader("📊 Visual Analytics")
        
        # Performance scatter
        fig = px.scatter(
            keyword_analysis,
            x='TOTAL_BUDGET_BURNT',
            y='TOTAL_ROI',
            size='TOTAL_CONVERSIONS',
            color='Status',
            hover_data=['KEYWORD'],
            title='Keyword Performance Matrix',
            labels={'TOTAL_BUDGET_BURNT': 'Budget Spent (₹)', 'TOTAL_ROI': 'ROI (x)'},
            color_discrete_map={
                '🟢 Scale Up': 'green',
                '🟡 Monitor': 'orange',
                '🔴 Pause': 'red'
            }
        )
        fig.add_hline(y=min_roi, line_dash="dash", annotation_text=f"Min ROI: {min_roi}x")
        st.plotly_chart(fig, use_container_width=True)
        
        # ROI distribution
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.histogram(
                keyword_analysis,
                x='TOTAL_ROI',
                nbins=30,
                title='ROI Distribution',
                color_discrete_sequence=['#1f77b4']
            )
            fig.add_vline(x=min_roi, line_dash="dash", line_color="red")
            fig.add_vline(x=2.0, line_dash="dash", line_color="green")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if len(city_analysis) > 0:
                fig = px.pie(
                    city_analysis.head(10),
                    values='TOTAL_BUDGET_BURNT',
                    names='CITY',
                    title='Budget Distribution (Top 10 Cities)'
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # TAB 6: EXPORT
    with tab6:
        st.subheader("📥 Export Complete Analysis")
        
        # Generate comprehensive report
        report = f"""
COMPREHENSIVE CAMPAIGN ANALYSIS REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Campaign Maturity: {maturity_level}
{'='*80}

FILES ANALYZED:
- Granular: ✓ ({len(granular_df)} rows)
- Placement: {'✓' if placement_df is not None else '✗'}
- Search Queries: {'✓' if search_df is not None else '✗'}

EXECUTIVE SUMMARY
-----------------
Total Spend: ₹{granular_df['TOTAL_BUDGET_BURNT'].sum():,.2f}
Total GMV: ₹{granular_df['TOTAL_GMV'].sum():,.2f}
ROI: {granular_df['TOTAL_ROI'].mean():.2f}x
Conversions: {granular_df['TOTAL_CONVERSIONS'].sum():.0f}
CTR: {granular_df['TOTAL_CTR'].mean()*100:.2f}%

KEYWORD INSIGHTS
----------------
Total Keywords: {len(keyword_analysis)}
To Scale: {len(keyword_analysis[keyword_analysis['Status'] == '🟢 Scale Up'])}
To Pause: {len(keyword_analysis[keyword_analysis['Status'] == '🔴 Pause'])}

Top 5 Keywords:
"""
        for _, row in keyword_analysis.head(5).iterrows():
            report += f"  - {row['KEYWORD']}: ROI {row['TOTAL_ROI']:.2f}x, ₹{row['TOTAL_GMV']:,.0f} GMV\n"
        
        if len(city_analysis) > 0:
            report += f"\nGEOGRAPHIC INSIGHTS\n-------------------\n"
            report += f"Total Cities: {len(city_analysis)}\n"
            report += f"Top City: {city_analysis.iloc[0]['CITY']} (ROI: {city_analysis.iloc[0]['TOTAL_ROI']:.2f}x)\n"
        
        if len(search_analysis) > 0:
            report += f"\nSEARCH QUERY INSIGHTS\n---------------------\n"
            report += f"Total Queries: {len(search_analysis)}\n"
            report += f"New Keyword Opportunities: {len(search_analysis[search_analysis['Status'] == '🟢 Add as Keyword'])}\n"
        
        if len(trend_df) > 0:
            report += f"\nHISTORICAL TRACKING\n-------------------\n"
            report += f"Total Uploads: {len(trend_df)}\n"
            report += f"First Upload: {trend_df.iloc[0]['dates']} - ROI: {trend_df.iloc[0]['roi']:.2f}x\n"
            report += f"Latest Upload: {trend_df.iloc[-1]['dates']} - ROI: {trend_df.iloc[-1]['roi']:.2f}x\n"
            if len(trend_df) > 1:
                improvement = ((trend_df.iloc[-1]['roi'] - trend_df.iloc[0]['roi']) / trend_df.iloc[0]['roi'] * 100)
                report += f"ROI Change: {improvement:+.1f}%\n"
        
        # Download buttons
        col1, col2 = st.columns(2)
        
        with col1:
            st.download_button(
                "📄 Download Text Report",
                report,
                f"campaign_report_{datetime.now().strftime('%Y%m%d')}.txt",
                "text/plain",
                use_container_width=True
            )
        
        with col2:
            # Excel export
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                keyword_analysis.to_excel(writer, sheet_name='Keywords', index=False)
                if len(city_analysis) > 0:
                    city_analysis.to_excel(writer, sheet_name='Cities', index=False)
                if len(product_analysis) > 0:
                    product_analysis.to_excel(writer, sheet_name='Products', index=False)
                if len(search_analysis) > 0:
                    search_analysis.to_excel(writer, sheet_name='Search Queries', index=False)
                if len(trend_df) > 0:
                    trend_df.to_excel(writer, sheet_name='Trends', index=False)
            
            output.seek(0)
            st.download_button(
                "📊 Download Excel (All Data)",
                output,
                f"campaign_analysis_{datetime.now().strftime('%Y%m%d')}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

else:
    # Landing page
    st.info("""
    ### 👋 Welcome to Campaign Intelligence Dashboard
    
    **Complete 3-File Analysis with Historical Tracking!** 
    
    #### What Makes This Special:
    
    🎯 **Upload All 3 Files Daily:**
    - **Granular File** → Detailed performance data (Required)
    - **Placement File** → Keyword-level insights (Recommended)  
    - **Search Query File** → User search behavior (Bonus insights!)
    
    📈 **Automatic Historical Tracking:**
    - System saves every upload automatically
    - Builds trend lines over time
    - Compares period-over-period
    - Shows your improvement journey
    
    🧠 **Smart Analysis:**
    - Combines all data sources for comprehensive insights
    - Detects campaign maturity (NEW/EARLY/MATURE)
    - Protects you from premature optimization
    - Recommends exact actions to take
    
    💡 **What You'll Get:**
    - Keyword opportunities from search queries
    - Geographic expansion recommendations
    - Product performance insights
    - Budget reallocation suggestions
    - Trend analysis over time
    - ROI improvement tracking
    
    #### Quick Start:
    1. Upload your **Granular file** (required)
    2. Upload **Placement** and **Search Query** files (for deeper insights)
    3. Label this upload (e.g., "Day 1", "Week 1")
    4. Click "Analyze & Save All Files"
    5. Come back tomorrow with fresh data!
    
    👈 **Start by uploading files in the sidebar!**
    """)
    
    # Show example of what insights look like
    st.markdown("---")
    st.subheader("📊 Example Insights You'll Get:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="insight-box">
            <strong>🏆 Top Performer</strong><br>
            <span style="font-size: 1.1rem;">Keyword: sabudana chiwda</span><br>
            <span class="metric-positive">ROI: 2.66x</span><br>
            <small>Generated ₹907 from ₹648 spend</small><br>
            <em>💡 Action: Increase budget by 200-300%</em>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="insight-box">
            <strong>⚠️ Budget Waste</strong><br>
            <span style="font-size: 1.1rem;">14 underperforming keywords</span><br>
            <span class="metric-negative">Wasting ₹2,122/day</span><br>
            <small>Average ROI: 0.15x</small><br>
            <em>💡 Action: Pause immediately to save budget</em>
        </div>
        """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 1rem;'>
    Campaign Intelligence Dashboard v3.0 | 
    3-File Analysis + Historical Tracking + Combined Insights
</div>
""", unsafe_allow_html=True)
