import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
from io import BytesIO

# Page configuration
st.set_page_config(
    page_title="Campaign Optimization Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .critical-alert {
        background-color: #ffebee;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #f44336;
    }
    .success-alert {
        background-color: #e8f5e9;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #4caf50;
    }
    .warning-alert {
        background-color: #fff3e0;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ff9800;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'granular_df' not in st.session_state:
    st.session_state.granular_df = None
if 'search_df' not in st.session_state:
    st.session_state.search_df = None

# Helper functions
@st.cache_data
def load_campaign_data(granular_file, search_file=None):
    """Load and process campaign data"""
    try:
        # Load granular data
        granular_df = pd.read_csv(granular_file, skiprows=6)
        
        # Clean and convert data types
        granular_df['TOTAL_ROI'] = pd.to_numeric(granular_df['TOTAL_ROI'], errors='coerce')
        granular_df['TOTAL_CTR'] = granular_df['TOTAL_CTR'].str.rstrip('%').astype('float') / 100.0
        granular_df['A2C_RATE'] = granular_df['A2C_RATE'].str.rstrip('%').astype('float') / 100.0
        granular_df['TOTAL_BUDGET_BURNT'] = pd.to_numeric(granular_df['TOTAL_BUDGET_BURNT'], errors='coerce')
        granular_df['TOTAL_CLICKS'] = pd.to_numeric(granular_df['TOTAL_CLICKS'], errors='coerce')
        granular_df['TOTAL_IMPRESSIONS'] = pd.to_numeric(granular_df['TOTAL_IMPRESSIONS'], errors='coerce')
        granular_df['TOTAL_GMV'] = pd.to_numeric(granular_df['TOTAL_GMV'], errors='coerce')
        granular_df['TOTAL_CONVERSIONS'] = pd.to_numeric(granular_df['TOTAL_CONVERSIONS'], errors='coerce')
        
        search_df = None
        if search_file:
            try:
                search_df = pd.read_csv(search_file, skiprows=6)
                search_df['TOTAL_ROI'] = pd.to_numeric(search_df['TOTAL_ROI'], errors='coerce')
                search_df['TOTAL_BUDGET_BURNT'] = pd.to_numeric(search_df['TOTAL_BUDGET_BURNT'], errors='coerce')
            except:
                pass
        
        return granular_df, search_df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None, None

def calculate_metrics(df):
    """Calculate key performance metrics"""
    return {
        'total_spend': df['TOTAL_BUDGET_BURNT'].sum(),
        'total_gmv': df['TOTAL_GMV'].sum(),
        'total_conversions': df['TOTAL_CONVERSIONS'].sum(),
        'avg_roi': df['TOTAL_ROI'].mean(),
        'avg_ctr': df['TOTAL_CTR'].mean(),
        'avg_a2c': df['A2C_RATE'].mean(),
        'cost_per_conversion': df['TOTAL_BUDGET_BURNT'].sum() / df['TOTAL_CONVERSIONS'].sum() if df['TOTAL_CONVERSIONS'].sum() > 0 else 0
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
    
    # Classify keywords
    keyword_perf['Status'] = keyword_perf.apply(lambda x: 
        '🟢 Scale Up' if x['TOTAL_ROI'] > 2.0 and x['TOTAL_CONVERSIONS'] > 0
        else ('🔴 Pause' if x['TOTAL_ROI'] < min_roi or x['CTR'] < min_ctr
        else '🟡 Monitor'), axis=1)
    
    return keyword_perf.sort_values('TOTAL_ROI', ascending=False)

def analyze_cities(df, min_roi=0.5):
    """Analyze city performance"""
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

def generate_recommendations(df, keyword_analysis, city_analysis, product_analysis):
    """Generate actionable recommendations"""
    recommendations = []
    
    # Keyword recommendations
    pause_keywords = keyword_analysis[keyword_analysis['Status'] == '🔴 Pause']
    if len(pause_keywords) > 0:
        recommendations.append({
            'priority': '🔴 CRITICAL',
            'category': 'Keyword Optimization',
            'action': f"Pause {len(pause_keywords)} underperforming keywords",
            'impact': f"Potential savings: ₹{pause_keywords['TOTAL_BUDGET_BURNT'].sum():,.2f}/day",
            'keywords': pause_keywords['KEYWORD'].tolist()[:5]
        })
    
    scale_keywords = keyword_analysis[keyword_analysis['Status'] == '🟢 Scale Up']
    if len(scale_keywords) > 0:
        recommendations.append({
            'priority': '🟢 HIGH',
            'category': 'Keyword Optimization',
            'action': f"Increase budget for {len(scale_keywords)} top-performing keywords",
            'impact': f"Expected ROI: {scale_keywords['TOTAL_ROI'].mean():.2f}x",
            'keywords': scale_keywords['KEYWORD'].tolist()[:5]
        })
    
    # City recommendations
    reduce_cities = city_analysis[city_analysis['Status'] == '🔴 Reduce']
    if len(reduce_cities) > 0:
        recommendations.append({
            'priority': '🟡 MEDIUM',
            'category': 'Geographic Optimization',
            'action': f"Reduce campaigns in {len(reduce_cities)} low-ROI cities",
            'impact': f"Potential savings: ₹{reduce_cities['TOTAL_BUDGET_BURNT'].sum():,.2f}/day",
            'cities': reduce_cities['CITY'].tolist()[:5]
        })
    
    expand_cities = city_analysis[city_analysis['Status'] == '🟢 Expand']
    if len(expand_cities) > 0:
        recommendations.append({
            'priority': '🟢 HIGH',
            'category': 'Geographic Optimization',
            'action': f"Expand budget in {len(expand_cities)} high-potential cities",
            'impact': f"Average ROI: {expand_cities['TOTAL_ROI'].mean():.2f}x",
            'cities': expand_cities['CITY'].tolist()[:5]
        })
    
    # Product recommendations
    if len(product_analysis) > 0:
        top_product = product_analysis.iloc[0]
        recommendations.append({
            'priority': '🟢 HIGH',
            'category': 'Product Focus',
            'action': f"Focus budget on: {top_product['PRODUCT_NAME']}",
            'impact': f"Current ROI: {top_product['TOTAL_ROI']:.2f}x",
            'product': top_product['PRODUCT_NAME']
        })
    
    return recommendations

# ============================================================================
# MAIN APP
# ============================================================================

# Header
st.markdown('<div class="main-header">📊 Campaign Optimization Dashboard</div>', unsafe_allow_html=True)
st.markdown("---")

# Sidebar
with st.sidebar:
    st.image("https://via.placeholder.com/150x50/1f77b4/ffffff?text=Campaign+AI", use_container_width=True)
    st.header("📁 Data Upload")
    
    granular_file = st.file_uploader(
        "Upload Granular File (Required)",
        type=['csv'],
        help="Upload IM_GRANULAR_*.csv file"
    )
    
    search_file = st.file_uploader(
        "Upload Search Query File (Optional)",
        type=['csv'],
        help="Upload IM_CAMPAIGN_X_SEARCH_QUERY_*.csv file"
    )
    
    st.markdown("---")
    st.header("⚙️ Settings")
    
    min_roi = st.slider("Minimum ROI Threshold", 0.0, 2.0, 0.5, 0.1)
    min_ctr = st.slider("Minimum CTR (%)", 0.0, 10.0, 2.0, 0.5) / 100
    
    st.markdown("---")
    st.markdown("### 📖 Quick Guide")
    st.markdown("""
    1. Upload your **Granular file**
    2. Optionally upload **Search Query file**
    3. Review **Key Metrics** & **Recommendations**
    4. Implement suggested **Actions**
    5. Download **Reports**
    """)

# Load data
if granular_file:
    with st.spinner("Loading and analyzing data..."):
        granular_df, search_df = load_campaign_data(granular_file, search_file)
        
        if granular_df is not None:
            st.session_state.data_loaded = True
            st.session_state.granular_df = granular_df
            st.session_state.search_df = search_df
            st.success("✅ Data loaded successfully!")

# Main content
if st.session_state.data_loaded:
    df = st.session_state.granular_df
    
    # Calculate metrics
    metrics = calculate_metrics(df)
    
    # ========================================================================
    # EXECUTIVE SUMMARY
    # ========================================================================
    st.header("📈 Executive Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Spend",
            f"₹{metrics['total_spend']:,.0f}",
            help="Total budget burnt across all campaigns"
        )
    
    with col2:
        roi_delta = f"{((metrics['avg_roi'] - 2.0) / 2.0 * 100):.1f}% vs target" if metrics['avg_roi'] > 0 else None
        st.metric(
            "Average ROI",
            f"{metrics['avg_roi']:.2f}x",
            roi_delta,
            delta_color="normal" if metrics['avg_roi'] >= 2.0 else "inverse",
            help="Target: 2.0x or higher"
        )
    
    with col3:
        st.metric(
            "Total GMV",
            f"₹{metrics['total_gmv']:,.0f}",
            help="Gross Merchandise Value generated"
        )
    
    with col4:
        st.metric(
            "Cost/Conversion",
            f"₹{metrics['cost_per_conversion']:.2f}",
            f"{((50 - metrics['cost_per_conversion']) / 50 * 100):.1f}% vs target",
            delta_color="normal" if metrics['cost_per_conversion'] <= 50 else "inverse",
            help="Target: ₹50 or less"
        )
    
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        st.metric(
            "Conversions",
            f"{metrics['total_conversions']:.0f}",
            help="Total number of conversions"
        )
    
    with col6:
        ctr_delta = f"{((metrics['avg_ctr'] - 0.05) / 0.05 * 100):.1f}% vs target" if metrics['avg_ctr'] > 0 else None
        st.metric(
            "CTR",
            f"{metrics['avg_ctr']*100:.2f}%",
            ctr_delta,
            delta_color="normal" if metrics['avg_ctr'] >= 0.05 else "inverse",
            help="Target: 5% or higher"
        )
    
    with col7:
        st.metric(
            "Add-to-Cart Rate",
            f"{metrics['avg_a2c']*100:.2f}%",
            help="Percentage of clicks that add to cart"
        )
    
    with col8:
        profit = metrics['total_gmv'] - metrics['total_spend']
        st.metric(
            "Profit/Loss",
            f"₹{profit:,.0f}",
            "Profitable" if profit > 0 else "Loss",
            delta_color="normal" if profit > 0 else "inverse",
            help="GMV minus spend"
        )
    
    # Performance alert
    if metrics['avg_roi'] < 1.0:
        st.markdown(f"""
        <div class="critical-alert">
            <strong>⚠️ CRITICAL ALERT:</strong> Your average ROI is {metrics['avg_roi']:.2f}x, which means you're losing money on these campaigns. 
            Immediate action required! See recommendations below.
        </div>
        """, unsafe_allow_html=True)
    elif metrics['avg_roi'] < 2.0:
        st.markdown(f"""
        <div class="warning-alert">
            <strong>⚡ WARNING:</strong> Your ROI ({metrics['avg_roi']:.2f}x) is below the 2.0x target. 
            Review and implement optimization recommendations below.
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="success-alert">
            <strong>✅ GOOD PERFORMANCE:</strong> Your ROI ({metrics['avg_roi']:.2f}x) is above target. 
            Continue optimizing to maximize returns!
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ========================================================================
    # TABS FOR DIFFERENT ANALYSES
    # ========================================================================
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🎯 Recommendations",
        "🔑 Keywords",
        "📍 Cities",
        "📦 Products",
        "📊 Charts",
        "📥 Export"
    ])
    
    # Perform analyses
    keyword_analysis = analyze_keywords(df, min_roi, min_ctr)
    city_analysis = analyze_cities(df, min_roi)
    product_analysis = analyze_products(df)
    recommendations = generate_recommendations(df, keyword_analysis, city_analysis, product_analysis)
    
    # ========================================================================
    # TAB 1: RECOMMENDATIONS
    # ========================================================================
    with tab1:
        st.header("🎯 Priority Recommendations")
        
        if recommendations:
            for i, rec in enumerate(recommendations, 1):
                with st.expander(f"{rec['priority']} - {rec['action']}", expanded=(i <= 3)):
                    st.markdown(f"**Category:** {rec['category']}")
                    st.markdown(f"**Impact:** {rec['impact']}")
                    
                    if 'keywords' in rec and rec['keywords']:
                        st.markdown(f"**Keywords:** {', '.join(rec['keywords'][:5])}")
                        if len(rec['keywords']) > 5:
                            st.markdown(f"*... and {len(rec['keywords']) - 5} more*")
                    
                    if 'cities' in rec and rec['cities']:
                        st.markdown(f"**Cities:** {', '.join(rec['cities'][:5])}")
                        if len(rec['cities']) > 5:
                            st.markdown(f"*... and {len(rec['cities']) - 5} more*")
                    
                    if 'product' in rec:
                        st.markdown(f"**Product:** {rec['product']}")
        else:
            st.info("No specific recommendations at this time. Your campaigns are performing well!")
        
        # Projected Impact
        st.markdown("---")
        st.subheader("💰 Projected 30-Day Impact")
        
        pause_keywords = keyword_analysis[keyword_analysis['Status'] == '🔴 Pause']
        potential_savings = pause_keywords['TOTAL_BUDGET_BURNT'].sum() * 30
        
        scale_keywords = keyword_analysis[keyword_analysis['Status'] == '🟢 Scale Up']
        potential_revenue = scale_keywords['TOTAL_GMV'].sum() * 3  # Assuming 3x scale
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Potential Monthly Savings", f"₹{potential_savings:,.0f}")
        with col2:
            st.metric("Potential Revenue Increase", f"₹{potential_revenue:,.0f}")
        with col3:
            st.metric("Net Impact", f"₹{potential_savings + potential_revenue:,.0f}")
    
    # ========================================================================
    # TAB 2: KEYWORDS
    # ========================================================================
    with tab2:
        st.header("🔑 Keyword Performance Analysis")
        
        # Summary cards
        col1, col2, col3 = st.columns(3)
        with col1:
            scale_count = len(keyword_analysis[keyword_analysis['Status'] == '🟢 Scale Up'])
            st.metric("Keywords to Scale", scale_count)
        with col2:
            monitor_count = len(keyword_analysis[keyword_analysis['Status'] == '🟡 Monitor'])
            st.metric("Keywords to Monitor", monitor_count)
        with col3:
            pause_count = len(keyword_analysis[keyword_analysis['Status'] == '🔴 Pause'])
            st.metric("Keywords to Pause", pause_count)
        
        # Filter options
        status_filter = st.multiselect(
            "Filter by Status",
            options=['🟢 Scale Up', '🟡 Monitor', '🔴 Pause'],
            default=['🟢 Scale Up', '🔴 Pause']
        )
        
        filtered_keywords = keyword_analysis[keyword_analysis['Status'].isin(status_filter)]
        
        # Display table
        st.dataframe(
            filtered_keywords[[
                'KEYWORD', 'Status', 'TOTAL_ROI', 'CTR', 'TOTAL_CONVERSIONS',
                'TOTAL_BUDGET_BURNT', 'TOTAL_GMV'
            ]].style.format({
                'TOTAL_ROI': '{:.2f}x',
                'CTR': '{:.2%}',
                'TOTAL_BUDGET_BURNT': '₹{:,.2f}',
                'TOTAL_GMV': '₹{:,.2f}',
                'TOTAL_CONVERSIONS': '{:.0f}'
            }),
            use_container_width=True,
            height=400
        )
        
        # Download button
        csv = filtered_keywords.to_csv(index=False)
        st.download_button(
            "📥 Download Keyword Analysis",
            csv,
            "keyword_analysis.csv",
            "text/csv"
        )
    
    # ========================================================================
    # TAB 3: CITIES
    # ========================================================================
    with tab3:
        st.header("📍 Geographic Performance Analysis")
        
        # Summary cards
        col1, col2, col3 = st.columns(3)
        with col1:
            expand_count = len(city_analysis[city_analysis['Status'] == '🟢 Expand'])
            st.metric("Cities to Expand", expand_count)
        with col2:
            monitor_count = len(city_analysis[city_analysis['Status'] == '🟡 Monitor'])
            st.metric("Cities to Monitor", monitor_count)
        with col3:
            reduce_count = len(city_analysis[city_analysis['Status'] == '🔴 Reduce'])
            st.metric("Cities to Reduce", reduce_count)
        
        # Top/Bottom performers
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏆 Top 10 Cities by ROI")
            top_cities = city_analysis.head(10)
            st.dataframe(
                top_cities[['CITY', 'TOTAL_ROI', 'TOTAL_BUDGET_BURNT', 'TOTAL_GMV', 'Status']].style.format({
                    'TOTAL_ROI': '{:.2f}x',
                    'TOTAL_BUDGET_BURNT': '₹{:,.2f}',
                    'TOTAL_GMV': '₹{:,.2f}'
                }),
                use_container_width=True
            )
        
        with col2:
            st.subheader("⚠️ Bottom 10 Cities by ROI")
            bottom_cities = city_analysis.tail(10)
            st.dataframe(
                bottom_cities[['CITY', 'TOTAL_ROI', 'TOTAL_BUDGET_BURNT', 'TOTAL_GMV', 'Status']].style.format({
                    'TOTAL_ROI': '{:.2f}x',
                    'TOTAL_BUDGET_BURNT': '₹{:,.2f}',
                    'TOTAL_GMV': '₹{:,.2f}'
                }),
                use_container_width=True
            )
        
        # Download button
        csv = city_analysis.to_csv(index=False)
        st.download_button(
            "📥 Download City Analysis",
            csv,
            "city_analysis.csv",
            "text/csv"
        )
    
    # ========================================================================
    # TAB 4: PRODUCTS
    # ========================================================================
    with tab4:
        st.header("📦 Product Performance Analysis")
        
        # Display product analysis
        st.dataframe(
            product_analysis.style.format({
                'TOTAL_ROI': '{:.2f}x',
                'TOTAL_CTR': '{:.2%}',
                'TOTAL_BUDGET_BURNT': '₹{:,.2f}',
                'TOTAL_GMV': '₹{:,.2f}',
                'TOTAL_CONVERSIONS': '{:.0f}'
            }),
            use_container_width=True,
            height=400
        )
        
        # Product comparison chart
        if len(product_analysis) > 1:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name='Budget Spent',
                x=product_analysis['PRODUCT_NAME'],
                y=product_analysis['TOTAL_BUDGET_BURNT'],
                yaxis='y',
                offsetgroup=1
            ))
            fig.add_trace(go.Bar(
                name='GMV Generated',
                x=product_analysis['PRODUCT_NAME'],
                y=product_analysis['TOTAL_GMV'],
                yaxis='y',
                offsetgroup=2
            ))
            fig.add_trace(go.Scatter(
                name='ROI',
                x=product_analysis['PRODUCT_NAME'],
                y=product_analysis['TOTAL_ROI'],
                yaxis='y2',
                mode='lines+markers',
                marker=dict(size=10, color='red')
            ))
            
            fig.update_layout(
                title='Product Performance: Budget vs GMV vs ROI',
                yaxis=dict(title='Amount (₹)'),
                yaxis2=dict(title='ROI (x)', overlaying='y', side='right'),
                barmode='group',
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Download button
        csv = product_analysis.to_csv(index=False)
        st.download_button(
            "📥 Download Product Analysis",
            csv,
            "product_analysis.csv",
            "text/csv"
        )
    
    # ========================================================================
    # TAB 5: CHARTS
    # ========================================================================
    with tab5:
        st.header("📊 Visual Analytics")
        
        # ROI Distribution
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("ROI Distribution by Keyword")
            fig = px.histogram(
                keyword_analysis,
                x='TOTAL_ROI',
                nbins=20,
                title='Keyword ROI Distribution',
                labels={'TOTAL_ROI': 'ROI (x)'},
                color_discrete_sequence=['#1f77b4']
            )
            fig.add_vline(x=min_roi, line_dash="dash", line_color="red", 
                         annotation_text=f"Min ROI: {min_roi}x")
            fig.add_vline(x=2.0, line_dash="dash", line_color="green", 
                         annotation_text="Target: 2.0x")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Budget Allocation by City")
            top_10_cities = city_analysis.head(10)
            fig = px.pie(
                top_10_cities,
                values='TOTAL_BUDGET_BURNT',
                names='CITY',
                title='Top 10 Cities by Budget'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Performance scatter
        st.subheader("Keyword Performance: ROI vs Spend")
        fig = px.scatter(
            keyword_analysis,
            x='TOTAL_BUDGET_BURNT',
            y='TOTAL_ROI',
            size='TOTAL_CONVERSIONS',
            color='Status',
            hover_data=['KEYWORD', 'CTR'],
            title='Keyword Performance Matrix',
            labels={
                'TOTAL_BUDGET_BURNT': 'Budget Spent (₹)',
                'TOTAL_ROI': 'ROI (x)'
            },
            color_discrete_map={
                '🟢 Scale Up': 'green',
                '🟡 Monitor': 'orange',
                '🔴 Pause': 'red'
            }
        )
        fig.add_hline(y=min_roi, line_dash="dash", line_color="red", 
                     annotation_text=f"Min ROI: {min_roi}x")
        st.plotly_chart(fig, use_container_width=True)
        
        # City ROI map
        st.subheader("City Performance Heatmap")
        fig = px.bar(
            city_analysis.head(20),
            x='TOTAL_ROI',
            y='CITY',
            orientation='h',
            title='Top 20 Cities by ROI',
            labels={'TOTAL_ROI': 'ROI (x)', 'CITY': 'City'},
            color='TOTAL_ROI',
            color_continuous_scale='RdYlGn'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # ========================================================================
    # TAB 6: EXPORT
    # ========================================================================
    with tab6:
        st.header("📥 Export Reports")
        
        st.markdown("""
        Download comprehensive reports and data exports for further analysis or sharing with your team.
        """)
        
        # Generate text report
        report = f"""
CAMPAIGN OPTIMIZATION REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*80}

EXECUTIVE SUMMARY
-----------------
Total Budget Spent: ₹{metrics['total_spend']:,.2f}
Total GMV Generated: ₹{metrics['total_gmv']:,.2f}
Average ROI: {metrics['avg_roi']:.2f}x
Total Conversions: {metrics['total_conversions']:.0f}
Average CTR: {metrics['avg_ctr']*100:.2f}%
Cost per Conversion: ₹{metrics['cost_per_conversion']:.2f}

PERFORMANCE STATUS
------------------
ROI Status: {'✅ Above Target' if metrics['avg_roi'] >= 2.0 else '⚠️ Below Target'}
CTR Status: {'✅ Above Target' if metrics['avg_ctr'] >= 0.05 else '⚠️ Below Target'}
Profitability: {'✅ Profitable' if metrics['total_gmv'] > metrics['total_spend'] else '❌ Loss-making'}

PRIORITY RECOMMENDATIONS ({len(recommendations)})
{'='*80}
"""
        
        for i, rec in enumerate(recommendations, 1):
            report += f"\n{i}. [{rec['priority']}] {rec['category']}\n"
            report += f"   Action: {rec['action']}\n"
            report += f"   Impact: {rec['impact']}\n"
            if 'keywords' in rec and rec['keywords']:
                report += f"   Keywords: {', '.join(rec['keywords'][:5])}\n"
            if 'cities' in rec and rec['cities']:
                report += f"   Cities: {', '.join(rec['cities'][:5])}\n"
            report += "\n"
        
        report += f"\n{'='*80}\n"
        report += "TOP 10 PERFORMING KEYWORDS\n"
        report += "-" * 80 + "\n"
        for _, row in keyword_analysis.head(10).iterrows():
            report += f"  • {row['KEYWORD']}: ROI {row['TOTAL_ROI']:.2f}x, "
            report += f"Spend ₹{row['TOTAL_BUDGET_BURNT']:.2f}, "
            report += f"GMV ₹{row['TOTAL_GMV']:.2f}\n"
        
        report += f"\n{'='*80}\n"
        report += "TOP 10 PERFORMING CITIES\n"
        report += "-" * 80 + "\n"
        for _, row in city_analysis.head(10).iterrows():
            report += f"  • {row['CITY']}: ROI {row['TOTAL_ROI']:.2f}x, "
            report += f"Spend ₹{row['TOTAL_BUDGET_BURNT']:.2f}\n"
        
        # Download buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.download_button(
                "📄 Download Full Report (TXT)",
                report,
                f"campaign_report_{datetime.now().strftime('%Y%m%d')}.txt",
                "text/plain"
            )
        
        with col2:
            # Create Excel export
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                keyword_analysis.to_excel(writer, sheet_name='Keywords', index=False)
                city_analysis.to_excel(writer, sheet_name='Cities', index=False)
                product_analysis.to_excel(writer, sheet_name='Products', index=False)
                
                # Summary sheet
                summary_df = pd.DataFrame({
                    'Metric': ['Total Spend', 'Total GMV', 'Avg ROI', 'Conversions', 'Avg CTR', 'Cost/Conv'],
                    'Value': [
                        f"₹{metrics['total_spend']:,.2f}",
                        f"₹{metrics['total_gmv']:,.2f}",
                        f"{metrics['avg_roi']:.2f}x",
                        f"{metrics['total_conversions']:.0f}",
                        f"{metrics['avg_ctr']*100:.2f}%",
                        f"₹{metrics['cost_per_conversion']:.2f}"
                    ]
                })
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            output.seek(0)
            st.download_button(
                "📊 Download Excel Report",
                output,
                f"campaign_analysis_{datetime.now().strftime('%Y%m%d')}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col3:
            # Export recommendations as JSON
            recommendations_json = json.dumps(recommendations, indent=2)
            st.download_button(
                "🔧 Download Recommendations (JSON)",
                recommendations_json,
                f"recommendations_{datetime.now().strftime('%Y%m%d')}.json",
                "application/json"
            )

else:
    # Landing page when no data is loaded
    st.info("👈 Please upload your campaign data files in the sidebar to get started.")
    
    st.markdown("""
    ## Welcome to Campaign Optimization Dashboard
    
    This tool helps you maximize your marketing ROI through data-driven insights and automated recommendations.
    
    ### Features:
    - 📊 **Real-time Performance Metrics** - Track ROI, CTR, conversions, and more
    - 🎯 **Automated Recommendations** - Get actionable suggestions based on your data
    - 🔑 **Keyword Analysis** - Identify winners and losers
    - 📍 **Geographic Insights** - Optimize by city/region
    - 📦 **Product Performance** - Focus on high-ROI products
    - 📈 **Visual Analytics** - Interactive charts and graphs
    - 📥 **Export Reports** - Download comprehensive reports in multiple formats
    
    ### Getting Started:
    1. Upload your **Granular file** (IM_GRANULAR_*.csv) in the sidebar
    2. Optionally upload **Search Query file** for deeper insights
    3. Adjust optimization thresholds in the sidebar settings
    4. Explore the dashboard tabs for detailed analysis
    5. Download reports and implement recommendations
    
    ### Sample Data Format:
    Your CSV files should have these key columns:
    - CAMPAIGN_NAME, KEYWORD, CITY, PRODUCT_NAME
    - TOTAL_ROI, TOTAL_CTR, TOTAL_BUDGET_BURNT
    - TOTAL_CONVERSIONS, TOTAL_GMV, TOTAL_IMPRESSIONS
    
    ---
    
    📚 For detailed documentation, check the README.md file included with this application.
    """)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 1rem;'>
    <p>Campaign Optimization Dashboard v1.0 | Built with Streamlit | 
    <a href='#' style='color: #1f77b4;'>Documentation</a> | 
    <a href='#' style='color: #1f77b4;'>Support</a></p>
</div>
""", unsafe_allow_html=True)
