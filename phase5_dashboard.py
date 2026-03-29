"""
Phase 5: Streamlit Dashboard
Pharma Territory Optimization Visualizer
Role: Full-Stack Data App Developer

A interactive dashboard to visualize:
- Doctor assignments across 50 territories
- KPI metrics (Targeting Score, Efficiency Gain, Platinum Coverage)
- Interactive map with filtering
- Workload distribution charts
"""

import streamlit as st
import pandas as pd
import numpy as np
import folium
from streamlit_folium import st_folium
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Pharma Territory Optimizer",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# CONFIGURATION
# ============================================================
@st.cache_data
def get_connection_config():
    return {
        'account': 'rwcfeut-wb78109',
        'user': 'SANDHIYABK',
        'password': 'k66T4jKv_LQDHXe',
        'warehouse': 'COMPUTE_WH',
        'database': 'PHARMA_OS_DB',
        'schema': 'SALES_OPS'
    }

# ============================================================
# DATA LOADING
# ============================================================
@st.cache_data(ttl=3600)
def load_data():
    """Load all data from Snowflake"""
    config = get_connection_config()
    
    conn = snowflake.connector.connect(
        account=config['account'],
        user=config['user'],
        password=config['password'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema']
    )
    
    # Load optimized territories
    query_optimized = """
        SELECT 
            ot.DOCTOR_ID,
            ot.OPTIMIZED_TERRITORY,
            ot.TIER,
            ot.TARGETING_SCORE,
            dd.REGION,
            dd.DOCTOR_NAME,
            dd.SPECIALTY,
            dd.LATITUDE,
            dd.LONGITUDE,
            dd.POTENTIAL_MULTIPLIER,
            dp.DECILE,
            dp.RANK_IN_DECILE,
            dp.OVERALL_RANK
        FROM PHARMA_OS_DB.SALES_OPS.FINAL_OPTIMIZED_TERRITORIES ot
        LEFT JOIN PHARMA_OS_DB.SALES_OPS.DIM_DOCTORS dd
            ON ot.DOCTOR_ID = dd.DOCTOR_ID
        LEFT JOIN PHARMA_OS_DB.SALES_OPS.STG_DOCTOR_PRIORITY dp
            ON ot.DOCTOR_ID = dp.DOCTOR_ID
    """
    
    df = pd.read_sql(query_optimized, conn)
    conn.close()
    
    # Extract territory number
    df['TERRITORY_NUM'] = df['OPTIMIZED_TERRITORY'].apply(
        lambda x: int(x.split('_')[1]) if x else 0
    )
    
    return df

@st.cache_data(ttl=3600)
def load_phase1_baseline():
    """Load Phase 1 baseline score"""
    config = get_connection_config()
    
    conn = snowflake.connector.connect(
        account=config['account'],
        user=config['user'],
        password=config['password'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema']
    )
    
    query = """
        SELECT SUM(TARGETING_SCORE) as PHASE1_SCORE
        FROM PHARMA_OS_DB.SALES_OPS.STG_DOCTOR_PRIORITY
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df['PHASE1_SCORE'].iloc[0]

# ============================================================
# SIDEBAR FILTERS
# ============================================================
def create_sidebar_filters(df):
    """Create sidebar filter controls"""
    st.sidebar.header("🎛️ Filters")
    
    # Region filter
    regions = ['All'] + sorted(df['REGION'].dropna().unique().tolist())
    selected_region = st.sidebar.selectbox("Region", regions)
    
    # Tier filter
    tiers = ['All'] + ['Platinum', 'Gold', 'Silver', 'Bronze']
    selected_tier = st.sidebar.selectbox("Doctor Tier", tiers)
    
    # Specialty filter
    specialties = ['All'] + sorted(df['SPECIALTY'].dropna().unique().tolist())
    selected_specialty = st.sidebar.selectbox("Specialty", specialties)
    
    # Territory filter
    territories = ['All'] + [f"Territory {i}" for i in range(1, 51)]
    selected_territory = st.sidebar.selectbox("Territory", territories)
    
    # Apply filters
    filtered_df = df.copy()
    
    if selected_region != 'All':
        filtered_df = filtered_df[filtered_df['REGION'] == selected_region]
    
    if selected_tier != 'All':
        filtered_df = filtered_df[filtered_df['TIER'] == selected_tier]
    
    if selected_specialty != 'All':
        filtered_df = filtered_df[filtered_df['SPECIALTY'] == selected_specialty]
    
    if selected_territory != 'All':
        territory_num = int(selected_territory.split()[1])
        filtered_df = filtered_df[filtered_df['TERRITORY_NUM'] == territory_num]
    
    return filtered_df, {
        'region': selected_region,
        'tier': selected_tier,
        'specialty': selected_specialty,
        'territory': selected_territory
    }

# ============================================================
# KPI METRICS
# ============================================================
def display_kpis(df, phase1_score):
    """Display KPI metric cards"""
    col1, col2, col3, col4 = st.columns(4)
    
    total_score = df['TARGETING_SCORE'].sum()
    optimized_score = total_score  # Already from optimized table
    efficiency_gain = ((optimized_score - phase1_score) / phase1_score) * 100
    
    platinum_count = len(df[df['TIER'] == 'Platinum'])
    total_count = len(df)
    platinum_coverage = (platinum_count / total_count * 100) if total_count > 0 else 0
    
    total_doctors = len(df)
    total_territories = df['OPTIMIZED_TERRITORY'].nunique()
    
    with col1:
        st.metric(
            label="💰 Total Potential Captured",
            value=f"{total_score:,.0f}",
            delta=f"{efficiency_gain:+.1f}% vs Baseline"
        )
    
    with col2:
        st.metric(
            label="📈 Efficiency Gain",
            value=f"{efficiency_gain:+.1f}%",
            delta="vs K-Means"
        )
    
    with col3:
        st.metric(
            label="🏆 Platinum Coverage",
            value=f"{platinum_coverage:.1f}%",
            delta=f"{platinum_count} doctors"
        )
    
    with col4:
        st.metric(
            label="👨‍⚕️ Total Doctors",
            value=f"{total_doctors:,}",
            delta=f"{total_territories} territories"
        )

# ============================================================
# INTERACTIVE MAP
# ============================================================
def create_map(df, region_filter):
    """Create Folium map with doctor locations"""
    
    # Set map center based on region
    if region_filter == 'Madurai':
        center = [9.9252, 78.1198]
        zoom = 10
    elif region_filter == 'Chennai':
        center = [13.0827, 80.2707]
        zoom = 11
    else:
        center = [11.5, 79.2]
        zoom = 6
    
    m = folium.Map(location=center, zoom_start=zoom, tiles='cartodbpositron')
    
    # Color mapping for tiers
    tier_colors = {
        'Platinum': '#E5E4E2',
        'Gold': '#FFD700',
        'Silver': '#C0C0C0',
        'Bronze': '#CD7F32'
    }
    
    # Add markers
    for _, row in df.iterrows():
        tier = row['TIER'] if pd.notna(row['TIER']) else 'Bronze'
        color = tier_colors.get(tier, 'gray')
        
        popup_html = f"""
        <b>🏥 {row['DOCTOR_NAME']}</b><br>
        <b>ID:</b> {row['DOCTOR_ID']}<br>
        <b>Specialty:</b> {row['SPECIALTY']}<br>
        <b>Tier:</b> {tier}<br>
        <b>Score:</b> {row['TARGETING_SCORE']:.2f}<br>
        <b>Territory:</b> {row['OPTIMIZED_TERRITORY']}
        """
        
        folium.CircleMarker(
            location=[row['LATITUDE'], row['LONGITUDE']],
            radius=6,
            popup=folium.Popup(popup_html, max_width=300),
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7,
            weight=2
        ).add_to(m)
    
    # Add legend
    legend_html = '''
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; 
                background-color: white; padding: 10px; border: 2px solid gray;
                border-radius: 5px; font-size: 14px;">
        <b>🏆 Doctor Tiers</b><br>
        <i style="background:#E5E4E2; width:15px; height:15px; display:inline-block; border-radius:50%;"></i> Platinum<br>
        <i style="background:#FFD700; width:15px; height:15px; display:inline-block; border-radius:50%;"></i> Gold<br>
        <i style="background:#C0C0C0; width:15px; height:15px; display:inline-block; border-radius:50%;"></i> Silver<br>
        <i style="background:#CD7F32; width:15px; height:15px; display:inline-block; border-radius:50%;"></i> Bronze
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m

# ============================================================
# WORKLOAD CHART
# ============================================================
def create_workload_chart(df):
    """Create workload distribution chart"""
    
    workload = df.groupby('OPTIMIZED_TERRITORY').size().reset_index(name='DOCTOR_COUNT')
    workload['REGION'] = workload['OPTIMIZED_TERRITORY'].apply(lambda x: x.split('_')[0])
    workload = workload.sort_values('DOCTOR_COUNT', ascending=False)
    
    fig = px.bar(
        workload,
        x='OPTIMIZED_TERRITORY',
        y='DOCTOR_COUNT',
        color='REGION',
        color_discrete_map={'Chennai': '#1f77b4', 'Madurai': '#ff7f0e'},
        title='👥 Workload Distribution: Doctors per Territory (Target: 80-120)',
        labels={'OPTIMIZED_TERRITORY': 'Territory', 'DOCTOR_COUNT': 'Number of Doctors'}
    )
    
    fig.add_hline(y=80, line_dash="dash", line_color="green", annotation_text="Min (80)")
    fig.add_hline(y=120, line_dash="dash", line_color="red", annotation_text="Max (120)")
    
    fig.update_layout(
        height=400,
        xaxis_tickangle=-45,
        showlegend=True
    )
    
    return fig

# ============================================================
# TIER DISTRIBUTION CHART
# ============================================================
def create_tier_chart(df):
    """Create tier distribution pie chart"""
    
    tier_counts = df.groupby('TIER').size().reset_index(name='COUNT')
    tier_order = ['Platinum', 'Gold', 'Silver', 'Bronze']
    tier_counts['TIER'] = pd.Categorical(tier_counts['TIER'], categories=tier_order, ordered=True)
    tier_counts = tier_counts.sort_values('TIER')
    
    fig = px.pie(
        tier_counts,
        values='COUNT',
        names='TIER',
        title='🥇 Doctor Tier Distribution',
        color='TIER',
        color_discrete_map={
            'Platinum': '#E5E4E2',
            'Gold': '#FFD700',
            'Silver': '#C0C0C0',
            'Bronze': '#CD7F32'
        },
        hole=0.4
    )
    
    fig.update_layout(height=350)
    
    return fig

# ============================================================
# SPECIALTY ANALYSIS CHART
# ============================================================
def create_specialty_chart(df):
    """Create specialty performance chart"""
    
    specialty_stats = df.groupby('SPECIALTY').agg({
        'TARGETING_SCORE': 'sum',
        'DOCTOR_ID': 'count'
    }).reset_index()
    specialty_stats.columns = ['SPECIALTY', 'TOTAL_SCORE', 'DOCTOR_COUNT']
    specialty_stats = specialty_stats.sort_values('TOTAL_SCORE', ascending=True)
    
    fig = px.bar(
        specialty_stats,
        x='SPECIALTY',
        y='TOTAL_SCORE',
        title='💊 Sales Performance by Specialty',
        color='TOTAL_SCORE',
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(
        height=350,
        xaxis_tickangle=-45,
        showlegend=False
    )
    
    return fig

# ============================================================
# TERRITORY PERFORMANCE CHART
# ============================================================
def create_territory_performance_chart(df):
    """Create territory performance chart"""
    
    territory_stats = df.groupby(['OPTIMIZED_TERRITORY', 'REGION']).agg({
        'TARGETING_SCORE': 'sum',
        'DOCTOR_ID': 'count'
    }).reset_index()
    territory_stats.columns = ['TERRITORY', 'REGION', 'TOTAL_SCORE', 'DOCTOR_COUNT']
    territory_stats = territory_stats.sort_values('TOTAL_SCORE', ascending=False).head(20)
    
    fig = px.bar(
        territory_stats,
        x='TERRITORY',
        y='TOTAL_SCORE',
        color='REGION',
        color_discrete_map={'Chennai': '#1f77b4', 'Madurai': '#ff7f0e'},
        title='🏆 Top 20 Territories by Performance'
    )
    
    fig.update_layout(
        height=350,
        xaxis_tickangle=-45
    )
    
    return fig

# ============================================================
# DATA TABLE
# ============================================================
def display_data_table(df):
    """Display filtered data table"""
    
    st.subheader("📋 Doctor Assignments")
    
    display_df = df[[
        'DOCTOR_ID', 'DOCTOR_NAME', 'SPECIALTY', 'REGION', 
        'TIER', 'TARGETING_SCORE', 'OPTIMIZED_TERRITORY', 'OVERALL_RANK'
    ]].copy()
    
    display_df = display_df.sort_values('OVERALL_RANK')
    
    st.dataframe(
        display_df,
        column_config={
            "DOCTOR_ID": "Doctor ID",
            "DOCTOR_NAME": "Name",
            "SPECIALTY": st.column_config.TextColumn("Specialty"),
            "REGION": st.column_config.TextColumn("Region"),
            "TIER": st.column_config.TextColumn("Tier"),
            "TARGETING_SCORE": st.column_config.NumberColumn("Score", format="%.2f"),
            "OPTIMIZED_TERRITORY": "Territory",
            "OVERALL_RANK": "Rank"
        },
        hide_index=True,
        use_container_width=True,
        height=400
    )

# ============================================================
# MAIN APP
# ============================================================
def main():
    # Header
    st.title("🏥 Pharma Sales Territory Optimizer")
    st.markdown("""
    <div style="background-color: #f0f2f6; padding: 15px; border-radius: 10px; margin-bottom: 20px;">
        <h3 style="color: #1f77b4;">Pharma OS - Territory Optimization Dashboard</h3>
        <p>Interactive visualization of 5,000 doctor assignments across 50 territories in Chennai & Madurai regions.</p>
    </div>
    """, unsafe_allow_html=True)
    
    try:
        # Load data
        df = load_data()
        phase1_score = load_phase1_baseline()
        
        # Create filters
        filtered_df, filters = create_sidebar_filters(df)
        
        # Display KPIs
        display_kpis(filtered_df, phase1_score)
        
        # Tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Map View", "📊 Analytics", "📈 Charts", "📋 Data"])
        
        with tab1:
            st.subheader("🗺️ Doctor Locations Map")
            st.markdown("*" + str(len(filtered_df)) + " doctors shown based on current filters*")
            
            m = create_map(filtered_df, filters['region'])
            st_folium(m, width='100%', height=600)
        
        with tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                st.plotly_chart(create_tier_chart(filtered_df), use_container_width=True)
            
            with col2:
                st.plotly_chart(create_specialty_chart(filtered_df), use_container_width=True)
            
            st.plotly_chart(create_workload_chart(filtered_df), use_container_width=True)
        
        with tab3:
            st.plotly_chart(create_territory_performance_chart(filtered_df), use_container_width=True)
        
        with tab4:
            display_data_table(filtered_df)
        
        # Footer
        st.markdown("""
        <div style="text-align: center; margin-top: 50px; padding: 20px; color: gray;">
            <p>Pharma Territory Optimizer | Built with Streamlit + Snowflake</p>
            <p style="font-size: 12px;">Phase 1: Data Synthesis | Phase 2: Snowflake Migration | Phase 3: Doctor Tiering | Phase 4: OR Optimization</p>
        </div>
        """, unsafe_allow_html=True)
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.info("Please ensure you have:")
        st.code("""
        1. Updated the password in get_connection_config()
        2. Run Phase 2 and Phase 4 to populate Snowflake tables
        3. Verified Snowflake connection credentials
        """, language="python")

if __name__ == "__main__":
    main()
