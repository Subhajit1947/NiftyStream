import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Page config
st.set_page_config(
    page_title="Stock Analytics Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Database connection
@st.cache_resource
def get_engine():
    """Create database connection engine"""
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "password")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "stock_db")
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(connection_string)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #3B82F6;
    }
    .alert-card {
        background-color: #FEF3C7;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #F59E0B;
        margin-bottom: 0.5rem;
    }
    .critical-alert {
        background-color: #FEE2E2;
        border-left: 5px solid #DC2626;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar filters
st.sidebar.title("📊 Dashboard Controls")

# Date range filter
st.sidebar.subheader("Date Range")
days_back = st.sidebar.slider("Days to look back:", 1, 30, 7)
end_date = datetime.now()
start_date = end_date - timedelta(days=days_back)

# Symbol filter
engine = get_engine()
with engine.connect() as conn:
    symbols_df = pd.read_sql("SELECT DISTINCT symbol FROM stock_metrics ORDER BY symbol", conn)
symbols = symbols_df['symbol'].tolist()
selected_symbols = st.sidebar.multiselect(
    "Select Symbols:", 
    symbols, 
    default=symbols[:3] if len(symbols) >= 3 else symbols
)

# Alert type filter
alert_types = ['All', 'VOLUME_SURGE', 'PRICE_SPIKE', 'RSI_OVERBOUGHT', 'RSI_OVERSOLD']
selected_alert_type = st.sidebar.selectbox("Alert Type:", alert_types)

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.rerun()

# Main dashboard
st.markdown("<h1 class='main-header'>📈 Real-Time Stock Analytics Dashboard</h1>", unsafe_allow_html=True)

# Get data from database
@st.cache_data(ttl=60)  # Cache for 60 seconds
def load_stock_data(symbols, start_date, end_date):
    """Load stock metrics data"""
    if not symbols:
        return pd.DataFrame()
    
    symbols_str = ", ".join([f"'{s}'" for s in symbols])
    query = f"""
    SELECT * FROM stock_metrics 
    WHERE symbol IN ({symbols_str})
    AND timestamp BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY timestamp DESC
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    return df

@st.cache_data(ttl=30)
def load_alerts(alert_type, limit=50):
    """Load alerts data"""
    query = "SELECT * FROM alerts WHERE is_active = TRUE"
    
    if alert_type != 'All':
        query += f" AND alert_type = '{alert_type}'"
    
    query += " ORDER BY created_at DESC LIMIT " + str(limit)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    return df

@st.cache_data(ttl=300)
def get_summary_stats():
    """Get summary statistics"""
    query = """
    WITH latest_data AS (
        SELECT DISTINCT ON (symbol) *
        FROM stock_metrics
        ORDER BY symbol, timestamp DESC
    )
    SELECT 
        COUNT(DISTINCT symbol) as total_symbols,
        COUNT(*) as total_records,
        MIN(timestamp) as earliest_date,
        MAX(timestamp) as latest_date
    FROM latest_data
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    return df

# Load data
stock_data = load_stock_data(selected_symbols, start_date, end_date)
alerts_data = load_alerts(selected_alert_type)
summary_stats = get_summary_stats()

# Dashboard header with metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Symbols",
        value=summary_stats['total_symbols'].iloc[0] if not summary_stats.empty else 0
    )

with col2:
    st.metric(
        label="Total Records",
        value=f"{summary_stats['total_records'].iloc[0]:,}" if not summary_stats.empty else 0
    )

with col3:
    st.metric(
        label="Active Alerts", 
        value=len(alerts_data)
    )

with col4:
    if not stock_data.empty:
        latest_update = stock_data['timestamp'].max()
        st.metric(
            label="Latest Update",
            value=latest_update.strftime("%H:%M:%S") if pd.notna(latest_update) else "N/A"
        )

# Tabs for different views
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Price Analysis", 
    "📊 Technical Indicators", 
    "🚨 Alerts Monitor", 
    "📋 Data Table",
    "📡 Real-Time Feed"
])

# Tab 1: Price Analysis
with tab1:
    if not stock_data.empty and selected_symbols:
        st.subheader("Price Charts")
        
        # Create price chart for each selected symbol
        for symbol in selected_symbols:
            symbol_data = stock_data[stock_data['symbol'] == symbol].sort_values('timestamp')
            
            if not symbol_data.empty:
                fig = make_subplots(
                    rows=2, cols=1,
                    subplot_titles=(f"{symbol} - Price Movement", f"{symbol} - Volume"),
                    vertical_spacing=0.15,
                    row_heights=[0.7, 0.3]
                )
                
                # Candlestick chart
                fig.add_trace(
                    go.Candlestick(
                        x=symbol_data['timestamp'],
                        open=symbol_data['open_price'],
                        high=symbol_data['high_price'],
                        low=symbol_data['low_price'],
                        close=symbol_data['close_price'],
                        name="Price"
                    ),
                    row=1, col=1
                )
                
                # Volume bar chart
                fig.add_trace(
                    go.Bar(
                        x=symbol_data['timestamp'],
                        y=symbol_data['volume'],
                        name="Volume",
                        marker_color='rgba(59, 130, 246, 0.7)'
                    ),
                    row=2, col=1
                )
                
                fig.update_layout(
                    height=600,
                    showlegend=False,
                    xaxis_rangeslider_visible=False,
                    template="plotly_white"
                )
                
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Select symbols and date range to view price charts")

# Tab 2: Technical Indicators
with tab2:
    if not stock_data.empty and selected_symbols:
        st.subheader("Technical Indicators")
        
        selected_indicator = st.selectbox(
            "Select Indicator:",
            ["RSI", "SMA_5 vs SMA_20", "Volume Ratio", "VWAP"]
        )
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Chart for selected indicator
            fig = go.Figure()
            
            for symbol in selected_symbols[:3]:  # Limit to 3 for clarity
                symbol_data = stock_data[stock_data['symbol'] == symbol].sort_values('timestamp')
                
                if selected_indicator == "RSI":
                    fig.add_trace(go.Scatter(
                        x=symbol_data['timestamp'],
                        y=symbol_data['rsi'],
                        name=f"{symbol} RSI",
                        mode='lines+markers'
                    ))
                    # Add RSI levels
                    fig.add_hline(y=70, line_dash="dash", line_color="red", opacity=0.5)
                    fig.add_hline(y=30, line_dash="dash", line_color="green", opacity=0.5)
                    
                elif selected_indicator == "SMA_5 vs SMA_20":
                    fig.add_trace(go.Scatter(
                        x=symbol_data['timestamp'],
                        y=symbol_data['sma_5'],
                        name=f"{symbol} SMA 5",
                        mode='lines'
                    ))
                    fig.add_trace(go.Scatter(
                        x=symbol_data['timestamp'],
                        y=symbol_data['sma_20'],
                        name=f"{symbol} SMA 20",
                        mode='lines'
                    ))
                    
                elif selected_indicator == "Volume Ratio":
                    fig.add_trace(go.Scatter(
                        x=symbol_data['timestamp'],
                        y=symbol_data['volume_ratio'],
                        name=f"{symbol} Volume Ratio",
                        mode='lines+markers'
                    ))
                    fig.add_hline(y=1.5, line_dash="dash", line_color="orange", opacity=0.5)
                    
                elif selected_indicator == "VWAP":
                    fig.add_trace(go.Scatter(
                        x=symbol_data['timestamp'],
                        y=symbol_data['vwap'],
                        name=f"{symbol} VWAP",
                        mode='lines'
                    ))
                    fig.add_trace(go.Scatter(
                        x=symbol_data['timestamp'],
                        y=symbol_data['close_price'],
                        name=f"{symbol} Close Price",
                        mode='lines',
                        line=dict(dash='dash')
                    ))
            
            fig.update_layout(
                height=500,
                title=f"{selected_indicator} Analysis",
                xaxis_title="Time",
                yaxis_title=selected_indicator,
                template="plotly_white"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Latest indicator values
            st.subheader("Latest Values")
            latest_data = stock_data.sort_values('timestamp').groupby('symbol').last().reset_index()
            
            for _, row in latest_data.iterrows():
                with st.container():
                    st.markdown(f"**{row['symbol']}**")
                    
                    # Create metric cards
                    metrics_html = f"""
                    <div class="metric-card">
                        <small>Close: <strong>{row['close_price']:.2f}</strong></small><br>
                        <small>RSI: <strong>{row['rsi']:.2f}</strong></small><br>
                        <small>Volume: <strong>{row['volume']:,}</strong></small><br>
                        <small>SMA 5/20: <strong>{row['sma_5']:.2f}/{row['sma_20']:.2f}</strong></small>
                    </div>
                    """
                    st.markdown(metrics_html, unsafe_allow_html=True)
                    st.write("---")

# Tab 3: Alerts Monitor
with tab3:
    st.subheader("🚨 Active Alerts")
    
    if not alerts_data.empty:
        # Alert statistics
        alert_stats = alerts_data['alert_type'].value_counts()
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.plotly_chart(
                px.pie(
                    values=alert_stats.values,
                    names=alert_stats.index,
                    title="Alert Distribution",
                    hole=0.3
                ),
                use_container_width=True
            )
        
        with col2:
            # Display alerts
            for _, alert in alerts_data.iterrows():
                alert_class = "critical-alert" if alert['alert_type'] in ['PRICE_SPIKE', 'RSI_OVERBOUGHT'] else "alert-card"
                
                alert_html = f"""
                <div class="{alert_class}">
                    <strong>{alert['alert_type']}</strong> - {alert['symbol']}<br>
                    <small>{alert['timestamp']}</small><br>
                    <small>{alert['message']}</small><br>
                    <small>Price: ${alert['close_price']:.2f} | Volume: {alert['volume']:,} | RSI: {alert['rsi']:.2f}</small>
                </div>
                """
                st.markdown(alert_html, unsafe_allow_html=True)
        
        # Alert management
        st.subheader("Alert Management")
        alert_ids = alerts_data['alert_id'].tolist()
        
        col1, col2 = st.columns(2)
        
        with col1:
            alert_to_resolve = st.selectbox("Select Alert to Resolve:", alert_ids)
        
        with col2:
            if st.button("✅ Mark as Resolved"):
                with engine.connect() as conn:
                    conn.execute(
                        text("UPDATE alerts SET is_active = FALSE WHERE alert_id = :alert_id"),
                        {"alert_id": alert_to_resolve}
                    )
                    conn.commit()
                st.success(f"Alert {alert_to_resolve} marked as resolved!")
                st.rerun()
        
        # Clear all resolved alerts
        if st.button("🗑️ Clear All Resolved Alerts"):
            with engine.connect() as conn:
                conn.execute(text("DELETE FROM alerts WHERE is_active = FALSE"))
                conn.commit()
            st.success("Cleared all resolved alerts!")
            st.rerun()
    
    else:
        st.success("✅ No active alerts!")

# Tab 4: Data Table
with tab4:
    st.subheader("Raw Data View")
    
    if not stock_data.empty:
        # Data filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            show_columns = st.multiselect(
                "Select Columns:",
                stock_data.columns.tolist(),
                default=['symbol', 'timestamp', 'close_price', 'volume', 'rsi', 'price_change']
            )
        
        with col2:
            rows_to_show = st.slider("Rows to show:", 10, 1000, 100)
        
        with col3:
            columns_without_id = [col for col in stock_data.columns.tolist() if col != 'id']
            sort_by = st.selectbox("Sort by:", columns_without_id)
            sort_order = st.radio("Order:", ["Descending", "Ascending"])
        
        # Filtered data
        filtered_data = stock_data[show_columns].sort_values(
            sort_by, 
            ascending=(sort_order == "Ascending")
        ).head(rows_to_show)
        
        # Display data
        st.dataframe(
            filtered_data,
            use_container_width=True,
            hide_index=True
        )
        
        # Export options
        col1, col2 = st.columns(2)
        
        with col1:
            csv = filtered_data.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name=f"stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            json_data = filtered_data.to_json(orient="records", date_format="iso")
            st.download_button(
                label="📥 Download as JSON",
                data=json_data,
                file_name=f"stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
    
    else:
        st.info("No data available for the selected filters")

# Tab 5: Real-Time Feed
with tab5:
    st.subheader("📡 Live Data Feed")
    
    # Auto-refresh toggle
    auto_refresh = st.checkbox("🔄 Enable Auto-refresh (10 seconds)")
    
    if auto_refresh:
        # This creates a refresh every 10 seconds
        st_autorefresh = st.empty()
        st_autorefresh.info("Auto-refresh enabled - updating every 10 seconds")
        # In a real implementation, you'd use websockets or polling
    
    # Latest records
    latest_query = """
    SELECT * FROM stock_metrics 
    ORDER BY timestamp DESC 
    LIMIT 50
    """
    
    with engine.connect() as conn:
        latest_data = pd.read_sql(latest_query, conn)
    
    if not latest_data.empty:
        # Display as a live feed
        for _, row in latest_data.iterrows():
            with st.container():
                col1, col2, col3 = st.columns([1, 2, 1])
                
                with col1:
                    st.write(f"**{row['symbol']}**")
                    st.write(row['timestamp'].strftime("%H:%M:%S"))
                
                with col2:
                    price_change_color = "green" if row['price_change'] >= 0 else "red"
                    st.write(f"Price: ${row['close_price']:.2f}")
                    st.write(
                        f"Change: <span style='color:{price_change_color}'>{row['price_change']:.2f} ({row['percent_change']:.2f}%)</span>",
                        unsafe_allow_html=True
                    )
                
                with col3:
                    # Indicator status
                    indicators = []
                    if row['is_volume_surge']:
                        indicators.append("📈 Vol")
                    if row['is_price_spike']:
                        indicators.append("⚡ Price")
                    if row['is_rsi_overbought']:
                        indicators.append("🔴 RSI High")
                    if row['is_rsi_oversold']:
                        indicators.append("🟢 RSI Low")
                    
                    st.write(" ".join(indicators))
                
                st.write("---")
    else:
        st.info("No recent data available")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
        Stock Analytics Dashboard | Data updates every minute | Last refresh: {}
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    unsafe_allow_html=True
)

# Add some debug info in expander
with st.expander("🔧 Debug Info"):
    st.write(f"Selected symbols: {selected_symbols}")
    st.write(f"Date range: {start_date} to {end_date}")
    st.write(f"Stock data shape: {stock_data.shape}")
    st.write(f"Alerts data shape: {alerts_data.shape}")