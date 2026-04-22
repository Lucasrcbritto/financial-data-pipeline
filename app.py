import pandas as pd
import streamlit as st

# Page config
st.set_page_config(
    page_title="Financial Data Pipeline",
    page_icon="📈",
    layout="wide",
)

# Load data
@st.cache_data
def load_data():
    return pd.read_parquet("data/04_feature/market_data_features.parquet")

df = load_data()

# Sidebar
st.sidebar.title("Filters")
tickers = sorted(df["ticker"].unique())
selected_ticker = st.sidebar.selectbox("Ticker", tickers)
date_min = df["date"].min()
date_max = df["date"].max()
date_range = st.sidebar.date_input(
    "Date range",
    value=(date_min, date_max),
    min_value=date_min,
    max_value=date_max,
)

# Filter data
mask = (
    (df["ticker"] == selected_ticker)
    & (df["date"] >= pd.Timestamp(date_range[0]))
    & (df["date"] <= pd.Timestamp(date_range[1]))
)
filtered = df[mask].sort_values("date")

# Title
st.title(f"📈 {selected_ticker} — Financial Dashboard")
st.caption(f"{date_range[0]} to {date_range[1]}")

# KPI row
col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest Close", f"${filtered['close'].iloc[-1]:.2f}")
col2.metric("Daily Return", f"{filtered['daily_return'].iloc[-1]*100:.2f}%")
col3.metric("RSI (14)", f"{filtered['rsi_14'].iloc[-1]:.1f}")
col4.metric("Avg Volume", f"{filtered['volume'].mean():,.0f}")

st.divider()

# Price + SMA chart
st.subheader("Price & Moving Averages")
price_cols = ["close"] + [c for c in filtered.columns if c.startswith("sma_")]
st.line_chart(filtered.set_index("date")[price_cols])

# Volatility chart
st.subheader("Rolling Volatility (Annualised)")
vol_cols = [c for c in filtered.columns if c.startswith("rolling_vol_")]
st.line_chart(filtered.set_index("date")[vol_cols])

# RSI chart
st.subheader("RSI (14)")
st.line_chart(filtered.set_index("date")[["rsi_14"]])

# Macro overlay
st.subheader("Macro Indicators")
macro_cols = [c for c in filtered.columns if c in ["FEDFUNDS", "UNRATE", "CPIAUCSL", "T10Y2Y"]]
if macro_cols:
    selected_macro = st.multiselect("Select indicators", macro_cols, default=macro_cols[:2])
    if selected_macro:
        macro_data = filtered.set_index("date")[selected_macro].dropna()
        macro_normalized = (macro_data - macro_data.min()) / (macro_data.max() - macro_data.min()) * 100
        st.caption("Indicators normalized to 0–100 scale for comparability")
        st.line_chart(macro_normalized)

# Raw data
with st.expander("Raw data"):
    st.dataframe(filtered.reset_index(drop=True))