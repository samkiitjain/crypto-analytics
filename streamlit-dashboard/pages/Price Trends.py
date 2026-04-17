import streamlit as st
import plotly.graph_objects as go
from utils.bq_client import get_coin_prices

# ---------------------------------------------------------------------------
# Page config
# Each page file needs its own set_page_config call.
# layout="wide" must match app.py otherwise the layout shifts when navigating.
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Price Trends", page_icon="📊", layout="wide")

st.title("📊 Price Trends")

# ---------------------------------------------------------------------------
# Read shared state from session_state
#
# These values were written by app.py when the user selected from the sidebar.
# We read them here instead of re-defining the widgets.
#
# The .get() pattern is defensive — if someone navigates directly to this page
# without going through app.py first, session_state won't have these keys yet.
# .get("key", default) returns the default instead of raising a KeyError.
# ---------------------------------------------------------------------------
coin = st.session_state.get("selected_coin", None)
start_date = st.session_state.get("start_date", None)
end_date = st.session_state.get("end_date", None)

# Guard — if filters aren't set yet, tell the user to go to the home page first
if not coin or not start_date or not end_date:
    st.warning("Please select a coin and date range from the home page first.")
    st.stop()

st.subheader(f"{coin.capitalize()} — OHLC Candlestick Chart")
st.caption(f"From {start_date} to {end_date}")  # st.caption renders small grey text

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
df = get_coin_prices(coin, start_date, end_date)

if df.empty:
    st.warning("No price data found for the selected filters.")
    st.stop()

# ---------------------------------------------------------------------------
# Candlestick chart using Plotly
#
# Plotly is a charting library. It works in two steps:
#   1. Build a "figure" object (fig) by adding traces and layout settings
#   2. Pass the figure to st.plotly_chart() to render it
#
# A "trace" in Plotly is one data series on a chart.
# go.Candlestick is a specific trace type for OHLC financial data.
# go.Figure is the container that holds traces and layout together.
# ---------------------------------------------------------------------------
fig = go.Figure(
    data=[
        go.Candlestick(
            x=df["value_timestamp"],    # x-axis — the datetime column
            open=df["open_usd"],        # bottom of candle body (or top if red)
            high=df["high_usd"],        # top of the wick
            low=df["low_usd"],          # bottom of the wick
            close=df["close_usd"],      # top of candle body (or bottom if red)
            name=coin,
            # Plotly colours candles automatically:
            # green = close > open (price went up)
            # red   = close < open (price went down)
        )
    ]
)

# update_layout modifies the figure's appearance after creation.
# This is a common Plotly pattern — build the data first, style second.
fig.update_layout(
    title=f"{coin.capitalize()} Price (USD)",
    xaxis_title="Date",
    yaxis_title="Price (USD)",
    xaxis_rangeslider_visible=False,    # hides the mini range slider below the chart
    height=500,                         # chart height in pixels
    template="plotly_dark",             # dark theme — fits a data dashboard aesthetic
)

# use_container_width=True makes the chart stretch to fill the full page width
st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Summary statistics below the chart
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Period Summary")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Open", f"${df.iloc[0]['open_usd']:,.2f}")

with col2:
    st.metric("Latest Close", f"${df.iloc[-1]['close_usd']:,.2f}")

with col3:
    st.metric("Period High", f"${df['high_usd'].max():,.2f}")

with col4:
    st.metric("Period Low", f"${df['low_usd'].min():,.2f}")

# ---------------------------------------------------------------------------
# Raw data table — collapsible
#
# st.expander creates a collapsible section. Content inside is hidden by default.
# expanded=False means it starts collapsed.
# This is good practice — show the chart prominently, hide raw data behind a click.
# ---------------------------------------------------------------------------
with st.expander("Show raw data", expanded=False):
    # st.dataframe renders an interactive table — sortable, scrollable
    st.dataframe(df, use_container_width=True)