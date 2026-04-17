import streamlit as st
from utils.bq_client import get_coins

# ---------------------------------------------------------------------------
# Page configuration
# st.set_page_config() MUST be the first Streamlit call in app.py.
# It sets global page properties. If you call it anywhere else, Streamlit
# raises an error.
#
# layout="wide" makes the content use the full browser width instead of
# the default narrow centered column.
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="CryptoLens",
    page_icon="📈",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Sidebar — shared filters
#
# st.sidebar is a special container. Anything placed inside it renders in
# the left panel, not the main page body.
#
# We define the coin selector and date range here in app.py so they are
# available on every page via st.session_state.
# If we put them only on one page, switching pages would lose the selection.
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("CryptoLens 📈")
    st.markdown("---")  # renders a horizontal divider line

    # load_coins() queries dim_coins and returns e.g. ["bitcoin", "ethereum", ...]
    # This result is cached for 1 hour — not re-queried on every rerun.
    coins = get_coins()

    # st.selectbox renders a dropdown.
    # First arg  — label shown above the dropdown
    # Second arg — list of options to show
    # The return value is whatever the user currently has selected.
    selected_coin = st.selectbox("Select Coin", coins)

    st.markdown("---")

    # st.date_input renders a date picker.
    # We use Python's datetime module to set sensible defaults.
    # value= sets the initial date when the app first loads.
    import datetime

    # Default range — last 30 days
    default_end = datetime.date.today()
    default_start = default_end - datetime.timedelta(days=30)

    start_date = st.date_input("From", value=default_start)
    end_date = st.date_input("To", value=default_end)

    # Basic validation — stop the app if the date range is invalid.
    # st.error() renders a red error box.
    # st.stop() halts the script execution at that point — nothing below runs.
    # This is Streamlit's equivalent of an early return guard.
    if start_date > end_date:
        st.error("'From' date must be before 'To' date.")
        st.stop()

# ---------------------------------------------------------------------------
# Persist selections in session_state
#
# As explained in the mental model — regular variables don't survive reruns.
# session_state does. We store the sidebar selections here so every page
# can read them without re-rendering the sidebar widgets themselves.
#
# Any page can then read:
#     st.session_state.selected_coin
#     st.session_state.start_date
#     st.session_state.end_date
# ---------------------------------------------------------------------------
st.session_state.selected_coin = selected_coin
st.session_state.start_date = start_date.isoformat()    # convert date to "2024-01-01" string
st.session_state.end_date = end_date.isoformat()        # BigQuery expects ISO string in our queries

# ---------------------------------------------------------------------------
# Main page body — landing / home screen
# ---------------------------------------------------------------------------
st.title("CryptoLens Dashboard")
st.markdown(
    """
    Welcome to CryptoLens — a crypto market analytics dashboard built on
    a fully automated data pipeline.

    **Use the sidebar to select a coin and date range**, then navigate to a page:

    - 📊 **Price Trends** — OHLC candlestick chart
    - 🗞️ **Sentiment** — daily sentiment scores alongside price movement
    - 🔗 **Correlation** — did sentiment predict price movement?
    """
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Quick stats — show a few headline numbers on the landing page
#
# st.columns(3) splits the page into 3 equal columns.
# Each column is a container — you put widgets inside using "with".
# st.metric() renders a KPI card with a label, value, and optional delta.
# ---------------------------------------------------------------------------
st.subheader(f"Quick Stats — {selected_coin.capitalize()}")

from utils.bq_client import get_coin_prices

# Convert date objects to strings for the query
prices_df = get_coin_prices(selected_coin, start_date.isoformat(), end_date.isoformat())

if prices_df.empty:
    st.warning("No price data found for the selected filters.")
else:
    latest = prices_df.iloc[-1]     # iloc[-1] means "last row" — most recent date
    earliest = prices_df.iloc[0]    # iloc[0] means "first row" — oldest date

    # Price change over the selected period
    price_change = latest["close_usd"] - earliest["close_usd"]
    price_change_pct = (price_change / earliest["close_usd"]) * 100

    col1, col2, col3 = st.columns(3)

    with col1:
        # st.metric(label, value, delta)
        # delta automatically colours green if positive, red if negative
        st.metric(
            label="Latest Close",
            value=f"${latest['close_usd']:,.2f}",       # :,.2f — comma thousands, 2 decimal places
            delta=f"{price_change_pct:+.2f}%",          # :+.2f — always show + or - sign
        )

    with col2:
        st.metric(
            label="Period High",
            value=f"${prices_df['high_usd'].max():,.2f}",   # .max() across all rows
        )

    with col3:
        st.metric(
            label="Period Low",
            value=f"${prices_df['low_usd'].min():,.2f}",    # .min() across all rows
        )