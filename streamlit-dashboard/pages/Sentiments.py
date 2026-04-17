import streamlit as st
import plotly.graph_objects as go
from utils.bq_client import get_coin_prices, get_sentiments

st.set_page_config(page_title="Sentiment", page_icon="🗞️", layout="wide")

st.title("🗞️ Sentiment Analysis")

# ---------------------------------------------------------------------------
# Read shared filters from session_state
# ---------------------------------------------------------------------------
coin = st.session_state.get("selected_coin", None)
start_date = st.session_state.get("start_date", None)
end_date = st.session_state.get("end_date", None)

if not coin or not start_date or not end_date:
    st.warning("Please select a coin and date range from the home page first.")
    st.stop()

st.subheader(f"{coin.capitalize()} — Price vs Sentiment")
st.caption(f"From {start_date} to {end_date}")

# ---------------------------------------------------------------------------
# Load both datasets
# ---------------------------------------------------------------------------
prices_df = get_coin_prices(coin, start_date, end_date)
sentiment_df = get_sentiments(coin, start_date, end_date)

if prices_df.empty:
    st.warning("No price data found for the selected filters.")
    st.stop()

if sentiment_df.empty:
    st.warning("No sentiment data found for the selected filters.")
    st.stop()

# ---------------------------------------------------------------------------
# Dual-axis chart — price line + sentiment bar
#
# A dual-axis chart has two y-axes sharing the same x-axis.
# Left y-axis  → price (USD) — scale is in thousands
# Right y-axis → sentiment compound score — scale is -1.0 to 1.0
#
# Without dual axes, the sentiment line would be invisible because
# -1 to 1 is tiny compared to $2,000+.
#
# In Plotly, dual axes are created by:
#   1. Adding a second y-axis in update_layout (yaxis2)
#   2. Telling each trace which y-axis it belongs to via yaxis="y2"
# ---------------------------------------------------------------------------
fig = go.Figure()

# --- Trace 1: Close price line (left y-axis) ---
# go.Scatter with mode="lines" draws a line chart.
# mode="lines" means only lines, no dots at data points.
# mode="lines+markers" would add dots too.
fig.add_trace(
    go.Scatter(
        x=prices_df["value_timestamp"],
        y=prices_df["close_usd"],
        name="Close Price (USD)",
        mode="lines",
        line=dict(color="#1f77b4", width=2),  # dict() here sets line style properties
        yaxis="y1",                            # binds this trace to the left y-axis
    )
)

# --- Trace 2: Sentiment compound score bars (right y-axis) ---
# go.Bar draws a bar chart.
# marker_color uses a list comprehension to colour each bar individually:
#   green  if sentiment > 0.05  (positive)
#   red    if sentiment < -0.05 (negative)
#   grey   otherwise            (neutral)
# This matches the VADER label thresholds used in your ingestion layer.
fig.add_trace(
    go.Bar(
        x=sentiment_df["published_at"],
        y=sentiment_df["avg_sentiment"],
        name="Avg Sentiment Score",
        marker_color=[
            "#2ca02c" if v > 0.05 else "#d62728" if v < -0.05 else "#7f7f7f"
            for v in sentiment_df["avg_sentiment"]
            # This is a list comprehension — Python's concise way of building a list.
            # For each value v in avg_sentiment, pick a colour based on the condition.
            # Equivalent to a for loop that appends to a colour list.
        ],
        opacity=0.6,    # 0.0 = fully transparent, 1.0 = fully opaque
        yaxis="y2",     # binds this trace to the right y-axis
    )
)

# --- Layout — configure both y-axes ---
fig.update_layout(
    title=f"{coin.capitalize()} Close Price vs Daily Sentiment",
    xaxis_title="Date",
    height=500,

    # Left y-axis — price
    yaxis=dict(
        title="Close Price (USD)",
        title_font=dict(color="#1f77b4"),
        tickfont=dict(color="#1f77b4"),
    ),

    yaxis2=dict(
        title="Sentiment Score",
        title_font=dict(color="#7f7f7f"),
        tickfont=dict(color="#7f7f7f"),
        overlaying="y",
        side="right",
        range=[-1, 1],
        zeroline=True,
        zerolinecolor="#cccccc",
    ),

    legend=dict(x=0, y=1.1, orientation="h"),   # legend above the chart, horizontal
    barmode="overlay",
)

st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Sentiment breakdown metrics
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Sentiment Breakdown")

col1, col2, col3, col4 = st.columns(4)

# .mean() calculates the average of a pandas Series (column)
avg = sentiment_df["avg_sentiment"].mean()

# value_counts() counts how many times each unique value appears.
# This tells us how many days were positive, negative, neutral.
label_counts = sentiment_df["sentiment_label"].value_counts()

# .get("positive", 0) — safely get the count, default to 0 if label never appeared
positive_days = label_counts.get("positive", 0)
negative_days = label_counts.get("negative", 0)
neutral_days = label_counts.get("neutral", 0)

with col1:
    st.metric("Avg Sentiment Score", f"{avg:+.3f}")

with col2:
    st.metric("🟢 Positive Days", positive_days)

with col3:
    st.metric("🔴 Negative Days", negative_days)

with col4:
    st.metric("⚪ Neutral Days", neutral_days)

# ---------------------------------------------------------------------------
# Sentiment label over time — secondary view
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Daily Sentiment Label")

# st.dataframe with column config lets you customise how each column renders.
# st.column_config.DateColumn renders the date in a readable format.
# st.column_config.ProgressColumn renders a bar inside the cell — useful for
# showing a score between -1 and 1 visually.
st.dataframe(
    sentiment_df[["published_at", "avg_sentiment", "sentiment_label"]],
    column_config={
        "published_at": st.column_config.DateColumn("Date"),
        "avg_sentiment": st.column_config.ProgressColumn(
            "Avg Sentiment",
            min_value=-1,
            max_value=1,
            format="%+.3f",     # always show + or - sign, 3 decimal places
        ),
        "sentiment_label": st.column_config.TextColumn("Label"),
    },
    use_container_width=True,
    hide_index=True,    # hides the default 0,1,2,3 row index column
)