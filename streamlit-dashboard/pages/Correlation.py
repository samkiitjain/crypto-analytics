import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from utils.bq_client import get_price_sentiment as load_price_sentiment

st.set_page_config(page_title="Correlation", page_icon="🔗", layout="wide")

st.title("🔗 Sentiment vs Price Correlation")

# ---------------------------------------------------------------------------
# Read shared filters from session_state
# ---------------------------------------------------------------------------
coin = st.session_state.get("selected_coin", None)
start_date = st.session_state.get("start_date", None)
end_date = st.session_state.get("end_date", None)

if not coin or not start_date or not end_date:
    st.warning("Please select a coin and date range from the home page first.")
    st.stop()

st.subheader(f"{coin.capitalize()} — Did Sentiment Predict Price Movement?")
st.caption(f"From {start_date} to {end_date}")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
df = load_price_sentiment(coin, start_date, end_date)

if df.empty:
    st.warning("No data found for the selected filters.")
    st.stop()

# ---------------------------------------------------------------------------
# Feature engineering — compute price change % from the dataframe
#
# We need "how much did price change day over day" to compare against sentiment.
#
# pct_change() is a pandas method that computes the percentage change
# between each row and the previous row.
# For example: [100, 105, 98] → [NaN, 5.0%, -6.67%]
# The first row is always NaN because there's no previous row to compare to.
#
# * 100 converts the decimal to a percentage (0.05 → 5.0)
#
# .dropna() removes rows where any column has a NaN value.
# We do this after pct_change() to remove that first NaN row.
# ---------------------------------------------------------------------------
df["price_change_pct"] = df["close_usd"].pct_change() * 100
df = df.dropna(subset=["price_change_pct", "sentiment_compound"])

if df.empty:
    st.warning("Not enough data points to compute correlation.")
    st.stop()

# ---------------------------------------------------------------------------
# Correlation coefficient
#
# .corr() computes the Pearson correlation coefficient between two Series.
# Result is a value between -1.0 and +1.0:
#   +1.0 = perfect positive correlation (sentiment up → price up)
#   -1.0 = perfect negative correlation (sentiment up → price down)
#    0.0 = no linear relationship
#
# In practice, anything above 0.3 or below -0.3 is considered meaningful
# for financial data.
# ---------------------------------------------------------------------------
correlation = df["sentiment_compound"].corr(df["price_change_pct"])

# ---------------------------------------------------------------------------
# Interpretation helper
# ---------------------------------------------------------------------------
def interpret_correlation(r: float) -> tuple[str, str]:
    """
    Returns a human-readable label and colour for a correlation coefficient.

    Args:
        r: Pearson correlation coefficient (-1.0 to 1.0)

    Returns:
        tuple of (label string, hex colour string)
    """
    # abs() returns the absolute value — we check strength regardless of direction
    strength = abs(r)

    if strength >= 0.7:
        strength_label = "Strong"
    elif strength >= 0.4:
        strength_label = "Moderate"
    elif strength >= 0.2:
        strength_label = "Weak"
    else:
        strength_label = "Very weak / No"

    direction = "positive" if r >= 0 else "negative"
    colour = "#2ca02c" if r >= 0 else "#d62728"

    return f"{strength_label} {direction} correlation", colour


label, colour = interpret_correlation(correlation)

# ---------------------------------------------------------------------------
# Correlation summary at the top
# ---------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.metric("Pearson Correlation (r)", f"{correlation:+.3f}")

with col2:
    # st.markdown with inline HTML lets us colour the text.
    # unsafe_allow_html=True is required — Streamlit blocks HTML by default
    # for security reasons. We use it here only for cosmetic colouring.
    st.markdown(
        f"**Interpretation:** <span style='color:{colour}'>{label}</span>",
        unsafe_allow_html=True,
    )

st.markdown("---")

# ---------------------------------------------------------------------------
# Scatter plot — sentiment score vs price change %
#
# Each dot is one day.
# x-axis = sentiment compound score for that day
# y-axis = price change % for that day
#
# If sentiment predicts price, dots should cluster along a diagonal line
# (bottom-left to top-right for positive correlation).
# ---------------------------------------------------------------------------
fig = go.Figure()

# Colour each dot by sentiment label
# We build three separate traces so the legend shows the three categories.
for sentiment_label, dot_colour in [
    ("positive", "#2ca02c"),
    ("negative", "#d62728"),
    ("neutral", "#7f7f7f"),
]:
    # Boolean mask — filters the dataframe to only rows matching this label.
    # df["sentiment_label"] == sentiment_label produces a True/False Series.
    # df[mask] keeps only the True rows.
    mask = df["sentiment_label"] == sentiment_label
    subset = df[mask]

    if subset.empty:
        continue    # skip this trace if no data for this label

    fig.add_trace(
        go.Scatter(
            x=subset["sentiment_compound"],
            y=subset["price_change_pct"],
            mode="markers",             # markers only — no lines connecting dots
            name=sentiment_label.capitalize(),
            marker=dict(
                color=dot_colour,
                size=8,
                opacity=0.7,
            ),
            # hovertemplate controls what appears when user hovers over a dot.
            # <br> is HTML line break.
            # %{x:.3f} formats the x value to 3 decimal places.
            # %{customdata[0]} references extra data we attach below.
            customdata=subset[["value_timestamp"]].values,
            hovertemplate=(
                "Date: %{customdata[0]}<br>"
                "Sentiment: %{x:.3f}<br>"
                "Price Change: %{y:+.2f}%<br>"
                "<extra></extra>"   # <extra></extra> hides the trace name in hover
            ),
        )
    )

# Add a vertical line at x=0 and horizontal line at y=0
# These divide the chart into four quadrants:
#   Top-right    = positive sentiment, price up   (sentiment was right)
#   Bottom-left  = negative sentiment, price down (sentiment was right)
#   Top-left     = negative sentiment, price up   (sentiment was wrong)
#   Bottom-right = positive sentiment, price down (sentiment was wrong)
fig.add_hline(y=0, line_dash="dash", line_color="#cccccc", line_width=1)
fig.add_vline(x=0, line_dash="dash", line_color="#cccccc", line_width=1)

fig.update_layout(
    title="Sentiment Score vs Next-Day Price Change %",
    xaxis_title="Sentiment Compound Score",
    yaxis_title="Price Change %",
    height=500,
    legend=dict(x=0, y=1.1, orientation="h"),
)

st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Quadrant analysis — count dots in each quadrant
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Quadrant Analysis")
st.caption("How often did sentiment correctly predict price direction?")

# Count rows in each quadrant using boolean conditions combined with &
# The parentheses around each condition are required when combining with &
correct_positive = len(df[(df["sentiment_compound"] > 0) & (df["price_change_pct"] > 0)])
correct_negative = len(df[(df["sentiment_compound"] < 0) & (df["price_change_pct"] < 0)])
wrong_positive   = len(df[(df["sentiment_compound"] > 0) & (df["price_change_pct"] < 0)])
wrong_negative   = len(df[(df["sentiment_compound"] < 0) & (df["price_change_pct"] > 0)])

total = correct_positive + correct_negative + wrong_positive + wrong_negative
accuracy = ((correct_positive + correct_negative) / total * 100) if total > 0 else 0

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("🟢 Sentiment ↑, Price ↑", correct_positive)

with col2:
    st.metric("🔴 Sentiment ↓, Price ↓", correct_negative)

with col3:
    st.metric("❌ Sentiment ↑, Price ↓", wrong_positive)

with col4:
    st.metric("❌ Sentiment ↓, Price ↑", wrong_negative)

with col5:
    st.metric("Directional Accuracy", f"{accuracy:.1f}%")