import streamlit as st
import pandas as pd
import json
from confluent_kafka import Consumer
import plotly.express as px

# --- Kafka config ---
conf = {
    'bootstrap.servers': 'pkc-ewzgj.europe-west4.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'PTIXVVZQMGGZXODW',
    'sasl.password': 'fbvBdgp8JZCWZLyQx/+Ca9f+bfGA1l2kOQX+XnD30AKqeVUimNCAU0GZxJtJEgdX',
    'group.id': 'streamlit-viz-group',
    'auto.offset.reset': 'latest'
}

TOPIC = 'customer_reviews'

# --- Streamlit layout ---
st.set_page_config(page_title="💬 Sentiment Dashboard", layout="wide")
st.markdown("## 🧠 Sentiment Analysis Dashboard")
st.caption("Live Kafka stream of Uzbek product reviews")

# --- Kafka Consumer ---
consumer = Consumer(conf)
consumer.subscribe([TOPIC])

# Session state to persist reviews
if "data" not in st.session_state:
    st.session_state["data"] = []

# Poll for new messages
new_rows = []
for _ in range(30):
    msg = consumer.poll(timeout=0.2)
    if msg and msg.value():
        try:
            record = json.loads(msg.value().decode('utf-8'))
            new_rows.append(record)
        except:
            continue

st.session_state["data"].extend(new_rows)
df = pd.DataFrame(st.session_state["data"][-150:])  # last 150 records

if not df.empty:
    # --- Clean table display ---
    st.markdown("### 📋 Latest Reviews")
    st.dataframe(
        df[[
            "date", "publisher", "product_name", "review_score", "sentiment", "topic", "recommended_action"
        ]].sort_values("date", ascending=False),
        use_container_width=True
    )

    # --- Sentiment distribution ---
    st.markdown("### 📊 Sentiment Breakdown")
    sentiment_counts = df["sentiment"].value_counts().reset_index()
    sentiment_counts.columns = ["Sentiment", "Count"]

    fig = px.bar(
        sentiment_counts,
        x="Sentiment", y="Count",
        color="Sentiment",
        color_discrete_sequence=px.colors.qualitative.Set2,
        height=300
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Auto-refresh note ---
st.markdown("⌛ Auto-refreshing every time new messages arrive.")
