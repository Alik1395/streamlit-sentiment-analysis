import streamlit as st
import pandas as pd
import json
from confluent_kafka import Consumer
import plotly.express as px

# Kafka config
conf = {
    'bootstrap.servers': 'your-confluent-bootstrap-url:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'your-confluent-username',
    'sasl.password': 'your-confluent-password',
    'group.id': 'streamlit-dashboard',
    'auto.offset.reset': 'latest'
}

TOPIC = 'customer_reviews'

# Create Streamlit layout
st.set_page_config(page_title="📊 Review Stream Dashboard", layout="wide")
st.title("📈 Real-time Review Stream Dashboard")

# Kafka consumer
consumer = Consumer(conf)
consumer.subscribe([TOPIC])

# Session state to persist messages
if "messages" not in st.session_state:
    st.session_state["messages"] = []

# Poll Kafka for new messages
new_data = []
for _ in range(20):  # poll 20 new records per refresh
    msg = consumer.poll(timeout=0.1)
    if msg is None:
        continue
    if msg.error():
        st.warning(f"Kafka error: {msg.error()}")
        continue
    try:
        val = json.loads(msg.value().decode('utf-8'))
        new_data.append(val)
    except Exception as e:
        st.error(f"JSON decode error: {e}")

# Update stream
st.session_state["messages"].extend(new_data)
df = pd.DataFrame(st.session_state["messages"][-200:])  # show last 200 rows

# Display Data Table
st.subheader("📋 Latest Reviews")
st.dataframe(df[[
    "date", "publisher", "product_name", "review_score", "sentiment", "topic", "recommended_action"
]].sort_values("date", ascending=False), use_container_width=True)

# Sentiment Distribution
if not df.empty:
    st.subheader("📊 Sentiment Distribution")
    sentiment_chart = df["sentiment"].value_counts().reset_index()
    sentiment_chart.columns = ["Sentiment", "Count"]
    fig = px.bar(sentiment_chart, x="Sentiment", y="Count", color="Sentiment", height=300)
    st.plotly_chart(fig, use_container_width=True)

# Auto-refresh
st.experimental_rerun()
