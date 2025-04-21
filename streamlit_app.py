import streamlit as st
from confluent_kafka import Consumer
import pandas as pd
import json

# Kafka config
conf = {
    'bootstrap.servers': '...',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '...',
    'sasl.password': '...',
    'group.id': 'streamlit-dashboard-group',
    'auto.offset.reset': 'latest'
}

# Streamlit setup
st.set_page_config(page_title="Sentiment Dashboard", layout="wide")
st.title("🧠 Sentiment Analysis Dashboard")

# Kafka consumer setup
consumer = Consumer(conf)
consumer.subscribe(['customer_reviews'])

# Placeholder for live updating
placeholder = st.empty()
messages = []

def sentiment_icon(sentiment):
    if sentiment.lower() == "positive":
        return "🟢 Positive"
    elif sentiment.lower() == "neutral":
        return "🟡 Neutral"
    elif sentiment.lower() == "negative":
        return "🔴 Negative"
    else:
        return "⚪ n/a"

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        st.error(f"Kafka error: {msg.error()}")
        continue

    try:
        value = json.loads(msg.value().decode("utf-8"))
        row = {
            "Review": value.get("text", ""),
            "Sentiment": sentiment_icon(value.get("sentiment", "n/a")),
            "Next Best Action": value.get("next_best_action", "n/a")
        }
        messages.append(row)
        messages = messages[-50:]  # Limit to latest 50 messages
        df = pd.DataFrame(messages)

        with placeholder:
            st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.warning(f"Could not parse message: {e}")
