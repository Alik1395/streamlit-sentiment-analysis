import streamlit as st
from confluent_kafka import Consumer
import pandas as pd
import json

# Kafka credentials from your config
conf = {
    'bootstrap.servers': 'pkc-ewzgj.europe-west4.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'PTIXVVZQMGGZXODW',
    'sasl.password': 'fbvBdgp8JZCWZLyQx/+Ca9f+bfGA1l2kOQX+XnD30AKqeVUimNCAU0GZxJtJEgdX',
    'group.id': 'streamlit-dashboard-group',
    'auto.offset.reset': 'latest',
    'session.timeout.ms': 50000
}

# Streamlit layout
st.set_page_config(page_title="Sentiment Dashboard", layout="wide")
st.title("🧠 Sentiment Analysis Dashboard")

placeholder = st.empty()

# Kafka consumer setup
consumer = Consumer(conf)
consumer.subscribe(['customer_reviews'])  # ✅ your topic here

# Store messages
messages = []

with placeholder.container():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            st.error(f"Kafka error: {msg.error()}")
            continue
        try:
            value = json.loads(msg.value().decode('utf-8'))
            row = {
                "review": value.get("text", ""),
                "sentiment": value.get("sentiment", "n/a"),
                "next_best_action": value.get("next_best_action", "n/a")
            }
            messages.append(row)
            df = pd.DataFrame(messages)
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not parse message: {e}")
