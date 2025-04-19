import streamlit as st
import pandas as pd

st.title("Sentiment Analysis Dashboard")

# Sample static data (replace with real logic or API later)
data = pd.DataFrame({
    "review": ["Great product", "Worst ever"],
    "sentiment": ["positive", "negative"],
    "next_best_action": ["send thank you", "escalate to support"]
})

st.dataframe(data)
