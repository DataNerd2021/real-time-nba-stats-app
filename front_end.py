import streamlit as st
import pandas as pd
from confluent_kafka import Producer, Consumer

st.set_page_config(page_title="Real-Time NBA Stats App", page_icon=":basketball:")

st.title(":basketball: Real-Time NBA Stats App")
