import streamlit as st
import pandas as pd
from confluent_kafka import Producer, Consumer

st.title("Real-Time NBA Stats App")
