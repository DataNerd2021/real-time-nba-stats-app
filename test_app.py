import streamlit as st
import pandas as pd
from confluent_kafka import Consumer, KafkaError
import json
from collections import deque
import re

# Set page config at the very beginning
st.set_page_config(page_title="NBA Live Play-by-Play", page_icon="🏀", layout="wide")

def create_kafka_consumer():
    """Create and return a Kafka consumer instance."""
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'streamlit-app',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(conf)

def process_message(value):
    """Process a single message from Kafka."""
    play = json.loads(value.decode('utf-8'))
    play['clock'] = play['clock'].replace('PT', '').replace('M', ':').replace('S', '')

    # Remove updated statistics from the description
    play['description'] = re.sub(r'\([^)]*\)', '', play['description']).strip()

    return play

def update_dataframe(plays):
    """Create and format a DataFrame from the plays."""
    df = pd.DataFrame(plays)
    df = df.drop(columns=['actionNumber'])
    df = df.rename(columns={
        'clock': 'Time Remaining',
        'scoreAway': 'Away',
        'scoreHome': 'Home',
        'period': 'Period'
    })

    return df[['Period', 'Time Remaining', 'description', 'Home', 'Away', 'home_team', 'away_team']]

def display_data(df):
    """Display the updated data in the Streamlit app."""
    st.header("Scoreboard")
    col1, col2, col3, col4 = st.columns(4)

    home_score = int(df.iloc[0]['Home'])
    away_score = int(df.iloc[0]['Away'])

    with col2:
        st.write(f"<h3 style='text-align: center;'>{df.iloc[0]['home_team']}</h3>\n<h2 style='text-align: center;'>{home_score}</h2>", unsafe_allow_html=True)
    with col3:
        st.write(f"<h3 style='text-align: center;'>{df.iloc[0]['away_team']}</h3>\n<h2 style='text-align: center;'>{away_score}</h2>", unsafe_allow_html=True)

    st.info(f"Period: {df.iloc[0]['Period']} | Time Remaining: {df.iloc[0]['Time Remaining']}")
    st.subheader("Latest Play")
    st.write(df.iloc[0]['description'])

    st.subheader("Recent Plays")
    st.dataframe(df[['Period', 'Time Remaining', 'description']], height=400)

    if st.session_state.show_all_data:
        st.subheader("Full Dataset")
        st.dataframe(df)


def main():
    st.title('🏀 NBA Live Play-by-Play Stream')
    st.markdown("""
    This app shows real-time play-by-play data from NBA games.
    The data is streamed from a Kafka topic and updated live.
    """)

    if 'show_all_data' not in st.session_state:
        st.session_state.show_all_data = False

    st.checkbox("Show all data", key="show_all_data")

    consumer = create_kafka_consumer()
    consumer.subscribe(['nba_playbyplay'])

    plays = deque(maxlen=100)

    placeholder = st.empty()

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            st.error(f"Error: {msg.error()}")
            break

        try:
            play = process_message(msg.value())
            plays.appendleft(play)

            df = update_dataframe(plays)

            with placeholder.container():
                display_data(df)

        except json.JSONDecodeError:
            st.warning(f"Error decoding JSON: {msg.value()}")

    consumer.close()

if __name__ == "__main__":
    main()