import streamlit as st
import pandas as pd
from confluent_kafka import Consumer, KafkaError
import json
from collections import deque
import re

TAG_MAPPING = {
    "fastbreak": "Fast Break",
    "secondchancepoints": "Second Chance Points",
    "pointsinthepaint": "Points in the Paint",
    "leadchange": "Lead Change",
    "tiedscore": "Tied Score",
    "turnover": "Turnover",
    # Add more mappings as needed
}

def clean_tag(tag):
    """Map a tag to its cleaned version or capitalize if not in mapping."""
    return TAG_MAPPING.get(tag.lower(), tag.capitalize())

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

    # Clean up the tags
    if 'qualifiers' in play and play['qualifiers']:
        play['qualifiers'] = [clean_tag(tag) for tag in play['qualifiers']]

    # Ensure player, shotDistance, and pie fields are present
    play['player'] = play.get('player', 'Unknown Player')
    play['shotDistance'] = play.get('shotDistance', 'N/A')
    play['pie'] = play.get('pie', 0)

    return play

def update_dataframe(plays):
    """Create and format a DataFrame from the plays."""
    df = pd.DataFrame(plays)
    df = df.drop(columns=['actionNumber'])
    df = df.rename(columns={
        'clock': 'Time Remaining',
        'qualifiers': 'Tags',
        'scoreAway': 'Away',
        'scoreHome': 'Home',
        'period': 'Period'
    })

    # Clean up tags in the DataFrame
    df['Tags'] = df['Tags'].apply(lambda tags: [clean_tag(tag) for tag in tags] if isinstance(tags, list) else [])

    return df[['Period', 'Time Remaining', 'description', 'Tags', 'Home', 'Away', 'player', 'pie']]

def display_data(df, placeholder):
    """Display the updated data in the Streamlit app."""
    with placeholder.container():
        # Find the player with the highest PIE score for each team
        home_team = df.iloc[0]['Home']
        away_team = df.iloc[0]['Away']

        home_player = df[df['Home'] == home_team].sort_values(by='pie', ascending=False).iloc[0]
        away_player = df[df['Away'] == away_team].sort_values(by='pie', ascending=False).iloc[0]

        st.subheader("Top Players by PIE Score")
        col1, col2 = st.columns(2)
        with col1:
            st.metric(f"Home Team: {home_team}", f"{home_player['player']} (PIE: {home_player['pie']})")
        with col2:
            st.metric(f"Away Team: {away_team}", f"{away_player['player']} (PIE: {away_player['pie']})")

        st.subheader("Latest Play")
        st.info(f"Period: {df.iloc[0]['Period']} | Time Remaining: {df.iloc[0]['Time Remaining']}")
        st.write(df.iloc[0]['description'])

        if df.iloc[0]['Tags']:
            st.write("Tags:", ", ".join(df.iloc[0]['Tags']))

        st.subheader("Recent Plays")
        st.dataframe(df[['Period', 'Time Remaining', 'description']], height=400)

        if st.session_state.show_all_data:
            st.subheader("Full Dataset")
            st.dataframe(df)

def main():
    """Main function to run the Streamlit app for displaying NBA play-by-play stream."""
    st.set_page_config(page_title="NBA Live Play-by-Play", page_icon="🏀", layout="wide")

    st.title('🏀 NBA Live Play-by-Play Stream')
    st.markdown("""
    This app shows real-time play-by-play data from NBA games.
    The data is streamed from a Kafka topic and updated live.
    """)

    # Initialize session state
    if 'show_all_data' not in st.session_state:
        st.session_state.show_all_data = False

    # Checkbox to control full dataset display
    st.checkbox("Show all data", key="show_all_data", value=st.session_state.show_all_data, on_change=lambda: setattr(st.session_state, 'show_all_data', not st.session_state.show_all_data))

    consumer = create_kafka_consumer()
    consumer.subscribe(['nba_playbyplay'])

    placeholder = st.empty()
    plays = deque(maxlen=100)

    try:
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
                display_data(df, placeholder)
            except json.JSONDecodeError:
                st.warning(f"Error decoding JSON: {msg.value()}")

    finally:
        consumer.close()

if __name__ == "__main__":
    main()