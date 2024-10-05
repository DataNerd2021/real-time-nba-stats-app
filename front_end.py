import streamlit as st
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from nba_api.stats.endpoints import scoreboardv2
import json
from datetime import datetime
import pytz

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'streamlit_app',
    'auto.offset.reset': 'earliest'
}

# Initialize Kafka consumer
consumer = Consumer(kafka_config)

st.set_page_config(page_title="Real-Time NBA Stats App", page_icon=":basketball:")

st.title(":basketball: Real-Time NBA Stats App (Beta)")

st.write("This app allows you to search for NBA games and view real-time plays and stats for those games.")

# Fetch today's games in Mountain Time
mountain_tz = pytz.timezone("US/Mountain")
today = datetime.now(mountain_tz).strftime("%Y-%m-%d")
games_data = json.loads(scoreboardv2.ScoreboardV2(game_date=today).get_json())

if len(games_data['resultSets'][0]['rowSet']) == 0:
    st.write("<h2>No games found for today.<br>Try again tomorrow.</h2>", unsafe_allow_html=True)
else:
    games = games_data['resultSets'][0]['rowSet']
    games_info = games_data['resultSets'][1]['rowSet']
    st.write('')
    st.write('')
    st.header("Select a Game:")

    # Create a container for game buttons
    with st.container():
        for i, game in enumerate(games):
            game_id = game[2]
            game_time_str = game[4]
            # Remove the 'ET' and parse the time
            game_time = datetime.strptime(game_time_str.replace(' ET', ''), "%I:%M %p")
            # Assume the time is in Eastern Time, create a timezone-aware datetime
            eastern_tz = pytz.timezone("US/Eastern")
            game_time = eastern_tz.localize(game_time)
            # Convert to Mountain Time
            game_time_mt = game_time.astimezone(mountain_tz)
            game_time_str = game_time_mt.strftime("%I:%M %p MT")
            home_team = games_info[i*2][4]
            away_team = games_info[i*2+1][4]
            label = f"{away_team} @ {home_team} ({game_time_str})"
            if st.button(label=label, key=f"game_{game_id}", use_container_width=True):
                st.session_state.selected_game_id = game_id
                st.session_state.selected_game_label = label

    # Display selected game and start ingestion
    if 'selected_game_id' in st.session_state:
        st.write(f"Selected Game: {st.session_state.selected_game_label}")

        if st.button("View Game Plays"):
            try:
                # Subscribe to the topic for the selected game
                topic = f"nba-plays"
                consumer.subscribe([topic])

                # Create an empty list to store the plays
                plays = []

                # Create a placeholder for the DataFrame
                df_placeholder = st.empty()

                # Display the results
                st.write("Latest Game Plays:")

                # Poll for messages
                while True:
                    msg = consumer.poll(1.0)

                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            st.error(f"Error: {msg.error()}")
                            break

                    # Process the message
                    play = json.loads(msg.value().decode('utf-8'))
                    plays.append(play)

                    # Create a DataFrame from the plays
                    df = pd.DataFrame(plays)
                    df = df[df['teamTricode'].isin([away_team, home_team])]
                    df['clock'] = df['clock'].str.replace('PT', '').replace('M', ':').replace('S', '')

                    # Update the DataFrame display
                    df_placeholder.dataframe(df)

                    # Keep only the last 10 plays
                    if len(plays) > 10:
                        plays.pop(0)
            except Exception as e:
                st.error(f"An error occurred while fetching game plays: {str(e)}")
            finally:
                # Close the consumer
                consumer.close()