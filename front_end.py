import streamlit as st
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from nba_api.stats.endpoints import scoreboardv2, teamdetails
from nba_api.live.nba.endpoints import boxscore
import json
import time
from datetime import datetime
import pytz
import sqlite3
from requests import ReadTimeout, ConnectionError

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Adjust this if needed
    'group.id': 'streamlit_app',
    'auto.offset.reset': 'latest'
}

# Database connection
db_name = 'nba_plays.db'
conn = sqlite3.connect(db_name)
cursor = conn.cursor()

# Initialize Kafka consumer
consumer = Consumer(kafka_config)

st.set_page_config(page_title="Real-Time NBA Stats App", page_icon=":basketball:")

st.title(":basketball: Real-Time NBA Stats App (Beta)")

st.write("This app allows you to search for NBA games and view real-time plays for those games.")

def is_game_over(game_id):
    try:
        box = boxscore.BoxScore(game_id)
        game_data = box.get_dict()
        game_status = game_data['game']['gameStatus']
        return game_status == 3  # 3 indicates the game has ended
    except Exception as e:
        print(f"Error checking game status for {game_id}: {str(e)}")
        return False
def is_halftime(game_id):
    try:
        box = boxscore.BoxScore(game_id)
        game_data = box.get_dict()
        period = game_data['game']['period']
        clock_running = game_data['game']['gameStatusText']
        return period == 2 and clock_running == 'Halftime'
    except Exception as e:
        print(f"Error checking halftime status for {game_id}: {str(e)}")
        return False

def get_all_plays_from_db(game_id):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
    SELECT * FROM plays
    WHERE game_id = ?
    ORDER BY action_number DESC
    ''', (game_id,))
    plays = cursor.fetchall()
    conn.close()
    return plays

@st.cache_data(ttl=3600)
def get_team_name(team_id, max_retries=3, delay=2):
    for attempt in range(max_retries):
        try:
            team_details = teamdetails.TeamDetails(team_id=team_id)
            team_data = json.loads(team_details.get_json())
            return team_data['resultSets'][0]['rowSet'][0][1]
        except (ReadTimeout, ConnectionError) as e:
            if attempt < max_retries - 1:
                st.warning(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                st.error(f"Failed to fetch team name after {max_retries} attempts. Please try again later.")
                return f"Team ID {team_id}"
        except Exception as e:
            st.error(f"An unexpected error occurred: {str(e)}")
            return f"Team ID {team_id}"

def get_all_previous_plays(game_id):
    consumer = Consumer(kafka_config)
    consumer.subscribe(['nba-plays'])

    plays = []
    start_time = time.time()

    while time.time() - start_time < 10:  # Poll for 10 seconds to get historical data
        msg = consumer.poll(0.1)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                st.error(f"Error: {msg.error()}")
                break

        play_data = json.loads(msg.value().decode('utf-8'))
        if play_data['game_id'] == game_id:
            plays.append(play_data)

    consumer.close()
    return plays

def format_dataframe(df):
    # Clean up 'clock' field if it exists
    if 'clock' in df.columns:
        df['clock'] = df['clock'].apply(lambda x: x.replace('PT', '').replace('M', ':').replace('S', '') if isinstance(x, str) else x)

    # Add 'Court Coordinates' column if 'x' and 'y' exist
    if 'x' in df.columns and 'y' in df.columns:
        df['Court Coordinates'] = df.apply(lambda row: f"({row['x']}, {row['y']})" if pd.notnull(row['x']) and pd.notnull(row['y']) else None, axis=1)

    # Rename columns
    column_mapping = {
        'period': 'Period',
        'clock': 'Time Remaining',
        'teamTricode': 'Team',
        'description': 'Play Description',
        'actionType': 'Action Type',
        'subType': 'Action Subtype',
        'descriptor': 'Descriptor',
        'qualifiers': 'Tags'
    }
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    # Reorder columns
    columns_order = [
        'Period', 'Time Remaining', 'Team', 'Play Description', 'Action Type', 'Action Subtype',
        'Descriptor', 'Tags', 'Court Coordinates', 'scoreHome', 'scoreAway'
    ]
    df = df[[col for col in columns_order if col in df.columns]]

    return df

# Fetch today's games in Mountain Time
mountain_tz = pytz.timezone("US/Mountain")
today = datetime.now(mountain_tz).strftime("%Y-%m-%d")
games_data = json.loads(scoreboardv2.ScoreboardV2(game_date=today).get_json())

if len(games_data['resultSets'][0]['rowSet']) == 0:
    st.write("<h2>No games found for today.<br>Try again tomorrow.</h2>", unsafe_allow_html=True)
else:
    games = games_data['resultSets'][0]['rowSet']
    st.write('')
    st.write('')
    st.header("Select a Game:")

    # Create a container for game buttons
    with st.container():
        for game in games:
            game_id = game[2]
            game_time_str = game[4]
            home_team_id = game[6]
            away_team_id = game[7]
            home_team = get_team_name(home_team_id)
            away_team = get_team_name(away_team_id)
            label = f"{home_team} vs. {away_team} ({game_time_str.strip()})"
            if st.button(label=label, key=f"game_{game_id}", use_container_width=True):
                st.session_state.selected_game_id = game_id
                st.session_state.selected_game_label = label
                st.session_state.home_team = home_team
                st.session_state.away_team = away_team



    if 'selected_game_id' in st.session_state:
        if st.button("View Game Plays"):
            game_over = is_game_over(st.session_state.selected_game_id)

            # Create placeholders for the score, DataFrame, and refresh timer
            score_placeholder = st.empty()
            df_placeholder = st.empty()
            refresh_placeholder = st.empty()

            # Get all previous plays
            previous_plays = get_all_previous_plays(st.session_state.selected_game_id)
            if previous_plays:
                df_previous = pd.DataFrame(previous_plays)
                df_previous = format_dataframe(df_previous)
                df_previous = df_previous.sort_values('Time Remaining', ascending=False)

                if not df_previous.empty:
                    latest_play = df_previous.iloc[0]
                    if 'scoreHome' in latest_play and 'scoreAway' in latest_play:
                        score_placeholder.header(f"Current Score: {st.session_state.home_team} {latest_play['scoreHome']} - {st.session_state.away_team} {latest_play['scoreAway']}")

                    st.subheader("Previous Plays")
                    df_placeholder.dataframe(df_previous, hide_index=True)
                else:
                    st.write("No previous plays found for this game.")
            else:
                st.write("No previous plays found for this game.")

            def update_display():
                columns = [
                            'game_id', 'action_number', 'clock', 'timeActual', 'period', 'periodType',
                            'team_id', 'teamTricode', 'actionType', 'subType', 'descriptor',
                            'qualifiers', 'personId', 'x', 'y', 'possession', 'scoreHome', 'scoreAway', 'description'
                        ]
                all_plays = get_all_plays_from_db(st.session_state.selected_game_id)
                if not game_over:
                    consumer = Consumer(kafka_config)
                    consumer.subscribe(['nba-plays'])
                    start_time = time.time()
                    
                    try:
                        while time.time() - start_time < 3:
                            msg = consumer.poll(0.1)
                            if msg is None:
                                continue
                            if msg.error():
                                if msg.error().code() == KafkaError._PARTITION_EOF:
                                    continue
                                else:
                                    st.error(f"Error: {msg.error()}")
                                    break

                            play_data = json.loads(msg.value().decode('utf-8'))
                            if play_data['gameId'] == st.session_state.selected_game_id:
                                play_tuple = tuple(play_data.get(col, None) for col in columns)
                                all_plays.append(play_tuple)
                    finally:
                        consumer.close()

                    if all_plays:
                        
                        df = pd.DataFrame(all_plays, columns=columns)

                        # Convert 'qualifiers' from JSON string to list
                        df['qualifiers'] = df['qualifiers'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)

                        # Clean up 'clock' field
                        df['clock'] = df['clock'].apply(lambda x: x.replace('PT', '').replace('M', ':').replace('S', '') if isinstance(x, str) else x)

                        # Add 'Court Coordinates' column
                        df['Court Coordinates'] = df.apply(lambda row: f"({row['x']}, {row['y']})" if pd.notnull(row['x']) and pd.notnull(row['y']) else None, axis=1)

                        # Rename columns
                        df = df.rename(columns={
                            'period': 'Period',
                            'clock': 'Time Remaining',
                            'teamTricode': 'Team',
                            'description': 'Play Description',
                            'actionType': 'Action Type',
                            'subType': 'Action Subtype',
                            'descriptor': 'Descriptor',
                            'qualifiers': 'Tags'
                        })

                        # Reorder columns
                        columns_order = [
                            'Period', 'Time Remaining', 'Team', 'Play Description', 'Action Type', 'Action Subtype',
                            'Descriptor', 'Tags', 'Court Coordinates', 'scoreHome', 'scoreAway'
                        ]
                        df = df[columns_order]
                        
                        df = df.sort_values(by=['Period', 'Time Remaining'], ascending=[False, True])

                        if not df.empty:
                            latest_play = df.iloc[0]
                            score_placeholder.header(f"Current Score: {st.session_state.home_team} {latest_play['scoreHome']} - {st.session_state.away_team} {latest_play['scoreAway']}")

                            # Display selected columns
                            df_placeholder.dataframe(df, hide_index=True)
                        else:
                            st.write("No plays found for this game.")
                    else:
                        st.write("No plays found for this game in the database.")
                else:
                    consumer = Consumer(kafka_config)
                    consumer.subscribe(['nba-plays'])
                    plays = []
                    start_time = time.time()

                    while time.time() - start_time < 3:  # Poll for 5 seconds
                        msg = consumer.poll(0.1)
                        if msg is None:
                            continue
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                continue
                            else:
                                st.error(f"Error: {msg.error()}")
                                break

                        play_data = json.loads(msg.value().decode('utf-8'))
                        if play_data['gameId'] == st.session_state.selected_game_id:
                            plays.append(play_data)

                    consumer.close()

                    if plays:
                        df = pd.DataFrame(plays)
                        df = format_dataframe(df)

                        if not df.empty:
                            latest_play = df.iloc[0]
                            if 'scoreHome' in latest_play and 'scoreAway' in latest_play:
                                score_placeholder.header(f"Current Score: {st.session_state.home_team} {latest_play['scoreHome']} - {st.session_state.away_team} {latest_play['scoreAway']}")

                            st.subheader("Latest Plays")
                            df_placeholder.dataframe(df, hide_index=True)
                        else:
                            df_placeholder.dataframe(df, hide_index=True)
                            df_placeholder.write("No new plays in the last 5 seconds.")
                    else:
                        df_placeholder.write("No new plays in the last 5 seconds.")

            # Polling loop
            while not game_over:
                if is_halftime(st.session_state.selected_game_id):
                    refresh_interval = 60  # 1 minute during halftime
                else:
                    refresh_interval = 3  # 5 seconds during regular play

                for i in range(refresh_interval, 0, -1):
                    if is_halftime(st.session_state.selected_game_id):
                        refresh_placeholder.markdown(f"🏀 Halftime - Next refresh in **{i}** seconds")
                    else:
                        refresh_placeholder.markdown(f"🔄 Next refresh in **{i}** seconds")
                    time.sleep(1)
                update_display()  # Update the display
                game_over = is_game_over(st.session_state.selected_game_id)

            st.write("Game has ended. Final plays displayed above.")