import streamlit as st
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from nba_api.stats.endpoints import scoreboardv2, teamdetails
from nba_api.live.nba.endpoints import boxscore
import json
from datetime import datetime
import pytz
import sqlite3

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'streamlit_app',
    'auto.offset.reset': 'earliest'
}

# Database connection
db_name = 'nba_plays.db'
conn = sqlite3.connect(db_name)
cursor = conn.cursor()

# Initialize Kafka consumer
consumer = Consumer(kafka_config)

st.set_page_config(page_title="Real-Time NBA Stats App", page_icon=":basketball:")

st.title(":basketball: Real-Time NBA Stats App (Beta)")

st.write("This app allows you to search for NBA games and view real-time plays and stats for those games.")

def is_game_over(game_id):
    try:
        box = boxscore.BoxScore(game_id)
        game_data = box.get_dict()
        game_status = game_data['game']['gameStatus']
        return game_status == 3  # 3 indicates the game has ended
    except Exception as e:
        print(f"Error checking game status for {game_id}: {str(e)}")
        return False

def get_all_plays_from_db(game_id):
    cursor.execute('''
    SELECT * FROM plays
    WHERE game_id = ?
    ORDER BY action_number DESC
    ''', (game_id,))
    return cursor.fetchall()

def get_team_name(team_id):
    team_details = teamdetails.TeamDetails(team_id=team_id)
    team_data = json.loads(team_details.get_json())
    return team_data['resultSets'][0]['rowSet'][0][1]

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
            label = f"{away_team} vs. {home_team} ({game_time_str.strip()})"
            if st.button(label=label, key=f"game_{game_id}", use_container_width=True):
                st.session_state.selected_game_id = game_id
                st.session_state.selected_game_label = label
                st.session_state.home_team = home_team
                st.session_state.away_team = away_team

    # Display selected game and start ingestion
    if 'selected_game_id' in st.session_state:
        st.write(f"Selected Game: {st.session_state.selected_game_label}")
        if st.button("View Game Plays"):
            game_over = is_game_over(st.session_state.selected_game_id)
            
            if game_over:
                st.write("This game has ended. Displaying all plays from the database.")
            else:
                st.write("This game is still in progress. Displaying current plays from the database.")
            all_plays = get_all_plays_from_db(st.session_state.selected_game_id)
            if all_plays:
                df = pd.DataFrame(all_plays, columns=['game_id', 'action_number', 'clock', 'timeActual', 'period', 'periodType',
                                                      'team_id', 'teamTricode', 'actionType', 'subType', 'descriptor',
                                                      'qualifiers', 'personId', 'x', 'y', 'possession', 'scoreHome', 'scoreAway', 'description'])
                df['clock'] = df['clock'].str.replace('PT', '').str.replace('M', ':').str.replace('S', '')
                df = df[['period', 'teamTricode', 'clock', 'description', 'scoreHome', 'scoreAway']]
                df.rename(columns={'teamTricode': 'team', 'clock': 'time remaining'}, inplace=True)
                
                if not df.empty:
                    # Display final score
                    final_play = df.iloc[0]
                    st.header(f"Final Score: {st.session_state.away_team} {final_play['scoreAway']} - {st.session_state.home_team} {final_play['scoreHome']}")
                    
                    # Display all plays
                    st.dataframe(df)
                else:
                    st.write("No plays found for this game.")
            else:
                st.write("No plays found for this game in the database.")
        else:
            try:
                # Subscribe to the topic for the selected game
                topic = "nba-plays"
                consumer.subscribe([topic])

                # Create empty lists to store the plays and scores
                plays = []
                home_score = 0
                away_score = 0

                # Create placeholders for the score and DataFrame
                score_placeholder = st.empty()
                df_placeholder = st.empty()

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

                    # Update scores
                    home_score = int(play['scoreHome'])
                    away_score = int(play['scoreAway'])

                    # Update score display
                    score_placeholder.header(f"Current Score: {st.session_state.away_team} {away_score} - {st.session_state.home_team} {home_score}")

                    # Create a DataFrame from the plays
                    df = pd.DataFrame(plays)
                    df = df[df['teamTricode'].isin([st.session_state.away_team, st.session_state.home_team])]
                    df['clock'] = df['clock'].str.replace('PT', '').str.replace('M', ':').str.replace('S', '')
                    df = df.sort_values(by='actionNumber', ascending=False)
                    df = df[['period', 'teamTricode', 'clock', 'description']]
                    df.rename(columns={'teamTricode': 'team', 'clock': 'time remaining'}, inplace=True)
                    df = df.drop_duplicates()

                    # Update the DataFrame display
                    df_placeholder.dataframe(df)
            except Exception as e:
                st.error(f"An error occurred while fetching game plays: {str(e)}")

            finally:
                consumer.close()