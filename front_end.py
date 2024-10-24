import streamlit as st
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from nba_api.stats.endpoints import scoreboardv2, teamdetails
from nba_api.live.nba.endpoints import boxscore
import json
import time
from datetime import datetime, date
import pytz
import duckdb
from requests import ReadTimeout, ConnectionError
import plotly.graph_objects as go
from duckdb import IOException
import os

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Adjust this if needed
    'group.id': 'streamlit_app',
    'auto.offset.reset': 'latest'
}

# Initialize Kafka consumer
consumer = Consumer(kafka_config)

st.set_page_config(page_title="Real-Time NBA Stats App", page_icon=":basketball:", layout="wide")

# custom CSS code
st.markdown("""
    <style>
        .title-container {
            display: flex;
            justify-content: center;
            align-items: center;
        }
        .title-text {
            font-size: 4em;
            font-weight: bold;
            margin-left: 10px;
        }
        .emoji {
            font-size: 4em;
    </style>
    """, unsafe_allow_html=True)

st.markdown("""<div class="title-container">
                <span class="emoji">🏀</span>
                <span class="title-text">Real-Time NBA Stats App (Beta)</span>
               </div>""", unsafe_allow_html=True)

st.markdown("<h5 style='text-align: center; margin-top: 10px;'>This app allows you to search for NBA games and view real-time plays for those games.</h3>", unsafe_allow_html=True)

def is_game_over(game_id):
    try:
        box = boxscore.BoxScore(game_id)
        game_data = box.get_dict()
        game_status = game_data['game']['gameStatus']
        if game_status == 3:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error checking game status for {game_id}: {str(e)}")
        return False

def is_halftime(game_id):
    try:
        box = boxscore.BoxScore(game_id)
        game_data = box.get_dict()
        period = game_data['game']['period']
        game_status = game_data['game']['gameStatus']
        if game_status == 2:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error checking halftime status for {game_id}: {str(e)}")
        return False

def get_all_plays_from_file(game_id):
    today = date.today().strftime('%Y-%m-%d')
    try:
        plays = duckdb.sql(f"SELECT * FROM read_json('Game Plays/{game_id}_{today}.jsonl', auto_detect=True)")
    except IOError:
        st.write('Game has not started yet. Please check later.')
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
    consumer.subscribe(['nba-game-plays'])

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
        if play_data['gameId'] == game_id:
            plays.append(play_data)

    consumer.close()
    return plays
def update_display():
                columns = [
                            'game_id', 'action_number', 'clock', 'timeActual', 'period', 'periodType',
                            'team_id', 'teamTricode', 'actionType', 'subType', 'descriptor',
                            'qualifiers', 'personId', 'x', 'y', 'possession', 'scoreHome', 'scoreAway', 'description'
                        ]
                all_plays = get_all_plays_from_file(st.session_state.selected_game_id)
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
def update_stats_display():
                today = date.today().strftime('%Y-%m-%d')
                file = f'Game Statistics/{st.session_state.selected_game_id}_{today}.jsonl'
                
                if not os.path.exists(file):
                    st.write("Game has not started. Please check game start time above.")
                    return False

                # Sample data
                categories = duckdb.sql(f"""
                                        SELECT 
                                            'Offensive Rating',
                                            'Defensive Rating',
                                            'Net Rating',
                                            'Points In The Paint',
                                            'Second Chance Points',
                                            'Points From Turnovers',
                                            'Effective FG %',
                                            'True Shooting %',
                                            'Assist-Turnover Ratio'
                        """).to_df().columns.tolist()

                inverse_categories = ['Defensive Rating']

                home_stats = duckdb.sql(f"""
                                WITH home_players AS(
                                    SELECT 
                                        UNNEST(home_players) AS home_players
                                    FROM read_json('Game Statistics/{st.session_state.selected_game_id}_{today}.jsonl', auto_detect=True)
                                ),
                                
                                home_player_stats AS(
                                    SELECT 
                                        home_players.statistics AS player_stats,
                                        home_players.name AS player_name
                                    FROM home_players
                                )
                                
                                SELECT 
                                    ROUND((home_team.points/(home_team.fieldGoalsAttempted + (0.44 * home_team.freeThrowsAttempted) - home_team.reboundsOffensive + home_team.turnovers))*100, 2) AS 'Offensive Rating',
                                    ROUND((away_team.points/(away_team.fieldGoalsAttempted + (0.44 * away_team.freeThrowsAttempted) - away_team.reboundsOffensive + away_team.turnovers))*100, 2) AS 'Defensive Rating',
                                    ROUND((home_team.points/(home_team.fieldGoalsAttempted + (0.44 * home_team.freeThrowsAttempted) - home_team.reboundsOffensive + home_team.turnovers))*100, 2) 
                                        - ROUND((away_team.points/(away_team.fieldGoalsAttempted + (0.44 * away_team.freeThrowsAttempted) - away_team.reboundsOffensive + away_team.turnovers))*100, 2) AS 'Net Rating',
                                    home_team.pointsInThePaint AS 'Points In The Paint',
                                    home_team.pointsSecondChance AS 'Second Chance Points',
                                    home_team.pointsFromTurnovers AS 'Points From Turnovers',
                                    ROUND(home_team.fieldGoalsEffectiveAdjusted, 4)*100 AS 'Effective FG %',
                                    ROUND(home_team.trueShootingPercentage, 4)*100 AS 'True Shooting %',
                                    home_team.assistsTurnoverRatio AS 'Assist-Turnover Ratio',
                                FROM read_json('Game Statistics/{st.session_state.selected_game_id}_{today}.jsonl', auto_detect=True)
                                CROSS JOIN home_player_stats
                                GROUP BY home_team, away_team
                                LIMIT 1""").to_df().values.tolist()[0]

                away_stats = duckdb.sql(f"""
                                 WITH away_players AS(
                                    SELECT 
                                        UNNEST(away_players) AS away_players
                                    FROM read_json('Game Statistics/{st.session_state.selected_game_id}_{today}.jsonl', auto_detect=True)
                                ),
                                
                                away_player_stats AS(
                                    SELECT 
                                        away_players.statistics AS player_stats,
                                        away_players.name AS player_name
                                    FROM away_players
                                )
                                
                                SELECT 
                                    ROUND((away_team.points/(away_team.fieldGoalsAttempted + (0.44 * away_team.freeThrowsAttempted) - away_team.reboundsOffensive + away_team.turnovers))*100, 2) AS 'Offensive Rating',
                                    ROUND((home_team.points/(home_team.fieldGoalsAttempted + (0.44 * home_team.freeThrowsAttempted) - home_team.reboundsOffensive + home_team.turnovers))*100, 2) AS 'Defensive Rating',
                                    ROUND((away_team.points/(away_team.fieldGoalsAttempted + (0.44 * away_team.freeThrowsAttempted) - away_team.reboundsOffensive + away_team.turnovers))*100, 2) 
                                        - ROUND((home_team.points/(home_team.fieldGoalsAttempted + (0.44 * home_team.freeThrowsAttempted) - home_team.reboundsOffensive + home_team.turnovers))*100, 2) AS 'Net Rating',
                                    away_team.pointsInThePaint AS 'Points In The Paint',
                                    away_team.pointsSecondChance AS 'Second Chance Points',
                                    away_team.pointsFromTurnovers AS 'Points From Turnovers',
                                    ROUND(away_team.fieldGoalsEffectiveAdjusted, 4)*100 AS 'Effective FG %',
                                    ROUND(away_team.trueShootingPercentage, 4)*100 AS 'True Shooting %',
                                    away_team.assistsTurnoverRatio AS 'Assist-Turnover Ratio',
                                FROM read_json('Game Statistics/{st.session_state.selected_game_id}_{today}.jsonl', auto_detect=True)
                                CROSS JOIN away_player_stats
                                GROUP BY home_team, away_team
                                LIMIT 1""").to_df().values.tolist()[0]
                home_mvp = duckdb.sql(f"""
                                    WITH home_players AS(
                                        SELECT 
                                            UNNEST(home_players) AS home_players
                                        FROM read_json('Game Statistics/{st.session_state.selected_game_id}_{today}.jsonl', auto_detect=True)
                                    ),
           
                                    home_player_stats AS(
                                        SELECT home_players.statistics AS player_stats,
                                        home_players.name AS player_name
                                        FROM home_players
                                    ),

                                    mvp AS(
                                    SELECT 
                                        home_team.team_name AS team_name,
                                        player_name AS 'Team MVP', 
                                        MAX((player_stats.points::FLOAT + player_stats.fieldGoalsMade::FLOAT + player_stats.freeThrowsMade::FLOAT - player_stats.fieldGoalsAttempted::FLOAT 
                                        - player_stats.freeThrowsAttempted::FLOAT + player_stats.reboundsDefensive::FLOAT + (player_stats.reboundsOffensive::FLOAT/2::FLOAT) + player_stats.assists::FLOAT 
                                        + player_stats.steals::FLOAT + (player_stats.blocks::FLOAT/2::FLOAT) - player_stats.foulsPersonal::FLOAT - player_stats.turnovers::FLOAT)::FLOAT
                                            / 
                                        (home_team.points + home_team.fieldGoalsMade + home_team.freeThrowsMade - home_team.fieldGoalsAttempted - home_team.freeThrowsAttempted + home_team.reboundsDefensive 
                                        + (home_team.reboundsOffensive::FLOAT/2::FLOAT) + home_team.assists + home_team.steals + (home_team.blocks::FLOAT/2::FLOAT) - home_team.foulsPersonal - home_team.turnovers)::FLOAT) AS 'PIE'
                                    FROM read_json('Game Statistics/{st.session_state.selected_game_id}_{today}.jsonl', auto_detect=True)
                                    CROSS JOIN home_player_stats
                                    GROUP BY home_team, away_team, player_name
                                    ORDER BY PIE DESC
                                    LIMIT 1)
                                    
                                    SELECT player_name
                                    FROM mvp""").to_df().values.tolist()[0]
                
                away_mvp = duckdb.sql(f"""
                                    WITH away_players AS(
                                        SELECT 
                                            UNNEST(away_players) AS away_players
                                        FROM read_json('Game Statistics/{st.session_state.selected_game_id}_{today}.jsonl', auto_detect=True)
                                    ),
           
                                    away_player_stats AS(
                                        SELECT away_players.statistics AS player_stats,
                                        away_players.name AS player_name
                                        FROM away_players
                                    ),

                                    mvp AS(
                                    SELECT 
                                        away_team.team_name AS team_name,
                                        player_name AS 'Team MVP', 
                                        MAX((player_stats.points::FLOAT + player_stats.fieldGoalsMade::FLOAT + player_stats.freeThrowsMade::FLOAT - player_stats.fieldGoalsAttempted::FLOAT 
                                        - player_stats.freeThrowsAttempted::FLOAT + player_stats.reboundsDefensive::FLOAT + (player_stats.reboundsOffensive::FLOAT/2::FLOAT) + player_stats.assists::FLOAT 
                                        + player_stats.steals::FLOAT + (player_stats.blocks::FLOAT/2::FLOAT) - player_stats.foulsPersonal::FLOAT - player_stats.turnovers::FLOAT)::FLOAT
                                            / 
                                        (away_team.points + away_team.fieldGoalsMade + away_team.freeThrowsMade - away_team.fieldGoalsAttempted - away_team.freeThrowsAttempted + away_team.reboundsDefensive 
                                        + (away_team.reboundsOffensive::FLOAT/2::FLOAT) + away_team.assists + away_team.steals + (away_team.blocks::FLOAT/2::FLOAT) - away_team.foulsPersonal - away_team.turnovers)::FLOAT) AS 'PIE'
                                    FROM read_json('Game Statistics/{st.session_state.selected_game_id}_{today}.jsonl', auto_detect=True)
                                    CROSS JOIN away_player_stats
                                    GROUP BY home_team, away_team, player_name
                                    ORDER BY PIE DESC
                                    LIMIT 1)
                                    
                                    SELECT player_name
                                    FROM mvp""").to_df().values.tolist()[0]

                st.markdown(f"""<div class="title-container">
                <span class="title-text">{st.session_state.home_team} vs. {st.session_state.away_team} Stats Comparison</span>
               </div>""", unsafe_allow_html=True)
                st.header(f"""Team MVPs:""")
                st.markdown(f"""Home: {home_mvp}<br>Away: {away_mvp}""", unsafe_allow_html=True)

                selected_categories = st.multiselect(
                    'Select categories',
                    options=['Show All'] + categories,
                    default=['Show All']
                )

                if 'Show All' in selected_categories:
                    selected_categories = categories

                return categories, home_stats, away_stats, selected_categories, inverse_categories
def create_figure(categories, home_stats, away_stats, selected_categories, inverse_categories):
                fig = go.Figure()
                
                home_colors = [
                    'green' if (a >= b and category not in inverse_categories) or (a < b and category in inverse_categories)
                    else 'red'
                    for a, b, category in zip(home_stats, away_stats, categories)
                    if category in selected_categories
                ]
                
                away_colors = [
                    'green' if (a <= b and category not in inverse_categories) or (a > b and category in inverse_categories)
                    else 'red'
                    for a, b, category in zip(home_stats, away_stats, categories)
                    if category in selected_categories
                    ]

                # Prepare data for selected categories
                selected_home_stats = [val for val, cat in zip(home_stats, categories) if cat in selected_categories]
                selected_away_stats = [val for val, cat in zip(away_stats, categories) if cat in selected_categories]
                selected_cats = [cat for cat in categories if cat in selected_categories]

                # Add bars for home team
                fig.add_trace(go.Bar(
                    y=[cat for cat in categories if cat in selected_categories],
                    x=[-val for val, cat in zip(home_stats, categories) if cat in selected_categories],
                    name=st.session_state.home_team,
                    orientation='h',
                    marker_color=home_colors,
                    text=selected_home_stats,
                    textposition='outside',
                    textfont=dict(size=10),
                    insidetextanchor='start',
                    hovertemplate=[f'{st.session_state.home_team}<br>{cat}: {val}' for cat, val in zip(categories, home_stats) if cat in selected_categories]
                ))

                # Add bars for away team
                fig.add_trace(go.Bar(
                    y=[cat for cat in categories if cat in selected_categories],
                    x=[val for val, cat in zip(away_stats, categories) if cat in selected_categories],
                    name=st.session_state.away_team,
                    orientation='h',
                    marker_color=away_colors,
                    text=selected_away_stats,
                    textposition='outside',
                    textfont=dict(size=10),
                    insidetextanchor='end',
                    hovertemplate=[f"{st.session_state.away_team}<br>{cat}: {val}" for cat, val in zip(categories, away_stats) if cat in selected_categories]
                ))

                # Update layout
                fig.update_layout(
                    barmode='relative',
                    title=f"{st.session_state.home_team} vs {st.session_state.away_team} - Team Statistics",
                    height=400,
                    yaxis=dict(
                        title='',
                        tickfont=dict(size=12),
                        categoryorder='array',
                        categoryarray=selected_cats
                    ),
                    xaxis=dict(
                        title='',
                        tickfont=dict(size=10),
                        zeroline=True,
                        zerolinewidth=2,
                        zerolinecolor='black'
                    ),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    margin=dict(l=0, r=0, t=40, b=0),
                    annotations=[
                        dict(
                            x=-0.25,
                            y=1.05,
                            xref='paper',
                            yref='paper',
                            text=st.session_state.home_team,
                            showarrow=False,
                            font=dict(size=14, color='blue')
                        ),
                        dict(
                            x=0.75,
                            y=1.05,
                            xref='paper',
                            yref='paper',
                            text=st.session_state.away_team,
                            showarrow=False,
                            font=dict(size=14, color='red')
                        )
                    ]
                )

                # Add vertical line at x=0
                fig.add_shape(
                    type="line",
                    x0=0, y0=-0.5, x1=0, y1=len(selected_cats)-0.5,
                    line=dict(color="black", width=2)
                )

                # Update x-axis to show absolute values
                max_value = max(max(abs(val) for val in selected_home_stats), max(abs(val) for val in selected_away_stats))
                tick_vals = list(range(-int(max_value), int(max_value)+1, max(1, int(max_value//5))))
                fig.update_xaxes(tickvals=tick_vals, ticktext=[str(abs(val)) for val in tick_vals])

                return fig
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
        elif st.button('View Team Statistics', key='view_stats'):
            st.session_state.viewing_stats = True
            game_over = is_game_over(st.session_state.selected_game_id)

            # Create placeholders for the DataFrame
            df_placeholder = st.empty()

            
            

            try:
                result = update_stats_display()
                if result:
                    categories, home_stats, away_stats, selected_categories, inverse_categories = result
                if result:
                    categories, home_stats, away_stats, selected_categories, inverse_categories = result
                    fig = create_figure(categories, home_stats, away_stats, selected_categories, inverse_categories)
                    st.plotly_chart(fig, use_container_width=True)
                time.sleep(5)  # Update the display every 5 seconds
                game_over = is_game_over(st.session_state.selected_game_id)
            except IOException:
                st.write('Game has not started yet. Please check game start time.')
                
            if game_over:
                st.write("Game has ended. Final statistics displayed above.")
        elif 'viewing_stats' in st.session_state and st.session_state.viewing_stats:
            if st.button('Refresh Team Statistics', key='refresh_stats'):
                game_over = is_game_over(st.session_state.selected_game_id)

                # Create placeholders for the DataFrame
                df_placeholder = st.empty()

                try:
                    result = update_stats_display()
                    if result:
                        categories, home_stats, away_stats, selected_categories, inverse_categories = result
                        fig = create_figure(categories, home_stats, away_stats, selected_categories, inverse_categories)
                        st.plotly_chart(fig, use_container_width=True)
                    time.sleep(5)  # Update the display every 5 seconds
                    game_over = is_game_over(st.session_state.selected_game_id)
                except IOException:
                    st.write('Game has not started yet. Please check game start time.')
                    
                if game_over:
                    st.write("Game has ended. Final statistics displayed above.")