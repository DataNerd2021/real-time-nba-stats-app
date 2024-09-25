from kafka import KafkaProducer
from kafka import KafkaConsumer
from nba_api.live.nba.endpoints import playbyplay
from nba_api.stats.endpoints import scoreboardv2
import json
import time
import pandas as pd
from datetime import datetime
from simple_term_menu import TerminalMenu

# set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# set up Kafka consumer
consumer = KafkaConsumer(
    'nba_playbyplay',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def send_message(topic, data):
    producer.send(topic, value=data)
    producer.flush()

def fetch_game_plays(game_id, home_team, away_team):
    try:
        plays = playbyplay.PlayByPlay(game_id).get_dict()
        game_plays = plays['game']['actions']
        if not game_plays:
            print(f"No plays found for game ID: {game_id}")
            return []

        play_messages = []
        for play in game_plays:
            fields = ['actionNumber', 'period', 'description', 'clock', 'qualifiers', 'scoreAway', 'scoreHome', 'x', 'y']
            play_message = {field: play.get(field) for field in fields}
            play_message['home_team'] = home_team
            play_message['away_team'] = away_team
            play_messages.append(play_message)
            print(play_message)

        return play_messages
    except Exception as e:
        print(f"Error fetching game plays: {e}")
        return []

def produce_nba_playbyplay_messages(game_id, home_team, away_team, topic):
    while True:
        plays = fetch_game_plays(game_id, home_team, away_team)

        if not plays:
            print("No plays to send. Waiting before retrying...")
            time.sleep(60)  # Wait for 60 seconds before trying again
            continue

        for play in plays:
            send_message(topic, play)
            print(f"Sent message: {play}")
            time.sleep(5)

        time.sleep(60)  # Wait for 60 seconds before fetching new plays

if datetime.today() >= datetime.strptime("10-04-2024", '%m-%d-%Y'):
    game_date = datetime.today().strftime('%m-%d-%Y')
else:
    game_date = "04-01-2024"

game_ids = pd.read_parquet('Static Files/2024-25_game_ids.parquet')
teams = pd.read_parquet('Static Files/team_details.parquet')
headers = json.loads(scoreboardv2.ScoreboardV2(game_date=game_date).get_json())['resultSets'][0]['headers']
df = pd.DataFrame(json.loads(scoreboardv2.ScoreboardV2(game_date=game_date).get_json())['resultSets'][0]['rowSet'], columns=headers)
home_teams = teams.merge(df, left_on='TEAM_ID', right_on='HOME_TEAM_ID', how='inner')
home_teams['home_team'] = home_teams['CITY'] + ' ' + home_teams['NICKNAME']
home_teams = home_teams[['GAME_ID', 'home_team']]
visitor_teams = teams.merge(df, left_on='TEAM_ID', right_on='VISITOR_TEAM_ID', how='inner')
visitor_teams['away_team'] = visitor_teams['CITY'] + ' ' + visitor_teams['NICKNAME']
visitor_teams = visitor_teams[['GAME_ID', 'away_team']]
matchups = home_teams.merge(visitor_teams, how='left').fillna('External')
terminal_menu = TerminalMenu(matchups.apply(lambda row: f"{row['home_team']} vs {row['away_team']} (Game ID: {row['GAME_ID']})", axis=1).tolist())
print(f'Showing all games for {game_date}:\n')
choice_index = terminal_menu.show()
selected_game_id = matchups.iloc[choice_index]['GAME_ID']
selected_home_team = matchups.iloc[choice_index]['home_team']
selected_away_team = matchups.iloc[choice_index]['away_team']
produce_nba_playbyplay_messages(game_id=selected_game_id, home_team=selected_home_team, away_team=selected_away_team, topic='nba_playbyplay')