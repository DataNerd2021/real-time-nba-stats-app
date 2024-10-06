from nba_api.stats.endpoints import scoreboardv2
from nba_api.live.nba.endpoints import playbyplay, boxscore
from confluent_kafka import Producer, Consumer, KafkaError
from nba_api.live.nba.endpoints import playbyplay
from nba_api.stats.endpoints import scoreboardv2
import json
import threading
import pytz
import requests
import sqlite3

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'nba-play-producer'
}

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'kafka_to_sqlite_transfer',
    'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
}

producer = Producer(conf)

topic = 'nba-plays'

# Database setup
db_name = 'nba_plays.db'
conn = sqlite3.connect(db_name)
cursor = conn.cursor()

# Create table if it doesn't exist
def create_table_if_not_exists(cursor):
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS plays (
        game_id TEXT,
        action_number INTEGER,
        clock TEXT,
        timeActual TEXT,
        period INTEGER,
        periodType TEXT,
        team_id INTEGER,
        teamTricode TEXT,
        actionType TEXT,
        subType TEXT,
        descriptor TEXT,
        qualifiers TEXT,
        personId INTEGER,
        x REAL,
        y REAL,
        possession INTEGER,
        scoreHome TEXT,
        scoreAway TEXT,
        description TEXT,
        PRIMARY KEY (game_id, action_number)
    )
    ''')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] with key {msg.key().decode("utf-8")}')

def get_todays_games():
    today = date.today().strftime('%Y-%m-%d')
    print(today)
    scoreboard = scoreboardv2.ScoreboardV2(game_date=today)
    games_data = json.loads(scoreboard.get_json())
    games = games_data['resultSets'][0]['rowSet']
    return [(game[2], f"{game[6]} vs {game[7]}") for game in games]

def is_game_over(game_id):
    try:
        box = boxscore.BoxScore(game_id)
        game_data = box.get_dict()
        game_status = game_data['game']['gameStatus']
        return game_status == 3  # 3 indicates the game has ended
    except Exception as e:
        print(f"Error checking game status for {game_id}: {str(e)}")
        return False

def insert_play(cursor, game_id, play):
    cursor.execute('''
    INSERT OR REPLACE INTO plays (
        game_id, action_number, clock, timeActual, period, periodType,
        team_id, teamTricode, actionType, subType, descriptor,
        qualifiers, personId, x, y, possession, scoreHome, scoreAway, description
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        game_id, play.get('actionNumber'), play.get('clock'),
        play.get('timeActual'), play.get('period'), play.get('periodType'),
        play.get('teamId'), play.get('teamTricode'), play.get('actionType'),
        play.get('subType'), play.get('descriptor'),
        json.dumps(play.get('qualifiers')), play.get('personId'),
        play.get('x'), play.get('y'), play.get('possession'),
        play.get('scoreHome'), play.get('scoreAway'), play.get('description')
    ))

def stream_game_plays(game_id, game_info):
    print(f"Starting to stream plays for game: {game_info}")
    last_event_num = 0
    consecutive_empty_responses = 0
    max_empty_responses = 10  # Adjust this value as needed

    while True:
        try:
            if is_game_over(game_id):
                print(f"Game {game_info} has ended. Stopping stream.")
                break

            pbp = playbyplay.PlayByPlay(game_id)
            plays = pbp.get_dict().get('game', {}).get('actions', [])
            
            if not plays:
                consecutive_empty_responses += 1
                if consecutive_empty_responses >= max_empty_responses:
                    print(f"No new plays for game {game_info} after {max_empty_responses} attempts. Stopping stream.")
                    break
                print(f"No plays available for game {game_info}. Waiting... (Attempt {consecutive_empty_responses})")
                time.sleep(30)  # Wait longer if no plays are available
                continue

            consecutive_empty_responses = 0  # Reset the counter when we get plays
            new_plays = [play for play in plays if play['actionNumber'] > last_event_num]

            for play in new_plays:
                play['gameId'] = game_id  # Add game_id to the play data
                play_json = json.dumps(play)
                key = f"{game_id}_{play['actionNumber']:05d}"
                producer.produce(topic, key=key, value=play_json, callback=delivery_report)
                producer.flush()
                insert_play(play)  # Insert play into the database

                last_event_num = max(last_event_num, play['actionNumber'])

            if len(new_plays) > 0:
                print(f"Game {game_info}: Sent {len(new_plays)} new plays. Last event number: {last_event_num}")
            else:
                print(f"No new plays for game {game_info}. Last event number: {last_event_num}")

        except json.JSONDecodeError as e:
            print(f"JSON Decode Error for game {game_info}: {str(e)}")
            time.sleep(10)  # Wait before retrying
        except requests.exceptions.RequestException as e:
            print(f"Network error for game {game_info}: {str(e)}")
            time.sleep(30)  # Wait longer before retrying after a network error
        except Exception as e:
            print(f"Error processing game {game_info}: {str(e)}")
            time.sleep(10)  # Wait before retrying

        time.sleep(10)  # Poll for new plays every 10 seconds

def transfer_kafka_to_sqlite():
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    create_table_if_not_exists(cursor)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('Reached end of partition')
                else:
                    print(f'Error while consuming message: {msg.error()}')
    except KeyboardInterrupt:
        print("Transfer interrupted by user")

    finally:
        consumer.close()

def stream_all_games(games):
    threads = []
    for game_id, game_info in games:
        thread = threading.Thread(target=stream_game_plays, args=(game_id, game_info))
        threads.append(thread)
        thread.start()

    # Start the consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.start()

    # Wait for all game threads to complete
    for thread in threads:
        thread.join()

    # Wait for the consumer thread to complete (it will run indefinitely until interrupted)
    consumer_thread.join()


if __name__ == "__main__":
    today = date.today().strftime('%Y-%m-%d')
    if today:
        games = get_games_for_date()
        if games:
            print(f"Found {len(games)} games. Starting to stream all games...")
            stream_all_games(games)
            print("All games have been streamed.")
    else:
        print("Game Ended")