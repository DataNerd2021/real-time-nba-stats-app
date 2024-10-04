import streamlit as st
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaError
from nba_api.stats.endpoints import scoreboardv2
import json
import threading
import time
from datetime import datetime
import pytz

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Adjust this if needed
    'group.id': 'streamlit_app',
    'auto.offset.reset': 'latest'
}

# Initialize Kafka consumer
consumer = Consumer(kafka_config)

# Initialize Kafka producer
producer = Producer(kafka_config)

# Function to get today's games in US/Mountain timezone
def get_todays_games():
    mountain_tz = pytz.timezone('America/Denver')
    today = datetime.now(mountain_tz).strftime('%Y-%m-%d')
    games_data = scoreboardv2.ScoreboardV2(game_date=today).get_dict()
    games = games_data['resultSets'][0]['rowSet']
    game_info = games_data['resultSets'][1]['rowSet']

    current_time = datetime.now(mountain_tz).time()

    # Filter games that are currently occurring
    ongoing_games = []
    for game in games:
        game_id = game[2]
        home_team = game_info[games.index(game) * 2][4]
        away_team = game_info[games.index(game) * 2 + 1][4]
        game_start_time = game[0]  # Assuming this is the start time in UTC

        # Convert game start time to Mountain Time
        game_start_time_mountain = datetime.strptime(str(game_start_time), '%Y-%m-%dT%H:%M:%S').astimezone(mountain_tz).time()

        # Check if the game is ongoing
        if game_start_time_mountain <= current_time:
            ongoing_games.append((game_id, home_team, away_team))

    return ongoing_games

# Function to stream plays for a single game
def stream_game_plays(game_id):
    print(f"Starting to stream plays for game: {game_id}")

    # Simulate streaming game plays (replace this with actual logic)
    for i in range(10):  # Simulating 10 plays for demonstration
        play_data = {
            'game_id': game_id,
            'play_number': i,
            'description': f"Play {i} for game {game_id}"
        }

        # Produce the play data to Kafka
        producer.produce('nba-plays', json.dumps(play_data).encode('utf-8'))
        producer.flush()  # Ensure the message is sent immediately
        time.sleep(1)  # Simulate time between plays

# Function to stream all games
def stream_all_games(games: list):
    threads = []

    for game in games:
        game_id = game[0]  # Assuming game[0] contains the game_id
        thread = threading.Thread(target=stream_game_plays, args=(game_id,))
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

# Function to consume messages from Kafka
def consume_messages():
    consumer.subscribe(['nba-plays'])  # Adjust the topic name as needed

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f'Error while consuming message: {msg.error()}')
                    break
            else:
                # Process the message
                play_data = json.loads(msg.value().decode('utf-8'))
                print(f"Consumed message: {play_data}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Streamlit app
if __name__ == "__main__":

    # Get today's games
    games = get_todays_games()

    if games:
        print(f"Found {len(games)} ongoing games. Starting to stream all games...")
        stream_all_games(games)
    else:
        print("No ongoing games found for today.")