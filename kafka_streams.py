from nba_api.stats.endpoints import leaguegamefinder
from nba_api.live.nba.endpoints import playbyplay
from confluent_kafka import Producer
import json
import time
from datetime import datetime
import threading

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'nba-play-producer'
}

# Create Kafka producer
producer = Producer(kafka_config)

# Kafka topic name
topic = 'nba-plays'

def get_recent_games(num_games=10):
    gamefinder = leaguegamefinder.LeagueGameFinder(league_id_nullable='00', season_nullable='2023-24', date_nullable=datetime.now().strftime('%Y-%m-%d'))
    games = gamefinder.get_data_frames()[0]
    recent_games = games.sort_values('GAME_DATE', ascending=False).head(num_games)

    return [(row['GAME_ID'], f"{row['MATCHUP']} - {row['GAME_DATE']}") for _, row in recent_games.iterrows()]

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] with key {msg.key().decode("utf-8")}')

def stream_game_plays(game_id, game_info):
    print(f"Starting to stream plays for game: {game_info}")
    pbp = playbyplay.PlayByPlay(game_id)
    plays = pbp.get_dict()['game']['actions']

    for i, play in enumerate(plays, 1):
        play_json = json.dumps(play)
        # Create a key combining game_id and play number
        key = f"{game_id}_{i:05d}"  # Pad with zeros to ensure correct sorting
        producer.produce(topic, key=key, value=play_json, callback=delivery_report)
        producer.flush()
        if i % 50 == 0:  # Print progress every 50 plays
            print(f"Game {game_id}: Sent {i}/{len(plays)} plays")
        time.sleep(0.05)  # Reduced sleep time for faster processing

    print(f"\nCompleted: Sent {len(plays)} plays for game {game_id} to topic '{topic}'")

def stream_all_games(games):
    threads = []
    for game_id, game_info in games:
        thread = threading.Thread(target=stream_game_plays, args=(game_id, game_info))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    print("Fetching recent games...")
    recent_games = get_recent_games()
    if not recent_games:
        print("No games available. This should not happen unless there's an issue with the NBA API.")
    else:
        print(f"Found {len(recent_games)} recent games. Starting to stream all games...")
        stream_all_games(recent_games)
        print("All games have been streamed.")