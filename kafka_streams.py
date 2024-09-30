from nba_api.live.nba.endpoints import scoreboard, playbyplay
from confluent_kafka import Producer
import json
import time
from datetime import datetime, timedelta

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'nba-play-producer'
}

# Create Kafka producer
producer = Producer(kafka_config)

# Kafka topic name
topic = 'nba-plays'

def get_games(date):
    games = scoreboard.ScoreBoard(game_date=date).get_dict()['games']
    return [(game['gameId'], f"{game['awayTeam']['teamName']} vs {game['homeTeam']['teamName']}") for game in games]

def get_available_games():
    today = datetime.now().strftime("%Y-%m-%d")
    games = get_games(today)

    if not games:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        games = get_games(yesterday)

    return games

def select_game(games):
    print("Available games:")
    for i, (game_id, game_name) in enumerate(games, 1):
        print(f"{i}. {game_name}")

    while True:
        try:
            selection = int(input("Enter the number of the game you want to stream: ")) - 1
            if 0 <= selection < len(games):
                return games[selection][0]
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def stream_game_plays(game_id):
    pbp = playbyplay.PlayByPlay(game_id)
    plays = pbp.get_dict()['game']['actions']

    for play in plays:
        play_json = json.dumps(play)
        producer.produce(topic, value=play_json, callback=delivery_report)
        producer.flush()
        time.sleep(0.1)

    print(f"Sent {len(plays)} plays to topic '{topic}'")

if __name__ == "__main__":
    available_games = get_available_games()
    if not available_games:
        print("No games available. Please try again later.")
    else:
        selected_game_id = select_game(available_games)
        stream_game_plays(selected_game_id)