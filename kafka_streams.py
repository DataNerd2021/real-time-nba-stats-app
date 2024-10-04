from nba_api.stats.endpoints import leaguegamefinder
from nba_api.live.nba.endpoints import playbyplay
from confluent_kafka import Producer, Consumer, KafkaError
import json
import time
from datetime import datetime, timedelta
import threading
import pandas as pd

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'nba-play-producer'
}

producer = Producer(conf)

# Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'nba-play-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)

topic = 'nba-plays'



def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] with key {msg.key().decode("utf-8")}')


def get_games_for_date(date):
    gamefinder = leaguegamefinder.LeagueGameFinder(
        date_from_nullable=date.strftime('%m/%d/%Y'),
        date_to_nullable=date.strftime('%m/%d/%Y'),
        league_id_nullable='00'
    )
    games_df = gamefinder.get_data_frames()[0]
    return [(str(row['GAME_ID']), f"{row['TEAM_NAME']} vs {row['MATCHUP'].split()[-1]}") for _, row in games_df.iterrows()]

def stream_game_plays(game_id, game_info):
    print(f"Starting to stream plays for game: {game_info}")
    pbp = playbyplay.PlayByPlay(game_id)
    last_event_num = 0

    while True:
        plays = pbp.get_dict()['game']['actions']
        new_plays = [play for play in plays if play['actionNumber'] > last_event_num]

        for play in new_plays:
            play_json = json.dumps(play)
            key = f"{game_id}_{play['actionNumber']:05d}"
            producer.produce(topic, key=key, value=play_json, callback=delivery_report)
            producer.flush()


            last_event_num = play['actionNumber']
            time.sleep(1)  # Add a small delay to simulate real-time ingestion

        if len(new_plays) > 0:
            print(f"Game {game_info}: Sent {len(new_plays)} new plays. Total plays: {last_event_num}")
        else:
            print(f"No new plays for game {game_info}. Ending stream.")
            break

        time.sleep(3)  # Poll for new plays every 5 seconds

def consume_messages():
    consumer.subscribe([topic])

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
        pass
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
    today = datetime.today().strftime('%Y-%m-%d')
    if today:
        games = get_games_for_date(today)
        if games:
            print(f"Found {len(games)} games. Starting to stream all games...")
            stream_all_games(games)
            print("All games have been streamed.")
        else:
            print(f"No games found for {today}")
    else:
        print("Game Ended")