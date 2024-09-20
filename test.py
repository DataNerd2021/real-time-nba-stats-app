from kafka import KafkaProducer
from kafka import KafkaConsumer
from nba_api.live.nba.endpoints import playbyplay
import json
import time

# set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v:json.dumps(v).encode('utf-8')
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

def fetch_game_plays(game_id):
    try:
        plays = playbyplay.PlayByPlay(game_id).get_dict()
        game_plays = plays['game']['actions']
        if not game_plays:
            print(f"No plays found for game ID: {game_id}")
            return []

        play_messages = []
        for play in game_plays:
            fields = ['actionNumber', 'period', 'description', 'clock', 'qualifiers', 'scoreAway', 'scoreHome']
            play_message = {field: play.get(field) for field in fields}
            play_messages.append(play_message)
            print(play_message)

        return play_messages
    except Exception as e:
        print(f"Error fetching game plays: {e}")
        return []

def produce_nba_playbyplay_messages(game_id, topic):
    while True:
        plays = fetch_game_plays(game_id)

        if not plays:
            print("No plays to send. Waiting before retrying...")
            time.sleep(60)  # Wait for 60 seconds before trying again
            continue

        for play in plays:
            send_message(topic, play)
            print(f"Sent message: {play}")
            time.sleep(3)

        time.sleep(60)  # Wait for 60 seconds before fetching new plays
produce_nba_playbyplay_messages(game_id=input("Enter game ID: "), topic='nba_playbyplay')