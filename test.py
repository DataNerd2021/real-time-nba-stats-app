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
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
def send_message(topic, data):
    producer.send(topic, value=data)
    producer.flush()

def fetch_game_plays(game_id):
    pbp = playbyplay.PlayByPlay(game_id)
    return pbp.get_dict()['game']['actions']



def produce_nba_playbyplay_messages(game_id, topic):
    while True:
        plays = fetch_game_plays(game_id)

        for play in plays:
            send_message(topic, play)
            print(f"Sent message: {play}")

            time.sleep(0.5)

produce_nba_playbyplay_messages(game_id=input("Enter game ID: "), topic='nba_playbyplay')