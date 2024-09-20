from kafka import KafkaProducer
from nba_api.live.nba.endpoints import playbyplay
import polars as pl

producer = KafkaProducer(bootstrap_servers='localhost:1234')
for _ in range(100):
    producer.send('foobar', b'some_message_bytes')