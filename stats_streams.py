from confluent_kafka import Producer


# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'nba-game-stats'
}

producer = Producer(conf)