from confluent_kafka import Consumer, Producer

conf = {
    'bootstrap.servers': 'host.docker.internal:9092',
    'group.id': 'streamlit-app',
    'auto.offset.reset': 'earliest'
}

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def game_play_ingestion_message(err, msg:str):
    """
    This outputs a message to the console indicating whether the game play event was successfully ingested
    """
    if err is None:
        print(f'Game Play Sent to {msg.topic()}. Preview: {msg.value()[:10]}')
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

producer.produce('test', key='hello', value='world', callback=game_play_ingestion_message)
producer.flush()
