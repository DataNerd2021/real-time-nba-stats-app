from confluent_kafka import Consumer, Producer
from nba_api.live.nba.endpoints import playbyplay, boxscore

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg:str):
    """
    Reports the success or failure of message delivery.
    """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Game Play Ingested! Topic: {} Partition: {} Offset: {} Preview: {}'.format(msg.topic(), msg.partition(), msg.offset(), msg.value()[:10]))



producer.produce('test', key='hello', value='world', callback=delivery_report)
producer.flush()
