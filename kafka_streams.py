from confluent_kafka import Consumer, Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Game Play Ingested! Topic: {} Partition: {} Offset: {} Preview: {}'.format(msg.topic(), msg.partition(), msg.offset(), msg.value()[:10]))

producer.produce('test', key='hello', value='world', callback=delivery_report)
producer.flush()
