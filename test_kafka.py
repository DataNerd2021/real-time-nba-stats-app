from kafka import KafkaProducer
import json

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers='45.79.71.164:9092',  # Replace with your Kafka broker address (no http://)
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize messages to JSON
)

# Define the topic name
topic_name = 'new-test-topic'  # Change to your desired topic name

# Create a message
message = {
    'key': 'value',  # Replace with your message key-value pairs
    'message': 'Third message now'
}

# Send the message to the specified topic
producer.send(topic_name, value=message)

# Flush the producer to ensure the message is sent
flush = producer.flush()

producer.close()

print(f'Message sent to topic "{topic_name}": {message}')
