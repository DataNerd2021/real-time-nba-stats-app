from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
import json
import time
import sys

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Adjust this if needed
    'group.id': 'nba_exporter',
    'auto.offset.reset': 'earliest'
}

# Initialize Kafka consumer
consumer = Consumer(kafka_config)

# Topic to export
topic = 'nba-plays'

# File to export the messages to
output_file = 'nba_game_plays.jsonl'

print(f"Starting to export messages from '{topic}' topic to {output_file}")

def write_message(file, msg):
    # Decode the message value and include the key
    play_data = json.loads(msg.value().decode('utf-8'))
    play_data['message_key'] = msg.key().decode('utf-8') if msg.key() else None

    # Write only the message value data to the file
    json.dump(play_data, file, ensure_ascii=False)
    file.write('\n')  # Add a newline for JSONL format

try:
    # Get metadata about the topic
    metadata = consumer.list_topics(topic)
    if topic not in metadata.topics:
        raise ValueError(f"Topic '{topic}' does not exist.")

    partitions = metadata.topics[topic].partitions
    print(f"Topic '{topic}' has {len(partitions)} partitions.")

    # Assign the consumer to all partitions of the topic
    topic_partitions = [TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions]
    consumer.assign(topic_partitions)

    # Seek to the beginning of each partition
    for tp in topic_partitions:
        try:
            low, high = consumer.get_watermark_offsets(tp)
            print(f"Partition {tp.partition}: Low offset = {low}, High offset = {high}")
            consumer.seek(tp)
        except Exception as e:
            print(f"Error seeking partition {tp.partition}: {str(e)}")
    while True:
        with open(output_file, 'w') as f:
            # Read all existing messages
            print("Reading existing messages...")
            message_count = 0
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Error: {msg.error()}")
                        break
                write_message(f, msg)
                message_count += 1
                if message_count % 1000 == 0:
                    print(f"Processed {message_count} messages")

            print(f"\nFinished reading {message_count} existing messages.")
            sys.exit()
except KeyboardInterrupt:
    print("\nExport process stopped by user")
except Exception as e:
    print(f"An error occurred: {str(e)}")
finally:
    consumer.close()
    print(f"\nExport complete. {message_count} messages saved to {output_file}")