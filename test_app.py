import streamlit as st
import pandas as pd
from confluent_kafka import Consumer, KafkaError
import json
from collections import deque

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'streamlit-app',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# Streamlit app
def main():
    st.title('NBA Play-by-Play Stream')

    # Create a placeholder for the streaming data
    placeholder = st.empty()

    # Subscribe to the Kafka topic
    consumer.subscribe(['nba_playbyplay'])

    # Use a deque to store the most recent plays (adjust maxlen as needed)
    plays = deque(maxlen=100)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    st.error(f"Error: {msg.error()}")
                    break

            # Parse the message value
            try:
                value = json.loads(msg.value().decode('utf-8'))
                plays.appendleft(value)  # Add new play to the beginning of the deque

                # Convert plays to DataFrame
                df = pd.DataFrame(list(plays))

                # Update the placeholder with the new data
                with placeholder.container():
                    st.header(f"Score: {df.iloc[0]['scoreHome']} - {df.iloc[0]['scoreAway']}")
                    st.dataframe(df)
            except json.JSONDecodeError:
                st.warning(f"Error decoding JSON: {msg.value()}")

    finally:
        consumer.close()

if __name__ == "__main__":
    main()