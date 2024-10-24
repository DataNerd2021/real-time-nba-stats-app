import streamlit as st
import time
import pandas as pd
from confluent_kafka import Consumer
import duckdb
import json

def create_kafka_consumer(bootstrap_servers, group_id, topic):
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    
    return consumer

def consume_messages(consumer, timeout=1.0):
    records = []
    while True:
        msg = consumer.poll(timeout)
        if msg is None:
            break
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        
        # Parse the message as JSON
        value = json.loads(msg.value().decode('utf-8'))
        records.append(value)
    
    # Convert records to a DataFrame for display
    if records:
        df = pd.DataFrame(records)
        return df
    else:
        return pd.DataFrame()

# Streamlit App
def run_streamlit_app():
    # Kafka setup
    bootstrap_servers = 'localhost:9092'  # Adjust for your setup
    group_id = 'streamlit_app'
    topic = 'nba-game-plays'

    # Create Kafka Consumer
    consumer = create_kafka_consumer(bootstrap_servers, group_id, topic)

    st.title("Real-time Kafka Message Viewer")
    st.subheader("Displaying messages as they're ingested into the DataFrame")

    # Create an empty placeholder for the DataFrame
    placeholder = st.empty()
    
    # Real-time loop to fetch and display messages
    df = pd.DataFrame()
    
    while True:
        # Consume messages
        new_df = consume_messages(consumer)
        
        # Append new messages to the existing DataFrame
        df = pd.concat([df, new_df], ignore_index=True)
        
        if not df.empty:
            # Use DuckDB to explode nested fields
            query = """
            SELECT * EXCLUDE (qualifiers, players),
                   qualifiers.value AS qualifiers,
                   players.playerName,
                   players.personId,
                   players.teamId,
                   players.teamTricode,
                   players.playerPosition
            FROM df,
                 UNNEST(qualifiers) AS qualifiers,
                 UNNEST(players) AS players
            WHERE gameId = '0022400061'
            """
            exploded_df = duckdb.sql(query).to_df()
            
            # Display the updated DataFrame in the Streamlit app
            placeholder.dataframe(exploded_df)
        
        # Sleep before fetching new messages
        time.sleep(2)

if __name__ == "__main__":
    run_streamlit_app()
