from nba_api.live.nba.endpoints import boxscore
from confluent_kafka import Producer
import json
import time
from datetime import datetime, date
import threading
import pytz
import requests
import sqlite3

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'nba-play-producer'
}

producer = Producer(conf)

topic = 'team-level-stats'

# Database setup
db_name = 'team_level_stats.db'
conn = sqlite3.connect(db_name, check_same_thread=False)
cursor = conn.cursor()

def create_table_if_not_exists(cursor):
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS team_stats(
        game_id INT,
        home_team_id INT,
        home_team_name VARCHAR(100),
        home_team_score INT,
        away_team_id INT
    )
    )''')

