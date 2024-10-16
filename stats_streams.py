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
        away_team_id INT,
        away_team_name VARCHAR(100),
        away_team_score INT,
        home_assists INT,
        home_assistsTurnoverRatio INT,
        home_benchPoints INT,
        home_biggestLead INT,
        home_biggestLeadScore VARCHAR(15),
        home_biggestScoringRun INT,
        home_biggestScoringRunScore VARCHAR(15),
        home_blocks INT,
        home_blocksReceived INT,
        home_fastBreakPointsAttempted INT,
        home_fastBreakPointsMade INT,
        home_fastBreakPointsPercentage FLOAT,
        home_fieldGoalsAttempted INT,
        home_fieldGoalsEffectiveAdjusted INT,
        home_fieldGoalsMade INT,
        home_fieldGoalsPercentage FLOAT,
        home_foulsOffensive INT,
        home_foulsDrawn INT,
        home_foulsPersonal INT,
        home_foulsTeam INT,
        home_foulsTechnical INT,
        home_foulsTeamTechnical INT,
        home_freeThrowsAttempted INT,
        home_freeThrowsMade INT,
        home_freeThrowsPercentage FLOAT,
        home_leadChanges INT,
        home_minutes TEXT,
        home_minutesCalculated TEXT,
        home_points INT,
        home_pointsAgainst INT,
        home_pointsFastBreak INT,
        home_pointsFromTurnovers INT,
        home_pointsInThePaint INT,
        home_pointsInThePaintAttempted INT,
        home_pointsInThePaintMade INT,
        home_pointsInThePaintPercentage FLOAT,
        home_pointsSecondChance INT,
        home_reboundsDefensive INT,
        home_reboundsOffensive INT,
        home_reboundsPersonal INT,
        home_reboundsTeam INT,
        home_reboundsTeamDefensive INT
        home_reboundsTeamOffensive INT,
        home_reboundsTotal INT,
    )''')

