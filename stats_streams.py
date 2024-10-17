from nba_api.live.nba.endpoints import boxscore
from nba_api.stats.endpoints import scoreboardv2
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
        home_secondChancePointsAttempted INT,
        home_secondChancePointsMade INT,
        home_secondChancePointsPercentage FLOAT,
        home_steals INT,
        home_threePointersAttempted INT,
        home_threePointersMade INT,
        home_threePointersPercentage FLOAT,
        home_timeLeading INT,
        home_timesTied INT,
        home_trueShootingAttempts FLOAT,
        home_trueShootingPercentage FLOAT,
        home_turnovers INT,
        home_turnoversTeam INT,
        home_turnoversTotal INT,
        home_twoPointersAttempted INT,
        home_twoPointersMade INT,
        home_twoPointersPercentage FLOAT,
        away_assists INT,
        away_assistsTurnoverRatio INT,
        away_benchPoints INT,
        away_biggestLead INT,
        away_biggestLeadScore VARCHAR(15),
        away_biggestScoringRun INT,
        away_biggestScoringRunScore VARCHAR(15),
        away_blocks INT,
        away_blocksReceived INT,
        away_fastBreakPointsAttempted INT,
        away_fastBreakPointsMade INT,
        away_fastBreakPointsPercentage FLOAT,
        away_fieldGoalsAttempted INT,
        away_fieldGoalsEffectiveAdjusted INT,
        away_fieldGoalsMade INT,
        away_fieldGoalsPercentage FLOAT,
        away_foulsOffensive INT,
        away_foulsDrawn INT,
        away_foulsPersonal INT,
        away_foulsTeam INT,
        away_foulsTechnical INT,
        away_foulsTeamTechnical INT,
        away_freeThrowsAttempted INT,
        away_freeThrowsMade INT,
        away_freeThrowsPercentage FLOAT,
        away_leadChanges INT,
        away_minutes TEXT,
        away_minutesCalculated TEXT,
        away_points INT,
        away_pointsAgainst INT,
        away_pointsFastBreak INT,
        away_pointsFromTurnovers INT,
        away_pointsInThePaint INT,
        away_pointsInThePaintAttempted INT,
        away_pointsInThePaintMade INT,
        away_pointsInThePaintPercentage FLOAT,
        away_pointsSecondChance INT,
        away_reboundsDefensive INT,
        away_reboundsOffensive INT,
        away_reboundsPersonal INT,
        away_reboundsTeam INT,
        away_reboundsTeamDefensive INT
        away_reboundsTeamOffensive INT,
        away_reboundsTotal INT,
        away_secondChancePointsAttempted INT,
        away_secondChancePointsMade INT,
        away_secondChancePointsPercentage FLOAT,
        away_steals INT,
        away_threePointersAttempted INT,
        away_threePointersMade INT,
        away_threePointersPercentage FLOAT,
        away_timeLeading INT,
        away_timesTied INT,
        away_trueShootingAttempts FLOAT,
        away_trueShootingPercentage FLOAT,
        away_turnovers INT,
        away_turnoversTeam INT,
        away_turnoversTotal INT,
        away_twoPointersAttempted INT,
        away_twoPointersMade INT,
        away_twoPointersPercentage FLOAT
    )''')
    conn.commit()

create_table_if_not_exists(cursor)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] with key {msg.key().decode("utf-8")}')

def get_todays_games():
    today = date.today().strftime('%Y-%m-%d')
    scoreboard = scoreboardv2.ScoreboardV2(game_date=today)
    games_data = json.loads(scoreboard.get_json())
    games = games_data['resultSets'][0]['rowSet']
    return [(game[2], f"{game[6]} vs {game[7]}") for game in games]

def is_game_over(game_id):
    try:
        box = boxscore.BoxScore(game_id)
        game_data = box.get_dict()
        game_status = game_data['game']['gameStatus']
        return game_status == 3  # 3 indicates the game has ended
    except Exception as e:
        print(f"Error checking game status for {game_id}: {str(e)}")
        return False

def insert_team_stats(cursor, game_id, team_stats):
    cursor.execute('''
    INSERT INTO team_stats (
        game_id, home_team_id, home_team_name, home_team_score, away_team_id, away_team_name,
        away_team_score, home_assists, home_assistsTurnoverRatio, home_benchPoints, home_biggestLead, home_biggestLeadScore,
        home_biggestScoringRun, home_biggestScoringRunScore, home_blocks, home_blocksReceived, home_fastBreakPointsAttempted,
        home_fastBreakPointsMade, home_fastBreakPointsPercentage, home_fieldGoalsAttempted, home_fieldGoalsEffectiveAdjusted, home_fieldGoalsMade,
        home_fieldGoalsPercentage, home_foulsOffensive, home_foulsDrawn, home_foulsPersonal, home_foulsTeam, home_foulsTechnical,
        home_foulsTeamTechnical, home_freeThrowsAttempted, home_freeThrowsMade, home_freeThrowsPercentage, home_leadChanges, home_minutes,
        home_minutesCalculated, home_points, home_pointsAgainst, home_pointsFastBreak, home_pointsFromTurnovers, home_pointsInThePaint,
        home_pointsInThePaintAttempted, home_pointsInThePaintMade, home_pointsInThePaintPercentage, home_pointsSecondChance, home_reboundsDefensive,
        home_reboundsOffensive, home_reboundsPersonal, home_reboundsTeam, home_reboundsTeamDefensive, home_reboundsTeamOffensive,
        home_reboundsTotal, home_secondChancePointsAttempted, home_secondChancePointsMade, home_secondChancePointsPercentage,
        home_steals, home_threePointersAttempted, home_threePointersMade, home_threePointersPercentage, home_timeLeading, home_timesTied,
        home_trueShootingAttempts, home_trueShootingPercentage, home_turnovers, home_turnoversTeam, home_turnoversTotal, home_twoPointersAttempted,
        home_twoPointersMade, home_twoPointersPercentage, away_assists, away_assistsTurnoverRatio, away_benchPoints, away_biggestLead,
        away_biggestLeadScore, away_biggestScoringRun, away_biggestScoringRunScore, away_blocks, away_blocksReceived, away_fastBreakPointsAttempted, 
        away_fastBreakPointsMade, away_fastBreakPointsPercentage, away_fieldGoalsAttempted, away_fieldGoalsEffectiveAdjusted, 
        away_fieldGoalsMade, away_fieldGoalsPercentage, away_foulsOffensive, away_foulsDrawn, away_foulsPersonal, away_foulsTeam, away_foulsTechnical,
        away_foulsTeamTechnical, away_freeThrowsAttempted, away_freeThrowsMade, away_freeThrowsPercentage, away_leadChanges, away_minutes,
        away_minutesCalculated, away_points, away_pointsAgainst, away_pointsFastBreak, away_pointsFromTurnovers, away_pointsInThePaint,
        away_pointsInThePaintAttempted, away_pointsInThePaintMade, away_pointsInThePaintPercentage, away_pointsSecondChance, away_reboundsDefensive,
        away_reboundsOffensive, away_reboundsPersonal, away_reboundsTeam, away_reboundsTeamDefensive, away_reboundsTeamOffensive, away_reboundsTotal,
        away_secondChancePointsAttempted, away_secondChancePointsMade, away_secondChancePointsPercentage, away_steals, away_threePointersAttempted, 
        away_threePointersMade, away_threePointersPercentage, away_timeLeading, away_timesTied, away_trueShootingAttempts, away_trueShootingPercentage,
        away_turnovers, away_turnoversTeam, away_turnoversTotal, away_twoPointersAttempted, away_twoPointersMade, away_twoPointersPercentage) 
        
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''')

def stream_team_stats(game_id, game_info):
    print(f"Starting to stream team-level stats for game: {game_info}")
    last_event_num = 0
    consecutive_empty_responses = 0
    max_empty_responses = 10  # Adjust this value as needed

    while True:
        try:
            if is_game_over(game_id):
                print(f"Game {game_info} has ended. Stopping stream.")
                break

            json_obj = json.loads(boxscore.BoxScore(game_id=game_id).get_json())
            
            game_id = json_obj['game']['gameId']
            home_team_stats = json_obj['game']['homeTeam']
            away_team_stats = json_obj['game']['awayTeam']

            game_data = {
                'game_id': game_id,
                'home_team_id': home_team_stats['teamId'],
                'home_team_name': f"{home_team_stats['teamCity']} {home_team_stats['teamName']}",
                'home_team_score': home_team_stats['score'],
                'away_team_id': away_team_stats['teamId'],
                'away_team_name': f"{away_team_stats['teamCity']} {away_team_stats['teamName']}",
                'away_team_score': away_team_stats['score']
            }

            for stat, value in home_team_stats['statistics'].items():
                game_data[f'home_{stat}'] = value
            for stat, value in away_team_stats['statistics'].items():
                game_data[f'away_{stat}'] = value
