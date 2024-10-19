from confluent_kafka import Producer
from datetime import date, datetime, timedelta
from nba_api.stats.endpoints import scoreboardv2
import json
from nba_api.live.nba.endpoints import boxscore
import time
import os
import threading


# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'nba-game-stats'
}

producer = Producer(conf)

topic = 'nba-game-stats'

def get_todays_games(game_date=date.today().strftime('%Y-%m-%d')):
    """
    Return a list of all games occurring on the given date
    """
    scoreboard = scoreboardv2.ScoreboardV2(game_date=game_date)
    raw_games = json.loads(scoreboard.get_json())
    games = raw_games['resultSets'][0]['rowSet']
    return [(game[2], f"{game[6]} vs {game[7]}") for game in games]

def find_games():
    """
    Recursively search for games, going back in time by one day until games are found
    """
    current_date = date.today()
    while True:
        games = get_todays_games(game_date=current_date.strftime('%Y-%m-%d'))
        if games:
            return games, current_date
        current_date -= timedelta(days=1)
        print(current_date)

def is_game_over(game_id: str):
    """ 
    Determines whether or not a game is over
    """
    try:
        box = boxscore.BoxScore(game_id)
        game = box.get_dict()
        game_status = game['game']['gameStatus']
        if game_status == 3:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error checking game status for {game_id}: {str(e)}")
        return False

def stream_game_stats(game_id, game_info):
    """ 
    Ingest updated team and player level stats from Producer
    """
    print(f"Starting to gather statistics for game: {game_id}")
    
    while True:
        try:
            if is_game_over(game_id):
                user_continue = input(f"Game {game_id} has ended. Would you like to re-stream it? (y/n): ")
                if user_continue.lower() == "y":
                    print(f"Re-streaming game {game_id}...")
                elif user_continue.lower() == "n":
                    print(f'Game {game_info} has ended. Stopping stream.')
                    return
                else:
                    print("Invalid input. Please enter 'y' or 'n'.")
                    continue
            
            box = boxscore.BoxScore(game_id).get_dict()
            game_data = box.get('game', {})
            
            # Get home team stats
            home_team_stats = game_data.get('homeTeam', {}).get('statistics', {})
            home_team_stats['team_id'] = game_data.get('homeTeam', {}).get('teamId')
            home_team_stats['team_name'] = game_data.get('homeTeam', {}).get('teamName')
            
            # Get away team stats
            away_team_stats = game_data.get('awayTeam', {}).get('statistics', {})
            away_team_stats['team_id'] = game_data.get('awayTeam', {}).get('teamId')
            away_team_stats['team_name'] = game_data.get('awayTeam', {}).get('teamName')
            
            # Get player stats for home team
            home_player_stats = game_data.get('homeTeam', {}).get('players', [])
            for player in home_player_stats:
                player['team_id'] = home_team_stats['team_id']
                player['team_name'] = home_team_stats['team_name']
            
            # Get player stats for away team
            away_player_stats = game_data.get('awayTeam', {}).get('players', [])
            for player in away_player_stats:
                player['team_id'] = away_team_stats['team_id']
                player['team_name'] = away_team_stats['team_name']
            
            # Combine all stats
            all_stats = {
                'game_id': game_id,
                'home_team': home_team_stats,
                'away_team': away_team_stats,
                'home_players': home_player_stats,
                'away_players': away_player_stats,
                'timestamp': datetime.now().isoformat()
            }
            
            # Convert to JSON and send to Kafka
            stats_json = json.dumps(all_stats)
            message_key = f"{game_id}_{int(time.time())}"
            producer.produce(topic, key=message_key, value=stats_json, callback=delivery_report)
            producer.flush()
            
            # Overwrite the stats in the JSON file
            overwrite_stats_to_json_file(game_id, all_stats)
            
            # Wait for a short period before the next update
            time.sleep(10)  # Adjust this value as needed
            
        except Exception as e:
            print(f"Error streaming stats for game {game_info}: {str(e)}")
            time.sleep(30)

def delivery_report(err, msg):
    if err is None:
        print(f"Game Statistics ingested from {msg.key().decode('utf-8')[:10]}")
    else:
        print(f"Error ingesting message: {err}")

def overwrite_stats_to_json_file(game_id, stats):
    """ 
    Overwrite the stats in the assigned JSON file
    """
    today = date.today().strftime('%Y-%m-%d')
    filename = f"{game_id}_{today}.jsonl"
    
    os.makedirs('Game Statistics', exist_ok=True)
    file_path = os.path.join('Game Statistics', filename)
    
    with open(file_path, 'w') as f:
        json.dump(stats, f)
        f.write('\n')

def stream_all_games(games):
    threads = []
    for game_id, game_info in games:
        thread = threading.Thread(target=stream_game_stats, args=(game_id, game_info))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    games, game_date = find_games()
    if games:
        print(f"Found {len(games)} games for {game_date.strftime('%Y-%m-%d')}. Starting to stream all games...")
        stream_all_games(games)
        print("All games have been streamed")
    else:
        print("No games found after searching")