from confluent_kafka import Producer
from datetime import date
from nba_api.stats.endpoints import scoreboardv2
from nba_api.live.nba.endpoints import boxscore, playbyplay
import json
import time
import os
import requests
import threading

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'nba-game-play-producer'}

producer = Producer(conf)

topic = 'nba-game-plays'

def get_todays_games():
    """
    Return a list of all games occuring today
    """
    today = date.today().strftime('%Y-%m-%d')
    scoreboard = scoreboardv2.ScoreboardV2(game_date=today)
    raw_games = json.loads(scoreboard.get_json())
    games = raw_games['resultSets'][0]['rowSet']
    return [(game[2], f"{game[6]} vs {game[7]}") for game in games]

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

def stream_game_plays(game_id, game_info):
    """
    Ingest game plays from Producer
    """
    print(f"Starting to stream plays for game: {game_info}")
    last_event_num = 0
    consecutive_empty_responses = 0
    max_empty_responses = 30
    
    while True:
        try:
            if is_game_over(game_id):
                print(f"Game {game_info} has ended. Stopping stream.")
                break
            raw_plays = playbyplay.PlayByPlay(game_id)
            plays = raw_plays.get_dict().get('game', {}).get('actions', [])
            
            if not plays:
                consecutive_empty_responses += 1
                if consecutive_empty_responses >= max_empty_responses:
                    print(f"No new plays for game {game_info} after {max_empty_responses} attempts. Stopping stream.")
                    break
                print(f"No plays available for game {game_info}. Waiting... (Attempt {consecutive_empty_responses})")
                time.sleep(30)
                continue
            consecutive_empty_responses = 0
            new_plays = [play for play in plays if play['actionNumber'] > last_event_num]
            
            for play in new_plays:
                play['gameId'] = game_id
                play_json = json.dumps(play)
                message_key = f"{game_id}_{play['actionNumber']:05d}"
                producer.produce(topic, key=message_key, value=play_json, callback=delivery_report)
                producer.flush()
                add_game_play_to_json_file(game_id=game_id, play=play)
                
                last_event_num = max(last_event_num, play['actionNumber'])
            if len(new_plays) > 0:
                print(f"Game {game_info}: Sent {len(new_plays)} new plays. Last event number: {last_event_num}")
            else:
                print(f"No new plays for game {game_info}. Last event number: {last_event_num}")
        except json.JSONDecodeError as e:
            print(f"JSON Decode error for game {game_info}. {str(e)}")
            time.sleep(10)
        except requests.exceptions.RequestException as e:
            print(f"Network error for game {game_info}: {str(e)}")
        except Exception as e:
            print(f"Error processing game play: {str(e)}")
            time.sleep(10)
        
        time.sleep(10)
                
def delivery_report(err, msg):
    if err is None:
        print(f"Game play ingested from {msg.key().decode('utf-8')[:10]}")
    else:
        print(f"Error ingesting message: {err}")

def add_game_play_to_json_file(game_id, play):
    """
    Add each ingested game play to an assigned json file
    """
    today = date.today().strftime('%Y-%m-%d')
    filename = f"{game_id}_{today}.jsonl"
    
    os.makedirs('Game Plays', exist_ok=True)
    file_path = os.path.join('Game Plays', filename)
    
    
    with open(file_path, 'a') as f:
        json.dump(play, f)
        f.write('\n')

def stream_all_games(games):
    threads = []
    for game_id, game_info in games:
        thread = threading.Thread(target=stream_game_plays, args=(game_id, game_info))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
if __name__ == "__main__":
    games = get_todays_games()
    if games:
        print(f"Found {len(games)} games. Strating to stream all games...")
        stream_all_games(games)
        print("All games have been streamed.")
    else:
        print("No games found for today.")
    