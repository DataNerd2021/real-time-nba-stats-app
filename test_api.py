from flask import Flask, jsonify
from nba_api.live.nba.endpoints import playbyplay
import pandas as pd

app = Flask(__name__)

@app.route('/playbyplay/<game_id>')
def simulation(game_id):
    print(f"Starting mock live updates for game: {game_id}")
    play_by_play = playbyplay.PlayByPlay(game_id)
    plays = play_by_play.get_dict()['game']['actions']
    df = pd.DataFrame(columns=['game_id','period','period_time_remaining','description','home_score','away_score'])
    plays_df = []
    for _ in range(len(plays)):
        if plays:
            latest_play = plays[_]
            try:
                period = latest_play['period']
                period_time_remaining = latest_play['clock'].replace('PT', '').replace('M', ':')[:5]
                description = latest_play['description']
                home_score = latest_play['scoreHome']
                away_score = latest_play['scoreAway']
                df2 = pd.DataFrame({'game_id':game_id, 'period':period, 'period_time_remaining':period_time_remaining, 'description':description, 'home_score':home_score, 'away_score':away_score}, index=[0])
                plays_df.append(df2)
                # print(f"Q{period} {game_clock}: {description} | Score: {home_score} - {away_score}")
            except KeyError:
                pass
        else:
            print("no plays yet...")
    output = pd.concat(plays_df, ignore_index=True)
    return jsonify(output.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)