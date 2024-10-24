import pandas as pd
import streamlit as st
import time
from nba_api.stats.endpoints import PlayByPlay

# Define a generator to fetch play-by-play data
def fetch_play_by_play_data(game_id):
    """
    Fetches play-by-play data for a given game ID from the NBA API.

    Args:
    - game_id: The ID of the game to fetch play-by-play data for.

    Yields:
    - dict: A dictionary representing a play's details.
    """
    pbp = PlayByPlay(game_id=game_id)
    plays = pbp.get_data_frames()[0]  # Fetch the first DataFrame

    # Iterate over each play and yield relevant information
    for _, row in plays.iterrows():
        yield {
            'game_id': row['GAME_ID'],
            'action_number': row['ACTION_NUMBER'],
            'clock': row['CLOCK'],
            'timeActual': row['TIME_REMAINING'],
            'period': row['PERIOD'],
            'periodType': row['PERIOD_TYPE'],
            'team_id': row['TEAM_ID'],
            'teamTricode': row['TEAM_TRICODE'],
            'actionType': row['ACTION_TYPE'],
            'subtype': row['SUB_TYPE'],
            'descriptor': row['DESCRIPTION'],
            'qualifiers': row['QUALIFIERS'],
            'person_id': row['PERSON_ID'],
            'x': row['X'],
            'y': row['Y'],
            'possession': row['POSSESSION'],
            'scoreHome': row['SCORE_HOME'],
            'scoreAway': row['SCORE_AWAY'],
            'description': row['DESCRIPTION']
        }

# Streamlit App to display the play-by-play data
def run_streamlit_simulation():
    st.title("NBA Play-by-Play Data Stream")
    st.subheader("Fetching play-by-play data from NBA API in real-time")

    game_id = st.text_input("Enter Game ID:", value="0022300012")  # Example game ID
    if not game_id:
        st.warning("Please enter a valid Game ID.")
        return

    # Streamlit placeholder for displaying the DataFrame
    placeholder = st.empty()

    # Create an empty DataFrame to store play-by-play data
    df = pd.DataFrame(columns=[
        'game_id', 'action_number', 'clock', 'timeActual', 'period',
        'periodType', 'team_id', 'teamTricode', 'actionType', 'subtype',
        'descriptor', 'qualifiers', 'person_id', 'x', 'y', 'possession',
        'scoreHome', 'scoreAway', 'description'
    ])

    # Fetch play-by-play data
    try:
        for play in fetch_play_by_play_data(game_id):
            # Append the new play data to the DataFrame
            df = pd.concat([df, pd.DataFrame([play])], ignore_index=True)
            # Update the DataFrame display in the Streamlit app
            placeholder.dataframe(df)
            time.sleep(0.5)  # Add a delay to simulate real-time updates
    except Exception as e:
        st.error(f"Error fetching data: {e}")

# Run the Streamlit app
if __name__ == "__main__":
    run_streamlit_simulation()
