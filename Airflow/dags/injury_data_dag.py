import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from airflow.decorators import dag, task
import psycopg2
import os
import dotenv

load_dotenv()

def get_postgres_con():
    conn_params = {
    "host": os.getenv('server_ip'),
    "database": 'NbaData',
    "user": os.getenv('postgres_user'),
    "password": os.getenv('postgres_password'),
    "port": 15432 
    }
    conn_str = f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
    engine = create_engine(conn_str)
    return engine

engine = get_postgres_con()

default_args = {
    'owner': 'Daymon',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='update_nba_injury_data_v01',
    default_args=default_args,
    start_date=datetime(2024, 10, 1, 12, 0),
    catchup=False,
    schedule_interval='@hourly',
    description='updates the NBA injury data in the database',
)
def hello_world_dag():
    
    @task()
    def get_new_injuries():
        player_injuries = pd.concat(pd.read_html('https://www.espn.com/nba/injuries')).fillna('N/A')
        
        player_injuries.rename(columns={
            'NAME': 'name',
            'POS': 'pos',
            'EST. RETURN DATE': 'estreturndate',
            'STATUS': 'status',
            'COMMENT': 'comment',
            'date_updated': 'lastupdated'
            }, inplace=True)
        return player_injuries

    @task()
    def get_data_from_DB():
        try: 
            df = pd.read_sql_table('NbaData', con=engine)
            if df.empty:
                return df
            else:
                df.drop(columns=['lastupdated'], inplace=True)
                return df
        except:
            return pd.DataFrame()
    @task()
    def compare_data(new_injuries: pd.DataFrame, old_injuries: pd.DataFrame):
        if old_injuries.empty:
            new_injuries['new_or_updated'] = 1
            return new_injuries
        else:
            # Use merge with indicator to find rows only in new_df and identical rows
            merged = new_injuries.merge(old_injuries, how='outer', indicator=True)
            
            # Get rows that are only in new_df
            new_rows = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
            
            # Get rows that are identical in both DataFrames
            identical_rows = new_injuries.merge(old_injuries, how='inner')
            new_rows['new_or_updated'] = 1
            identical_rows['new_or_updated'] = 0

            # Combine the new_rows and identical_rows DataFrames
            combined_df = pd.concat([new_rows, identical_rows], ignore_index=True)

            return combined_df

    @task()
    def update_db_table(player_injuries: pd.DataFrame):
        player_injuries['lastupdated'] = datetime.today()
        player_injuries.to_sql('NbaData', con=engine, if_exists='replace', index=False)
        return f"Injury data updated successfully. {len(player_injuries)} records updated."

    # Set task dependencies
    new_injuries = get_new_injuries()
    old_injuries = get_data_from_DB()
    compared_data = compare_data(new_injuries, old_injuries)
    update_db_table(compared_data)

dag = hello_world_dag()