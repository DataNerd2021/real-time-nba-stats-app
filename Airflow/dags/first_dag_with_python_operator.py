from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Daymon',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def greet(name, age):
    print(f'Hello, world! My name is {name} and I am {age} years old.')

with DAG(
    default_args=default_args,
    dag_id='first_dag_with_python_operator_v02',
    description='My first DAG using a python operator',
    start_date=datetime(2024, 10, 1),
    schedule_interval='@hourly',
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'name': 'Daymon', 'age': 25},
    )

    task1