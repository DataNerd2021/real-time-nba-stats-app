from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'Daymon',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='dag_with_taskflow_taskflow_decorator_v01',
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule_interval='@hourly',
    description='My first DAG using a taskflow API',
)
def hello_world_dag():
    @task()
    def get_name():
        return "Daymon"
    @task()
    def get_age():
        return 25
    @task()
    def greet(name, age):
        print(f'Hello, world! My name is {name} and I am {age} years old.')

    name = get_name()
    age = get_age()
    greet(name, age)

dag = hello_world_dag()