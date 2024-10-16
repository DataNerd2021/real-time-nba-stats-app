from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

my_file_name = 'my_file.csv'
my_key = 'my_file.csv'
bucket_name = 'raw-game-plays'



with DAG(
    dag_id='upload_file_to_s3_taskflow_dag',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
   
    @task
    def upload_file_to_s3(filename: str, key: str, bucket_name: str):
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_file(
            filename=filename,
            key=key,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"Uploaded {filename} to s3://{bucket_name}/{key}")

    
    upload_file_to_s3(
        filename='/path/to/your/localfile.csv',
        key='path/in/s3/yourfile.csv',
        bucket_name='your-s3-bucket-name'
    )

