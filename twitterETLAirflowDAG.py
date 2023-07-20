from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from twitterETL import fetchTweets_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'twitterairflowdag',
    default_args=default_args,
    description='DAG to perform twitter ETL task!',
    schedule_interval=timedelta(days=1),
)

fetchETL = PythonOperator(
    task_id='twitterETL',
    python_callable=run_twitter_etl,
    dag=dag, 
)

run_etl
