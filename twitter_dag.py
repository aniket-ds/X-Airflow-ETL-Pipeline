from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from x_etl import run_x_etl

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 17, 10),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'x_dag',
    default_args=default_args,
    description='X DAG with ETL process!',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complete_x_etl',
    python_callable=run_x_etl,
    dag=dag, 
)

run_etl
