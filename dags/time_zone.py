import time
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Benny Hsiao',
    'start_date': datetime(2022, 8, 15),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes = 5)
}


def test(date):
    print(f'UTC time :{date}')

def test_tw_time_zone(date):
    print(f'TW time : {date.astimezone(timezone(timedelta(hours=8)))}')



with DAG(
    dag_id = 'time_zone_testing',
    default_args = default_args,
    description = 'time_zone_testing'
) as dag:
    scrapy_task = PythonOperator(
        task_id = 'time_zone_testing',
        python_callable = test,
        op_kwargs = {"date": datetime.now()}
    )

    time_zone = PythonOperator(
        task_id = 'TW_time_zone_testing',
        python_callable = test_tw_time_zone,
        op_kwargs = {"date": datetime.now()}
    ) 