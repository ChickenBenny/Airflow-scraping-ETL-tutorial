import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

default_args = {
    'owner': 'Benny Hsiao',
    'start_date': datetime(2022, 8, 15, 10, 0),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes = 5)
}


def push_text(ti):
    ti.xcom_push(key = 'message', value = 'Testing')

with DAG(
    dag_id = 'telegram_bot',
    default_args = default_args,
    description = 'testing_telegram_bot'
) as dag:
    generate_message = PythonOperator(
        task_id = 'generate_message',
        python_callable = push_text,
    )

    sending_message = TelegramOperator(
        task_id = 'telegram_bot',
        telegram_conn_id = "telegram_id",
        chat_id = "your id",
        text = "{{ ti.xcom_pull(task_ids='generate_message', key='message') }}"
    )

    generate_message >> sending_message