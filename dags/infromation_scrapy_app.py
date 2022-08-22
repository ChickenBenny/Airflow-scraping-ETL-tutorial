import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Benny Hsiao',
    'start_date': datetime(2022, 8, 15),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes = 5)
}


def fn_scrapy():
    print('trigger scrapy task.')
    print('Check whether stock market has opened today.')
    stock_market = True
    if stock_market == True:
        print('爬取資料')
        print('資料清洗')
        print('儲存資料')
        print('寄送爬取成功訊息')
    else:
        print('今日為開盤')


with DAG(
    dag_id = 'information_scrapy_app',
    default_args = default_args,
    description = 'daily scrapy app'
) as dag:
    scrapy_task = PythonOperator(
        task_id = 'scrapy_task',
        python_callable = fn_scrapy
    )