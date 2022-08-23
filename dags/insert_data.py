import time
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Benny Hsiao',
    'start_date': datetime(2022, 8, 15, 10, 0),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes = 5)
}

POSTGRES_CONN_ID = 'database'

def scrapy(ti):
    date = datetime(2022, 8, 19).strftime("%Y%m%d")
    url = f"https://www.twse.com.tw/fund/BFI82U?response=json&dayDate={date}"
    res = requests.get(url)
    res = res.json()
    if res['stat'] == "OK":
        data = []
        for i in (res['data'])[: -1]:
            for j in range(1, len(i)):
                data.append(i[j].replace(',', ''))
        print("資料爬取成功")
        if(len(data) == 12):
            ti.xcom_push(key = 'date', value = res['date'])
            ti.xcom_push(key = 'dealer_buy', value = data[0])
            ti.xcom_push(key = 'dealer_sell', value = data[1])
            ti.xcom_push(key = 'dealer_dif', value = data[2])
            ti.xcom_push(key = 'dealer_buy_hedge', value = data[3])
            ti.xcom_push(key = 'dealer_sell_hedge', value = data[4])
            ti.xcom_push(key = 'dealer_dif_hedge', value = data[5])
            ti.xcom_push(key = 'investment_buy', value = data[6])
            ti.xcom_push(key = 'investment_sell', value = data[7])
            ti.xcom_push(key = 'investment_dif', value = data[8])
            ti.xcom_push(key = 'foreign_buy', value = data[9])
            ti.xcom_push(key = 'foreign_sell', value = data[10])
            ti.xcom_push(key = 'foreign_dif', value = data[11])

            return 'insert_data'
        else:
            print("格式有變動，請重新設計爬蟲")
    else:
        print('無交易資料')



def insert_data(ti):
    date = ti.xcom_pull(task_ids = 'scrapy_task', key = 'date')
    data = ti.xcom_pull(task_ids = 'scrapy_task', key = 'data')
    hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    query = f'''INSERT INTO investment_data(dt, dealer_buy, dealer_sell, dealer_dif, dealer_buy_hedge, dealer_sell_hedge, dealer_dif_hedge, investment_buy, investment_sell, investment_dif, foreign_buy, foreign_sell, foreign_dif) VALUES('{date}', '{data[0]}', '{data[1]}', '{data[2]}', '{data[3]}', '{data[4]}', '{data[5]}', '{data[6]}', '{data[7]}', '{data[8]}', '{data[9]}', '{data[10]}', '{data[11]}');'''
    print(query)
    cursor.execute(query)
    cursor.close()
    conn.close()


def read_db():
    hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    query = '''SELECT * FROM investment_data;'''    
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    print(rows)    


with DAG(
    dag_id = 'insert_db_testing',
    default_args = default_args,
    description = 'daily scrapy app'
) as dag:
    scrapy_task = PythonOperator(
        task_id = 'scrapy_task',
        python_callable = scrapy,
    )

    insert_task = PostgresOperator(
        task_id = 'insert_data',
        postgres_conn_id  = POSTGRES_CONN_ID,
        sql = '''
            INSERT INTO investment_data(
                dt, dealer_buy, dealer_sell, dealer_dif, dealer_buy_hedge, dealer_sell_hedge, dealer_dif_hedge, investment_buy, investment_sell, investment_dif, foreign_buy, foreign_sell, foreign_dif
            ) VALUES (
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='date') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='dealer_buy') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='dealer_sell') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='dealer_dif') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='dealer_buy_hedge') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='dealer_sell_hedge') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='dealer_dif_hedge') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='investment_buy') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='investment_sell') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='investment_dif') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='foreign_buy') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='foreign_sell') }}',
                '{{ ti.xcom_pull(task_ids='scrapy_task', key='foreign_dif') }}'
            );
        '''
    )

    read_database = PythonOperator(
        task_id = 'read_database',
        python_callable = read_db
    )

    scrapy_task >> insert_task
    insert_task >> read_database