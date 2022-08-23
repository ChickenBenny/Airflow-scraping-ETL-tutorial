import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator


POSTGRES_CONN_ID = 'database'
TELEGRAM_CONN_ID = 'telegram_id'
CHAT_ID = '809480369'

default_args = {
    'owner': 'Benny Hsiao',
    'start_date': datetime(2022, 8, 23, 10, 0),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes = 5)
}

def market_information():
    date = datetime.now().strftime("%Y%m%d")
    # date = '20220820'
    # date = '20100106'
    url = f"https://www.twse.com.tw/fund/BFI82U?response=json&dayDate={date}"
    res = requests.get(url)
    res = res.json()    
    if res['stat'] == 'OK':
        return 'scrapy'
    else:
        return 'do_nothing'

def scrapy_condition(ti):
    flag = ti.xcom_pull(task_ids='market_information', key='return_value')
    if flag == 'scrapy':
        return 'web_scrapy'
    else:
        return 'do_nothing'

def web_scrapy(ti):
    date = datetime.now().strftime("%Y%m%d")
    # date = '20220820'
    # date = '20100106'
    url = f"https://www.twse.com.tw/fund/BFI82U?response=json&dayDate={date}"
    res = requests.get(url)
    res = res.json()
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
        return 'ok'
    else:
        return 'error'

def length_condition(ti):
    flag = ti.xcom_pull(task_ids='web_scrapy', key='return_value')    
    if flag == 'ok':
        return 'insert_data'
    else:
        return 'alert'


with DAG(
    dag_id = 'Final_app',
    default_args = default_args,
    description = 'daily scrapy app'
) as dag:
    get_market_information = PythonOperator(
        task_id = 'market_information',
        python_callable = market_information,
    )

    whether_scrapy_or_not = BranchPythonOperator(
        task_id = 'scrapy_condition',
        python_callable = scrapy_condition
    )

    do_nothing_task = DummyOperator(task_id = 'do_nothing')

    scrapy_task = PythonOperator(
        task_id = 'web_scrapy',
        python_callable = web_scrapy
    )

    whether_length_is_right = BranchPythonOperator(
        task_id = 'length_condition',
        python_callable = length_condition
    )

    insert_data_task = PostgresOperator(
        task_id = 'insert_data',
        postgres_conn_id  = POSTGRES_CONN_ID,
        sql = '''
            INSERT INTO investment_data(
                dt, dealer_buy, dealer_sell, dealer_dif, dealer_buy_hedge, dealer_sell_hedge, dealer_dif_hedge, investment_buy, investment_sell, investment_dif, foreign_buy, foreign_sell, foreign_dif
            ) VALUES (
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='date') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='dealer_buy') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='dealer_sell') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='dealer_dif') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='dealer_buy_hedge') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='dealer_sell_hedge') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='dealer_dif_hedge') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='investment_buy') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='investment_sell') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='investment_dif') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='foreign_buy') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='foreign_sell') }}',
                '{{ ti.xcom_pull(task_ids='web_scrapy', key='foreign_dif') }}'
            );
        '''
    )

    alert_bot = TelegramOperator(
        task_id = 'alert',
        telegram_conn_id = TELEGRAM_CONN_ID,
        chat_id = CHAT_ID,
        text = "資料格式有變動，請重新設計爬蟲"        
    )

    notice_bot = TelegramOperator(
        task_id = 'notice',
        telegram_conn_id = TELEGRAM_CONN_ID,
        chat_id = CHAT_ID,
        text = "{{ti.xcom_pull(task_ids='web_scrapy', key='date')}} 資料以正確爬取"        
    )

    get_market_information >> whether_scrapy_or_not
    whether_scrapy_or_not >> do_nothing_task
    whether_scrapy_or_not >> scrapy_task
    scrapy_task >> whether_length_is_right
    whether_length_is_right >> insert_data_task >> notice_bot
    whether_length_is_right >> alert_bot
