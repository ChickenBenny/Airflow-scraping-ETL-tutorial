# Airflow 與爬蟲菜雞的修行之旅
這是我使用Airflow + Python爬蟲自動化爬取三大法人交易資訊的開發筆記，撰寫方式是參考蜘蛛人大大李孟的文章。還沒看過的朋友可以先去看看，蜘蛛人的大大的文章都是值得一看再看([傳送門](https://leemeng.tw/a-story-about-airflow-and-data-engineering-using-how-to-use-python-to-catch-up-with-latest-comics-as-an-example.html))。
### 一同修行將獲得
* 撰寫簡易 ETL 的能力
* 了解如何使用Airflow進階的Operator(Branch、Postgres、Telegram)
* 簡易的爬蟲技能
* 串接第三方API的經驗
### App 開發的動機
我的碩士論文的主題是深度學習在股市上的應用，需要日常性的去爬取相關的資料。每日去更改App的日期、清理資料、彙整，重複性的工作令我感到非常煩躁。一直以來都想用自動化的方式取代，但一直沒有動力去執行。前陣子看到蜘蛛人大大利用Airflow來追漫畫的文章，讀完之後覺得非常有趣，想透過仿造他的專案來學習Airflow，並解決困擾已久的問題。於是便有了這篇學習筆記!

![](https://i.imgur.com/9OYKMNx.png)

### 為何使用選擇使用 Airflow ?
其實要撰寫爬蟲的機器人其實不難，在撰寫此篇文章之前，我就寫好了爬蟲的機器人。但因為程式並不是無時無刻的需要運行，只需要在交易日結算後進行爬取即可，因此需要定時去修改參數。你可能會問，那為何不使用 scheduler 就好? Scheduler 確實是解決這個問題很好的解法之一，但主要是我未來希望能做資料工程相關的工作。希望能藉由這個機會學習使用Airflow這套軟體，也藉由蜘蛛人大大的文章，一同學習及成長。

![](https://i.imgur.com/TyCLcUj.png)

### 工作流程
![](https://i.imgur.com/DtHGoYt.png)

* 上證交所查看當日是否有交易紀錄
    * 股市有開盤
        * Trigger爬蟲程式
        * 爬取交易資訊
        * 資料清洗
        * 存入資料庫 or csv file
        * 發送爬取消息
    * 股市沒開盤
        * 不做事
### 整體架構
![](https://i.imgur.com/3xowPg1.png)

### Airflow & Python 實作
#### 環境搭建
這隻程式我是使用 Docker container 的方式架設，而非架設在 Local 端，主要的好處事後續在移植的時候方便，並且不會影響到 Local 端的程式版本。你可能會問那為什麼不使用虛擬環境呢 ? 請詳 : [好文推推](https://www.opc-router.com/what-is-docker/)

1. Create .env 定義環境變數
```
AIRFLOW_UID=50000
AIRFLOW_GID=0

# Meta-Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow Core
AIRFLOW__CORE__FERNET_KEY=UKMzEm3yIuFYEq1y3-2FxPNWSVwRASpahmQ9kQfEr8E=
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW_UID=0

# Backend DB
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=False

# Airflow Init
_AIRFLOW_DB_UPGRADE=True
_AIRFLOW_WWW_USER_CREATE=True
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```
2. 撰寫Docker-compose file，這裡我們只需要Webserver、Scheduler、Worker和Metadatabse
```
version: '3'
x-common:
  &common
  build: ./python_package
  user: "${AIRFLOW_UID}:0"
  env_file: 
    - .env
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - /var/run/docker.sock:/var/run/docker.sock

x-depends-on:
  &depends-on
  depends_on:
    postgres:
      condition: service_healthy


services:
  postgres:
    image: postgres:13
    container_name: postgres
    ports:
      - "5434:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    env_file:
      - .env

  scheduler:
    <<: *common
    <<: *depends-on
    container_name: airflow-scheduler
    command: scheduler
    restart: on-failure
    ports:
      - "8793:8793"

  webserver:
    <<: *common
    <<: *depends-on
    container_name: airflow-webserver
    restart: always
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 30s
      retries: 5
  
  airflow-init:
    <<: *common
    container_name: airflow-init
    entrypoint: /bin/bash
    command:
      - -c
      - |
        mkdir -p /sources/logs /sources/dags /sources/plugins
        chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins}
        exec /entrypoint airflow version
```
3. 利用`docker-compose up`測試環境是否搭建成功，應該要有三個容器在運行。

    ![](https://i.imgur.com/rQIlhWE.png)



#### APP 設計架構
```
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
```
這邊我們把所有Function都先用`print`取代，專注在整個App的架構。若對 Airflow 基本操作有些陌生的朋友，可以先去看我的 [Airflow tutorial](https://github.com/ChickenBenny/Airflow-tutorial)。

#### 爬蟲程式設計
首先要爬取三大法人的交易資訊，我們必須要先釐清該去哪裡取得這些資訊。根據台灣證交所的資訊顯示，若當日股市有開盤，證交所繪於每日的8:30 - 13:30收集相關的交易、搓合和成交的資訊，並於下午公布於網站上。
1. 每日三大法人的交易資訊放在證交所的[三大法人買賣金額統計表頁面](https://www.twse.com.tw/zh/page/trading/fund/BFI82U.html)。

    ![](https://i.imgur.com/p1G5VU1.png)
2. 利用頁面的搜尋引勤去觸發網頁獲取資料，並且觀察開發人員工具(F12)，檢視是否存在規律。
    我們可以每次傳送新的資料請求，會跑出一個名叫 BFI82U 的request。
    
    ![](https://i.imgur.com/RMZidAF.png)
    
    * Request method 為 GET
    * Request URL 為 `https://www.twse.com.tw/fund/BFI82U?response=json&dayDate=${date}`

3. 我們可以透過以上的觀察，撰寫我們的爬蟲程式。

    ![](https://i.imgur.com/up2LJrn.png)

4. 發現網站是以json的格式去儲存資料，並且當天若有開盤`stat`會顯示為`OK`，若無則是`很抱歉，沒有符合條件的資料!`。`stat`可以作為我們後續判斷，有沒有成功盤取資料的條件。
![](https://i.imgur.com/Jr6vK4a.png)
5. 每日的交易資訊他是存在`data`中，我們可以透過`(res.json())['data']`取得資料。

    ![](https://i.imgur.com/QHH1wm1.png)

6. 將資料更改為後續能取用的樣態。

    ![](https://i.imgur.com/QzAP5sO.png)



#### 寄發時間測試
在使用Airflow的時候，有兩點要特別注意。
1. Airflow他的`Start_date`並不是任務第一次觸發的時間，第一次觸發的時間是`start_date` + `schedule_interval` 。
2. Airflow預設的時區是UTC，像是我們在做股票預測，日期對我們來說是十分重要的。因此我們需要使用`timedelta`來轉換我們的時區。`astimezone(timezone(timedelta(hours=8)))`能幫我們達到轉換時區的效果。

* 測試範例
```
def test(date):
    print(f'UTC time :{date}')
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
```
我們可以查看執行的log，會發現轉換後的時間與台灣時間一樣。

![](https://i.imgur.com/JOe80bi.png)

測試完時區我們可以開始定義我們爬蟲App每日的觸發時間。股市每日收盤是在下午1:30分，大多會於3-4點左右證交所會公布結算資料。因此我們可以粗略定義我們的爬蟲程式在每日的下午六點會去爬取資料，接者執行後面的邏輯。
```
default_args = {
    'owner': 'Benny Hsiao',
    'start_date': datetime(2022, 8, 15, 10, 0),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes = 5)
}
```

#### 資料庫設計
在這個架構中，我們使用 PostgresSQL 當作我們的資料庫，由於三大法人的交易資訊格式固定，並且我希望後續在做模型訓練時，能夠是有固定格式並且整理好的資料。
1. 修改docker-compose 新增一個postgres的service，主要用於儲存爬取的資料。
```
  database:
    image: postgres:13
    container_name: database
    ports:
      - "5432:5432"
    env_file:
      - .env
    volumes: 
      - ./database/pg:/var/lib/postgresql
      - ./database/sql/create_table.sql:/docker-entrypoint-initdb.d/create_tables.sql
```
*Notice : 這邊要注意postgres的是使用與airflow相同的.env檔案，因此postgres的帳號、密碼及DB皆為`airflow`。*

2. 創建`create_table.sql`用於資料庫初始化時建立table。
```
-- create a table to store the data extract from youtube
CREATE TABLE IF NOT EXISTS investment_data (
    dt TEXT NOT NULL,
    dealer_buy TEXT NOT NULL,
    dealer_sell TEXT NOT NULL,
    dealer_dif TEXT NOT NULL,
    investment_buy TEXT NOT NULL,
    investment_sell TEXT NOT NULL,
    investment_dif TEXT NOT NULL,
    foreign_buy TEXT NOT NULL,
    foreign_sell TEXT NOT NULL,
    foreign_dif TEXT NOT NULL
); 
```
*Notice : 我們這邊所有資訊都是使用TEXT儲存，因為三大法人交易資訊的金額很容易**超過float的上限**。*

3. 使用`docker-compose up`確認database和table是否成功建立。
```
$ docker-compose up
$ docker exec -it database sh
# psql -U airflow
airflow-# SELECT * FROM investment_data;
```
![](https://i.imgur.com/yUMOJOu.png)

#### 爬蟲資料儲存到Postgres database
我們每日爬取的資料，會先透過爬蟲的任務將資料推到Xcom，再藉由Postgres的operator寫入我們的database。
1. 藉由Web UI的connection串接我們的database，connection id我們在這邊設定為`database`。

    ![](https://i.imgur.com/qERMDLy.png)

3. 撰寫Postgres Operator將資料從Xcom從pull下來，airflow的變數是以jinjia方式去傳遞`{{ti.xcom_pull(task_ids='id', key='key')}}`的。
```
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
```
3. 進入Web UI檢查log，確認是否成功執行。
4. 進入database檢查是否正確存入。
```
$ docker exec -it database sh
# psql -U airflow
airflow=# SELECT * FROM investment_data WHERE dt='20220819'
```
![](https://i.imgur.com/vMePmc2.png)


#### Telegram chat bot 寄送消息
這邊串接Telegram會稍微比較複雜，覺得困難的朋友可以選擇不做，或是上網找Telegram chat bot的教學。
1. 打開Telegram並搜尋BotFather，輸入`/newbot`創建新的Chat bot，接著分別輸入`bot name`和`bot username`。
2. 搜尋Get My ID取得chat id。
3. 進入Web UI設定Connection。
    ![](https://i.imgur.com/HtY1NHl.png)
    Notice : `Password` 要填入API Key，`Host`則填入Chat id
4. 撰寫發送消息的TelegramOperator。
```
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
        chat_id = "809480369",
        text = "{{ ti.xcom_pull(task_ids='generate_message', key='message') }}"
    )

    generate_message >> sending_message
```

### 組樂高時間，把所有的東西組合在一起!!
在把我們寫好的Code組在一起之前，要先了解airflow裡非常重要的邏輯運算Operator，他負責決定了我們pipline的下一站要前往哪裡。
#### BranchPythonOperaotr
假設今天有兩個任務能夠選擇，這時候我們需要加入BranchOperator來告訴我們的airflow接下來該往哪裡走。範例如下:
![](https://i.imgur.com/PX9NIW0.png)

```
def branch():
    a = False
    if a:
        return 'A'
    else:
        return 'B'

def A():
    print("A")

def B():
    print("B")

with DAG(
    dag_id = 'branch',
    default_args = default_args,
    description = 'branch'
) as dag:
    branch_test = BranchPythonOperator(
        task_id = 'branch',
        python_callable = branch,
    )

    task_a = PythonOperator(
        task_id = "A",
        python_callable = A
    )

    task_b = PythonOperator(
        task_id = "B",
        python_callable = B
    )

    branch_test >> task_a
    branch_test >> task_b
```
我們可以在branch呼叫的function中，加入判斷條件，並`return task_id`來告訴airflow，我們接下來該往裡走。這部分蜘蛛人大大描述得十分清楚，請大家[左轉](https://leemeng.tw/a-story-about-airflow-and-data-engineering-using-how-to-use-python-to-catch-up-with-latest-comics-as-an-example.html#%E5%9C%A8%E5%B7%A5%E4%BD%9C%E6%B5%81%E7%A8%8B%E5%85%A7%E5%8A%A0%E5%85%A5%E6%A2%9D%E4%BB%B6%E5%88%86%E6%94%AF)去拜讀一下。

#### 開始組裝樂高

我們組裝的思路:
* 判斷當日是否有開盤交易(使用stat == 'OK'來判斷)
    * stat == 'OK'
        * 觸發爬蟲APP(藉由len(data)決定下一步
            * len(data) == 12
                * 呼叫Postgres Operator儲存資料
                * 透過telegram寄發爬取成功消息
            * len(data) != 12
                * 表示證交所儲存資料格式改變
                * 寄發須調整爬蟲程式的消息
             
    * stat != 'OK'
        * 不做事也不寄發消息

按照上面的組裝思路，我們把它畫成流程圖方便我們去做設計及參考。
![](https://i.imgur.com/LwwhMiK.png)

#### 開始Coding
1. 取得開盤資訊並把判斷結果拋至xcom，讓Branch去判斷下一步。
```
def market_information():
    date = datetime.now().strftime("%Y%m%d")
    url = f"https://www.twse.com.tw/fund/BFI82U?response=json&dayDate={date}"
    res = requests.get(url)
    res = res.json()    
    if res['stat'] == 'OK':
        return 'scrapy'
    else:
        return 'do_nothing'
```
2. Branch取得market_information，接者判斷是否要進行爬蟲。
```
def scrapy_condition(ti):
    flag = ti.xcom_pull(task_ids='market_information', key='return_value')
    if flag == 'scrapy':
        return 'scrapy'
    else:
        return 'do_nothing'
        
// task_dependency
    get_market_information >> whether_scrapy_or_not
    whether_scrapy_or_not >> do_nothing_task
    whether_scrapy_or_not >> scrapy_task
```
3. 新增do_nothing的operator
4. 爬蟲app
```
def web_scrapy(ti):
    date = datetime.now().strftime("%Y%m%d")
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
```
5. 判斷是爬取資料格式是否有變動
```
def length_condition(ti):
    flag = ti.xcom_pull(task_ids='web_scrapy', key='return_value')    
    if flag == 'ok':
        return 'insert_data'
    else:
        return 'alert'
```
6. alert chat bot設計
```
alert_bot = TelegramOperator(
    task_id = 'alert',
    telegram_conn_id = TELEGRAM_CONN_ID,
    chat_id = CHAT_ID,
    text = "資料格式有變動，請重新設計爬蟲"        
)
```
7. insert_data任務設計
```
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
```
*Notice : 記得去database查看是否有正確儲存*
![](https://i.imgur.com/0uPrB0S.png)

8. 最後的測試，我們的APP有三種結果，分別如下:
    1. 當天未開盤，不做任何事
    2. 資料格式變動，寄發alert
    3. 爬取正確

    因此我們需要設計三個不同的時間日期去做測試，分別是假日、2014以前(證交所Ifrs改版後有更改資料儲存樣態)以及正常交易日。因此我們取`20220820`, `20100106`, `20220823`去執行測試。測試結果並無異常，因此我們完成了我們的App了!!

![](https://i.imgur.com/w5qcwdU.png)

### 菜雞修練之旅結束，前往更高的境界
* 學會了ETL的排程
* 學會簡易的爬蟲
* 學會使用BranchOperator
* 串接Telegram API

喜歡的朋友歡迎幫忙按星星，我會持續撰寫教學的Project!!