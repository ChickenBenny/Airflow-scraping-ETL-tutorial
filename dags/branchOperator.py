import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator


default_args = {
    'owner': 'Benny Hsiao',
    'start_date': datetime(2022, 8, 15, 10, 0),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes = 5)
}


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