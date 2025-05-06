from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Hàm Python dùng cho task
def print_hello():
    print("Hello Airflow")

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='simple_test_dag',
    default_args=default_args,
    description='A simple test DAG',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # chạy mỗi ngày
    catchup=False,
    tags=['example'],
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: print("Start DAG"),
    )

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )

    start_task >> hello_task  # Thiết lập thứ tự task
