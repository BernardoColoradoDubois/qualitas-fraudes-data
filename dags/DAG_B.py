from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def random_function(**kwargs):
    print("DAG B has been triggered.")

with DAG(
    dag_id='DAG_B',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=["example"],
) as dag:
    run_this = PythonOperator(
        task_id='run_this',
        python_callable=random_function,
    )
