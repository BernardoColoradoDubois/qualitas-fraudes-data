from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.operators.python import PythonOperator


def task_1_function():
    print("Running task 1")

def task_2_function():
    print("Running task 2")


with DAG(
    dag_id='DAG_A',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  
    catchup=False,
    tags=["example"],
) as dag:
    
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=task_1_function,
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=task_2_function,
    )

    trigger_b = TriggerDagRunOperator(
        task_id="trigger_dag_b",
        trigger_dag_id="DAG_B",  
    )

    task_1 >> task_2 >> trigger_b
