import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'list_python_packages',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval='0 0 1 1 *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id='list_packages',
    bash_command='pip list',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31 - 1,
    do_xcom_push=False
)