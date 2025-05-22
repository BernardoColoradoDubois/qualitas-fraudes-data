import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocStartClusterOperator


PROJECT_ID = 'qlts-nonprod-data-tools'
REGION = 'us-central1'
CLUSTER_NAME = 'verificaciones-dataproc'

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_start_dataproc_cluster',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['dataproc', 'start'],
) as dag:

    init_start = BashOperator(
        task_id='init_start',
        bash_command='echo Starting Dataproc cluster...',
    )

    start_cluster = DataprocStartClusterOperator(
        task_id='start_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    finish_start = BashOperator(
        task_id='finish_start',
        bash_command='echo Cluster successfully started.',
    )

    init_start >> start_cluster >> finish_start


