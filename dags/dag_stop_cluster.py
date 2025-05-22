import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocStopClusterOperator


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
    dag_id='dag_stop_dataproc_cluster',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['dataproc', 'stop'],
) as dag:

    init_stop = BashOperator(
        task_id='init_stop',
        bash_command='echo Stopping Dataproc cluster...',
    )

    stop_cluster = DataprocStopClusterOperator(
        task_id='stop_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    finish_stop = BashOperator(
        task_id='finish_stop',
        bash_command='echo Cluster successfully stopped.',
    )

    init_stop >> stop_cluster >> finish_stop
