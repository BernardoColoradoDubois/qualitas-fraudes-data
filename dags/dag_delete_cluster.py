import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionGetInstanceOperator
from airflow.providers.google.cloud.operators.datafusion import DataFusionPipelineType 

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


from lib.utils import get_bucket_file_contents

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

from airflow.utils.trigger_rule import TriggerRule

# --- Configs ---
PROJECT_ID = 'qlts-nonprod-data-tools'
REGION = 'us-central1'
CLUSTER_NAME = 'create-dataproc-cluster'
INSTANCE_NAME = 'qlts-data-fusion-dev'
PIPELINE_NAME_1 = 'saul_qlts_dev_verificaciones_apercab_bsc'

init_date = '2025-03-01'
final_date = '2025-03-31'

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



with DAG(
    dag_id='dag_delete_cluster',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['DELETE', 'dataproc', 'test'],
) as dag:
    


    # Task 4: Delete the cluster regardless of pipeline success/failure
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE  # ensures it runs even if upstream fails
    )


    # DAG dependency chain
    delete_cluster
