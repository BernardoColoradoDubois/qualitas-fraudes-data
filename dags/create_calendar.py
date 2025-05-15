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

init_date = '2000-01-01'
final_date = '2050-12-31'

default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 3,
  'retry_delay': timedelta(minutes=2)
}

dag = DAG(
  'create_calendar',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
)

dm_calendario = BigQueryInsertJobOperator(
  task_id="dm_calendario",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/CALENDARIO/DM_CALENDARIO.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_CALENDARIO',
    'init_date': init_date,
    'final_date': final_date,
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)