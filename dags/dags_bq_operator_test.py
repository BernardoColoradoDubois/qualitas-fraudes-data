import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator
from lib.utils import execute_query_workflow,get_bucket_file_contents
import json

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 1,
  'retry_delay': timedelta(minutes=1)
}

dag = DAG(
  'bq_operator_test',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=10)
)

lanzar_query_bq = BigQueryInsertJobOperator(
  task_id="lanzar_query_bq",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/TEST/TEST.sql'),
      "useLegacySql": False,
    }
  },
  params={"column_name": "total"},
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

lanzar_query_bq