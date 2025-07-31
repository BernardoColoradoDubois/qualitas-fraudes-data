import airflow
import json
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from lib.utils import get_bucket_file_contents,upload_storage_csv_to_bigquery


default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 3,
  'retry_delay': timedelta(minutes=2)
}

dag = DAG(
  'load_csv',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
)

init = BashOperator(task_id='init',bash_command='echo "Iniciando el DAG"',dag=dag)

merge_control_de_agentes = PythonOperator(
  task_id='merge_control_de_agentes',
  python_callable=get_bucket_file_contents,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
    'folder': 'CONTROL_DE_AGENTES/',
  },
  dag=dag
)

load_control_de_agentes = PythonOperator(
  task_id='load_control_de_agentes',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/CONTROL_DE_AGENTES/CONTROL_DE_AGENTES_202506_01.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'CONTROL_DE_AGENTES',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.control_de_agentes.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

init >> merge_control_de_agentes >> load_control_de_agentes