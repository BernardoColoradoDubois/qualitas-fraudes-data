import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.models import Variable


from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

CLUSTER_MANAGER = Variable.get("CLUSTER_MANAGER", deserialize_json=True)
CLUSTER_TYPE = CLUSTER_MANAGER['cluster_type']
CLUSTER_NAME = CLUSTER_MANAGER['cluster_name']

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
  'delete_cluster',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
  tags=['MX','AUTOS','VERIFICACIONES','INSUMOS']
)

delete_cluster = DataprocDeleteClusterOperator(
  task_id="delete_cluster",
  project_id="qlts-nonprod-data-tools",
  cluster_name=CLUSTER_NAME,
  region="us-central1",
  dag=dag
)