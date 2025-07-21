import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.models import Variable


from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator

VERIFICACIONES_DATAPROC_TEST_CLUSTER_CONFIG = Variable.get("VERIFICACIONES_DATAPROC_TEST_CLUSTER_CONFIG", deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
  'create_cluster',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
  tags=['MX','AUTOS','VERIFICACIONES','INSUMOS']
)

create_cluster = DataprocCreateClusterOperator(
  task_id="create_cluster",
  project_id="qlts-nonprod-data-tools",
  cluster_config=VERIFICACIONES_DATAPROC_TEST_CLUSTER_CONFIG,
  region="us-central1",
  cluster_name="verificaciones-dataproc",
  num_retries_if_resource_is_not_ready=3,
  dag=dag
)