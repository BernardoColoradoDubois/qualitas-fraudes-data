import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from lib.utils import cluster_select


from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator

CLUSTER_MANAGER = Variable.get("CLUSTER_MANAGER", deserialize_json=True)
CLUSTER_TYPE = CLUSTER_MANAGER['cluster_type']
CLUSTER_NAME = CLUSTER_MANAGER['cluster_name']

VERIFICACIONES_DATAPROC_TEST_CLUSTER_CONFIG = Variable.get("VERIFICACIONES_DATAPROC_TEST_CLUSTER_CONFIG", deserialize_json=True)
VERIFICACIONES_DATAPROC_BIG_CLUSTER_CONFIG = Variable.get("VERIFICACIONES_DATAPROC_BIG_CLUSTER_CONFIG", deserialize_json=True)
VERIFICACIONES_DATAPROC_SMALL_CLUSTER_CONFIG = Variable.get("VERIFICACIONES_DATAPROC_SMALL_CLUSTER_CONFIG", deserialize_json=True)

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

cluster_selector = BranchPythonOperator(
    task_id='cluster_selector',
    python_callable=cluster_select,
    op_kwargs={
        'cluster_type': CLUSTER_TYPE,
        'big_cluster_label':'create_big_cluster',
        'small_cluster_label':'create_small_cluster',
        'test_cluster_label':'create_test_cluster'
    },
    dag=dag
)

create_test_cluster = DataprocCreateClusterOperator(
  task_id="create_test_cluster",
  project_id="qlts-nonprod-data-tools",
  cluster_config=VERIFICACIONES_DATAPROC_TEST_CLUSTER_CONFIG,
  region="us-central1",
  cluster_name=CLUSTER_NAME,
  num_retries_if_resource_is_not_ready=3,
  dag=dag
)

create_small_cluster = DataprocCreateClusterOperator(
  task_id="create_small_cluster",
  project_id="qlts-nonprod-data-tools",
  cluster_config=VERIFICACIONES_DATAPROC_SMALL_CLUSTER_CONFIG,
  region="us-central1",
  cluster_name=CLUSTER_NAME,
  num_retries_if_resource_is_not_ready=3,
  dag=dag
)

create_big_cluster = DataprocCreateClusterOperator(
  task_id="create_big_cluster",
  project_id="qlts-nonprod-data-tools",
  cluster_config=VERIFICACIONES_DATAPROC_BIG_CLUSTER_CONFIG,
  region="us-central1",
  cluster_name=CLUSTER_NAME,
  num_retries_if_resource_is_not_ready=3,
  dag=dag
)

cluster_selector >> [create_big_cluster, create_small_cluster, create_test_cluster]