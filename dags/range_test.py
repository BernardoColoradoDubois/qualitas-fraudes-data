import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator



from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionGetInstanceOperator
from airflow.providers.google.cloud.operators.datafusion import DataFusionPipelineType 

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from lib.utils import get_date_interval,get_cluster_tipe_creator


VERIFICACIONES_LOAD_INTERVAL = Variable.get("VERIFICACIONES_LOAD_INTERVAL", default_var="YESTERDAY")

interval = get_date_interval(project_id='qlts-dev-mx-au-bro-verificacio',period=VERIFICACIONES_LOAD_INTERVAL)

init_date = interval['init_date']
final_date = interval['final_date']

default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 3,
  'retry_delay': timedelta(minutes=2)
}

dag = DAG(
  'range_test',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
)

validate_date_interval = BigQueryInsertJobOperator(
  task_id="validate_date_interval",
  configuration={
    "query": {
      "query": "SELECT DATE_DIFF(DATE '{{task.params.init_date}}', DATE '{{task.params.final_date}}', DAY) AS days_diff;",
      "useLegacySql": False,
    },
  },
  params={
    'init_date':init_date,
    'final_date':final_date
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

select_cluster_creator = BranchPythonOperator(
  task_id="select_cluster_creator",
  python_callable=get_cluster_tipe_creator,
  op_kwargs={
    'init_date':init_date,
    'final_date':final_date,
    'small_cluster_label':'create_small_cluster',
    'big_cluster_label':'create_big_cluster'
  },
  provide_context=True,
  dag=dag
)

create_small_cluster = BashOperator(task_id="create_small_cluster",bash_command="echo 'Creating small cluster'",dag=dag)

create_big_cluster = BashOperator(task_id="create_big_cluster",bash_command="echo 'Creating Big cluster'",dag=dag)

start_pipeline = BashOperator(task_id="start_pipeline",trigger_rule='one_success',bash_command="echo 'Starting pipeline'",dag=dag)

validate_date_interval >> select_cluster_creator
select_cluster_creator >> [create_small_cluster,create_big_cluster] >> start_pipeline
