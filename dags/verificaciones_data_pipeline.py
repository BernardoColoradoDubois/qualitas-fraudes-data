import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionGetInstanceOperator
from airflow.providers.google.cloud.sensors.datafusion import CloudDataFusionPipelineStateSensor
from airflow.providers.google.cloud.operators.datafusion import DataFusionPipelineType 

default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 1,
  'retry_delay': timedelta(minutes=1)
}

dag = DAG(
  'verificaciones_data_pipeline',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=10),
)

init_landing = BashOperator(
  task_id='init_landing',
  bash_command='echo init landing',
  dag=dag,
)

get_datafusion_instance = CloudDataFusionGetInstanceOperator(
  task_id="get_datafusion_instance",
  location='LOCATION',
  instance_name='INSTANCE_NAME',
  project_id='PROJECT_ID',
  dag=dag,
)

load_apercab_bsc = CloudDataFusionStartPipelineOperator(
  task_id="start_pipeline",
  location='LOCATION',
  instance_name='INSTANCE_NAME',
  namespace='NAMESPACE',
  pipeline_name='PIPELINE_NAME',
  project_id='PROJECT_ID',
  pipeline_type = DataFusionPipelineType.BATCH,
  asynchronous= True,
  runtime_args='QUERY_CONDITIONS',
  dag=dag,
)

end_landing = BashOperator(
  task_id='end_landing',
  bash_command='echo end landing',
  dag=dag,
)

init_landing >> get_datafusion_instance
