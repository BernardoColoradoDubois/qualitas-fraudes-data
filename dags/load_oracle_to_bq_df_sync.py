from airflow import DAG
from airflow.providers.google.cloud.operators.datafusion import (
    CloudDataFusionStartPipelineOperator,
    CloudDataFusionGetInstanceOperator,
)
from airflow.providers.google.cloud.sensors.datafusion import CloudDataFusionPipelineStateSensor
from airflow.providers.google.cloud.operators.datafusion import DataFusionPipelineType 
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago
from airflow.models import Variable
# from airflow.operators.python import PythonOperator
from datetime import timedelta
import datetime
import json


# ==========================================================
# Retrieve variables from Airflow instance env variables
#===========================================================
ENV_VARS = json.loads(Variable.get("load_oracle_to_bq"));

QUERY_CONDITIONS= ENV_VARS["QUERY_CONDITIONS"]
PROJECT_ID = ENV_VARS["PROJECT_ID"]
LOCATION = ENV_VARS["LOCATION"]
INSTANCE_NAME = ENV_VARS["DATAFUSION_INSTANCE"]
NAMESPACE = ENV_VARS["NAMESPACE"]
PIPELINE_NAME = ENV_VARS["PIPELINE_NAME"]


# =========================================================================================
# We defined the following rules for the DAG
#   -POKE_INTERVAL_: How frequently in seconds will the sensor be 
#                   checking the datafusion pipeline status?
#   - POKE_MODE_: poke for short pipelines | reschedule for longer pipelines
#   - SENSOR_TIME_OUT_ : Overall time for the sensor to wait for one of the expected status
#===========================================================================================

DAG_NAME_='qlts_datafusion_test_sync';
POKE_INTERVAL_=60;
POKE_MODE_="reschedule";
SENSOR_TIME_OUT_=60 * 60 * 24 * 7; 

# Define default arguments
default_args = {
    'start_date': days_ago(0),
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    DAG_NAME_,
    default_args=default_args,
    description='DAG to trigger and monitors a Cloud Data Fusion pipeline in GCP',
    schedule_interval=datetime.timedelta(days=1),
    catchup=False,
    tags=['oracle', 'datafusion', 'bigquery'],
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=10),
)


# Task 1: Get Data Fusion instance details
get_instance = CloudDataFusionGetInstanceOperator(
    task_id="get_datafusion_instance",
    location=LOCATION,
    instance_name=INSTANCE_NAME,
    project_id=PROJECT_ID,
    dag=dag,
)

# Task 2: Start the pipeline
start_pipeline = CloudDataFusionStartPipelineOperator(
    task_id="start_pipeline",
    location=LOCATION,
    instance_name=INSTANCE_NAME,
    namespace=NAMESPACE,
    pipeline_name=PIPELINE_NAME,
    project_id=PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous= False,
    pipeline_timeout=3600,
    runtime_args=QUERY_CONDITIONS,
    deferrable=True,
    poll_interval=30,
    dag=dag
)

end = BashOperator(task_id='end',bash_command='echo end',dag=dag)

get_instance >> start_pipeline >> end
