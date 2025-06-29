import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from airflow.models import Variable

from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionGetInstanceOperator
from airflow.providers.google.cloud.operators.datafusion import DataFusionPipelineType 

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

from lib.utils import get_bucket_file_contents,get_date_interval,get_cluster_tipe_creator

VERIFICACIONES_CONFIG_VARIABLES = Variable.get("VERIFICACIONES_CONFIG_VARIABLES", deserialize_json=True)

DATA_PROJECT_ID = VERIFICACIONES_CONFIG_VARIABLES['DATA_PROJECT_ID']
DATA_PROJECT_REGION = VERIFICACIONES_CONFIG_VARIABLES['DATA_PROJECT_REGION']
DATA_DATAFUSION_INSTANCE_NAME = VERIFICACIONES_CONFIG_VARIABLES['DATA_DATAFUSION_INSTANCE_NAME']
DATA_DATAFUSION_TEMPORARY_BUCKET_NAME =  VERIFICACIONES_CONFIG_VARIABLES['DATA_DATAFUSION_TEMPORARY_BUCKET_NAME']
DATA_DATAFUSION_NAMESPACE = VERIFICACIONES_CONFIG_VARIABLES['DATA_DATAFUSION_NAMESPACE']
DATA_DATAPROC_CLUSTER_NAME = VERIFICACIONES_CONFIG_VARIABLES['DATA_DATAPROC_CLUSTER_NAME']
DATA_DATAPROC_PROFILE_NAME = VERIFICACIONES_CONFIG_VARIABLES['DATA_DATAPROC_PROFILE_NAME']
DATA_COMPOSER_WORKSPACE_BUCKET_NAME = VERIFICACIONES_CONFIG_VARIABLES['DATA_COMPOSER_WORKSPACE_BUCKET_NAME']

VERIFICACIONES_PROJECT_ID = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_PROJECT_ID']
VERIFICACIONES_PROJECT_REGION = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_PROJECT_REGION']
VERIFICACIONES_LAN_DATASET_NAME = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_LAN_DATASET_NAME']
VERIFICACIONES_RTL_DATASET_NAME = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_RTL_DATASET_NAME']
VERIFICACIONES_STG_DATASET_NAME = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_STG_DATASET_NAME']
VERIFICACIONES_DM_DATASET_NAME = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_DM_DATASET_NAME']
VERIFICACIONES_SEED_DATASET_NAME = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_SEED_DATASET_NAME']
VERIFICACIONES_CONNECTION_DEFAULT = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_CONNECTION_DEFAULT']
VERIFICACIONES_CONNECTION_DEFAULT = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_CONNECTION_DEFAULT']

APP_ORACLE_HOST = VERIFICACIONES_CONFIG_VARIABLES['APP_ORACLE_HOST']
APP_ORACLE_SERVICE_NAME = VERIFICACIONES_CONFIG_VARIABLES['APP_ORACLE_SERVICE_NAME']
APP_ORACLE_USER = VERIFICACIONES_CONFIG_VARIABLES['APP_ORACLE_USER']
APP_ORACLE_PASSWORD = VERIFICACIONES_CONFIG_VARIABLES['APP_ORACLE_PASSWORD']
APP_ORACLE_INJECT_SCHEMA_NAME = VERIFICACIONES_CONFIG_VARIABLES['APP_ORACLE_INJECT_SCHEMA_NAME']
APP_ORACLE_INSUMOS_SCHEMA_NAME = VERIFICACIONES_CONFIG_VARIABLES['APP_ORACLE_INSUMOS_SCHEMA_NAME']

VERIFICACIONES_DATAPROC_BIG_CLUSTER_CONFIG = Variable.get("VERIFICACIONES_DATAPROC_BIG_CLUSTER_CONFIG", deserialize_json=True)
VERIFICACIONES_DATAPROC_SMALL_CLUSTER_CONFIG = Variable.get("VERIFICACIONES_DATAPROC_SMALL_CLUSTER_CONFIG", deserialize_json=True)
VERIFICACIONES_LOAD_INTERVAL = Variable.get("VERIFICACIONES_LOAD_INTERVAL", default_var="YESTERDAY")

interval = get_date_interval(project_id='qlts-dev-mx-au-bro-verificacio',period=VERIFICACIONES_LOAD_INTERVAL)

init_date = interval['init_date']
final_date = interval['final_date']


def get_datafusion_inject_runtime_args(table_name:str, inject_table_name:str, insumos_table_name:str, size:str,init_date=None, final_date=None):

  if size == 'XS':
    executor_cores='1'
    executor_memory='2048'

  elif size == 'S':
    executor_cores='2'
    executor_memory='4096'
  elif size == 'M':
    executor_cores='2'
    executor_memory='8192'
  elif size == 'L':
    executor_cores='2'
    executor_memory='16384'


  inject_runtime_args={
    'app.pipeline.overwriteConfig': 'true',
    'task.executor.system.resources.cores': executor_cores,
    'task.executor.system.resources.memory': executor_memory,
    'dataproc.cluster.name': DATA_DATAPROC_CLUSTER_NAME,
    'system.profile.name': DATA_DATAPROC_PROFILE_NAME, 
    'APP_ORACLE_HOST':APP_ORACLE_HOST,
    'APP_ORACLE_SERVICE_NAME':APP_ORACLE_SERVICE_NAME,
    'APP_ORACLE_USER':APP_ORACLE_USER,
    'APP_ORACLE_PASSWORD':APP_ORACLE_PASSWORD,     
    'TEMPORARY_BUCKET_NAME':DATA_DATAFUSION_TEMPORARY_BUCKET_NAME,
    'DATASET_NAME':VERIFICACIONES_DM_DATASET_NAME,
    'TABLE_NAME':table_name,
    'INJECT_SCHEMA_NAME':APP_ORACLE_INJECT_SCHEMA_NAME,
    'INJECT_TABLE_NAME':inject_table_name,
    'INSUMOS_SCHEMA_NAME':APP_ORACLE_INSUMOS_SCHEMA_NAME,
    'INSUMOS_TABLE_NAME':insumos_table_name
  }

  if init_date is not None and final_date is not None:
    inject_runtime_args.update({
      'init_date': init_date,
      'final_date': final_date
    })

  if size == 'L':
    inject_runtime_args.update({
      "spark:spark.yarn.am.cores": "2",
      'spark.dynamicAllocation.enabled': 'true',
      'spark.shuffle.service.enabled': 'true',
      'spark.dynamicAllocation.minExecutors': '4',
      'spark.dynamicAllocation.maxExecutors': '50',
      'spark.dynamicAllocation.initialExecutors': '2',
      'spark.dynamicAllocation.executorIdleTimeout': '60s',
      'spark.dynamicAllocation.schedulerBacklogTimeout': '1s',
      'spark:spark.yarn.am.memory': '2g'

    })

  return inject_runtime_args


def get_datafusion_load_runtime_args(table_name:str,size:str,init_date=None, final_date=None):
  
  base_args = {
    'app.pipeline.overwriteConfig': 'true',
    'task.executor.system.resources.cores': '2',
    'task.executor.system.resources.memory': '8192',
    'dataproc.cluster.name': DATA_DATAPROC_CLUSTER_NAME,
    'system.profile.name': DATA_DATAPROC_PROFILE_NAME,
    'TEMPORARY_BUCKET_NAME': DATA_DATAFUSION_TEMPORARY_BUCKET_NAME,
    'DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
    'TABLE_NAME': table_name,  
  }
  
  if size == 'XS':
    base_args = {
      'app.pipeline.overwriteConfig': 'true',
      'task.executor.system.resources.cores': '1',
      'task.executor.system.resources.memory': '2048',
      'dataproc.cluster.name': DATA_DATAPROC_CLUSTER_NAME,
      'system.profile.name': DATA_DATAPROC_PROFILE_NAME,
      'TEMPORARY_BUCKET_NAME': DATA_DATAFUSION_TEMPORARY_BUCKET_NAME,
      'DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'TABLE_NAME': table_name,
    }
  elif size == 'S':
    base_args = {
      'app.pipeline.overwriteConfig': 'true',
      'task.executor.system.resources.cores': '2',
      'task.executor.system.resources.memory': '4096',
      'dataproc.cluster.name': DATA_DATAPROC_CLUSTER_NAME,
      'system.profile.name': DATA_DATAPROC_PROFILE_NAME,
      'TEMPORARY_BUCKET_NAME': DATA_DATAFUSION_TEMPORARY_BUCKET_NAME,
      'DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'TABLE_NAME': table_name,
    } 
  elif size == 'M':
    base_args = {
      'app.pipeline.overwriteConfig': 'true',
      'task.executor.system.resources.cores': '2',
      'task.executor.system.resources.memory': '8192',
      'dataproc.cluster.name': DATA_DATAPROC_CLUSTER_NAME,
      'system.profile.name': DATA_DATAPROC_PROFILE_NAME,
      'TEMPORARY_BUCKET_NAME': DATA_DATAFUSION_TEMPORARY_BUCKET_NAME,
      'DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'TABLE_NAME': table_name,
    }   
  elif size == 'L':
    base_args = {
      'app.pipeline.overwriteConfig': 'true',
      'task.executor.system.resources.cores': '2',
      'task.executor.system.resources.memory': '16384',
      'dataproc.cluster.name': DATA_DATAPROC_CLUSTER_NAME,
      'system.profile.name': DATA_DATAPROC_PROFILE_NAME,
      'TEMPORARY_BUCKET_NAME': DATA_DATAFUSION_TEMPORARY_BUCKET_NAME,
      'DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'TABLE_NAME': table_name,
      'spark.sql.adaptive.enabled': 'true',
      'spark.sql.adaptive.skewJoin.enabled': 'true',
      'spark.sql.adaptive.skewJoin.skewedPartitionFactor': '5',
      'spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes': '268435456',  #256MB en bytes
      'spark.dynamicAllocation.enabled': 'true',
      'spark.shuffle.service.enabled': 'true',
      'spark.dynamicAllocation.minExecutors': '4',
      'spark.dynamicAllocation.maxExecutors': '50',
      'spark.dynamicAllocation.initialExecutors': '2',
      'spark.dynamicAllocation.executorIdleTimeout': '60s',
      'spark.dynamicAllocation.schedulerBacklogTimeout': '1s',
}

  if init_date is not None and final_date is not None:
    base_args.update({
      'init_date': init_date,
      'final_date': final_date
    })
    
  return base_args
  

default_args = {
  'start_date': airflow.utils.dates.days_ago(1),
  'retries': 4,
  'retry_delay': timedelta(minutes=5)
}

dag = DAG(
  'qualitas_verificaciones_data_pipeline_rocket',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 13 * * *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=400),
  tags=['MX','AUTOS',DATA_DATAFUSION_NAMESPACE,'INSUMOS']
)

landing = BashOperator(task_id='landing',bash_command='echo init landing',dag=dag)


@task_group(group_id='init_landing',dag=dag)
def init_landing():
  
  
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
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  create_big_cluster = DataprocCreateClusterOperator(
    task_id="create_big_cluster",
    project_id=DATA_PROJECT_ID,
    cluster_config=VERIFICACIONES_DATAPROC_BIG_CLUSTER_CONFIG,
    region=DATA_PROJECT_REGION,
    cluster_name=DATA_DATAPROC_CLUSTER_NAME,
    num_retries_if_resource_is_not_ready=3,
    dag=dag
  )
  
  create_small_cluster = DataprocCreateClusterOperator(
    task_id="create_small_cluster",
    project_id=DATA_PROJECT_ID,
    cluster_config=VERIFICACIONES_DATAPROC_SMALL_CLUSTER_CONFIG,
    region=DATA_PROJECT_REGION,
    cluster_name=DATA_DATAPROC_CLUSTER_NAME,
    num_retries_if_resource_is_not_ready=3,
    dag=dag
  )
  
  select_cluster_creator = BranchPythonOperator(
    task_id="select_cluster_creator",
    python_callable=get_cluster_tipe_creator,
    op_kwargs={
      'init_date':init_date,
      'final_date':final_date,
      'small_cluster_label': 'init_landing.create_small_cluster',
      'big_cluster_label': 'init_landing.create_big_cluster'
    },
    provide_context=True,
    dag=dag
  )  
  
  get_datafusion_instance = CloudDataFusionGetInstanceOperator(
    task_id="get_datafusion_instance",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    trigger_rule='one_success',
    project_id=DATA_PROJECT_ID,
    dag=dag,
  )
  
  validate_date_interval>>select_cluster_creator>>[create_big_cluster,create_small_cluster] >> get_datafusion_instance
  
@task_group(group_id='landing_bsc_siniestros',dag=dag)
def landing_bsc_siniestros():
  
  load_apercab_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_apercab_bsc",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_apercab_bsc',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    pipeline_timeout=3600,
    asynchronous=False,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('APERCAB_BSC',size='L',init_date=init_date, final_date=final_date),
    dag=dag
  )

  load_maseg_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_maseg_bsc",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_maseg_bsc',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('MASEG_BSC',size='M'),
    dag=dag
  )

  load_pagoprove = CloudDataFusionStartPipelineOperator(
    task_id="load_pagoprove",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_pagoprove',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('PAGOPROVE',size='L',init_date=init_date, final_date=final_date),
    dag=dag
  )
  """
  load_pagosproveedores = CloudDataFusionStartPipelineOperator(
    task_id="load_pagosproveedores",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_pagosproveedores',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('PAGOSPROVEEDORES',size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )
  """

  load_prestadores = CloudDataFusionStartPipelineOperator(
    task_id="load_prestadores",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_prestadores',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('PRESTADORES',size='S'),
    dag=dag
  )

  load_reservas_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_reservas_bsc",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_reservas_bsc',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('RESERVAS_BSC',size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )

  load_testado_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_testado_bsc",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_testado_bsc',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('TESTADO_BSC',size='XS'),
    dag=dag
  )

  load_tipoproveedor = CloudDataFusionStartPipelineOperator(
    task_id="load_tipoproveedor",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_tipoproveedor',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('TIPOPROVEEDOR',size='XS'),
    dag=dag
  )

  load_tsuc_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_tsuc_bsc",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_tsuc_bsc',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('TSUC_BSC',size='XS'),
    dag=dag
  )
  
  load_valuacion_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_valuacion_bsc",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_valuacion_bsc',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('VALUACION_BSC',size='M', init_date=init_date, final_date=final_date),
    dag=dag
  )
  
  
@task_group(group_id='landing_siniestros',dag=dag)
def landing_siniestros():

  load_cat_causa = CloudDataFusionStartPipelineOperator(
    task_id="load_cat_causa",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_cat_causa',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('CAT_CAUSA', size='XS'),
    dag=dag
  )

  load_cobranza = CloudDataFusionStartPipelineOperator(
    task_id="load_cobranza",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_cobranza',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('COBRANZA', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )

  load_cobranza_hist = CloudDataFusionStartPipelineOperator(
    task_id="load_cobranza_hist",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_cobranza_hist',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('COBRANZA_HIST',size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  
  load_sas_sinies = CloudDataFusionStartPipelineOperator(
    task_id="load_sas_sinies",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_sas_sinies',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('SAS_SINIES', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  
  
  load_etiqueta_siniestro = CloudDataFusionStartPipelineOperator(
    task_id="load_etiqueta_siniestro",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_etiqueta_siniestro',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('ETIQUETA_SINIESTRO', size='M', init_date=init_date, final_date=final_date),
    dag=dag
  )

  load_registro = CloudDataFusionStartPipelineOperator(
    task_id="load_registro",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_registro',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('REGISTRO',size='S', init_date=init_date, final_date=final_date),
    dag=dag
  )
  

@task_group(group_id='landing_sise',dag=dag)
def landing_sise():

  load_fraud_di = CloudDataFusionStartPipelineOperator(
    task_id="load_fraud_di",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_fraud_di',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('FRAUD_DI',size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )

  load_fraud_pv = CloudDataFusionStartPipelineOperator(
    task_id="load_fraud_pv",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_fraud_pv',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('FRAUD_PV',size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )

  load_fraud_rp = CloudDataFusionStartPipelineOperator(
    task_id="load_fraud_rp",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_fraud_rp',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('FRAUD_RP',size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )
  
  
  
@task_group(group_id='landing_dua',dag=dag)
def landing_dua():
  
  load_datos_dua = CloudDataFusionStartPipelineOperator(
    task_id="load_datos_dua",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_datos_dua',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('DATOS_DUA', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )
  
  
@task_group(group_id='landing_datos_generales',dag=dag)
def landing_datos_generales():
  
  load_datosgenerales = CloudDataFusionStartPipelineOperator(
    task_id="load_datosgenerales",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_datos_generales',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('DATOSGENERALES', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  

  
@task_group(group_id='end_landing',dag=dag)
def end_landing():
  
  delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=DATA_PROJECT_ID,
    cluster_name=DATA_DATAPROC_CLUSTER_NAME,
    region=DATA_PROJECT_REGION,
  )





@task_group(group_id='bq_elt',dag=dag)
def bq_elt():

  # ASEGURADO
  dm_asegurados = BigQueryInsertJobOperator(
    task_id="dm_asegurados",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ASEGURADOS/DM_ASEGURADOS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'MASEG_BSC',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_ASEGURADOS',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  # PAGOS_PROVEEDORES
  rtl_pagos_proveedores = BigQueryInsertJobOperator(
    task_id="rtl_pagos_proveedores",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PAGOS_PROVEEDORES/RTL_PAGOS_PROVEEDORES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'PAGOPROVE',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'DEST_TABLE_NAME': 'RTL_PAGOS_PROVEEDORES',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  dm_pagos_proveedores = BigQueryInsertJobOperator(
    task_id="dm_pagos_proveedores",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PAGOS_PROVEEDORES/DM_PAGOS_PROVEEDORES.sql'),
        "useLegacySql": False,
      },
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RTL_PAGOS_PROVEEDORES',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_PAGOS_PROVEEDORES',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  # PROVEEDORES
  
  stg_proveedores_1 = BigQueryInsertJobOperator(
    task_id="stg_proveedores_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PROVEEDORES/STG_PROVEEDORES_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'PRESTADORES',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_PROVEEDORES_1',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  stg_proveedores_2 = BigQueryInsertJobOperator(
    task_id="stg_proveedores_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PROVEEDORES/STG_PROVEEDORES_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_PROVEEDORES_1',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_PROVEEDORES_2',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  dm_proveedores = BigQueryInsertJobOperator(
    task_id="dm_proveedores",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PROVEEDORES/DM_PROVEEDORES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_PROVEEDORES_2',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_PROVEEDORES',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  rtl_coberturas_movimientos = BigQueryInsertJobOperator(
    task_id="rtl_coberturas_movimientos",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/COBERTURAS_MOVIMIENTOS/RTL_COBERTURAS_MOVIMIENTOS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RESERVAS_BSC',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'DEST_TABLE_NAME': 'RTL_COBERTURAS_MOVIMIENTOS'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  dm_coberturas_movimientos = BigQueryInsertJobOperator(
    task_id="dm_coberturas_movimientos",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/COBERTURAS_MOVIMIENTOS/DM_COBERTURAS_MOVIMIENTOS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RTL_COBERTURAS_MOVIMIENTOS',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_COBERTURAS_MOVIMIENTOS',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  dm_estados = BigQueryInsertJobOperator(
    task_id="dm_estados",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ESTADOS/DM_ESTADOS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'TESTADO_BSC',
      'SOURCE_SECOND_TABLE_NAME': 'ESTADOS_MEXICO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_ESTADOS',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  dm_oficinas = BigQueryInsertJobOperator(
    task_id="dm_oficinas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/OFICINAS/DM_OFICINAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'TSUC_BSC',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_OFICINAS',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  dm_tipos_proveedores = BigQueryInsertJobOperator(
    task_id="dm_tipos_proveedores",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TIPOS_PROVEEDORES/DM_TIPOS_PROVEEDORES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'TIPOPROVEEDOR',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_TIPOS_PROVEEDORES',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  dm_causas = BigQueryInsertJobOperator(
    task_id="dm_causas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/CAUSAS/DM_CAUSAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'CAT_CAUSA',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_CAUSAS',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  stg_etiqueta_siniestro_1 = BigQueryInsertJobOperator(
    task_id="stg_etiqueta_siniestro_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'ETIQUETA_SINIESTRO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_1',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  stg_etiqueta_siniestro_2 = BigQueryInsertJobOperator(
    task_id="stg_etiqueta_siniestro_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_1',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_2',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  stg_etiqueta_siniestro_3 = BigQueryInsertJobOperator(
    task_id="stg_etiqueta_siniestro_3",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_3.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_2',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_3',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )

  rtl_etiqueta_siniestro = BigQueryInsertJobOperator(
    task_id="rtl_etiqueta_siniestro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ETIQUETA_SINIESTRO/RTL_ETIQUETA_SINIESTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_3',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'DEST_TABLE_NAME': 'RTL_ETIQUETA_SINIESTRO',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )

  dm_etiqueta_siniestro = BigQueryInsertJobOperator(
    task_id="dm_etiqueta_siniestro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ETIQUETA_SINIESTRO/DM_ETIQUETA_SINIESTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RTL_ETIQUETA_SINIESTRO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_ETIQUETA_SINIESTRO',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )

  stg_registro = BigQueryInsertJobOperator(
    task_id="stg_registro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/REGISTRO/STG_REGISTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'REGISTRO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_REGISTRO',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  rtl_registro = BigQueryInsertJobOperator(
    task_id="rtl_registro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/REGISTRO/RTL_REGISTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_REGISTRO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'DEST_TABLE_NAME': 'RTL_REGISTRO',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  dm_registro = BigQueryInsertJobOperator(
    task_id="dm_registro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/REGISTRO/DM_REGISTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RTL_REGISTRO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_REGISTRO',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  stg_siniestros = BigQueryInsertJobOperator(
    task_id="stg_siniestros",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/SINIESTROS/STG_SINIESTROS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'SAS_SINIES',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_SINIESTROS'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  rtl_siniestros = BigQueryInsertJobOperator(
    task_id="rtl_siniestros",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/SINIESTROS/RTL_SINIESTROS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'DEST_TABLE_NAME': 'RTL_SINIESTROS'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  dm_siniestros = BigQueryInsertJobOperator(
    task_id="dm_siniestros",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/SINIESTROS/DM_SINIESTROS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RTL_SINIESTROS',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_SINIESTROS',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  stg_dua = BigQueryInsertJobOperator(
    task_id="stg_dua",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/DUA/STG_DUA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'DATOS_DUA',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_DUA',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  rtl_dua = BigQueryInsertJobOperator(
    task_id="rtl_dua",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/DUA/RTL_DUA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_DUA',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'DEST_TABLE_NAME': 'RTL_DUA',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  dm_dua = BigQueryInsertJobOperator(
    task_id="dm_dua",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/DUA/DM_DUA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RTL_DUA',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_DUA',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  stg_polizas_vigentes_1 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'FRAUD_PV',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_POLIZAS_VIGENTES_1',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  stg_polizas_vigentes_2 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_POLIZAS_VIGENTES_1',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_POLIZAS_VIGENTES_2',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  stg_polizas_vigentes_3 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_3",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_3.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_POLIZAS_VIGENTES_2',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_POLIZAS_VIGENTES_3',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  stg_polizas_vigentes_4 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_4",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_4.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_POLIZAS_VIGENTES_3',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_POLIZAS_VIGENTES_4',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  rtl_polizas_vigentes = BigQueryInsertJobOperator(
    task_id="rtl_polizas_vigentes",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/POLIZAS_VIGENTES/RTL_POLIZAS_VIGENTES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_POLIZAS_VIGENTES_4',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'DEST_TABLE_NAME': 'RTL_POLIZAS_VIGENTES',
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  dm_polizas_vigentes = BigQueryInsertJobOperator(
    task_id="dm_polizas_vigentes",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/POLIZAS_VIGENTES/DM_POLIZAS_VIGENTES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RTL_POLIZAS_VIGENTES',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_POLIZAS_VIGENTES',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )  

  stg_pagos_polizas = BigQueryInsertJobOperator(
    task_id="stg_pagos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PAGOS_POLIZAS/STG_PAGOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'FRAUD_RP',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_PAGOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  rtl_pagos_polizas = BigQueryInsertJobOperator(
    task_id="rtl_pagos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PAGOS_POLIZAS/RTL_PAGOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_PAGOS_POLIZAS',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'DEST_TABLE_NAME': 'RTL_PAGOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  dm_pagos_polizas = BigQueryInsertJobOperator(
    task_id="dm_pagos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PAGOS_POLIZAS/DM_PAGOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RTL_PAGOS_POLIZAS',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_PAGOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  stg_incisos_polizas_1 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/INCISOS_POLIZAS/STG_INCISOS_POLIZAS_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'FRAUD_DI',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_INCISOS_POLIZAS_1',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  stg_incisos_polizas_2 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/INCISOS_POLIZAS/STG_INCISOS_POLIZAS_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_INCISOS_POLIZAS_1',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_INCISOS_POLIZAS_2',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  stg_incisos_polizas_3 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_3",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/INCISOS_POLIZAS/STG_INCISOS_POLIZAS_3.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_INCISOS_POLIZAS_2',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_INCISOS_POLIZAS_3',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  stg_incisos_polizas_4 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_4",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/INCISOS_POLIZAS/STG_INCISOS_POLIZAS_4.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_INCISOS_POLIZAS_3',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_INCISOS_POLIZAS_4',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  rtl_incisos_polizas = BigQueryInsertJobOperator(
    task_id="rtl_incisos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/INCISOS_POLIZAS/RTL_INCISOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_INCISOS_POLIZAS_4',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'DEST_TABLE_NAME': 'RTL_INCISOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  dm_incisos_polizas = BigQueryInsertJobOperator(
    task_id="dm_incisos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/INCISOS_POLIZAS/DM_INCISOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RTL_INCISOS_POLIZAS',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_INCISOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  stg_valuaciones_1 = BigQueryInsertJobOperator(
    task_id="stg_valuaciones_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/VALUACIONES/STG_VALUACIONES_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'VALUACION_BSC',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_VALUACIONES_1',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  stg_valuaciones_2 = BigQueryInsertJobOperator(
    task_id="stg_valuaciones_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/VALUACIONES/STG_VALUACIONES_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_VALUACIONES_1',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_VALUACIONES_2',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )  

  rtl_valuaciones = BigQueryInsertJobOperator(
    task_id="rtl_valuaciones",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/VALUACIONES/RTL_VALUACIONES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_VALUACIONES_2',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'DEST_TABLE_NAME': 'RTL_VALUACIONES',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_valuaciones = BigQueryInsertJobOperator(
    task_id="dm_valuaciones",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/VALUACIONES/DM_VALUACIONES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RTL_VALUACIONES',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_VALUACIONES',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_datos_generales = BigQueryInsertJobOperator(
    task_id="dm_datos_generales",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/DATOS_GENERALES/DM_DATOS_GENERALES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'DATOSGENERALES',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_DATOS_GENERALES'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  stg_apercab_1 = BigQueryInsertJobOperator(
    task_id="stg_apercab_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/APERCAB/STG_APERCAB_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'APERCAB_BSC',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_APERCAB_1'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  stg_apercab_2 = BigQueryInsertJobOperator(
    task_id="stg_apercab_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/APERCAB/STG_APERCAB_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_APERCAB_1',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_APERCAB_2'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  rtl_apercab = BigQueryInsertJobOperator(
    task_id="rtl_apercab",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/APERCAB/RTL_APERCAB.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_APERCAB_2',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'DEST_TABLE_NAME': 'RTL_APERCAB'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_apercab = BigQueryInsertJobOperator(
    task_id="dm_apercab",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/APERCAB/DM_APERCAB.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_RTL_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RTL_APERCAB',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_APERCAB',
      'init_date':init_date,
      'final_date':final_date
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )



  rtl_pagos_proveedores  >> dm_pagos_proveedores
  rtl_coberturas_movimientos >> dm_coberturas_movimientos
  stg_etiqueta_siniestro_1 >> stg_etiqueta_siniestro_2 >> stg_etiqueta_siniestro_3 >> rtl_etiqueta_siniestro >> dm_etiqueta_siniestro
  stg_registro >> rtl_registro >> dm_registro
  stg_siniestros >> rtl_siniestros >> dm_siniestros
  stg_dua >> rtl_dua >> dm_dua
  stg_polizas_vigentes_1 >> stg_polizas_vigentes_2 >> stg_polizas_vigentes_3 >> stg_polizas_vigentes_4 >> rtl_polizas_vigentes >> dm_polizas_vigentes
  stg_pagos_polizas >> rtl_pagos_polizas >> dm_pagos_polizas
  stg_incisos_polizas_1 >> stg_incisos_polizas_2 >> stg_incisos_polizas_3 >> stg_incisos_polizas_4 >> rtl_incisos_polizas >> dm_incisos_polizas
  stg_valuaciones_1 >> stg_valuaciones_2 >> rtl_valuaciones >> dm_valuaciones
  stg_proveedores_1 >> stg_proveedores_2 >> dm_proveedores
  stg_apercab_1 >> stg_apercab_2 >> rtl_apercab >> dm_apercab



@task_group(group_id='recreate_cluster',dag=dag)
def recreate_cluster():

  
  select_cluster_creator = BranchPythonOperator(
    task_id="select_cluster_creator",
    python_callable=get_cluster_tipe_creator,
    op_kwargs={
      'init_date':init_date,
      'final_date':final_date,
      'small_cluster_label': 'recreate_cluster.create_small_cluster',
      'big_cluster_label': 'recreate_cluster.create_big_cluster'
    },
    provide_context=True,
    dag=dag
  )  
  
  create_big_cluster = DataprocCreateClusterOperator(
    task_id="create_big_cluster",
    project_id=DATA_PROJECT_ID,
    cluster_config=VERIFICACIONES_DATAPROC_BIG_CLUSTER_CONFIG,
    region=DATA_PROJECT_REGION,
    cluster_name=DATA_DATAPROC_CLUSTER_NAME,
    num_retries_if_resource_is_not_ready=3,
    dag=dag
  )
  
  create_small_cluster = DataprocCreateClusterOperator(
    task_id="create_small_cluster",
    project_id=DATA_PROJECT_ID,
    cluster_config=VERIFICACIONES_DATAPROC_SMALL_CLUSTER_CONFIG,
    region=DATA_PROJECT_REGION,
    cluster_name=DATA_DATAPROC_CLUSTER_NAME,
    num_retries_if_resource_is_not_ready=3,
    dag=dag
  )
  
  get_datafusion_instance = CloudDataFusionGetInstanceOperator(
    task_id="get_datafusion_instance",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    trigger_rule='one_success',
    project_id=DATA_PROJECT_ID,
    dag=dag,
  )
  
  select_cluster_creator >> [create_big_cluster,create_small_cluster] >> get_datafusion_instance
  
@task_group(group_id='injection',dag=dag)
def injection():
  inject_dm_asegurados = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_asegurados",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_asegurados',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_ASEGURADOS", "STG_ASEGURADOS", "DM_ASEGURADOS", "M"),
    dag=dag
  )

  inject_dm_coberturas_movimientos = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_coberturas_movimientos",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_coberturas_movimientos',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_COBERTURAS_MOVIMIENTOS", "STG_COBERTURAS_MOVIMIENTOS", "DM_COBERTURAS_MOVIMIENTOS", "L", init_date=init_date, final_date=final_date),
    dag=dag
  )

  inject_dm_estados = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_estados",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_estados',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_ESTADOS", "STG_ESTADOS", "DM_ESTADOS", "XS"),
    dag=dag
  )
  
  inject_dm_pagos_proveedores = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_pagos_proveedores",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_pagos_proveedores',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_PAGOS_PROVEEDORES", "STG_PAGOS_PROVEEDORES", "DM_PAGOS_PROVEEDORES", "L", init_date=init_date, final_date=final_date),
    dag=dag
  )

  inject_dm_proveedores = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_proveedores",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_proveedores',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_PROVEEDORES", "STG_PROVEEDORES", "DM_PROVEEDORES", "L"),
    dag=dag
  )

  inject_dm_tipos_proveedores = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_tipos_proveedores",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_tipos_proveedores',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_TIPOS_PROVEEDORES", "STG_TIPOS_PROVEEDORES", "DM_TIPOS_PROVEEDORES", "XS"),
    dag=dag
  )
  
  inject_dm_causas = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_causas",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_causas',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_CAUSAS", "STG_CAUSAS", "DM_CAUSAS", "XS"),
    dag=dag
  )

  inject_dm_etiqueta_siniestro = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_etiqueta_siniestro",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_etiqueta_siniestro',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_ETIQUETA_SINIESTRO", "STG_ETIQUETA_SINIESTRO", "DM_ETIQUETA_SINIESTRO", "M", init_date=init_date, final_date=final_date),
    dag=dag
  )

  inject_dm_registro = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_registro",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_registro',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_REGISTRO", "STG_REGISTRO", "DM_REGISTRO", "S", init_date=init_date, final_date=final_date),
    dag=dag
  )

  inject_dm_dua = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_dua",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_dua',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_DUA", "STG_DUA", "DM_DUA", "L", init_date=init_date, final_date=final_date),
    dag=dag
  )

  inject_dm_oficinas = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_oficinas",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_oficinas',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_OFICINAS", "STG_OFICINAS", "DM_OFICINAS", "M"),
    dag=dag
  )
  
  inject_dm_polizas_vigentes = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_polizas_vigentes",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_polizas_vigentes',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_POLIZAS_VIGENTES", "STG_POLIZAS_VIGENTES", "DM_POLIZAS_VIGENTES", "L", init_date=init_date, final_date=final_date),
    dag=dag
  )
  
  inject_dm_pagos_polizas = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_pagos_polizas",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_pagos_polizas',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_PAGOS_POLIZAS", "STG_PAGOS_POLIZAS", "DM_PAGOS_POLIZAS", "L", init_date=init_date, final_date=final_date),
    dag=dag
  )
  
  inject_dm_incisos_polizas = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_incisos_polizas",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_incisos_polizas',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_INCISOS_POLIZAS", "STG_INCISOS_POLIZAS", "DM_INCISOS_POLIZAS", "L", init_date=init_date, final_date=final_date),
    dag=dag
  )
  
  inject_dm_valuaciones = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_valuaciones",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_valuaciones',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_VALUACIONES", "STG_VALUACIONES", "DM_VALUACIONES", "M", init_date=init_date, final_date=final_date),
    dag=dag
  )
  
  inject_dm_datos_generales = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_datos_generales",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_datos_generales',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_DATOS_GENERALES", "STG_DATOS_GENERALES", "DM_DATOS_GENERALES", "L", init_date=init_date, final_date=final_date),
    dag=dag
  )
  
  inject_dm_agentes = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_agentes",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_agentes',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_AGENTES", "STG_AGENTES", "DM_AGENTES", "XS"),
    dag=dag
  )
  
  inject_dm_gerentes = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_gerentes",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_gerentes',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_GERENTES", "STG_GERENTES", "DM_GERENTES", "XS"),
    dag=dag
  )
  
  inject_dm_apercab = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_apercab",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_apercab',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_APERCAB", "STG_APERCAB", "DM_APERCAB", "M", init_date=init_date, final_date=final_date ),
    dag=dag
  )
  
  #SINIESTROS ULTIMA INYECCION
  inject_dm_siniestros = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_siniestros",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_siniestros',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_SINIESTROS", "STG_SINIESTROS", "DM_SINIESTROS", "L", init_date=init_date, final_date=final_date ),
    dag=dag
  )
  
  # TODOS LOS INYECT APUNTAN A SINIESTROS PARA 
  [ 
   inject_dm_estados
   ,inject_dm_asegurados
   ,inject_dm_coberturas_movimientos
   ,inject_dm_pagos_proveedores
   ,inject_dm_proveedores
   ,inject_dm_tipos_proveedores
   ,inject_dm_causas
   ,inject_dm_etiqueta_siniestro
   ,inject_dm_registro
   ,inject_dm_dua
   ,inject_dm_oficinas
   ,inject_dm_polizas_vigentes
   ,inject_dm_pagos_polizas
   ,inject_dm_incisos_polizas
   ,inject_dm_valuaciones
   ,inject_dm_datos_generales
   ,inject_dm_agentes
   ,inject_dm_gerentes
   ,inject_dm_apercab
  ] >> inject_dm_siniestros
  
@task_group(group_id='end_injection',dag=dag)
def end_injection():
  
  delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=DATA_PROJECT_ID,
    cluster_name=DATA_DATAPROC_CLUSTER_NAME,
    region=DATA_PROJECT_REGION
  )
  
landing >> init_landing() >> [landing_bsc_siniestros(),landing_siniestros(),landing_sise(),landing_dua(),landing_datos_generales()] >> end_landing() >> bq_elt() >> recreate_cluster() >> injection() >> end_injection()