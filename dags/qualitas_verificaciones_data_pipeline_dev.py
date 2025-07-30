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

VERIFICACIONES_CONFIG_VARIABLES_DEV = Variable.get("VERIFICACIONES_CONFIG_VARIABLES_DEV", deserialize_json=True)

DATA_PROJECT_ID = VERIFICACIONES_CONFIG_VARIABLES_DEV['DATA_PROJECT_ID']
DATA_PROJECT_REGION = VERIFICACIONES_CONFIG_VARIABLES_DEV['DATA_PROJECT_REGION']
DATA_DATAFUSION_INSTANCE_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['DATA_DATAFUSION_INSTANCE_NAME']
DATA_DATAFUSION_TEMPORARY_BUCKET_NAME =  VERIFICACIONES_CONFIG_VARIABLES_DEV['DATA_DATAFUSION_TEMPORARY_BUCKET_NAME']
DATA_DATAFUSION_NAMESPACE = VERIFICACIONES_CONFIG_VARIABLES_DEV['DATA_DATAFUSION_NAMESPACE']
DATA_DATAPROC_CLUSTER_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['DATA_DATAPROC_CLUSTER_NAME']
DATA_DATAPROC_PROFILE_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['DATA_DATAPROC_PROFILE_NAME']
DATA_COMPOSER_WORKSPACE_BUCKET_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['DATA_COMPOSER_WORKSPACE_BUCKET_NAME']

VERIFICACIONES_PROJECT_ID = VERIFICACIONES_CONFIG_VARIABLES_DEV['VERIFICACIONES_PROJECT_ID']
VERIFICACIONES_PROJECT_REGION = VERIFICACIONES_CONFIG_VARIABLES_DEV['VERIFICACIONES_PROJECT_REGION']
VERIFICACIONES_LAN_DATASET_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['VERIFICACIONES_LAN_DATASET_NAME']
VERIFICACIONES_RTL_DATASET_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['VERIFICACIONES_RTL_DATASET_NAME']
VERIFICACIONES_STG_DATASET_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['VERIFICACIONES_STG_DATASET_NAME']
VERIFICACIONES_DM_DATASET_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['VERIFICACIONES_DM_DATASET_NAME']
VERIFICACIONES_SEED_DATASET_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['VERIFICACIONES_SEED_DATASET_NAME']
VERIFICACIONES_CONNECTION_DEFAULT = VERIFICACIONES_CONFIG_VARIABLES_DEV['VERIFICACIONES_CONNECTION_DEFAULT']
VERIFICACIONES_CONNECTION_DEFAULT = VERIFICACIONES_CONFIG_VARIABLES_DEV['VERIFICACIONES_CONNECTION_DEFAULT']

APP_ORACLE_HOST = VERIFICACIONES_CONFIG_VARIABLES_DEV['APP_ORACLE_HOST']
APP_ORACLE_SERVICE_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['APP_ORACLE_SERVICE_NAME']
APP_ORACLE_USER = VERIFICACIONES_CONFIG_VARIABLES_DEV['APP_ORACLE_USER']
APP_ORACLE_PASSWORD = VERIFICACIONES_CONFIG_VARIABLES_DEV['APP_ORACLE_PASSWORD']
APP_ORACLE_INJECT_SCHEMA_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['APP_ORACLE_INJECT_SCHEMA_NAME']
APP_ORACLE_INSUMOS_SCHEMA_NAME = VERIFICACIONES_CONFIG_VARIABLES_DEV['APP_ORACLE_INSUMOS_SCHEMA_NAME']

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
    'spark:spark.app.name': inject_table_name,
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
  'qualitas_verificaciones_data_pipeline_dev',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 15 * * *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=400),
  tags=['MX','AUTOS','VERIFICACIONES','INSUMOS']
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

   
  load_tcober_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_tcober_bsc",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_tcober_bsc',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('TCOBER_BSC', size='XS'),
    dag=dag
  )  
  
  load_orden_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_orden_bsc",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_orden_bsc',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('ORDEN_BSC', size='S',init_date=init_date, final_date=final_date),
    dag=dag
  )  
  
  
  load_pagosauditoria_sise = CloudDataFusionStartPipelineOperator(
    task_id="load_pagosauditoria_sise",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_pagosauditoria_sise',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('PAGOSAUDITORIA_SISE', size='M',init_date=init_date, final_date=final_date),
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
  

@task_group(group_id='landing_valuaciones',dag=dag)
def landing_valuaciones():
  
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

  load_analistacdr = CloudDataFusionStartPipelineOperator(
    task_id="load_analistacdr",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_analistacdr',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('ANALISTACDR', size='XS'),
    dag=dag
  )  
  
  load_causacambiovale = CloudDataFusionStartPipelineOperator(
    task_id="load_causacambiovale",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_causacambiovale',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('CAUSACAMBIOVALE', size='XS'),
    dag=dag
  )  
  
  load_cerco = CloudDataFusionStartPipelineOperator(
    task_id="load_cerco",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_cerco',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('CERCO', size='XS'),
    dag=dag
  )  
  
  load_color = CloudDataFusionStartPipelineOperator(
    task_id="load_color",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_color',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('COLOR', size='XS'),
    dag=dag
  )  
  
  load_complemento = CloudDataFusionStartPipelineOperator(
    task_id="load_complemento",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_complemento',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('COMPLEMENTO', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  
  
  load_datosvehiculo = CloudDataFusionStartPipelineOperator(
    task_id="load_datosvehiculo",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_datosvehiculo',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('DATOSVEHICULO', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  
  
  load_enviohistorico = CloudDataFusionStartPipelineOperator(
    task_id="load_enviohistorico",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_enviohistorico',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('ENVIOHISTORICO', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  

  load_estado = CloudDataFusionStartPipelineOperator(
    task_id="load_estado",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_estado',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('ESTADO', size='XS'),
    dag=dag
  )  
  
  load_estatus = CloudDataFusionStartPipelineOperator(
    task_id="load_estatus",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_estatus',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('ESTATUS', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  
  
  load_estatusexpedientes = CloudDataFusionStartPipelineOperator(
    task_id="load_estatusexpedientes",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_estatusexpedientes',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('ESTATUSEXPEDIENTES', size='XS'),
    dag=dag
  )  
  
  load_fechas = CloudDataFusionStartPipelineOperator(
    task_id="load_fechas",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_fechas',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('FECHAS', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  
  
  load_historicoterminoentrega = CloudDataFusionStartPipelineOperator(
    task_id="load_historicoterminoentrega",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_historicoterminoentrega',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('HISTORICOTERMINOENTREGA', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  

  load_marca = CloudDataFusionStartPipelineOperator(
    task_id="load_marca",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_marca',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('MARCA', size='XS'),
    dag=dag
  )  
  
  load_proveedor = CloudDataFusionStartPipelineOperator(
    task_id="load_proveedor",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_proveedor',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('PROVEEDOR', size='S'),
    dag=dag
  )  
  
  load_refaccion = CloudDataFusionStartPipelineOperator(
    task_id="load_refaccion",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_refaccion',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('REFACCION', size='XS'),
    dag=dag
  )  
  
  load_supervisorintegral = CloudDataFusionStartPipelineOperator(
    task_id="load_supervisorintegral",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_supervisorintegral',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('SUPERVISORINTEGRAL', size='XS'),
    dag=dag
  )  
  
  load_talleres = CloudDataFusionStartPipelineOperator(
    task_id="load_talleres",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_talleres',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('TALLERES', size='XS'),
    dag=dag
  )  

  load_vale = CloudDataFusionStartPipelineOperator(
    task_id="load_vale",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_vale',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('VALE', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  
  
  load_valehistorico = CloudDataFusionStartPipelineOperator(
    task_id="load_valehistorico",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_valehistorico',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('VALEHISTORICO', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  
  
  load_valuacion = CloudDataFusionStartPipelineOperator(
    task_id="load_valuacion",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='load_valuacion',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('VALUACION', size='XS'),
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
      'SOURCE_SECOND_TABLE_NAME': 'PAGOSPROVEEDORES',
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



  dm_analista_cdr = BigQueryInsertJobOperator(
    task_id="dm_analista_cdr",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ANALISTA_CDR/DM_ANALISTA_CDR.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'ANALISTACDR',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_ANALISTA_CDR'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )


  dm_causa_cambio_vale = BigQueryInsertJobOperator(
    task_id="dm_causa_cambio_vale",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/CAUSA_CAMBIO_VALE/DM_CAUSA_CAMBIO_VALE.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'CAUSACAMBIOVALE',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_CAUSA_CAMBIO_VALE'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  dm_cerco= BigQueryInsertJobOperator(
    task_id="dm_cerco",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/CERCO/DM_CERCO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'CERCO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_CERCO'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_color= BigQueryInsertJobOperator(
    task_id="dm_color",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/COLOR/DM_COLOR.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'COLOR',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_COLOR'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_complemento= BigQueryInsertJobOperator(
    task_id="dm_complemento",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/COMPLEMENTO/DM_COMPLEMENTO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'COMPLEMENTO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_COMPLEMENTO'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_datos_vehiculo= BigQueryInsertJobOperator(
    task_id="dm_datos_vehiculo",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/DATOS_VEHICULO/DM_DATOS_VEHICULO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'DATOSVEHICULO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_DATOS_VEHICULO'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_envio_historico= BigQueryInsertJobOperator(
    task_id="dm_envio_historico",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ENVIO_HISTORICO/DM_ENVIO_HISTORICO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'ENVIOHISTORICO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_ENVIO_HISTORICO'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_estado= BigQueryInsertJobOperator(
    task_id="dm_estado",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ESTADO/DM_ESTADO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'ESTADO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_ESTADO'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_estatus= BigQueryInsertJobOperator(
    task_id="dm_estatus",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ESTATUS/DM_ESTATUS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'ESTATUS',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_ESTATUS'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_estatus_expedientes= BigQueryInsertJobOperator(
    task_id="dm_estatus_expedientes",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/ESTATUS_EXPEDIENTES/DM_ESTATUS_EXPEDIENTES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'ESTATUSEXPEDIENTES',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_ESTATUS_EXPEDIENTES'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_fechas= BigQueryInsertJobOperator(
    task_id="dm_fechas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/FECHAS/DM_FECHAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'FECHAS',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_FECHAS'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_historico_termino_entrega= BigQueryInsertJobOperator(
    task_id="dm_historico_termino_entrega",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/HISTORICO_TERMINO_ENTREGA/DM_HISTORICO_TERMINO_ENTREGA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'HISTORICOTERMINOENTREGA',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_HISTORICO_TERMINO_ENTREGA'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_marca= BigQueryInsertJobOperator(
    task_id="dm_marca",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/MARCA/DM_MARCA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'MARCA',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_MARCA'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_proveedor= BigQueryInsertJobOperator(
    task_id="dm_proveedor",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PROVEEDOR/DM_PROVEEDOR.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'PROVEEDOR',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_PROVEEDOR'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  
  stg_refaccion_1= BigQueryInsertJobOperator(
    task_id="stg_refaccion_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/REFACCION/STG_REFACCION_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'REFACCION',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_REFACCION_1'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  stg_refaccion_2= BigQueryInsertJobOperator(
    task_id="stg_refaccion_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/REFACCION/STG_REFACCION_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_REFACCION_1',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_REFACCION_2'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  dm_refacion= BigQueryInsertJobOperator(
    task_id="dm_refacion",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/REFACCION/DM_REFACCION.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_REFACCION_2',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_REFACCION'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_supervisor_integral= BigQueryInsertJobOperator(
    task_id="dm_supervisor_integral",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/SUPERVISOR_INTEGRAL/DM_SUPERVISOR_INTEGRAL.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'SUPERVISORINTEGRAL',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_SUPERVISOR_INTEGRAL'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  stg_talleres_1= BigQueryInsertJobOperator(
    task_id="stg_talleres_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TALLERES/STG_TALLERES_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'TALLERES',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_TALLERES_1'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  stg_talleres_2= BigQueryInsertJobOperator(
    task_id="stg_talleres_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TALLERES/STG_TALLERES_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_TALLERES_1',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'DEST_TABLE_NAME': 'STG_TALLERES_2'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_talleres= BigQueryInsertJobOperator(
    task_id="dm_talleres",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TALLERES/DM_TALLERES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_STG_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'STG_TALLERES_2',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_TALLERES'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_vale= BigQueryInsertJobOperator(
    task_id="dm_vale",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/VALE/DM_VALE.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'VALE',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_VALE'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  dm_vale_historico= BigQueryInsertJobOperator(
    task_id="dm_vale_historico",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/VALE_HISTORICO/DM_VALE_HISTORICO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'VALEHISTORICO',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_VALE_HISTORICO'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )
  
  
  dm_valuacion= BigQueryInsertJobOperator(
    task_id="dm_valuacion",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/VALUACION/DM_VALUACION.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'VALUACION',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_VALUACION'
    },
    location=VERIFICACIONES_PROJECT_REGION,
    gcp_conn_id=VERIFICACIONES_CONNECTION_DEFAULT,
    dag=dag 
  )

  dm_coberturas= BigQueryInsertJobOperator(
    task_id="dm_coberturas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/COBERTURAS/DM_COBERTURAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'SOURCE_DATASET_NAME': VERIFICACIONES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'TCOBER_BSC',
      'DEST_PROJECT_ID': VERIFICACIONES_PROJECT_ID,
      'DEST_DATASET_NAME': VERIFICACIONES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_COBERTURAS'
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

  stg_refaccion_1 >> stg_refaccion_2 >> dm_refacion
  stg_talleres_1 >> stg_talleres_2 >> dm_talleres



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
    pipeline_name='inject_dm_asegurados_dev',
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
    pipeline_name='inject_dm_coberturas_movimientos_dev',
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
    pipeline_name='inject_dm_estados_dev',
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
    pipeline_name='inject_dm_pagos_proveedores_dev',
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
    pipeline_name='inject_dm_proveedores_dev',
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
    pipeline_name='inject_dm_tipos_proveedores_dev',
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
    pipeline_name='inject_dm_causas_dev',
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
    pipeline_name='inject_dm_etiqueta_siniestro_dev',
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
    pipeline_name='inject_dm_registro_dev',
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
    pipeline_name='inject_dm_dua_dev',
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
    pipeline_name='inject_dm_oficinas_dev',
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
    pipeline_name='inject_dm_polizas_vigentes_dev',
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
    pipeline_name='inject_dm_pagos_polizas_dev',
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
    pipeline_name='inject_dm_incisos_polizas_dev',
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
    pipeline_name='inject_dm_valuaciones_dev',
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
    pipeline_name='inject_dm_datos_generales_dev',
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
    pipeline_name='inject_dm_agentes_dev',
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
    pipeline_name='inject_dm_gerentes_dev',
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
    pipeline_name='inject_dm_apercab_dev',
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
    pipeline_name='inject_dm_siniestros_dev',
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
  

 
  #INJECTS VALUACIONES

  '''  
  inject_dm_analista_cdr = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_analista_cdr",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_analista_cdr_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_ANALISTA_CDR", "STG_ANALISTA_CDR", "DM_ANALISTA_CDR", "XS"),
    dag=dag
  )
  
  inject_dm_causa_cambio_vale = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_causa_cambio_vale",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_causa_cambio_vale_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_CAUSA_CAMBIO_VALE", "STG_CAUSA_CAMBIO_VALE", "DM_CAUSA_CAMBIO_VALE", "XS"),
    dag=dag
  )
  
  inject_dm_cerco = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_cerco",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_cerco_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_CERCO", "STG_CERCO", "DM_CERCO", "XS"),
    dag=dag
  )
  
  inject_dm_color = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_color",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_color_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_COLOR", "STG_COLOR", "DM_COLOR", "XS"),
    dag=dag
  )
  
  inject_dm_complemento = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_complemento",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_complemento_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_COMPLEMENTO", "STG_COMPLEMENTO", "DM_COMPLEMENTO", "L"),
    dag=dag
  )
  '''

  inject_dm_datos_vehiculo = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_datos_vehiculo",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_datos_vehiculo_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_DATOS_VEHICULO", "STG_DATOS_VEHICULO", "DM_DATOS_VEHICULO", "L"),
    dag=dag
  )

  inject_dm_coberturas = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_coberturas",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_coberturas_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_COBERTURAS", "STG_COBERTURAS", "DM_COBERTURAS", "XS"),
    dag=dag
  )
  
  '''
  inject_dm_envio_historico = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_envio_historico",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_envio_historico_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_ENVIO_HISTORICO", "STG_ENVIO_HISTORICO", "DM_ENVIO_HISTORICO", "L"),
    dag=dag
  )
  
  
  inject_dm_estado = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_estado",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_estado_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_ESTADO", "STG_ESTADO", "DM_ESTADO", "XS"),
    dag=dag
  )
  
  inject_dm_estatus = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_estatus",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_estatus_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_ESTATUS", "STG_ESTATUS", "DM_ESTATUS", "L"),
    dag=dag
  )
  
  inject_dm_estatus_expendientes = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_estatus_expendientes",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_estatus_expendientes_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_ESTATUS_EXPEDIENTES", "STG_ESTATUS_EXPEDIENTES", "DM_ESTATUS_EXPEDIENTES", "XS"),
    dag=dag
  )
  
  inject_dm_fechas = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_fechas",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_fechas_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_FECHAS", "STG_FECHAS", "DM_FECHAS", "L"),
    dag=dag
  )
  
  inject_dm_historico_termino_entrega = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_historico_termino_entrega",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_historico_termino_entrega_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_HISTORICO_TERMINO_ENTREGA", "STG_HISTORICO_TERMINO_ENTREGA", "DM_HISTORICO_TERMINO_ENTREGA", "L"),
    dag=dag
  )
  
  inject_dm_marca = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_marca",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_marca_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_MARCA", "STG_MARCA", "DM_MARCA", "XS"),
    dag=dag
  )
  
  inject_dm_proveedor = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_proveedor",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_proveedor_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_PROVEEDOR", "STG_PROVEEDOR", "DM_PROVEEDOR", "S"),
    dag=dag
  )
  
  inject_dm_refaccion = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_refaccion",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_refaccion_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_REFACCION", "STG_REFACCION", "DM_REFACCION", "S"),
    dag=dag
  )
  
  inject_dm_supervisor_integral = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_supervisor_integral",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_supervisor_integral_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_SUPERVISOR_INTEGRAL", "STG_SUPERVISOR_INTEGRAL", "DM_SUPERVISOR_INTEGRAL", "XS"),
    dag=dag
  )
  
  inject_dm_talleres = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_talleres",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_talleres_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_TALLERES", "STG_TALLERES", "DM_TALLERES", "S"),
    dag=dag
  )
  
  
  inject_dm_vale = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_vale",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_vale_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_VALE", "STG_VALE", "DM_VALE", "L"),
    dag=dag
  )
  
  inject_dm_vale_historico = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_vale_historico",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_vale_historico_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_VALE_HISTORICO", "STG_VALE_HISTORICO", "DM_VALE_HISTORICO", "L"),
    dag=dag
  )
  
  inject_dm_valuacion = CloudDataFusionStartPipelineOperator(
    task_id="inject_dm_valuacion",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='inject_dm_valuacion_dev',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_inject_runtime_args("DM_VALUACION", "STG_VALUACION", "DM_VALUACION", "XS"),
    dag=dag
  )
  '''
  
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
#   ,inject_dm_analista_cdr
#   ,inject_dm_causa_cambio_vale
#   ,inject_dm_cerco
#   ,inject_dm_color
#   ,inject_dm_complemento
   ,inject_dm_datos_vehiculo
#   ,inject_dm_envio_historico
#   ,inject_dm_estado
#   ,inject_dm_estatus
#   ,inject_dm_estatus_expendientes
#   ,inject_dm_fechas
#   ,inject_dm_historico_termino_entrega
#   ,inject_dm_marca
#   ,inject_dm_proveedor
#   ,inject_dm_refaccion
#   ,inject_dm_supervisor_integral
#   ,inject_dm_talleres
#   ,inject_dm_vale
#   ,inject_dm_vale_historico
#   ,inject_dm_valuacion
   ,inject_dm_coberturas
  ] >> inject_dm_siniestros
  
@task_group(group_id='end_injection',dag=dag)
def end_injection():
  
  delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=DATA_PROJECT_ID,
    cluster_name=DATA_DATAPROC_CLUSTER_NAME,
    region=DATA_PROJECT_REGION
  )
  
landing >> init_landing() >> [landing_bsc_siniestros(),landing_siniestros(),landing_sise(),landing_dua(),landing_valuaciones()] >> end_landing() >> bq_elt() >> recreate_cluster() >> injection() >> end_injection()