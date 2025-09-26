import json
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

from lib.utils import get_bucket_file_contents,get_date_interval,get_cluster_tipe_creator,merge_storage_csv,upload_storage_csv_to_bigquery
from lib.utils import claves_ctas_especiales_to_csv,catalogo_direccion_comercial_to_csv,rechazos_to_csv

PREVENCION_FRAUDES_CONFIG_VARIABLES = Variable.get("PREVENCION_FRAUDES_CONFIG_VARIABLES", deserialize_json=True)

DATA_PROJECT_ID = PREVENCION_FRAUDES_CONFIG_VARIABLES['DATA_PROJECT_ID']
DATA_PROJECT_REGION = PREVENCION_FRAUDES_CONFIG_VARIABLES['DATA_PROJECT_REGION']
DATA_DATAFUSION_INSTANCE_NAME = PREVENCION_FRAUDES_CONFIG_VARIABLES['DATA_DATAFUSION_INSTANCE_NAME']
DATA_DATAFUSION_TEMPORARY_BUCKET_NAME =  PREVENCION_FRAUDES_CONFIG_VARIABLES['DATA_DATAFUSION_TEMPORARY_BUCKET_NAME']
DATA_DATAFUSION_NAMESPACE = PREVENCION_FRAUDES_CONFIG_VARIABLES['DATA_DATAFUSION_NAMESPACE']
DATA_DATAPROC_CLUSTER_NAME = PREVENCION_FRAUDES_CONFIG_VARIABLES['DATA_DATAPROC_CLUSTER_NAME']
DATA_DATAPROC_PROFILE_NAME = PREVENCION_FRAUDES_CONFIG_VARIABLES['DATA_DATAPROC_PROFILE_NAME']
DATA_COMPOSER_WORKSPACE_BUCKET_NAME = PREVENCION_FRAUDES_CONFIG_VARIABLES['DATA_COMPOSER_WORKSPACE_BUCKET_NAME']

PREVENCION_FRAUDES_BRO_PROJECT_ID = PREVENCION_FRAUDES_CONFIG_VARIABLES['PREVENCION_FRAUDES_BRO_PROJECT_ID']
PREVENCION_FRAUDES_BRO_BUCKET_NAME = PREVENCION_FRAUDES_CONFIG_VARIABLES['PREVENCION_FRAUDES_BRO_BUCKET_NAME']
PREVENCION_FRAUDES_PLA_PROJECT_ID = PREVENCION_FRAUDES_CONFIG_VARIABLES['PREVENCION_FRAUDES_PLA_PROJECT_ID']
PREVENCION_FRAUDES_ORO_PROJECT_ID = PREVENCION_FRAUDES_CONFIG_VARIABLES['PREVENCION_FRAUDES_ORO_PROJECT_ID']
PREVENCION_FRAUDES_PROJECT_REGION = PREVENCION_FRAUDES_CONFIG_VARIABLES['PREVENCION_FRAUDES_PROJECT_REGION']
PREVENCION_FRAUDES_BRO_DATASET_NAME = PREVENCION_FRAUDES_CONFIG_VARIABLES['PREVENCION_FRAUDES_BRO_DATASET_NAME']
PREVENCION_FRAUDES_PLA_DATASET_NAME = PREVENCION_FRAUDES_CONFIG_VARIABLES['PREVENCION_FRAUDES_PLA_DATASET_NAME']
PREVENCION_FRAUDES_ORO_DATASET_NAME = PREVENCION_FRAUDES_CONFIG_VARIABLES['PREVENCION_FRAUDES_ORO_DATASET_NAME']
PREVENCION_FRAUDES_CONNECTION_DEFAULT = PREVENCION_FRAUDES_CONFIG_VARIABLES['PREVENCION_FRAUDES_CONNECTION_DEFAULT']

PREVENCION_FRAUDES_DATA_PIPELINE_SCHEDULE_INTERVAL = PREVENCION_FRAUDES_CONFIG_VARIABLES['PREVENCION_FRAUDES_DATA_PIPELINE_SCHEDULE_INTERVAL']

PREVENCION_FRAUDES_DATAPROC_BIG_CLUSTER_CONFIG = Variable.get("PREVENCION_FRAUDES_DATAPROC_BIG_CLUSTER_CONFIG", deserialize_json=True)
PREVENCION_FRAUDES_DATAPROC_SMALL_CLUSTER_CONFIG = Variable.get("PREVENCION_FRAUDES_DATAPROC_SMALL_CLUSTER_CONFIG", deserialize_json=True)
PREVENCION_FRAUDES_LOAD_INTERVAL = Variable.get("VERIFICACIONES_LOAD_INTERVAL", default_var="YESTERDAY")

interval = get_date_interval(project_id='qlts-dev-mx-au-oro-verificacio',dataset='qlts_oro_op_verificaciones_dev',table='TAB_CALENDARIO',period=PREVENCION_FRAUDES_LOAD_INTERVAL)

init_date = interval['init_date']
final_date = interval['final_date']

def get_datafusion_load_runtime_args(table_name:str,size:str,init_date=None, final_date=None):
  
  base_args = {
    'app.pipeline.overwriteConfig': 'true',
    'task.executor.system.resources.cores': '2',
    'task.executor.system.resources.memory': '8192',
    'dataproc.cluster.name': DATA_DATAPROC_CLUSTER_NAME,
    'system.profile.name': DATA_DATAPROC_PROFILE_NAME,
    'TEMPORARY_BUCKET_NAME': DATA_DATAFUSION_TEMPORARY_BUCKET_NAME,
    'DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
    'TABLE_NAME': table_name,  
  }
  
  if size == 'XS':
    base_args.update({
      'task.executor.system.resources.cores': '1',
      'task.executor.system.resources.memory': '2048'
    })
  elif size == 'S':
    base_args.update({
      'task.executor.system.resources.cores': '2',
      'task.executor.system.resources.memory': '4096'
    })
  elif size == 'M':
    base_args.update({
      'task.executor.system.resources.cores': '2',
      'task.executor.system.resources.memory': '8192'
    })
  elif size == 'L':
    base_args.update({
      'task.executor.system.resources.cores': '2',
      'task.executor.system.resources.memory': '16384',
      'spark.sql.adaptive.enabled': 'true',
      'spark.sql.adaptive.skewJoin.enabled': 'true',
      'spark.sql.adaptive.skewJoin.skewedPartitionFactor': '5',
      'spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes': '268435456',
      'spark.dynamicAllocation.enabled': 'true',
      'spark.shuffle.service.enabled': 'true',
      'spark.dynamicAllocation.minExecutors': '4',
      'spark.dynamicAllocation.maxExecutors': '50',
      'spark.dynamicAllocation.initialExecutors': '2',
      'spark.dynamicAllocation.executorIdleTimeout': '60s',
      'spark.dynamicAllocation.schedulerBacklogTimeout': '1s'
    })
    
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
  'qualitas_prevencion_fraudes_data_pipeline',
  default_args=default_args,
  description='DAG con operadores load únicos del segundo DAG',
  schedule_interval='0 0 1 1 *',
  max_active_runs=1,
  catchup=False,
  dagrun_timeout=timedelta(minutes=400),
  tags=['MX','AUTOS','PREVENCION_FRAUDES']
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
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    dag=dag 
  )

  create_big_cluster = DataprocCreateClusterOperator(
    task_id="create_big_cluster",
    project_id=DATA_PROJECT_ID,
    cluster_config=PREVENCION_FRAUDES_DATAPROC_BIG_CLUSTER_CONFIG,
    region=DATA_PROJECT_REGION,
    cluster_name=DATA_DATAPROC_CLUSTER_NAME,
    num_retries_if_resource_is_not_ready=3,
    dag=dag
  )
  
  get_datafusion_instance = CloudDataFusionGetInstanceOperator(
    task_id="get_datafusion_instance",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    project_id=DATA_PROJECT_ID,
    dag=dag,
  )
  
  validate_date_interval >> create_big_cluster >> get_datafusion_instance
  
  
@task_group(group_id='load_files',dag=dag)
def load_files():

  merge_control_de_agentes = PythonOperator(
    task_id='merge_control_de_agentes',
    python_callable=merge_storage_csv,
    op_kwargs={
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'CONTROL_DE_AGENTES/',
      'folder_his': 'CONTROL_DE_AGENTES_HIS/',
      'destination_blob_name': 'CONTROL_DE_AGENTES_HIS.csv',
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'encoding': 'iso-8859-1'
    },
    dag=dag
  )

  load_control_de_agentes = PythonOperator(
    task_id='load_control_de_agentes',
    python_callable=upload_storage_csv_to_bigquery,
    op_kwargs={
      'gcs_uri': f'gs://{PREVENCION_FRAUDES_BRO_BUCKET_NAME}/CONTROL_DE_AGENTES_HIS/CONTROL_DE_AGENTES_HIS.csv',
      'dataset': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'table': 'CONTROL_DE_AGENTES',
      'schema_fields': json.loads(get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/schemas/files.control_de_agentes.json')),
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
    },
    dag=dag
  )

  merge_apertura_reporte = PythonOperator(
    task_id='merge_apertura_reporte',
    python_callable=merge_storage_csv,
    op_kwargs={
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'APERTURA_REPORTE/',
      'folder_his': 'APERTURA_REPORTE_HIS/',
      'destination_blob_name': 'APERTURA_REPORTE_HIS.csv',
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'encoding': 'iso-8859-1'
    },
    dag=dag
  )

  load_apertura_reporte = PythonOperator(
    task_id='load_apertura_reporte',
    python_callable=upload_storage_csv_to_bigquery,
    op_kwargs={
      'gcs_uri': f'gs://{PREVENCION_FRAUDES_BRO_BUCKET_NAME}/APERTURA_REPORTE_HIS/APERTURA_REPORTE_HIS.csv',
      'dataset': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'table': 'APERTURA_REPORTE',
      'schema_fields': json.loads(get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/schemas/files.apertura_reporte.json')),
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
    },
    dag=dag
  )

  merge_produccion1 = PythonOperator(
    task_id='merge_produccion1',
    python_callable=merge_storage_csv,
    op_kwargs={
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'PRODUCCION1/',
      'folder_his': 'PRODUCCION1_HIS/',
      'destination_blob_name': 'PRODUCCION1_HIS.csv',
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'encoding': 'iso-8859-1'
    },
    dag=dag
  )

  load_produccion1 = PythonOperator(
    task_id='load_produccion1',
    python_callable=upload_storage_csv_to_bigquery,
    op_kwargs={
      'gcs_uri': f'gs://{PREVENCION_FRAUDES_BRO_BUCKET_NAME}/PRODUCCION1_HIS/PRODUCCION1_HIS.csv',
      'dataset': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'table': 'PRODUCCION1',
      'schema_fields': json.loads(get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/schemas/files.produccion1.json')),
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
    },
    dag=dag
  )

  merge_produccion2 = PythonOperator(
    task_id='merge_produccion2',
    python_callable=merge_storage_csv,
    op_kwargs={
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'PRODUCCION2/',
      'folder_his': 'PRODUCCION2_HIS/',
      'destination_blob_name': 'PRODUCCION2_HIS.csv',
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'encoding': 'iso-8859-1'
    },
    dag=dag
  )

  load_produccion2 = PythonOperator(
    task_id='load_produccion2',
    python_callable=upload_storage_csv_to_bigquery,
    op_kwargs={
      'gcs_uri': f'gs://{PREVENCION_FRAUDES_BRO_BUCKET_NAME}/PRODUCCION2_HIS/PRODUCCION2_HIS.csv',
      'dataset': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'table': 'PRODUCCION2',
      'schema_fields': json.loads(get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/schemas/files.produccion2.json')),
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
    },
    dag=dag
  )

  merge_recuperaciones = PythonOperator(
    task_id='merge_recuperaciones',
    python_callable=merge_storage_csv,
    op_kwargs={
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'RECUPERACIONES/',
      'folder_his': 'RECUPERACIONES_HIS/',
      'destination_blob_name': 'RECUPERACIONES_HIS.csv',
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'encoding': 'iso-8859-1'
    },
    dag=dag
  )

  load_recuperaciones = PythonOperator(
    task_id='load_recuperaciones',
    python_callable=upload_storage_csv_to_bigquery,
    op_kwargs={
      'gcs_uri': f'gs://{PREVENCION_FRAUDES_BRO_BUCKET_NAME}/RECUPERACIONES_HIS/RECUPERACIONES_HIS.csv',
      'dataset': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'table': 'RECUPERACIONES',
      'schema_fields': json.loads(get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/schemas/files.recuperaciones.json')),
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
    },
    dag=dag
  )

  merge_sumas_aseg = PythonOperator(
    task_id='merge_sumas_aseg',
    python_callable=merge_storage_csv,
    op_kwargs={
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'SUMAS_ASEG/',
      'folder_his': 'SUMAS_ASEG_HIS/',
      'destination_blob_name': 'SUMAS_ASEG_HIS.csv',
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'encoding': 'iso-8859-1'
    },
    dag=dag
  )

  load_sumas_aseg = PythonOperator(
    task_id='load_sumas_aseg',
    python_callable=upload_storage_csv_to_bigquery,
    op_kwargs={
      'gcs_uri': f'gs://{PREVENCION_FRAUDES_BRO_BUCKET_NAME}/SUMAS_ASEG_HIS/SUMAS_ASEG_HIS.csv',
      'dataset': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'table': 'SUMAS_ASEG',
      'schema_fields': json.loads(get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/schemas/files.sumas_aseg.json')),
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
    },
    dag=dag
  )

  claves_ctas_especiales_excel_to_csv = PythonOperator(
    task_id='claves_ctas_especiales_excel_to_csv',
    python_callable=claves_ctas_especiales_to_csv,
    op_kwargs={
      'project_id':PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'CLAVES_CTAS_ESPECIALES_EXCEL',
      'file': 'CLAVES_CTAS_ESPECIALES 3.xlsx',
      'dest_folder': 'CLAVES_CTAS_ESPECIALES',
      'dest_file': 'CLAVES_CTAS_ESPECIALES.csv',
    },
    dag=dag
  )

  merge_claves_ctas_especiales = PythonOperator(
    task_id='merge_claves_ctas_especiales',
    python_callable=merge_storage_csv,
    op_kwargs={
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'CLAVES_CTAS_ESPECIALES/',
      'folder_his': 'CLAVES_CTAS_ESPECIALES_HIS/',
      'destination_blob_name': 'CLAVES_CTAS_ESPECIALES_HIS.csv',
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'encoding': 'utf-8-sig'
    },
    dag=dag
  )

  load_claves_ctas_especiales = PythonOperator(
    task_id='load_claves_ctas_especiales',
    python_callable=upload_storage_csv_to_bigquery,
    op_kwargs={
      'gcs_uri': f'gs://{PREVENCION_FRAUDES_BRO_BUCKET_NAME}/CLAVES_CTAS_ESPECIALES_HIS/CLAVES_CTAS_ESPECIALES_HIS.csv',
      'dataset': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'table': 'CLAVES_CTAS_ESPECIALES',
      'schema_fields': json.loads(get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/schemas/files.claves_ctas_especiales.json')),
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
    },
    dag=dag
  )

  catalogo_direccion_comercial_excel_to_csv = PythonOperator(
    task_id='catalogo_direccion_comercial_excel_to_csv',
    python_callable=catalogo_direccion_comercial_to_csv,
    op_kwargs={
      'project_id':PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'CIENCIA_DATOS/CATALOGO_DIRECCION_COMERCIAL',
      'file': 'Catalogo_direccion_comercial.xlsx',
      'dest_folder': 'CATALOGO_DIRECCION_COMERCIAL',
      'dest_file': 'CATALOGO_DIRECCION_COMERCIAL.csv',
    },
    dag=dag
  )

  merge_catalogo_direccion_comercial = PythonOperator(
    task_id='merge_catalogo_direccion_comercial',
    python_callable=merge_storage_csv,
    op_kwargs={
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'CATALOGO_DIRECCION_COMERCIAL/',
      'folder_his': 'CATALOGO_DIRECCION_COMERCIAL_HIS/',
      'destination_blob_name': 'CATALOGO_DIRECCION_COMERCIAL_HIS.csv',
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'encoding': 'utf-8-sig'
    },
    dag=dag
  )

  load_catalogo_direccion_comercial = PythonOperator(
    task_id='load_catalogo_direccion_comercial',
    python_callable=upload_storage_csv_to_bigquery,
    op_kwargs={
      'gcs_uri': f'gs://{PREVENCION_FRAUDES_BRO_BUCKET_NAME}/CATALOGO_DIRECCION_COMERCIAL_HIS/CATALOGO_DIRECCION_COMERCIAL_HIS.csv',
      'dataset': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'table': 'CATALOGO_DIRECCION_COMERCIAL',
      'schema_fields': json.loads(get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/schemas/files.catalogo_direccion_comercial.json')),
      'project_id': PREVENCION_FRAUDES_BRO_PROJECT_ID,
    },
    dag=dag
  )
  

  rechazos_excel_to_csv = PythonOperator(
    task_id='rechazos_excel_to_csv',
    python_callable=rechazos_to_csv,
    op_kwargs={
      'project_id':'qlts-dev-mx-au-bro-verificacio',
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'CIENCIA_DATOS/RECHAZOS',
      'file': 'RECHAZOS.xlsx',
      'dest_folder': 'RECHAZOS',
      'dest_file': 'RECHAZOS.csv',
    },
    dag=dag
  )

  merge_rechazos = PythonOperator(
    task_id='merge_rechazos',
    python_callable=merge_storage_csv,
    op_kwargs={
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'RECHAZOS/',
      'folder_his': 'RECHAZOS_HIS/',
      'destination_blob_name': 'RECHAZOS_HIS.csv',
      'project_id': 'qlts-dev-mx-au-bro-verificacio',
      'encoding': 'utf-8-sig'
    },
    dag=dag
  )

  load_rechazos = PythonOperator(
    task_id='load_rechazos',
    python_callable=upload_storage_csv_to_bigquery,
    op_kwargs={
      'gcs_uri': f'gs://{PREVENCION_FRAUDES_BRO_BUCKET_NAME}/RECHAZOS_HIS/RECHAZOS_HIS.csv',
      'dataset': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'table': 'RECHAZOS',
      'schema_fields': json.loads(get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/schemas/files.rechazos.json')),
      'project_id': 'qlts-dev-mx-au-bro-verificacio',
    },
    dag=dag
  )

  merge_cargos = PythonOperator(
    task_id='merge_cargos',
    python_callable=merge_storage_csv,
    op_kwargs={
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'CIENCIA_DATOS/TESORERIA/PcPay/Cargos',
      'folder_his': 'CARGOS_HIS/',
      'destination_blob_name': 'CARGOS_HIS.csv',
      'project_id': 'qlts-dev-mx-au-bro-verificacio',
      'encoding': 'utf-8-sig'
    },
    dag=dag
  )

  load_cargos = PythonOperator(
    task_id='load_cargos',
    python_callable=upload_storage_csv_to_bigquery,
    op_kwargs={
      'gcs_uri': f'gs://{PREVENCION_FRAUDES_BRO_BUCKET_NAME}/CARGOS_HIS/CARGOS_HIS.csv',
      'dataset': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'table': 'CARGOS',
      'schema_fields': json.loads(get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/schemas/files.cargos.json')),
      'project_id': 'qlts-dev-mx-au-bro-verificacio',
    },
    dag=dag
  )
  
  merge_contracargos = PythonOperator(
    task_id='merge_contracargos',
    python_callable=merge_storage_csv,
    op_kwargs={
      'bucket_name': PREVENCION_FRAUDES_BRO_BUCKET_NAME,
      'folder': 'CIENCIA_DATOS/TESORERIA/PcPay/Contracargos',
      'folder_his': 'CONTRACARGOS_HIS/',
      'destination_blob_name': 'CONTRACARGOS_HIS.csv',
      'project_id': 'qlts-dev-mx-au-bro-verificacio',
      'encoding': 'utf-8-sig'
    },
    dag=dag
  )

  load_contracargos = PythonOperator(
    task_id='load_contracargos',
    python_callable=upload_storage_csv_to_bigquery,
    op_kwargs={
      'gcs_uri': f'gs://{PREVENCION_FRAUDES_BRO_BUCKET_NAME}/CONTRACARGOS_HIS/CONTRACARGOS_HIS.csv',
      'dataset': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'table': 'CONTRACARGOS',
      'schema_fields': json.loads(get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/schemas/files.contracargos.json')),
      'project_id': 'qlts-dev-mx-au-bro-verificacio',
    },
    dag=dag
  )

  merge_control_de_agentes >> load_control_de_agentes
  merge_apertura_reporte >> load_apertura_reporte
  merge_produccion1 >> load_produccion1
  merge_produccion2 >> load_produccion2
  merge_recuperaciones >> load_recuperaciones
  merge_sumas_aseg >> load_sumas_aseg
  claves_ctas_especiales_excel_to_csv >> merge_claves_ctas_especiales >> load_claves_ctas_especiales
  catalogo_direccion_comercial_excel_to_csv >> merge_catalogo_direccion_comercial >> load_catalogo_direccion_comercial
  rechazos_excel_to_csv >> merge_rechazos >> load_rechazos
  merge_cargos >> load_cargos
  merge_contracargos >> load_contracargos


@task_group(group_id='unique_bsc_siniestros_operators',dag=dag)
def unique_bsc_siniestros_operators():
  
  load_tcausa_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_tcausa_bsc",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_bscsiniestros_sql_tcausa_bsc',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('TCAUSA_BSC', size='XS'),
    dag=dag
  )
  
  load_valuaciones = CloudDataFusionStartPipelineOperator(
    task_id="load_valuaciones",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_bscsiniestros_sql_valuacion',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('VALUACIONES', size='M', init_date=init_date, final_date=final_date),
    dag=dag
  )
  
  load_apercab_reing = CloudDataFusionStartPipelineOperator(
    task_id="load_apercab_reing",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_bscsiniestros_sql_apercab_reing',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('APERCAB_REING', size='M', init_date=init_date, final_date=final_date),
    dag=dag
  )

  load_orden_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_orden_bsc",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_bscsiniestros_sql_orden_bsc',
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
    pipeline_name='carga_qlts_au_ve_bscsiniestros_sql_pagosauditoria_sise',
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

@task_group(group_id='unique_valuaciones_operators',dag=dag)
def unique_valuaciones_operators():
  
  load_analistacdr = CloudDataFusionStartPipelineOperator(
    task_id="load_analistacdr",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_analistacdr',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_causacambiovale',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_cerco',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_color',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_complemento',
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
  
  load_enviohistorico = CloudDataFusionStartPipelineOperator(
    task_id="load_enviohistorico",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_enviohistorico',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_estado',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_estatus',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_estatusexpedientes',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_fechas',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_historicoterminoentrega',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_marca',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_proveedor',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_refaccion',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_supervisorintegral',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_talleres',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_vale',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_valehistorico',
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
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_valuacion',
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

  load_valeestatus = CloudDataFusionStartPipelineOperator(
    task_id="load_valeestatus",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_valeestatus',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('VALEESTATUS', size='XS'),
    dag=dag
  )  

  load_vista_vale = CloudDataFusionStartPipelineOperator(
    task_id="load_vista_vale",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_vista_vale',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('VISTA_VALE', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  

  load_valuador = CloudDataFusionStartPipelineOperator(
    task_id="load_valuador",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_valuador',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('VALUADOR', size='XS'),
    dag=dag
  )  

  load_categoria = CloudDataFusionStartPipelineOperator(
    task_id="load_categoria",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_categoria',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('CATEGORIA', size='XS'),
    dag=dag
  )  

  load_tipotot = CloudDataFusionStartPipelineOperator(
    task_id="load_tipotot",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_tipotot',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('TIPOTOT', size='XS'),
    dag=dag
  )  

  load_relacioncdr_sicdr = CloudDataFusionStartPipelineOperator(
    task_id="load_relacioncdr_sicdr",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_relacioncdr_sicdr',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('RELACIONCDR_SICDR', size='S'),
    dag=dag
  )  

  load_costo = CloudDataFusionStartPipelineOperator(
    task_id="load_costo",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_costo',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('COSTO', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )  

  load_expediente = CloudDataFusionStartPipelineOperator(
    task_id="load_expediente",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_expediente',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('EXPEDIENTE', size='L', init_date=init_date, final_date=final_date),
    dag=dag
  )    
  
  load_administradorrefacciones = CloudDataFusionStartPipelineOperator(
    task_id="load_administradorrefacciones",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_administradorrefacciones',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('ADMINISTRADORREFACCIONES', size='XS'),
    dag=dag
  )    
  
  load_asignacioncdr = CloudDataFusionStartPipelineOperator(
    task_id="load_asignacioncdr",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_asignacioncdr',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('ASIGNACIONCDR', size='XS'),
    dag=dag
  )

  load_fechapromesarealanlcdr = CloudDataFusionStartPipelineOperator(
    task_id="load_fechapromesarealanlcdr",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_fechapromesarealanlcdr',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('FECHAPROMESAREALANLCDR', size='S', init_date=init_date, final_date=final_date),
    dag=dag
  )    

  load_histoinvestigacion = CloudDataFusionStartPipelineOperator(
    task_id="load_histoinvestigacion",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_histoinvestigacion',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('HISTOINVESTIGACION', size='S', init_date=init_date, final_date=final_date),
    dag=dag
  )    

  load_unidad = CloudDataFusionStartPipelineOperator(
    task_id="load_unidad",
    location=DATA_PROJECT_REGION,
    instance_name=DATA_DATAFUSION_INSTANCE_NAME,
    namespace=DATA_DATAFUSION_NAMESPACE,
    pipeline_name='carga_qlts_au_ve_valmxpro_srv_ora_unidad',
    project_id=DATA_PROJECT_ID,
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_load_runtime_args('UNIDAD', size='XS'),
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



@task_group(group_id='file_elt',dag=dag)
def file_elt():
  
  apertura_reporte = BigQueryInsertJobOperator(
    task_id="apertura_reporte",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/APERTURA_REPORTE/APERTURA_REPORTE.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'SOURCE_DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'APERTURA_REPORTE',
      'DEST_PROJECT_ID': PREVENCION_FRAUDES_PLA_PROJECT_ID,
      'DEST_DATASET_NAME': PREVENCION_FRAUDES_PLA_DATASET_NAME,
      'DEST_TABLE_NAME': 'APERTURA_REPORTE',
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  control_de_agentes = BigQueryInsertJobOperator(
    task_id="control_de_agentes",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/CONTROL_DE_AGENTES/CONTROL_DE_AGENTES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'SOURCE_DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'CONTROL_DE_AGENTES',
      'DEST_PROJECT_ID': PREVENCION_FRAUDES_PLA_PROJECT_ID,
      'DEST_DATASET_NAME': PREVENCION_FRAUDES_PLA_DATASET_NAME,
      'DEST_TABLE_NAME': 'CONTROL_DE_AGENTES',
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  produccion1 = BigQueryInsertJobOperator(
    task_id="produccion1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PRODUCCION1/PRODUCCION1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'SOURCE_DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'PRODUCCION1',
      'DEST_PROJECT_ID': PREVENCION_FRAUDES_PLA_PROJECT_ID,
      'DEST_DATASET_NAME': PREVENCION_FRAUDES_PLA_DATASET_NAME,
      'DEST_TABLE_NAME': 'PRODUCCION1',
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  produccion2 = BigQueryInsertJobOperator(
    task_id="produccion2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PRODUCCION2/PRODUCCION2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'SOURCE_DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'PRODUCCION2',
      'DEST_PROJECT_ID': PREVENCION_FRAUDES_PLA_PROJECT_ID,
      'DEST_DATASET_NAME': PREVENCION_FRAUDES_PLA_DATASET_NAME,
      'DEST_TABLE_NAME': 'PRODUCCION2',
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  recuperaciones = BigQueryInsertJobOperator(
    task_id="recuperaciones",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/RECUPERACIONES/RECUPERACIONES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'SOURCE_DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RECUPERACIONES',
      'DEST_PROJECT_ID': PREVENCION_FRAUDES_PLA_PROJECT_ID,
      'DEST_DATASET_NAME': PREVENCION_FRAUDES_PLA_DATASET_NAME,
      'DEST_TABLE_NAME': 'RECUPERACIONES',
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  sumas_aseg = BigQueryInsertJobOperator(
    task_id="sumas_aseg",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/SUMAS_ASEG/SUMAS_ASEG.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'SOURCE_DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'SUMAS_ASEG',
      'DEST_PROJECT_ID': PREVENCION_FRAUDES_PLA_PROJECT_ID,
      'DEST_DATASET_NAME': PREVENCION_FRAUDES_PLA_DATASET_NAME,
      'DEST_TABLE_NAME': 'SUMAS_ASEG',
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )

  cargos = BigQueryInsertJobOperator(
    task_id="cargos",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/CARGOS/CARGOS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'SOURCE_DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'CARGOS',
      'DEST_PROJECT_ID': PREVENCION_FRAUDES_PLA_PROJECT_ID,
      'DEST_DATASET_NAME': PREVENCION_FRAUDES_PLA_DATASET_NAME,
      'DEST_TABLE_NAME': 'CARGOS',
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  catalogo_direccion_comercial = BigQueryInsertJobOperator(
    task_id="catalogo_direccion_comercial",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/CATALOGO_DIRECCION_COMERCIAL/CATALOGO_DIRECCION_COMERCIAL.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'SOURCE_DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'CATALOGO_DIRECCION_COMERCIAL',
      'DEST_PROJECT_ID': PREVENCION_FRAUDES_PLA_PROJECT_ID,
      'DEST_DATASET_NAME': PREVENCION_FRAUDES_PLA_DATASET_NAME,
      'DEST_TABLE_NAME': 'CATALOGO_DIRECCION_COMERCIAL',
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  claves_ctas_especiales = BigQueryInsertJobOperator(
    task_id="claves_ctas_especiales",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/CLAVES_CTAS_ESPECIALES/CLAVES_CTAS_ESPECIALES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'SOURCE_DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'CLAVES_CTAS_ESPECIALES',
      'DEST_PROJECT_ID': PREVENCION_FRAUDES_PLA_PROJECT_ID,
      'DEST_DATASET_NAME': PREVENCION_FRAUDES_PLA_DATASET_NAME,
      'DEST_TABLE_NAME': 'CLAVES_CTAS_ESPECIALES',
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  contracargos = BigQueryInsertJobOperator(
    task_id="contracargos",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/CONTRACARGOS/CONTRACARGOS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'SOURCE_DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'CONTRACARGOS',
      'DEST_PROJECT_ID': PREVENCION_FRAUDES_PLA_PROJECT_ID,
      'DEST_DATASET_NAME': PREVENCION_FRAUDES_PLA_DATASET_NAME,
      'DEST_TABLE_NAME': 'CONTRACARGOS',
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  rechazos = BigQueryInsertJobOperator(
    task_id="rechazos",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/RECHAZOS/RECHAZOS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': PREVENCION_FRAUDES_BRO_PROJECT_ID,
      'SOURCE_DATASET_NAME': PREVENCION_FRAUDES_BRO_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RECHAZOS',
      'DEST_PROJECT_ID': PREVENCION_FRAUDES_PLA_PROJECT_ID,
      'DEST_DATASET_NAME': PREVENCION_FRAUDES_PLA_DATASET_NAME,
      'DEST_TABLE_NAME': 'RECHAZOS',
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )


@task_group(group_id='tlp_elt',dag=dag)
def tlp_elt():

  usuariohomologado = BigQueryInsertJobOperator(
    task_id="usuariohomologado",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/USUARIOHOMOLOGADO/USUARIOHOMOLOGADO.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )


  todaslaspiezas_1 = BigQueryInsertJobOperator(
    task_id="todaslaspiezas_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TODASLASPIEZAS/TODASLASPIEZAS_1.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )

  todaslaspiezas_2 = BigQueryInsertJobOperator(
    task_id="todaslaspiezas_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TODASLASPIEZAS/TODASLASPIEZAS_2.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )

  todaslaspiezas_3 = BigQueryInsertJobOperator(
    task_id="todaslaspiezas_3",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TODASLASPIEZAS/TODASLASPIEZAS_3.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )

  todaslaspiezas_4 = BigQueryInsertJobOperator(
    task_id="todaslaspiezas_4",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TODASLASPIEZAS/TODASLASPIEZAS_4.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )

  todaslaspiezas_5 = BigQueryInsertJobOperator(
    task_id="todaslaspiezas_5",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TODASLASPIEZAS/TODASLASPIEZAS_5.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )

  todaslaspiezas_6 = BigQueryInsertJobOperator(
    task_id="todaslaspiezas_6",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TODASLASPIEZAS/TODASLASPIEZAS_6.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )

  todaslaspiezas_7 = BigQueryInsertJobOperator(
    task_id="todaslaspiezas_7",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TODASLASPIEZAS/TODASLASPIEZAS_7.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )

  todaslaspiezas_8 = BigQueryInsertJobOperator(
    task_id="todaslaspiezas_8",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TODASLASPIEZAS/TODASLASPIEZAS_8.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )


  todaslaspiezas_9 = BigQueryInsertJobOperator(
    task_id="todaslaspiezas_9",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/TODASLASPIEZAS/TODASLASPIEZAS_9.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )
  usuariohomologado >> todaslaspiezas_1
  todaslaspiezas_1 >> todaslaspiezas_2 >> todaslaspiezas_3 >> todaslaspiezas_4 >> \
  todaslaspiezas_5 >> todaslaspiezas_6 >> todaslaspiezas_7 >> todaslaspiezas_8 >> todaslaspiezas_9

@task_group(group_id='supervisor_cdr_elt',dag=dag)
def supervisor_cdr_elt():
  
  ejecutivas_seg = BigQueryInsertJobOperator(
    task_id="ejecutivas_seg",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/EJECUTIVAS_SEG/EJECUTIVAS_SEG.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )
    
  supervisor_cdr = BigQueryInsertJobOperator(
    task_id="supervisor_cdr",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/SUPERVISOR_CDR/SUPERVISOR_CDR.sql'),
        "useLegacySql": False,
      }
    },
    location=PREVENCION_FRAUDES_PROJECT_REGION,
    gcp_conn_id=PREVENCION_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,
    dag=dag 
  )
  
  ejecutivas_seg >> supervisor_cdr
  
# Flujo del DAG - Solo ejecutando los operadores únicos del segundo DAG
landing >> init_landing() >> [load_files(),unique_bsc_siniestros_operators(),unique_valuaciones_operators()] >> end_landing() >> file_elt() >> tlp_elt() >> supervisor_cdr_elt()