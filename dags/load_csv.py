import airflow
import json
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from lib.utils import get_bucket_file_contents,upload_storage_csv_to_bigquery,merge_storage_csv,agentes_to_csv


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
  tags=['VERIFICACIONES']
)

init = BashOperator(task_id='init',bash_command='echo "Iniciando el DAG"',dag=dag)

merge_control_de_agentes = PythonOperator(
  task_id='merge_control_de_agentes',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
    'folder': 'CONTROL_DE_AGENTES/',
    'folder_his': 'CONTROL_DE_AGENTES_HIS/',
    'destination_blob_name': 'CONTROL_DE_AGENTES_2025_HIS.csv',
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
    'encoding': 'iso-8859-1'
  },
  dag=dag
)

load_control_de_agentes = PythonOperator(
  task_id='load_control_de_agentes',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/CONTROL_DE_AGENTES_HIS/CONTROL_DE_AGENTES_2025_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'CONTROL_DE_AGENTES',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.control_de_agentes.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

merge_apertura_reporte = PythonOperator(
  task_id='merge_apertura_reporte',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
    'folder': 'APERTURA_REPORTE/',
    'folder_his': 'APERTURA_REPORTE_HIS/',
    'destination_blob_name': 'APERTURA_REPORTE_HIS.csv',
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
    'encoding': 'iso-8859-1'
  },
  dag=dag
)

load_apertura_reporte = PythonOperator(
  task_id='load_apertura_reporte',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/APERTURA_REPORTE_HIS/APERTURA_REPORTE_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'APERTURA_REPORTE',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.apertura_reporte.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

merge_produccion1 = PythonOperator(
  task_id='merge_produccion1',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
    'folder': 'PRODUCCION1/',
    'folder_his': 'PRODUCCION1_HIS/',
    'destination_blob_name': 'PRODUCCION1_HIS.csv',
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
    'encoding': 'iso-8859-1'
  },
  dag=dag
)

load_produccion1 = PythonOperator(
  task_id='load_produccion1',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/PRODUCCION1_HIS/PRODUCCION1_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'PRODUCCION1',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.produccion1.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

merge_produccion2 = PythonOperator(
  task_id='merge_produccion2',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
    'folder': 'PRODUCCION2/',
    'folder_his': 'PRODUCCION2_HIS/',
    'destination_blob_name': 'PRODUCCION2_HIS.csv',
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
    'encoding': 'iso-8859-1'
  },
  dag=dag
)

load_produccion2 = PythonOperator(
  task_id='load_produccion2',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/PRODUCCION2_HIS/PRODUCCION2_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'PRODUCCION2',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.produccion2.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

merge_recuperaciones = PythonOperator(
  task_id='merge_recuperaciones',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
    'folder': 'RECUPERACIONES/',
    'folder_his': 'RECUPERACIONES_HIS/',
    'destination_blob_name': 'RECUPERACIONES_HIS.csv',
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
    'encoding': 'iso-8859-1'
  },
  dag=dag
)

load_recuperaciones = PythonOperator(
  task_id='load_recuperaciones',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/RECUPERACIONES_HIS/RECUPERACIONES_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'RECUPERACIONES',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.recuperaciones.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

merge_sumas_aseg = PythonOperator(
  task_id='merge_sumas_aseg',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
    'folder': 'SUMAS_ASEG/',
    'folder_his': 'SUMAS_ASEG_HIS/',
    'destination_blob_name': 'SUMAS_ASEG_HIS.csv',
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
    'encoding': 'iso-8859-1'
  },
  dag=dag
)

load_sumas_aseg = PythonOperator(
  task_id='load_sumas_aseg',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/SUMAS_ASEG_HIS/SUMAS_ASEG_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'SUMAS_ASEG',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.sumas_aseg.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

merge_claves_ctas_especiales = PythonOperator(
  task_id='merge_claves_ctas_especiales',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
    'folder': 'CLAVES_CTAS_ESPECIALES/',
    'folder_his': 'CLAVES_CTAS_ESPECIALES_HIS/',
    'destination_blob_name': 'CLAVES_CTAS_ESPECIALES_HIS.csv',
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
    'encoding': 'utf-8-sig'
  },
  dag=dag
)

load_claves_ctas_especiales = PythonOperator(
  task_id='load_claves_ctas_especiales',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/CLAVES_CTAS_ESPECIALES_HIS/CLAVES_CTAS_ESPECIALES_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'CLAVES_CTAS_ESPECIALES',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.claves_ctas_especiales.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

#agentes_excel_to_csv = PythonOperator(
#  task_id='agentes_excel_to_csv',
#  python_callable=agentes_to_csv,
#  op_kwargs={
#    'project_id':'qlts-dev-mx-au-bro-verificacio',
#    'bucket_name': 'bucket_verificaciones',
#    'folder': 'AGENTES_GERENTES',
#    'file_name': 'Agentes_Gerentes.xlsx'
#  },
#  dag=dag
#)


init >> merge_control_de_agentes >> load_control_de_agentes
init >> merge_apertura_reporte >> load_apertura_reporte
init >> merge_produccion1 >> load_produccion1
init >> merge_produccion2 >> load_produccion2
init >> merge_recuperaciones >> load_recuperaciones
init >> merge_sumas_aseg >> load_sumas_aseg
init >> merge_claves_ctas_especiales >> load_claves_ctas_especiales

#init >> agentes_excel_to_csv