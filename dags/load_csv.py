import airflow
import json
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from lib.utils import get_bucket_file_contents,upload_storage_csv_to_bigquery,merge_storage_csv
from lib.utils import agentes_to_csv,gerentes_to_csv,claves_ctas_especiales_to_csv,catalogo_direccion_comercial_to_csv,rechazos_to_csv


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

claves_ctas_especiales_excel_to_csv = PythonOperator(
  task_id='claves_ctas_especiales_excel_to_csv',
  python_callable=claves_ctas_especiales_to_csv,
  op_kwargs={
    'project_id':'qlts-dev-mx-au-bro-verificacio',
    'bucket_name': 'bucket_verificaciones',
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

catalogo_direccion_comercial_excel_to_csv = PythonOperator(
  task_id='catalogo_direccion_comercial_excel_to_csv',
  python_callable=catalogo_direccion_comercial_to_csv,
  op_kwargs={
    'project_id':'qlts-dev-mx-au-bro-verificacio',
    'bucket_name': 'bucket_verificaciones',
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
    'bucket_name': 'bucket_verificaciones',
    'folder': 'CATALOGO_DIRECCION_COMERCIAL/',
    'folder_his': 'CATALOGO_DIRECCION_COMERCIAL_HIS/',
    'destination_blob_name': 'CATALOGO_DIRECCION_COMERCIAL_HIS.csv',
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
    'encoding': 'utf-8-sig'
  },
  dag=dag
)

load_catalogo_direccion_comercial = PythonOperator(
  task_id='load_catalogo_direccion_comercial',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/CATALOGO_DIRECCION_COMERCIAL_HIS/CATALOGO_DIRECCION_COMERCIAL_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'CATALOGO_DIRECCION_COMERCIAL',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.catalogo_direccion_comercial.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

agentes_excel_to_csv = PythonOperator(
  task_id='agentes_excel_to_csv',
  python_callable=agentes_to_csv,
  op_kwargs={
    'project_id':'qlts-dev-mx-au-bro-verificacio',
    'bucket_name': 'bucket_verificaciones',
    'folder': 'AGENTES_GERENTES',
    'file': 'Agentes_Gerentes.xlsx',
    'dest_folder': 'AGENTES',
    'dest_file': 'AGENTES.csv',
  },
  dag=dag
)

merge_agentes = PythonOperator(
  task_id='merge_agentes',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
    'folder': 'AGENTES/',
    'folder_his': 'AGENTES_HIS/',
    'destination_blob_name': 'AGENTES_HIS.csv',
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
    'encoding': 'utf-8-sig'
  },
  dag=dag
)

load_agentes = PythonOperator(
  task_id='load_agentes',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/AGENTES_HIS/AGENTES_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'AGENTES',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.agentes.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

gerentes_excel_to_csv = PythonOperator(
  task_id='gerentes_excel_to_csv',
  python_callable=gerentes_to_csv,
  op_kwargs={
    'project_id':'qlts-dev-mx-au-bro-verificacio',
    'bucket_name': 'bucket_verificaciones',
    'folder': 'AGENTES_GERENTES',
    'file': 'Agentes_Gerentes.xlsx',
    'dest_folder': 'GERENTES',
    'dest_file': 'GERENTES.csv',
  },
  dag=dag
)

merge_gerentes = PythonOperator(
  task_id='merge_gerentes',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
    'folder': 'GERENTES/',
    'folder_his': 'GERENTES_HIS/',
    'destination_blob_name': 'GERENTES_HIS.csv',
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
    'encoding': 'utf-8-sig'
  },
  dag=dag
)

load_gerentes = PythonOperator(
  task_id='load_gerentes',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/GERENTES_HIS/GERENTES_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'GERENTES',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.gerentes.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

merge_estados_mexico = PythonOperator(
  task_id='merge_estados_mexico',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
    'folder': 'ESTADOS_MEXICO/',
    'folder_his': 'ESTADOS_MEXICO_HIS/',
    'destination_blob_name': 'ESTADOS_MEXICO_HIS.csv',
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
    'encoding': 'utf-8-sig'
  },
  dag=dag
)

load_estados_mexico = PythonOperator(
  task_id='load_estados_mexico',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://bucket_verificaciones/ESTADOS_MEXICO_HIS/ESTADOS_MEXICO_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'ESTADOS_MEXICO',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.estados_mexico.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

rechazos_excel_to_csv = PythonOperator(
  task_id='rechazos_excel_to_csv',
  python_callable=rechazos_to_csv,
  op_kwargs={
    'project_id':'qlts-dev-mx-au-bro-verificacio',
    'bucket_name': 'bucket_verificaciones',
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
    'bucket_name': 'bucket_verificaciones',
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
    'gcs_uri': 'gs://bucket_verificaciones/RECHAZOS_HIS/RECHAZOS_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'RECHAZOS',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.rechazos.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

merge_cargos = PythonOperator(
  task_id='merge_cargos',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
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
    'gcs_uri': 'gs://bucket_verificaciones/CARGOS_HIS/CARGOS_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'CARGOS',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.cargos.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)

merge_contracargos = PythonOperator(
  task_id='merge_contracargos',
  python_callable=merge_storage_csv,
  op_kwargs={
    'bucket_name': 'bucket_verificaciones',
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
    'gcs_uri': 'gs://bucket_verificaciones/CONTRACARGOS_HIS/CONTRACARGOS_HIS.csv',
    'dataset': 'LAN_VERIFICACIONES',
    'table': 'CONTRACARGOS',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/schemas/files.contracargos.json')),
    'project_id': 'qlts-dev-mx-au-bro-verificacio',
  },
  dag=dag
)



init >> merge_control_de_agentes >> load_control_de_agentes
init >> merge_apertura_reporte >> load_apertura_reporte
init >> merge_produccion1 >> load_produccion1
init >> merge_produccion2 >> load_produccion2
init >> merge_recuperaciones >> load_recuperaciones
init >> merge_sumas_aseg >> load_sumas_aseg
init >> claves_ctas_especiales_excel_to_csv >> merge_claves_ctas_especiales >> load_claves_ctas_especiales
init >> catalogo_direccion_comercial_excel_to_csv >> merge_catalogo_direccion_comercial >> load_catalogo_direccion_comercial
init >> agentes_excel_to_csv >> merge_agentes >> load_agentes
init >> gerentes_excel_to_csv >> merge_gerentes >> load_gerentes
init >> merge_estados_mexico >> load_estados_mexico
init >> rechazos_excel_to_csv >> merge_rechazos >> load_rechazos
init >> merge_cargos >> load_cargos
init >> merge_contracargos >> load_contracargos
