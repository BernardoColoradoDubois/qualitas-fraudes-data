import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator
from lib.utils import get_bucket_file_contents, upload_storage_csv_to_bigquery
import json

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'load_test_data',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval='0 0 1 1 *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

# priority_weight has type int in Airflow DB, uses the maximum.
init = BashOperator(task_id='init',bash_command='echo init',dag=dag)

load_appercab = PythonOperator(
  task_id='load_appercab_bsc',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/APERCAB_BSC.csv',
    'dataset': 'sample_landing_siniestros_bsc',
    'table': 'apercab_bsc',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/siniestros_bsc.apercab_bsc.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)
  

load_pagprove = PythonOperator(
  task_id='load_pagprove',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/PAGPROVE.csv',
    'dataset': 'sample_landing_siniestros_bsc',
    'table': 'pagprove',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/siniestros_bsc.pagoprove.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)

load_pagos_proveedores = PythonOperator(
  task_id='load_pagos_proveedores',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/PAGOSPROVEEDORES.csv',
    'dataset': 'sample_landing_siniestros_bsc',
    'table': 'pagosproveedores',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/siniestros_bsc.pagosproveedores.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)

load_prestadores = PythonOperator(
  task_id='load_prestadores',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/Prestadores.csv',
    'dataset': 'sample_landing_siniestros_bsc',
    'table': 'prestadores',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/siniestros_bsc.prestadores.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)   

load_reservas_bsc = PythonOperator(
  task_id='load_reservas_bsc',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/RESERVAS_BSC.csv',
    'dataset': 'sample_landing_siniestros_bsc',
    'table': 'reservas_bsc',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/siniestros_bsc.reservas_bsc.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)

load_tsuc_bsc = PythonOperator(
  task_id='load_tsuc_bsc',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/TSUC_BSC.csv',
    'dataset': 'sample_landing_siniestros_bsc',
    'table': 'tsuc_bsc',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/siniestros_bsc.tsuc_bsc.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)

load_analistas = PythonOperator(
  task_id='load_analistas',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/ANALISTAS.csv',
    'dataset': 'sample_landing_siniestros',
    'table': 'analistas',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/siniestros.analistas.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)

load_cat_causa = PythonOperator(
  task_id='load_cat_causa',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/CAT_CAUSA.csv',
    'dataset': 'sample_landing_siniestros',
    'table': 'cat_causa',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/siniestros.cat_causa.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)

load_causa_cobertura = PythonOperator(
  task_id='load_causa_cobertura',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/CAUSA_COBERTURA.csv',
    'dataset': 'sample_landing_siniestros',
    'table': 'causa_cobertura',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/siniestros.causa_cobertura.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)

load_cobranza_hist = PythonOperator(
  task_id='load_cobranza_hist',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/COBRANZA_HIST.csv',
    'dataset': 'sample_landing_siniestros',
    'table': 'cobranza_hist',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/siniestros.cobranza_hist.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)

load_cobranza = PythonOperator(
  task_id='load_cobranza',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/COBRANZA.csv',
    'dataset': 'sample_landing_siniestros',
    'table': 'cobranza',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/siniestros.cobranza.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)

load_fraud_pv = PythonOperator(
  task_id='load_fraud_pv',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/FRAUD_PV.csv',
    'dataset': 'sample_landing_sise',
    'table': 'fraud_pv',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/sise.fraud_pv.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)

load_fraud_rp = PythonOperator(
  task_id='load_fraud_rp',
  python_callable=upload_storage_csv_to_bigquery,
  op_kwargs={
    'gcs_uri': 'gs://quafraudestorage/FRAUD_RP.csv',
    'dataset': 'sample_landing_sise',
    'table': 'fraud_rp',
    'schema_fields': json.loads(get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/schemas/sise.fraud_rp.json')),
    'project_id': 'qualitasfraude',
  },
  dag=dag
)

init >> load_appercab 
init >> load_pagprove
init >> load_pagos_proveedores
init >> load_prestadores
init >> load_reservas_bsc
init >> load_tsuc_bsc
init >> load_analistas
init >> load_cat_causa
init >> load_causa_cobertura
init >> load_cobranza_hist
init >> load_cobranza
init >> load_fraud_pv
init >> load_fraud_rp
