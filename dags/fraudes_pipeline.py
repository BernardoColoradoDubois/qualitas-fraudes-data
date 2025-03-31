import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator
from lib.utils import execute_query_workflow,get_bucket_file_contents
import json

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'fraudes_pipeline',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval='0 0 1 1 *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

init = BashOperator(task_id='init',bash_command='echo init',dag=dag)

dm_causa_cobertura = PythonOperator( 
  task_id='dm_causa_cobertura', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/CAUSAS/DM_CAUSA_COBERTURA.sql')
  }, 
  dag=dag 
)

dm_causas = PythonOperator( 
  task_id='dm_causas', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/CAUSAS/DM_CAUSAS.sql')
  }, 
  dag=dag 
)

dm_oficinas = PythonOperator( 
  task_id='dm_oficinas', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/OFICINAS/DM_OFICINAS.sql')
  }, 
  dag=dag 
)

dm_proveedores = PythonOperator( 
  task_id='dm_proveedores', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/PROVEEDORES/DM_PROVEEDORES.sql')
  }, 
  dag=dag 
)

stg_siniestros = PythonOperator( 
  task_id='stg_siniestros', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/SINIESTROS/STG_SINIESTROS.sql')
  }, 
  dag=dag 
)

dm_siniestros = PythonOperator( 
  task_id='dm_siniestros', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/SINIESTROS/DM_SINIESTROS.sql')
  }, 
  dag=dag 
)

stg_etiqueta_siniestro_1 = PythonOperator( 
  task_id='stg_etiqueta_siniestro_1', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_1.sql')
  }, 
  dag=dag 
)

stg_etiqueta_siniestro_2 = PythonOperator( 
  task_id='stg_etiqueta_siniestro_2', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_2.sql')
  }, 
  dag=dag 
)

stg_etiqueta_siniestro_3 = PythonOperator( 
  task_id='stg_etiqueta_siniestro_3', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_3.sql')
  }, 
  dag=dag 
)

dm_etiqueta_siniestro= PythonOperator( 
  task_id='dm_etiqueta_siniestro', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/DM_ETIQUETA_SINIESTRO.sql')
  }, 
  dag=dag 
)

dm_pagos_polizas= PythonOperator( 
  task_id='dm_pagos_polizas', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/PAGOS_POLIZAS/DM_PAGOS_POLIZAS.sql')
  }, 
  dag=dag 
)

# dag coberturas movimientos
dm_coberturas_movimiento = PythonOperator( 
  task_id='dm_coberturas_movimiento', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude',
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/COBERTURAS_MOVIMIENTOS/DM_COBERTURAS_MOVIMIENTOS.sql')
  }, 
  dag=dag 
)

# dag coberturas movimientos
dm_pagos_proveedores = PythonOperator( 
  task_id='dm_pagos_proveedores', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude',
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/PAGOS_PROVEEDORES/DM_PAGOS_PROVEEDORES.sql')
  }, 
  dag=dag 
)

dm_analistas = PythonOperator( 
  task_id='dm_analistas', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude',
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ANALISTAS/DM_ANALISTAS.sql')
  }, 
  dag=dag 
)

# dm_registro
dm_registro = PythonOperator( 
  task_id='dm_registro', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude',
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/REGISTRO/DM_REGISTRO.sql')
  }, 
  dag=dag 
)

stg_polizas_vigentes_1 = PythonOperator( 
  task_id='stg_polizas_vigentes_1', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude',
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_1.sql')
  }, 
  dag=dag 
)

stg_polizas_vigentes_2 = PythonOperator( 
  task_id='stg_polizas_vigentes_2', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude',
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_2.sql')
  }, 
  dag=dag 
)

dm_polizas_vigentes = PythonOperator( 
  task_id='dm_polizas_vigentes', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude',
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/POLIZAS_VIGENTES/DM_POLIZAS_VIGENTES.sql')
  }, 
  dag=dag 
)


init >> dm_causa_cobertura 
init >> dm_causas 
init >> dm_oficinas 
init >> dm_proveedores 
init >> stg_siniestros >> dm_siniestros 
init >> dm_pagos_polizas 
init >> dm_coberturas_movimiento 
init >> dm_pagos_proveedores 
init >> dm_analistas
init >> dm_registro
init >> stg_etiqueta_siniestro_1 >> stg_etiqueta_siniestro_2 >> stg_etiqueta_siniestro_3 >> dm_etiqueta_siniestro 
init >> stg_polizas_vigentes_1 >> stg_polizas_vigentes_2 >> dm_polizas_vigentes
