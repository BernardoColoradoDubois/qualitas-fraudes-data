from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_transform',
    default_args=default_args,
    description='Crea tabla DM_ETIQUETA_SINIESTRO con versionamiento SCD-2',
    schedule_interval=timedelta(days=1),  # Ajusta según tus necesidades
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    crear_dm_etiqueta_siniestro = BigQueryOperator(
        task_id='crear_dm_etiqueta_siniestro',
        sql='''
        CREATE OR REPLACE TABLE `DM_FRAUDES.DM_ETIQUETA_SINIESTRO` AS 
        SELECT 
          E1.Z_ID AS ID 
          ,E1.REPORTE AS ID_REPORTE 
          ,E1.SINIESTRO AS ID_SINIESTRO 
          ,E1.COD_STATUS AS CODIGO_ESTATUS 
          ,E1.FEC_INV AS FECHA_INVESTIGACION 
          ,E1.TEXT_INVEST AS TEXTO_INVESTIGACION 
          ,E1.USUARIO_INV AS USUARIO_INVESTIGACION 
          ,E1.TEXT_LOC AS TEXTO_LOCALIZADO 
          ,E1.FEC_LOC AS FECHA_LOCALIZADO 
          ,E1.FEC_CARGA AS FECHA_CARGA 
          ,E1.HORA_PROCESO FECHA_HORA_PROCESO 
          ,E1.BATCHDATE AS FECHA_LOTE 
          ,E1.SYSUSERID AS ID_USUARIO_SISTEMA 
          ,E1.CONSECUTIVO AS CONSECUTIVO 
          ,E1.FEC_CARGA AS VALIDO_DESDE 
          ,CASE  
            WHEN E2.FEC_CARGA IS NOT NULL THEN E2.FEC_CARGA  
            ELSE TIMESTAMP('9999-12-31 23:59:59') 
          END AS VALIDO_HASTA 
          ,CASE  
            WHEN E2.FEC_CARGA IS NOT NULL THEN FALSE 
            ELSE TRUE 
          END AS VALIDO 
        FROM `STG_FRAUDES.STG_ETIQUETA_SINIESTRO_3` AS E1 
        LEFT JOIN `STG_FRAUDES.STG_ETIQUETA_SINIESTRO_3` AS E2 
        ON E1.REPORTE = E2.REPORTE AND E1.CONSECUTIVO+1 = E2.CONSECUTIVO 
        ORDER BY E1.Z_ID, E1.CONSECUTIVO 
        ''',
        use_legacy_sql=False,  # Usa SQL estándar de BigQuery (no Legacy SQL)
        location='US',  # Ajusta según la ubicación de tu dataset en BigQuery
    )
