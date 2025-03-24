from google.cloud import storage
import re
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import cx_Oracle

#from airflow.providers.oracle.hooks.oracle import OracleHook
from sqlalchemy import create_engine

def get_bucket_file_contents(path):

  try:
    # Extraer bucket_name y file_path de la ruta completa
    match = re.match(r'gs://([^/]+)/(.*)', path)
    if not match:
        raise ValueError("Formato de ruta inválido. Debe ser 'gs://<bucket_name>/file_path'")
    
    bucket_name = match.group(1)
    file_path = match.group(2)
    
    # Inicializar el cliente de GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()
    return str(content)
  except Exception as e:
    print(f"Error al leer el archivo: {e}")
    return None


def upload_storage_csv_to_bigquery(gcs_uri,dataset,table,schema_fields,project_id,write_disposition="WRITE_TRUNCATE",skip_leading_rows=1,max_bad_records=0,**kwargs):

  client = bigquery.Client(project=project_id)
  
  table_id = f"{project_id}.{dataset}.{table}"
  
  schema = []
  
  for field in schema_fields:
    
    schema.append(bigquery.SchemaField(name=field['name'],field_type=field['type'],mode=field['mode']))
    
  job_config = bigquery.LoadJobConfig(
    schema=schema,
    allow_quoted_newlines=True,
    skip_leading_rows=1,
    max_bad_records=1,
    write_disposition=write_disposition,
    source_format=bigquery.SourceFormat.CSV
  ) 
  
  load_job = client.load_table_from_uri(
    gcs_uri, 
    table_id, 
    job_config=job_config
  )
  
  load_job.result()  # Waits for the job to complete.

  destination_table = client.get_table(table_id)  # Make an API request.

  print("Loaded {} rows.".format(destination_table.num_rows))

def execute_query_workflow(project_id,query,**kwargs):

  client = bigquery.Client(project=project_id)
  query_job = client.query(query)  # Make an API request.
  result = query_job.result()
  print(result.__dict__)  
  
def execute_query_to_load_oracle_database(project_id,query,**kwargs):

  client = bigquery.Client(project=project_id)
  query_job = client.query(query)
  result = query_job.result()
  
  df = result.to_dataframe()
  dt = [tuple(x) for x in df.values]

  os.environ["LD_LIBRARY_PATH"] = "/opt/oracle/instantclient_23_7"
  os.environ["PATH"] = "/opt/oracle/instantclient_23_7"


  conn_string = 'ADMIN/FqzJ3n3Kvwcftakshcmi@qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com:1521/ORCL'
  connection = cx_Oracle.connect(conn_string)
  cursor = connection.cursor()
  cursor.execute('TRUNCATE TABLE INSUMOS.DM_CAUSAS')
  sql='INSERT INTO INSUMOS.DM_CAUSAS VALUES(:1,:2,:3,:4,:5)'
  cursor.executemany(sql, dt)
  connection.commit()
  cursor.close()

