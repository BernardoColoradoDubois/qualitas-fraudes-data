from google.cloud import storage
import re
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime, timedelta
import pytz
import pandas as pd
from io import StringIO,BytesIO
#import openpyxl


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

#['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1']
def merge_storage_csv(project_id,bucket_name,folder,folder_his, destination_blob_name,encoding='utf-8',**kwargs):
  
  client = storage.Client(project=project_id)
    
    # Obtener el bucket
  bucket = client.bucket(bucket_name)
    
  objects = bucket.list_blobs(prefix=folder)
  
  csv_files = [obj for obj in objects if obj.name.endswith('.csv')]

  df_list = []

  for csv_file in csv_files:

    contenido_csv = csv_file.download_as_text(encoding=encoding)
    #df = pd.read_csv(StringIO(contenido_csv)) # it uses the headers as data


    #Assume all files have the same header
    df = pd.read_csv(StringIO(contenido_csv), header=0, low_memory=False, dtype=str)
    
    df_list.append(df)

  merged_df = pd.concat(df_list, ignore_index=True)
  print("Merged DataFrame shape:", merged_df.shape)
  merged_csv = merged_df.to_csv(index=False)

  # Upload merged CSV back to GCS
  output_path = f"{folder_his.rstrip('/')}/{destination_blob_name}" if folder_his else destination_blob_name
  out_blob = bucket.blob(output_path)
  out_blob.upload_from_string(merged_csv, content_type='text/csv')

  print(f"Uploaded merged CSV to gs://{bucket_name}/{output_path}")


    
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
  

def get_date_interval(project_id:str,dataset,table,period:str,**kwargs):
  
  init_date = 'yyyy-mm-dd'
  final_date = 'yyyy-mm-dd'
  
  print(f"period: {period}")
  
  if period == 'YESTERDAY':
    timezone = pytz.timezone('America/Mexico_City')
    today = datetime.now(timezone)
    yesterday = today - timedelta(days=1)
    string_date = yesterday.strftime('%Y-%m-%d')
    
    init_date = string_date
    final_date = string_date
        
    return {
      'init_date': init_date,
      'final_date': final_date
    }
    
  elif period == 'HISTORY':
    
    timezone = pytz.timezone('America/Mexico_City')
    today = datetime.now(timezone)
    yesterday = today - timedelta(days=1)
    string_date = yesterday.strftime('%Y-%m-%d')
    
    init_date = '1970-01-01'
    final_date = string_date
    
    return {
      'init_date': init_date,
      'final_date': final_date
    }
  
  elif 'RANGE:' in period:

    ## RANGE:(yyyy-mm-dd - yyyy-mm-dd)
    wrapped_range = period.strip('RANGE:')
    range = wrapped_range.strip(')').strip('(')
    init_date = range.split(' - ')[0]
    final_date = range.split(' - ')[1]

    init_date_number = int(init_date.replace('-', ''))
    final_date_number = int(final_date.replace('-', ''))


    if final_date_number >= init_date_number:

      return {
        'init_date': init_date,
        'final_date': final_date
      }
    
    else:

      return {
        'init_date': 'init_date',
        'final_date': 'final_date'
      }
      
  else:
    client = bigquery.Client(project=project_id)
    query = f"""
        WITH dates AS ( 
          SELECT DATE FROM `{dataset}.{table}` WHERE PERIOD_STRING = '{period}'
        ) 
        SELECT MIN(DATE) AS init_date, MAX(DATE) AS final_date FROM dates WHERE DATE < CURRENT_DATE('-06')
      """
    query_job = client.query(query)
    result = query_job.result()
    
    for row in result:
      init_date = row[0]
      final_date = row[1]
      
    init_date = init_date.strftime('%Y-%m-%d')
    final_date = final_date.strftime('%Y-%m-%d')
    
    return {
      'init_date': init_date,
      'final_date': final_date
    }


def get_cluster_tipe_creator(init_date:str,final_date:str,small_cluster_label:str,big_cluster_label:str,**kwargs):
  
  if init_date == final_date:
    return small_cluster_label
    
  else:
    return  big_cluster_label
  


def agentes_to_csv(project_id,bucket_name,folder,file,dest_folder,dest_file,**kwargs):
  
  client = storage.Client(project=project_id)
    
  # Obtener el bucket
  bucket = client.bucket(bucket_name)
    
  # Obtener el blob (archivo)
  blob = bucket.blob(f'{folder}/{file}')
    
  # Descargar el archivo como bytes
  excel_data = blob.download_as_bytes()
  
  sheet_name = 'Hoja1'

  column_names = [
    'CODIGO_OFICINA_OFICINA',
    'CODIGO_GERENTE_GERENTE',
    'CODIGO_AGENTE',
    'AGENTE',
  ]

  dtypes = {
    'CODIGO_OFICINA_OFICINA': 'string',
    'CODIGO_GERENTE_GERENTE': 'string',
    'CODIGO_AGENTE': 'string',
    'AGENTE': 'string',
  }

  df = pd.read_excel(excel_data,engine='openpyxl',sheet_name=sheet_name,skiprows=1,header=None,names=column_names,index_col=False,dtype=dtypes)

  df[['CODIGO_OFICINA', 'OFICINA']] = df['CODIGO_OFICINA_OFICINA'].str.split(' ', n=1, expand=True)
  df[['CODIGO_GERENTE', 'GERENTE']] = df['CODIGO_GERENTE_GERENTE'].str.split(' ', n=1, expand=True)

  df = df.drop(columns=['CODIGO_OFICINA_OFICINA'])
  df = df.drop(columns=['CODIGO_GERENTE_GERENTE'])


  df = df[['CODIGO_OFICINA' ,'CODIGO_GERENTE' ,'CODIGO_AGENTE' ,'AGENTE']].copy().drop_duplicates(subset=['CODIGO_AGENTE'])

  csv = df.to_csv(index=False)


  output_path = f"{dest_folder}/{dest_file}"
  out_blob = bucket.blob(output_path)
  out_blob.upload_from_string(csv, content_type='text/csv')
  
  
def select_datafusion_load(table,**kwargs):
  return ''
  
def continue_to_verificaciones(table,**kwargs):
  return ''
  
  
def select_bq_elt(table,**kwargs):
  return ''


def select_datafusion_inject(table,**kwargs):
  return ''

  
def gerentes_to_csv(project_id,bucket_name,folder,file,dest_folder,dest_file,**kwargs):
    
  client = storage.Client(project=project_id)
    
  # Obtener el bucket
  bucket = client.bucket(bucket_name)
    
  # Obtener el blob (archivo)
  blob = bucket.blob(f'{folder}/{file}')
    
  # Descargar el archivo como bytes
  excel_data = blob.download_as_bytes()
  
  sheet_name = 'Hoja1'

  column_names = [
    'CODIGO_OFICINA_OFICINA',
    'CODIGO_GERENTE_GERENTE',
    'CODIGO_AGENTE',
    'AGENTE',
  ]

  dtypes = {
    'CODIGO_OFICINA_OFICINA': 'string',
    'CODIGO_GERENTE_GERENTE': 'string',
    'CODIGO_AGENTE': 'string',
    'AGENTE': 'string',
  }
  
  df = pd.read_excel(excel_data,engine='openpyxl',sheet_name=sheet_name,skiprows=1,header=None,names=column_names,index_col=False,dtype=dtypes)

  df[['CODIGO_OFICINA', 'OFICINA']] = df['CODIGO_OFICINA_OFICINA'].str.split(' ', n=1, expand=True)
  df[['CODIGO_GERENTE', 'GERENTE']] = df['CODIGO_GERENTE_GERENTE'].str.split(' ', n=1, expand=True)

  df = df.drop(columns=['CODIGO_OFICINA_OFICINA'])
  df = df.drop(columns=['CODIGO_GERENTE_GERENTE'])
  
  df = df[['CODIGO_OFICINA' ,'CODIGO_GERENTE' ,'GERENTE']].copy().drop_duplicates(subset=['CODIGO_GERENTE'])

  csv = df.to_csv(index=False)

  output_path = f"{dest_folder}/{dest_file}"
  out_blob = bucket.blob(output_path)
  out_blob.upload_from_string(csv, content_type='text/csv')
  
  
def claves_ctas_especiales_to_csv(project_id,bucket_name,folder,file,dest_folder,dest_file,**kwargs):
  
  client = storage.Client(project=project_id)
    
  # Obtener el bucket
  bucket = client.bucket(bucket_name)
    
  # Obtener el blob (archivo)
  blob = bucket.blob(f'{folder}/{file}')
    
  # Descargar el archivo como bytes
  excel_data = blob.download_as_bytes()
  
  sheet_name = 'Hoja1'

  column_names = [
    'CUENTA'
    ,'CVE_AGENTE'
    ,'EJECUTIVA'
  ]

  dtypes = {
    'CUENTA': 'string',
    'CVE_AGENTE': 'string',
    'EJECUTIVA': 'string',
  }
  
  df = pd.read_excel(excel_data,engine='openpyxl',sheet_name=sheet_name,skiprows=1,header=None,names=column_names,index_col=False,dtype=dtypes)


  csv = df.to_csv(index=False)

  output_path = f"{dest_folder}/{dest_file}"
  out_blob = bucket.blob(output_path)
  out_blob.upload_from_string(csv, content_type='text/csv')
  
def catalogo_direccion_comercial_to_csv(project_id,bucket_name,folder,file,dest_folder,dest_file,**kwargs):

  client = storage.Client(project=project_id)
    
  # Obtener el bucket
  bucket = client.bucket(bucket_name)
    
  # Obtener el blob (archivo)
  blob = bucket.blob(f'{folder}/{file}')
    
  # Descargar el archivo como bytes
  excel_data = blob.download_as_bytes()
  
  sheet_name = 'CAT_OFICINAS'

  column_names = [
    'NO_OF'
    ,'OFICINA'
    ,'ZONA_ATENCION'
    ,'DIRECTOR_GENERAL'
    ,'SUBDIRECTOR_GENERAL'
  ]

  dtypes = {
    'NO_OF': 'string',
    'OFICINA': 'string',
    'ZONA_ATENCION': 'string',
    'DIRECTOR_GENERAL': 'string',
    'SUBDIRECTOR_GENERAL': 'string',
  }
  
  df = pd.read_excel(excel_data,engine='openpyxl',sheet_name=sheet_name,skiprows=1,header=None,names=column_names,index_col=False,dtype=dtypes)

  csv = df.to_csv(index=False)

  output_path = f"{dest_folder}/{dest_file}"
  out_blob = bucket.blob(output_path)
  out_blob.upload_from_string(csv, content_type='text/csv')