from google.cloud import storage
import re
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

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


def upload_storage_csv_to_bigquery(gcs_uri,dataset_id,table_id,schema_fields,project_id=None,write_disposition="WRITE_TRUNCATE",skip_leading_rows=1,**kwargs):

  print(f"Subiendo archivo {gcs_uri} a BigQuery")