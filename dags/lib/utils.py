from google.cloud import storage

def leer_archivo_bucket(bucket_name, file_path):
  try:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()
    return content
  except Exception as e:
    print(f"Error al leer el archivo: {e}")
    return None
