import os
from dotenv import load_dotenv

from google.oauth2.service_account import Credentials as GoogleCloudCredentials
from google.cloud.bigquery import Client
import pandas as pd
import cx_Oracle


# Cargamos las variables de entorno desde el archivo .env
load_dotenv()

# Accedemos a las variables de entorno de google
key_file_path = os.getenv("GCP_LOCAL_JSON_CREDENTIALS_PATH")

# Accedemos a las variables de entorno de oracle
oracle_user = os.getenv("APPLICATION_ORACLE_USER")
oracle_password = os.getenv("APPLICATION_ORACLE_PASSWORD")
oracle_host = os.getenv("APPLICATION_ORACLE_HOST")
oracle_port = os.getenv("APPLICATION_ORACLE_PORT")
oracle_service = os.getenv("APPLICATION_ORACLE_SERVICE")

# Inicializamos el cliente de BigQuery
credentials = GoogleCloudCredentials.from_service_account_file(filename=key_file_path)
client = Client(credentials=credentials)

# Realizamos una consulta a BigQuery y cargamos los resultados en un DataFrame`
query = "SELECT * FROM DM_FRAUDES.DM_POLIZAS_VIGENTES LIMIT 50000"
query_job = client.query(query)
result = query_job.result()

#  Convertimos el resultado a un DataFrame
df = result.to_dataframe()
df = df.replace({pd.NA: None, float('nan'): None})

# Convertimos el DataFrame a una lista de tuplas
dt = [tuple(x) for x in df.values]

# Conectamos a Oracle y cargamos los datos
conn_string = f'{oracle_user}/{oracle_password}@{oracle_host}:{oracle_port}/{oracle_service}'
connection = cx_Oracle.connect(conn_string)
cursor = connection.cursor()
cursor.execute('TRUNCATE TABLE INSUMOS.DM_POLIZAS_VIGENTES')
sql="INSERT INTO INSUMOS.DM_POLIZAS_VIGENTES" + " VALUES("+",".join([f":{i+1}" for i in range(df.shape[1])])+")"
cursor.executemany(sql, dt)
connection.commit()
cursor.close()
