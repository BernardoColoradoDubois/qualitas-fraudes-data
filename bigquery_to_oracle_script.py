import os
from dotenv import load_dotenv

from google.oauth2.service_account import Credentials as GoogleCloudCredentials
from google.cloud.bigquery import Client
from sqlalchemy import create_engine
import pandas as pd
import oracledb


# Cargamos las variables de entorno desde el archivo .env
load_dotenv()

# Accedemos a las variables de entorno
key_file_path = os.getenv("GCP_LOCAL_JSON_CREDENTIALS_PATH")
oracle_user = os.getenv("APPLICATION_ORACLE_USER")
oracle_password = os.getenv("APPLICATION_ORACLE_PASSWORD")
oracle_host = os.getenv("APPLICATION_ORACLE_HOST")
oracle_port = os.getenv("APPLICATION_ORACLE_PORT")
oracle_service = os.getenv("APPLICATION_ORACLE_SERVICE")

credentials = GoogleCloudCredentials.from_service_account_file(filename=key_file_path)

client = Client(credentials=credentials)

query = "SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_CAUSAS`;"

query_job = client.query(query)

result = query_job.result()

df = result.to_dataframe()

print(df.head())


dsn = f"{oracle_host}:{oracle_port}/{oracle_service}"

connection = oracledb.connect(
            user=oracle_user,
            password=oracle_password,
            dsn=dsn
        )


engine = create_engine(
            'oracle://', 
            creator=lambda: connection
        )

df.to_sql(
            name='DM_CAUSAS',
            con=engine,
            schema='INSUMOS',
            if_exists='append',  # Append to the table if it exists
            index=False,
            chunksize=1000
        )