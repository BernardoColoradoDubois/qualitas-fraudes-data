from dependency_injector import containers, providers
from google.oauth2.service_account import Credentials as GoogleCloudCredentials
from google.cloud.bigquery import Client
import cx_Oracle
from src.lib.bigquery_to_oracle import BigQueryToOracle

class DIContainer(containers.DeclarativeContainer):
  config = providers.Configuration()
  credentials = providers.Singleton(GoogleCloudCredentials.from_service_account_file, filename=config.key_file_path)
  client = providers.Singleton(Client, credentials=credentials)
  connection = providers.Singleton(cx_Oracle.connect, config.connection_string)
  bigquery_to_oracle = providers.Factory(BigQueryToOracle, bq_client=client, oracle_client=connection)


