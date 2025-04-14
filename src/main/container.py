from dependency_injector import containers, providers
from google.oauth2.service_account import Credentials as GoogleCloudCredentials
from google.cloud.bigquery import Client
import cx_Oracle
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.lib.password_encrypt import APIKeyValidator

from src.analistas.application_service import LoadAnalistas
from src.asegurados.application_service import LoadAsegurados
from src.causas.application_service import LoadCausas
from src.coberturas_movimientos.application_service import LoadCoberturasMovimientos
from src.estados.application_service import LoadEstados
from src.etiqueta_siniestro.application_service import LoadEtiquetaSiniestro
from src.incisos_polizas.application_service import LoadIncisoPolizas
from src.oficinas.application_service import LoadOficinas
from src.pagos_polizas.application_service import LoadPagosPolizas
from src.pagos_proveedores.application_service import LoadPagosProveedores
from src.polizas_vigentes.application_service import LoadPolizasVigentes
from src.proveedores.application_service import LoadProveedores
from src.registro.application_service import LoadRegistro
from src.tipos_proveedores.application_service import LoadTiposProveedores
from src.siniestros.application_service import LoadSiniestros


class DIContainer(containers.DeclarativeContainer):
  
  # configuraciones basicas y llaves
  config = providers.Configuration()
  credentials = providers.Singleton(GoogleCloudCredentials.from_service_account_file, filename=config.key_file_path)
  
  # clientes y conectores
  client = providers.Singleton(Client, credentials=credentials)
  connection = providers.Singleton(cx_Oracle.connect, config.connection_string)
  
  # infraestructuras
  bigquery_to_oracle = providers.Factory(BigQueryToOracle, bq_client=client, oracle_client=connection)
  api_key_validator = providers.Factory(APIKeyValidator, hashed_api_key=config.hashed_api_key)
  
  # servicios de aplicacion
  load_analistas = providers.Factory(LoadAnalistas, bigquery_to_oracle=bigquery_to_oracle)
  load_asegurados = providers.Factory(LoadAsegurados, bigquery_to_oracle=bigquery_to_oracle)
  load_causas = providers.Factory(LoadCausas, bigquery_to_oracle=bigquery_to_oracle)
  load_coberturas_movimientos = providers.Factory(LoadCoberturasMovimientos, bigquery_to_oracle=bigquery_to_oracle)
  load_etiqueta_siniestro = providers.Factory(LoadEtiquetaSiniestro, bigquery_to_oracle=bigquery_to_oracle)
  load_estados = providers.Factory(LoadEstados, bigquery_to_oracle=bigquery_to_oracle)
  load_incisos_polizas = providers.Factory(LoadIncisoPolizas, bigquery_to_oracle=bigquery_to_oracle)
  load_oficinas = providers.Factory(LoadOficinas, bigquery_to_oracle=bigquery_to_oracle)
  load_pagos_polizas = providers.Factory(LoadPagosPolizas, bigquery_to_oracle=bigquery_to_oracle)
  load_pagos_proveedores = providers.Factory(LoadPagosProveedores, bigquery_to_oracle=bigquery_to_oracle)
  load_polizas_vigentes = providers.Factory(LoadPolizasVigentes, bigquery_to_oracle=bigquery_to_oracle)
  load_proveedores = providers.Factory(LoadProveedores, bigquery_to_oracle=bigquery_to_oracle)
  load_registro = providers.Factory(LoadRegistro, bigquery_to_oracle=bigquery_to_oracle)
  load_tipos_proveedores = providers.Factory(LoadTiposProveedores, bigquery_to_oracle=bigquery_to_oracle)
  load_siniestros = providers.Factory(LoadSiniestros, bigquery_to_oracle=bigquery_to_oracle)


