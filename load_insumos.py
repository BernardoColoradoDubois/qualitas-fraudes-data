from flask import Flask
from src.main.container import DIContainer
from dotenv import load_dotenv
import os
from dependency_injector.wiring import Provide, inject
from src.lib.bigquery_to_oracle import BigQueryToOracle

from src.analistas.application_service import LoadAnalistas
from src.causas.application_service import LoadCausas
from src.coberturas_movimientos.application_service import LoadCoberturasMovimientos
from src.etiqueta_siniestro.application_service import LoadEtiquetaSiniestro
from src.oficinas.application_service import LoadOficinas
from src.pagos_polizas.application_service import LoadPagosPolizas
from src.pagos_proveedores.application_service import LoadPagosProveedores
from src.polizas_vigentes.application_service import LoadPolizasVigentes
from src.proveedores.application_service import LoadProveedores
from src.registro.application_service import LoadRegistro
from src.siniestros.application_service import LoadSiniestros

@inject
def load_dm_causas(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    response = bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_CAUSAS` ORDER BY ID", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_CAUSAS",
        schema="INSUMOS",
        table="DM_CAUSAS"
    )    
    print(response)
    

@inject
def load_dm_etiqueta_siniestro(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    response = bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_ETIQUETA_SINIESTRO` ORDER BY ID", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_ETIQUETA_SINIESTRO",
        schema="INSUMOS",
        table="DM_ETIQUETA_SINIESTRO"
    )    
    print(response)

@inject
def load_dm_oficinas(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    response = bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_OFICINAS` ORDER BY ID", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_OFICINAS",
        schema="INSUMOS",
        table="DM_OFICINAS"
    )    
    print(response)
    
@inject
def load_dm_pagos_polizas(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    response = bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PAGOS_POLIZAS` ORDER BY ID", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_PAGOS_POLIZAS",
        schema="INSUMOS",
        table="DM_PAGOS_POLIZAS"
    )   
    print(response)


@inject
def load_dm_proveedores(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    response = bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PROVEEDORES` ORDER BY ID", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_PROVEEDORES",
        schema="INSUMOS",
        table="DM_PROVEEDORES"
    )   
    print(response)
    
@inject
def load_dm_siniestros(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    response = bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `DM_FRAUDES.DM_SINIESTROS` WHERE CAST(FECHA_REGISTRO AS DATE) > '2025-01-01' LIMIT 10000;", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_SINIESTROS",
        schema="INSUMOS",
        table="DM_SINIESTROS"
    )   
    print(response)

@inject
def load_dm_pagos_proveedores(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    response = bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PAGOS_PROVEEDORES` ORDER BY ID LIMIT 10000;", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_PAGOS_PROVEEDORES",
        schema="INSUMOS",
        table="DM_PAGOS_PROVEEDORES"
    )   
    print(response)

@inject
def load_dm_coberturas_movimientos(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    response = bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_COBERTURAS_MOVIMIENTOS` ORDER BY ID LIMIT 10000;", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_COBERTURAS_MOVIMIENTOS",
        schema="INSUMOS",
        table="DM_COBERTURAS_MOVIMIENTOS"
    )   
    print(response)

@inject
def load_dm_analistas(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    response = bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_ANALISTAS` ORDER BY ID;", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_ANALISTAS",
        schema="INSUMOS",
        table="DM_ANALISTAS"
    )   
    print(response)

@inject
def load_dm_registro(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    response = bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_REGISTRO` ORDER BY ID_SINIESTRO LIMIT 10000;", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_REGISTRO",
        schema="INSUMOS",
        table="DM_REGISTRO" 
    )   
    print(response)
    
    
# Cargamos las variables de entorno
load_dotenv()

# Accedemos a las variables de entorno de google
key_file_path = os.getenv("GCP_LOCAL_JSON_CREDENTIALS_PATH")
    
#Accedemos a las variables de entorno de oracle
oracle_user = os.getenv("APPLICATION_ORACLE_USER")
oracle_password = os.getenv("APPLICATION_ORACLE_PASSWORD")
oracle_host = os.getenv("APPLICATION_ORACLE_HOST")
oracle_port = os.getenv("APPLICATION_ORACLE_PORT")
oracle_service = os.getenv("APPLICATION_ORACLE_SERVICE")

#instanciamos y configuramos contenedor de dependencias
container = DIContainer()    
container.config.key_file_path.override(key_file_path)
container.config.connection_string.override(f'{oracle_user}/{oracle_password}@{oracle_host}:{oracle_port}/{oracle_service}')
container.wire(modules=[__name__])

#cargamos los insumos
load_dm_causas()
load_dm_etiqueta_siniestro()
load_dm_oficinas()
load_dm_pagos_polizas()
load_dm_proveedores()
load_dm_pagos_proveedores()
load_dm_coberturas_movimientos()
load_dm_analistas()
load_dm_registro()
load_dm_siniestros()
