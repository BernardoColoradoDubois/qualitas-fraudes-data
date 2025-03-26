from flask import Flask
from src.main.container import DIContainer
from dotenv import load_dotenv
import os
from dependency_injector.wiring import Provide, inject
from src.lib.bigquery_to_oracle import BigQueryToOracle

@inject
def load_dm_causas(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_CAUSAS` ORDER BY ID", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_CAUSAS",
        schema="INSUMOS",
        table="DM_CAUSAS"
    )    

@inject
def load_dm_etiqueta_siniestro(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_ETIQUETA_SINIESTRO` ORDER BY ID", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_ETIQUETA_SINIESTRO",
        schema="INSUMOS",
        table="DM_ETIQUETA_SINIESTRO"
    )    

@inject
def load_dm_oficinas(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_OFICINAS` ORDER BY ID", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_OFICINAS",
        schema="INSUMOS",
        table="DM_OFICINAS"
    )    
    
@inject
def load_dm_pagos_polizas(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PAGOS_POLIZAS` ORDER BY ID", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_PAGOS_POLIZAS",
        schema="INSUMOS",
        table="DM_PAGOS_POLIZAS"
    )   


@inject
def load_dm_proveedores(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PROVEEDORES` ORDER BY ID", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_PROVEEDORES",
        schema="INSUMOS",
        table="DM_PROVEEDORES"
    )   
    
@inject
def load_dm_siniestros(bigquery_to_oracle=Provide[DIContainer.bigquery_to_oracle]) -> None:

    bigquery_to_oracle.run(
        extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_CAUSAS` ORDER BY ID", 
        preload_query="TRUNCATE TABLE INSUMOS.DM_CAUSAS",
        schema="INSUMOS",
        table="DM_CAUSAS"
    )   

load_dotenv()

# Accedemos a las variables de entorno de google
key_file_path = os.getenv("GCP_LOCAL_JSON_CREDENTIALS_PATH")
    
#Accedemos a las variables de entorno de oracle
oracle_user = os.getenv("APPLICATION_ORACLE_USER")
oracle_password = os.getenv("APPLICATION_ORACLE_PASSWORD")
oracle_host = os.getenv("APPLICATION_ORACLE_HOST")
oracle_port = os.getenv("APPLICATION_ORACLE_PORT")
oracle_service = os.getenv("APPLICATION_ORACLE_SERVICE")
        
container = DIContainer()    
container.config.key_file_path.override(key_file_path)
container.config.connection_string.override(f'{oracle_user}/{oracle_password}@{oracle_host}:{oracle_port}/{oracle_service}')
container.wire(modules=[__name__])

#big_query_to_oracle = container.big_query_to_oracle()

#big_query_to_oracle.run(
#    extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_CAUSAS` ORDER BY ID", 
#    preload_query="TRUNCATE TABLE INSUMOS.DM_CAUSAS",
#    schema="INSUMOS",
#    table="DM_CAUSAS"
#)

load_dm_causas()
load_dm_etiqueta_siniestro()
load_dm_oficinas()
load_dm_pagos_polizas()
load_dm_proveedores()
load_dm_siniestros()