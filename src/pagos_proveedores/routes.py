from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.lib.password_encrypt import APIKeyValidator
from src.main.container import DIContainer

blueprint = Blueprint('pagos_proveedores_routes', __name__)

@blueprint.route("/", methods=["POST"])
@inject
def load_pagos_proveedores(api_key_validator:APIKeyValidator = Provide[DIContainer.api_key_validator],bigquery_to_oracle: BigQueryToOracle = Provide[DIContainer.bigquery_to_oracle]):

  if api_key_validator.validate(request.headers.get('Authorization').replace('Bearer ', '')) == False:
    return jsonify({'message':'Forbidden'}), 403, {'ContentType':'application/json'}

  response = bigquery_to_oracle.run(
    extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PAGOS_PROVEEDORES` ORDER BY ID LIMIT 10000;", 
    preload_query="TRUNCATE TABLE INSUMOS.DM_PAGOS_PROVEEDORES",
    schema="INSUMOS",
    table="DM_PAGOS_PROVEEDORES"
  )    
  
  return response