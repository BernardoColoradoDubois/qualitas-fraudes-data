from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.lib.password_encrypt import APIKeyValidator
from src.main.container import DIContainer

blueprint = Blueprint('siniestros_routes', __name__)

# DM_REGISTRO
@blueprint.route("/", methods=["POST"])
@inject
def load_siniestros(api_key_validator:APIKeyValidator = Provide[DIContainer.api_key_validator],bigquery_to_oracle: BigQueryToOracle = Provide[DIContainer.bigquery_to_oracle]):

  if api_key_validator.validate(request.headers.get('Authorization').replace('Bearer ', '')) == False:
    return jsonify({'message':'Forbidden'}), 403, {'ContentType':'application/json'}

  response = bigquery_to_oracle.run(
    extraction_query="SELECT * FROM `DM_FRAUDES.DM_SINIESTROS` WHERE CAST(FECHA_REGISTRO AS DATE) > '2025-01-01' LIMIT 10000;", 
    preload_query="TRUNCATE TABLE INSUMOS.DM_SINIESTROS",
    schema="INSUMOS",
    table="DM_SINIESTROS"
  )   
  return response