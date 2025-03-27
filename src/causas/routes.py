from flask import Blueprint, jsonify
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.lib.password_encrypt import APIKeyValidator
from src.main.container import DIContainer

blueprint = Blueprint('causas_routes', __name__)

@blueprint.route("/", methods=["POST"])
@inject
def load_causas(api_key_validator:APIKeyValidator = Provide[DIContainer.api_key_validator],bigquery_to_oracle: BigQueryToOracle = Provide[DIContainer.bigquery_to_oracle]):
  
  response = bigquery_to_oracle.run(
    extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_CAUSAS` ORDER BY ID", 
    preload_query="TRUNCATE TABLE INSUMOS.DM_CAUSAS",
    schema="INSUMOS",
    table="DM_CAUSAS"
  )  
  
  return response
