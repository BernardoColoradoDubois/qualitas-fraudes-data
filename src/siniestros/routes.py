from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required

blueprint = Blueprint('siniestros_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_siniestros(bigquery_to_oracle: BigQueryToOracle = Provide[DIContainer.bigquery_to_oracle]):

  response = bigquery_to_oracle.run(
    extraction_query="SELECT * FROM `DM_FRAUDES.DM_SINIESTROS` WHERE CAST(FECHA_REGISTRO AS DATE) > '2025-01-01' LIMIT 10000;", 
    preload_query="TRUNCATE TABLE INSUMOS.DM_SINIESTROS",
    schema="INSUMOS",
    table="DM_SINIESTROS"
  )   

  return jsonify(response), 201, {'ContentType':'application/json'}