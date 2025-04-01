from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required


blueprint = Blueprint('polizas_vigentes_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_polizas_vigentes(bigquery_to_oracle: BigQueryToOracle = Provide[DIContainer.bigquery_to_oracle]):
    
  response = bigquery_to_oracle.run(
    extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_POLIZAS_VIGENTES` ORDER BY ID", 
    preload_query="TRUNCATE TABLE INSUMOS.DM_POLIZAS_VIGENTES",
    schema="INSUMOS",
    table="DM_POLIZAS_VIGENTES"
  )  
  
  return jsonify(response), 201, {'ContentType':'application/json'}