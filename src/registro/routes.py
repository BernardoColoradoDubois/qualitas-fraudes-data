from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.registro.application_service import LoadRegistro

blueprint = Blueprint('registro_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_registro_route(bigquery_to_oracle: BigQueryToOracle = Provide[DIContainer.bigquery_to_oracle]):

  response = bigquery_to_oracle.run(
    extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_REGISTRO` ORDER BY ID_SINIESTRO;", 
    preload_query="TRUNCATE TABLE INSUMOS.DM_REGISTRO",
    schema="INSUMOS",
    table="DM_REGISTRO"
  )    
  
  return jsonify(response), 201, {'ContentType':'application/json'}