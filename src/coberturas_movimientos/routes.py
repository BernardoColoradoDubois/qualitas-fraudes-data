from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.lib.password_encrypt import APIKeyValidator
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.coberturas_movimientos.application_service import LoadCoberturasMovimientos

blueprint = Blueprint('coberturas_movimientos_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_coberturas_movimientos_route(bigquery_to_oracle: BigQueryToOracle = Provide[DIContainer.bigquery_to_oracle]):

  response = bigquery_to_oracle.run(
    extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_COBERTURAS_MOVIMIENTOS` ORDER BY ID LIMIT 10000;", 
    preload_query="TRUNCATE TABLE INSUMOS.DM_COBERTURAS_MOVIMIENTOS",
    schema="INSUMOS",
    table="DM_COBERTURAS_MOVIMIENTOS"
  )    
  
  return jsonify(response), 201, {'ContentType':'application/json'}