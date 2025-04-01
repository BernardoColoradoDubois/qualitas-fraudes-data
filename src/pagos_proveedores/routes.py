from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required

blueprint = Blueprint('pagos_proveedores_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_pagos_proveedores(bigquery_to_oracle: BigQueryToOracle = Provide[DIContainer.bigquery_to_oracle]):

  response = bigquery_to_oracle.run(
    extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PAGOS_PROVEEDORES` ORDER BY ID LIMIT 10000;", 
    preload_query="TRUNCATE TABLE INSUMOS.DM_PAGOS_PROVEEDORES",
    schema="INSUMOS",
    table="DM_PAGOS_PROVEEDORES"
  )    
  
  return jsonify(response), 201, {'ContentType':'application/json'}