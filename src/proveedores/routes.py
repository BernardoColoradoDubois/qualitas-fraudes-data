from flask import Blueprint, jsonify ,request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.proveedores.application_service import LoadProveedores

blueprint = Blueprint('proveedores_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_proveedores(bigquery_to_oracle: BigQueryToOracle = Provide[DIContainer.bigquery_to_oracle]):
  
  response = bigquery_to_oracle.run(
    extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PROVEEDORES` ORDER BY ID", 
    preload_query="TRUNCATE TABLE INSUMOS.DM_PROVEEDORES",
    schema="INSUMOS",
    table="DM_PROVEEDORES"
  )   
  
  return jsonify(response), 201, {'ContentType':'application/json'}