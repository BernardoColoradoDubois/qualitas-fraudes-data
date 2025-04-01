from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.polizas_vigentes.application_service import LoadPolizasVigentes

blueprint = Blueprint('polizas_vigentes_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_polizas_vigentes_route(load_polizas_vigentes: LoadPolizasVigentes = Provide[DIContainer.load_polizas_vigentes]):
    
  response = load_polizas_vigentes.invoque()
  
  return jsonify(response), 201, {'ContentType':'application/json'}