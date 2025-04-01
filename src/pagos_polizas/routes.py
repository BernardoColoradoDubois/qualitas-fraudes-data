from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.pagos_polizas.application_service import LoadPagosPolizas

blueprint = Blueprint('pagos_polizas_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_pagos_polizas_route(load_pagos_polizas: LoadPagosPolizas = Provide[DIContainer.load_pagos_polizas]):
   
  response = load_pagos_polizas.invoque()
  
  return jsonify(response), 201, {'ContentType':'application/json'}