from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.pagos_polizas.application_service import LoadPagosPolizas
from src.pagos_polizas.dto import PagosPolizasDateRange

blueprint = Blueprint('pagos_polizas_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_pagos_polizas_route(load_pagos_polizas: LoadPagosPolizas = Provide[DIContainer.load_pagos_polizas]):
   
  payload=request.get_json(force=True)

  dto = PagosPolizasDateRange(
      init_date=payload["init-date"],
      final_date=payload["final-date"]
    )

  response = load_pagos_polizas.invoque(dto=dto)
  
  return jsonify(response), 201, {'ContentType':'application/json'}