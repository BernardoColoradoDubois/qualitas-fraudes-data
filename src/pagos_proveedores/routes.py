from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.pagos_proveedores.application_service import LoadPagosProveedores
from src.pagos_proveedores.dto import PagosProveedoresDateRange

blueprint = Blueprint('pagos_proveedores_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_pagos_proveedores_route(load_pagos_proveedores: LoadPagosProveedores = Provide[DIContainer.load_pagos_proveedores]):
  
  payload=request.get_json(force=True)

  dto = PagosProveedoresDateRange(
      init_date=payload["init-date"],
      final_date=payload["final-date"]
    )
  response = load_pagos_proveedores.invoque(dto=dto)  
  
  return jsonify(response), 201, {'ContentType':'application/json'}