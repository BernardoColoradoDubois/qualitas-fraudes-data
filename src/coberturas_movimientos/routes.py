from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.coberturas_movimientos.application_service import LoadCoberturasMovimientos
from src.coberturas_movimientos.dto import CobeberturasMovimientosDateRange

blueprint = Blueprint('coberturas_movimientos_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_coberturas_movimientos_route(load_coberturas_movimientos: LoadCoberturasMovimientos = Provide[DIContainer.load_coberturas_movimientos]):

  payload=request.get_json(force=True)

  dto = CobeberturasMovimientosDateRange(
    init_date=payload["init-date"],
    final_date=payload["final-date"]
  )

  response = load_coberturas_movimientos.invoque(dto=dto)    
  
  return jsonify(response), 201, {'ContentType':'application/json'}