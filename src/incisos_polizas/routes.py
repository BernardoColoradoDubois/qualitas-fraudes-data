from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.incisos_polizas.application_service import LoadIncisoPolizas
from src.incisos_polizas.dto import IncisosPolizasDateRange

blueprint = Blueprint('incisos_polizas_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_incisos_polizas(load_incisos_polizas: LoadIncisoPolizas = Provide[DIContainer.load_incisos_polizas]):

  payload=request.get_json(force=True)

  dto = IncisosPolizasDateRange(
    init_date=payload["init-date"],
    final_date=payload["final-date"]
  )

  response = load_incisos_polizas.invoque(dto=dto)
     
  return jsonify(response), 201, {'ContentType':'application/json'}