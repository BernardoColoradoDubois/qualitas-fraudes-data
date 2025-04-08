from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.siniestros.application_service import LoadSiniestros
from src.siniestros.dto import SiniestrosDateRange

blueprint = Blueprint('siniestros_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_siniestros_route(load_siniestros: LoadSiniestros = Provide[DIContainer.load_siniestros]):

  payload=request.get_json(force=True)

  dto = SiniestrosDateRange(
    init_date=payload["init-date"],
    final_date=payload["final-date"]
  )
  
  response = load_siniestros.invoque(dto=dto)

  return jsonify(response), 201, {'ContentType':'application/json'}