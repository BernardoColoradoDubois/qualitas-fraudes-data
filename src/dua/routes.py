from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.dua.application_service import LoadDua
from src.dua.dto import DuaDateRange

blueprint = Blueprint('dua_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_coberturas_movimientos_route(load_dua: LoadDua = Provide[DIContainer.load_dua]):

  payload=request.get_json(force=True)

  dto = DuaDateRange(
    init_date=payload["init-date"],
    final_date=payload["final-date"]
  )

  response = load_dua.invoque(dto=dto)    
  
  return jsonify(response), 201, {'ContentType':'application/json'}