from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.registro.application_service import LoadRegistro
from src.registro.dto import RegistroDateRange

blueprint = Blueprint('registro_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_registro_route(load_registro: LoadRegistro = Provide[DIContainer.load_registro]):
  
  payload=request.get_json(force=True)

  dto = RegistroDateRange(
      init_date=payload["init-date"],
      final_date=payload["final-date"]
    )
  
  response = load_registro.invoque(dto=dto)    
  
  return jsonify(response), 201, {'ContentType':'application/json'}