from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.registro.application_service import LoadRegistro

blueprint = Blueprint('registro_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_registro_route(load_registro: LoadRegistro = Provide[DIContainer.load_registro]):

  response = load_registro.invoque()    
  
  return jsonify(response), 201, {'ContentType':'application/json'}