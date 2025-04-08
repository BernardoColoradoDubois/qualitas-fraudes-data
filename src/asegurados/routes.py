from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.lib.password_encrypt import APIKeyValidator
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.asegurados.application_service import LoadAsegurados

blueprint = Blueprint('asegurados_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_asegurados_route(load_asegurados: LoadAsegurados = Provide[DIContainer.load_asegurados]):
    
  response = load_asegurados.invoque()  
  
  return jsonify(response), 201, {'ContentType':'application/json'}  