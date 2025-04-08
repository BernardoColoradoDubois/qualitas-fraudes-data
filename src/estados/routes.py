from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.estados.application_service import LoadEstados

blueprint = Blueprint('estados_routes', __name__)

@blueprint.route("/", methods=["POST"])  
@token_required
@inject
def load_estados_route(load_estados: LoadEstados = Provide[DIContainer.load_estados]):  
    
  response = load_estados.invoque()  
  
  return jsonify(response), 201, {'ContentType':'application/json'}  