from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.causas.application_service import LoadCausas

blueprint = Blueprint('causas_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_causas_route(load_causas: LoadCausas = Provide[DIContainer.load_causas]):
    
  response = load_causas.invoque()  
  
  return jsonify(response), 201, {'ContentType':'application/json'}