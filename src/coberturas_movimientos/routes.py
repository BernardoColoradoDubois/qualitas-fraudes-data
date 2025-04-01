from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.lib.password_encrypt import APIKeyValidator
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.coberturas_movimientos.application_service import LoadCoberturasMovimientos

blueprint = Blueprint('coberturas_movimientos_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_coberturas_movimientos_route(load_coberturas_movimientos: LoadCoberturasMovimientos = Provide[DIContainer.load_coberturas_movimientos]):

  response = load_coberturas_movimientos.invoque()    
  
  return jsonify(response), 201, {'ContentType':'application/json'}