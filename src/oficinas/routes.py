from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.oficinas.application_service import LoadOficinas

blueprint = Blueprint('ofcinas_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_oficinas_route(load_oficinas: LoadOficinas = Provide[DIContainer.load_oficinas]):

  response = load_oficinas.invoque()
  
  return jsonify(response), 201, {'ContentType':'application/json'}