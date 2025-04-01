from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.siniestros.application_service import LoadSiniestros

blueprint = Blueprint('siniestros_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_siniestros_route(load_siniestros: LoadSiniestros = Provide[DIContainer.load_siniestros]):

  response = load_siniestros.invoque()

  return jsonify(response), 201, {'ContentType':'application/json'}