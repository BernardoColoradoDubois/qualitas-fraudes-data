from flask import Blueprint, jsonify ,request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.proveedores.application_service import LoadProveedores

blueprint = Blueprint('proveedores_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_proveedores_route(load_proveedores: LoadProveedores = Provide[DIContainer.load_proveedores]):
  
  response = load_proveedores.invoque()  
  
  return jsonify(response), 201, {'ContentType':'application/json'}