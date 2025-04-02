from flask import Blueprint, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.main.container import DIContainer
from src.lib.middleware import token_required
from src.etiqueta_siniestro.application_service import LoadEtiquetaSiniestro
from src.etiqueta_siniestro.dto import EtiquetaSiniestroDateRange

blueprint = Blueprint('etiqueta_siniestro_routes', __name__)

@blueprint.route("/", methods=["POST"])
@token_required
@inject
def load_etiqueta_siniestro_route(load_etiqueta_siniestro: LoadEtiquetaSiniestro = Provide[DIContainer.load_etiqueta_siniestro]):

  payload=request.get_json(force=True)

  dto = EtiquetaSiniestroDateRange(
      init_date=payload["init-date"],
      final_date=payload["final-date"]
    )

  response = load_etiqueta_siniestro.invoque(dto=dto)
     
  return jsonify(response), 201, {'ContentType':'application/json'}