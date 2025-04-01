from flask import Blueprint, jsonify,request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.middleware import token_required

blueprint = Blueprint('main_routes', __name__)


@blueprint.route("/", methods=["GET"])
@inject
def root():
  
  response = {
    "msg": 'OK'
  }
  
  return jsonify(response), 200, {'ContentType':'application/json'}