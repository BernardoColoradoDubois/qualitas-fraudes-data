from flask import Blueprint, jsonify,request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.password_encrypt import APIKeyValidator

blueprint = Blueprint('main_routes', __name__)


@blueprint.route("/", methods=["GET"])
@inject
def root():
  
  return {
    "msg": 'OK'
  }