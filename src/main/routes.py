from flask import Blueprint, jsonify,request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.password_encrypt import APIKeyValidator

blueprint = Blueprint('main_routes', __name__)

@blueprint.route("/", methods=["GET"])
def root():
  return {
    "message": "It works!"
  }


@blueprint.route("/test", methods=["GET"])
@inject
def test(api_key_validator:APIKeyValidator = Provide[DIContainer.api_key_validator]):

  token = request.headers.get('Authorization')
  hash = api_key_validator.hashed_api_key
  
  return {
    "token": token,
    "hashed_api_key": hash,
  }
