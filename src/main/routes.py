from flask import Blueprint, jsonify,request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer
from src.lib.password_encrypt import APIKeyValidator

blueprint = Blueprint('main_routes', __name__)

@blueprint.route("/", methods=["GET"])
@inject
def root(api_key_validator:APIKeyValidator = Provide[DIContainer.api_key_validator]):
  
  if api_key_validator.validate(request.headers.get('Authorization').replace('Bearer ', '')) == False:
    return jsonify({'message':'Forbidden'}), 403, {'ContentType':'application/json'}

  return {
    "msg": 'welcome'
  }
