from flask import Blueprint, jsonify,request
from dependency_injector.wiring import inject, Provide
from src.main.container import DIContainer

blueprint = Blueprint('main_routes', __name__)

@blueprint.route("/", methods=["GET"])
@inject
def root():
  return {
    "message": "It works!"
  }


@blueprint.route("/test", methods=["GET"])
@inject
def test(hashed_api_key = Provide[DIContainer.hashed_api_key]):

  token = request.headers.get('Authorization')
  
  return {
    "token": token,
    "hashed_api_key": hashed_api_key
  }
