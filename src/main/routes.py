from flask import Blueprint, jsonify
from dependency_injector.wiring import inject, Provide

blueprint = Blueprint('main_routes', __name__)
@blueprint.route("/", methods=["GET"])
@inject
def root():
  return {
    "message": "It works!"
  }
