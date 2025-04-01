from flask import Blueprint,g, jsonify, request
from dependency_injector.wiring import inject, Provide
from src.lib.password_encrypt import APIKeyValidator
from src.main.container import DIContainer
from functools import wraps

def token_required(f):
  @wraps(f)
  @inject
  def decorated(*args, api_key_validator:APIKeyValidator=Provide[DIContainer.api_key_validator], **kwargs):
   
   auth_header = request.headers.get('Authorization')
   
   if not auth_header:
      return jsonify({'message':'Token is missing'}), 401, {'ContentType':'application/json'}
   
   token = auth_header.replace('Bearer ', '')
   
   if api_key_validator.validate(token) == False:
      return jsonify({'message':'Token is invalid'}), 401, {'ContentType':'application/json'}
   
   # Esta línea faltaba - retornar la función original cuando el token es válido
   return f(*args, **kwargs)
   
  return decorated