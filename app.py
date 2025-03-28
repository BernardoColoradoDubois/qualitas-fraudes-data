from flask import Flask
from src.main.container import DIContainer
from dotenv import load_dotenv
import os

import src.main.routes as main_routes
import src.causas.routes as causas_routes
import src.proveedores.routes as proveedores_routes
import src.oficinas.routes as oficinas_routes
import src.pagos_proveedores.routes as pagos_proveedores_routes

# Cargar variables de entorno siempre
load_dotenv()

# Acceder a las variables de entorno
key_file_path = os.getenv("GCP_LOCAL_JSON_CREDENTIALS_PATH")
oracle_user = os.getenv("APPLICATION_ORACLE_USER")
oracle_password = os.getenv("APPLICATION_ORACLE_PASSWORD")
oracle_host = os.getenv("APPLICATION_ORACLE_HOST")
oracle_port = os.getenv("APPLICATION_ORACLE_PORT")
oracle_service = os.getenv("APPLICATION_ORACLE_SERVICE")

# Acceder a las variables de entorno
hashed_api_key = os.getenv("FLASK_HASHED_API_KEY")

# Inicializar el contenedor
container = DIContainer()    
container.config.key_file_path.override(key_file_path)
container.config.connection_string.override(f'{oracle_user}/{oracle_password}@{oracle_host}:{oracle_port}/{oracle_service}')
container.config.hashed_api_key.override(hashed_api_key)

# Wire ANTES de crear la app
container.wire(modules=[causas_routes, proveedores_routes, oficinas_routes, pagos_proveedores_routes, main_routes])

# Crear la aplicación
app = Flask(__name__)
app.register_blueprint(causas_routes.blueprint, url_prefix='/causas')
app.register_blueprint(proveedores_routes.blueprint, url_prefix='/proveedores')
app.register_blueprint(oficinas_routes.blueprint, url_prefix='/oficinas')
app.register_blueprint(pagos_proveedores_routes.blueprint, url_prefix='/pagos-proveedores')
app.register_blueprint(main_routes.blueprint, url_prefix='/')

# Solo ejecutar el servidor si se llama directamente
if __name__ == "__main__":
  app.run(debug=True,port=80)