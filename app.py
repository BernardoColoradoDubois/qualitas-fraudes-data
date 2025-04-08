from flask import Flask
from src.main.container import DIContainer
from dotenv import load_dotenv
import os

# Importar rutas
import src.main.routes as main_routes
import src.analistas.routes as analistas_routes
import src.asegurados.routes as asegurados_routes
import src.causas.routes as causas_routes
import src.estados.routes as estados_routes
import src.etiqueta_siniestro.routes as etiqueta_siniestro_routes
import src.proveedores.routes as proveedores_routes
import src.oficinas.routes as oficinas_routes
import src.pagos_proveedores.routes as pagos_proveedores_routes
import src.coberturas_movimientos.routes as coberturas_movimientos_routes
import src.registro.routes as registro_routes
import src.polizas_vigentes.routes as polizas_vigentes_routes
import src.siniestros.routes as siniestros_routes
import src.tipos_proveedores.routes as tipos_proveedores_routes
import src.pagos_polizas.routes as pagos_polizas_routes

# Importar middleware
import src.lib.middleware as middleware

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
container.wire(modules=[
  main_routes
  ,analistas_routes
  ,asegurados_routes
  ,causas_routes
  ,coberturas_movimientos_routes
  ,estados_routes
  ,etiqueta_siniestro_routes
  ,oficinas_routes
  ,proveedores_routes
  ,pagos_polizas_routes
  ,pagos_proveedores_routes
  ,polizas_vigentes_routes
  ,registro_routes
  ,siniestros_routes
  ,tipos_proveedores_routes
  ,middleware
])

# Crear la aplicación
app = Flask(__name__)
app.register_blueprint(main_routes.blueprint, url_prefix='/')
app.register_blueprint(analistas_routes.blueprint, url_prefix='/analistas')
app.register_blueprint(asegurados_routes.blueprint, url_prefix='/asegurados')
app.register_blueprint(causas_routes.blueprint, url_prefix='/causas')
app.register_blueprint(coberturas_movimientos_routes.blueprint, url_prefix='/coberturas-movimientos')
app.register_blueprint(estados_routes.blueprint, url_prefix='/estados')
app.register_blueprint(etiqueta_siniestro_routes.blueprint, url_prefix='/etiqueta-siniestro')
app.register_blueprint(oficinas_routes.blueprint, url_prefix='/oficinas')
app.register_blueprint(pagos_polizas_routes.blueprint, url_prefix='/pagos-polizas')
app.register_blueprint(pagos_proveedores_routes.blueprint, url_prefix='/pagos-proveedores')
app.register_blueprint(polizas_vigentes_routes.blueprint, url_prefix='/polizas-vigentes')
app.register_blueprint(proveedores_routes.blueprint, url_prefix='/proveedores')
app.register_blueprint(registro_routes.blueprint, url_prefix='/registro')
app.register_blueprint(tipos_proveedores_routes.blueprint, url_prefix='/tipos-proveedores')
app.register_blueprint(siniestros_routes.blueprint, url_prefix='/siniestros')

# Solo ejecutar el servidor si se llama directamente
if __name__ == "__main__":
  app.run(debug=True,port=80)