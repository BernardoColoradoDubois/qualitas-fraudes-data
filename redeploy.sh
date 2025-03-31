#!/bin/bash
# Script de redeploy para la aplicación

# Actualizar código (asumiendo que usas git)
cd /opt/qualitas-fraudes-data
git pull origin master

# Actualizar dependencias
source /opt/qualitas-fraudes-data/venv/bin/activate
pip install -r /opt/qualitas-fraudes-data/flask-requirements.txt
deactivate

# Reiniciar servicio
sudo systemctl restart miapp.service

# Verificar estado
sudo systemctl restart nginx

# Verificar estado
sudo systemctl status miapp.service