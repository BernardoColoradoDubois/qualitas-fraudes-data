#!/bin/bash
# Script para instalar Oracle Instant Client en Cloud Composer

# Actualizar repositorios
apt-get update -y

# Instalar dependencias necesarias
apt-get install -y libaio1 wget unzip

# Crear directorio para Oracle Client
mkdir -p /opt/oracle

cd /opt/oracle

wget https://download.oracle.com/otn_software/linux/instantclient/2370000/instantclient-basic-linux.x64-23.7.0.25.01.zip

unzip  /opt/oracle/instantclient-basic-linux.x64-23.7.0.25.01.zip

sh -c "echo /opt/oracle/instantclient_23_7> \ 
  /etc/ld.so.conf.d/oracle-instantclient.conf"

ldconfig

export LD_LIBRARY_PATH=/opt/oracle/instantclient_23_7:$LD_LIBRARY_PATH
export PATH=/opt/oracle/instantclient_23_7:$PATH