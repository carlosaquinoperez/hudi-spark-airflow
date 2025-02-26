#!/bin/bash
set -e

export PGPASSWORD=airflow

# Espera hasta que se pueda ejecutar un SELECT 1 sobre la base 'metastore'
until psql -h postgres -U airflow -d metastore -c "SELECT 1" >/dev/null 2>&1; do
  echo "Postgres no responde, esperando 5 segundos..."
  sleep 5
done
echo "Postgres disponible."

# Comprobación adicional de conexión a la base de datos metastore
echo "Comprobando conexión a la base de datos metastore..."
psql -h postgres -U airflow -d metastore -c "SELECT 1;"

# Inicializar el esquema del Hive Metastore
echo "Inicializando el esquema del Hive Metastore..."
schematool -initSchema -dbType postgres \
  -userName airflow \
  -passWord airflow \
  -url "jdbc:postgresql://postgres:5432/metastore"

echo "Esquema del metastore inicializado correctamente."

# Arranca el servicio normal de Hive Metastore
exec hive --service metastore
