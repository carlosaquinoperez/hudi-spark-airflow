#!/bin/bash
set -e

export PGPASSWORD=airflow

# Espera a que PostgreSQL esté disponible
until psql -h postgres -U airflow -d metastore -c "SELECT 1" >/dev/null 2>&1; do
  echo "Postgres no responde, esperando 5 segundos..."
  sleep 5
done
echo "Postgres disponible."

# Comprobación de conexión a la base de datos metastore
echo "Comprobando conexión a la base de datos metastore..."
psql -h postgres -U airflow -d metastore -c "SELECT 1;"

# Verificar si el esquema del metastore ya está inicializado
# (se asume que la tabla 'bucketing_cols' forma parte del esquema de Hive)
EXISTS=$(psql -h postgres -U airflow -d metastore -tAc "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'bucketing_cols');")
if [ "$EXISTS" = "t" ]; then
  echo "El esquema del metastore ya está inicializado, omitiendo la inicialización."
else
  echo "Inicializando el esquema del Hive Metastore..."
  schematool -initSchema -dbType postgres \
    -userName airflow \
    -passWord airflow \
    -url "jdbc:postgresql://postgres:5432/metastore"
  echo "Esquema del metastore inicializado correctamente."
fi

# Arranca el servicio normal de Hive Metastore
exec hive --service metastore
