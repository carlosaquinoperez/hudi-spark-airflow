#!/bin/bash

# Configurar variables de entorno de Java y Spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

# Esperar unos segundos para asegurarse de que la base de datos de Airflow está lista
sleep 5

echo "🔹 Verificando si la base de datos ya está inicializada..."
if [ ! -f "/opt/airflow/airflow.db" ]; then
    echo "🔹 Inicializando la base de datos de Airflow..."
    airflow db init
else
    echo "✅ La base de datos ya está inicializada."
fi

echo "🔹 Verificando si el usuario admin ya existe..."
USER_EXISTS=$(airflow users list | grep -c "admin")

if [ "$USER_EXISTS" -eq "0" ]; then
    echo "✅ Usuario admin no encontrado. Creando usuario..."
    airflow users create \
        --username admin \
        --password admin \
        --firstname Carlos \
        --lastname Admin \
        --role Admin \
        --email admin@example.com
else
    echo "✅ Usuario admin ya existe. No es necesario crearlo."
fi

# Iniciar Airflow con el comando adecuado
if [ "$1" = "webserver" ]; then
    echo "🔹 Iniciando el servidor web de Airflow..."
    exec airflow webserver
elif [ "$1" = "scheduler" ]; then
    echo "🔹 Iniciando el scheduler de Airflow..."
    exec airflow scheduler
else
    exec "$@"
fi
