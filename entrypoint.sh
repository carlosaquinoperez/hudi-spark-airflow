#!/bin/bash

# Configurar variables de entorno de Java y Spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

# Esperar unos segundos para asegurarse de que la base de datos de Airflow está lista
sleep 5

# Validar si EC2_PUBLIC_IP está definida
if [ -z "$EC2_PUBLIC_IP" ]; then
    echo "⚠️ No se encontró la variable EC2_PUBLIC_IP. Usando localhost (127.0.0.1)."
    export EC2_PUBLIC_IP="127.0.0.1"
fi

echo "🌍 La IP pública de la instancia es: $EC2_PUBLIC_IP"

# Configurar la URL de MinIO en variables de entorno
export MINIO_ENDPOINT="http://$EC2_PUBLIC_IP:9000"
export SPARK_HADOOP_OPTS="-Dspark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT"

# Verificar la configuración
echo "🔹 MinIO Endpoint configurado en: $MINIO_ENDPOINT"

# Inicializar la base de datos si es necesario (USAR `db migrate` en lugar de `db init`)
echo "🔹 Migrando la base de datos de Airflow..."
airflow db upgrade

# Verificando si el usuario admin ya existe...
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

# Exportar variables para que las use Spark y Airflow
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export SPARK_HADOOP_OPTS="-Dspark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
                          -Dspark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
                          -Dspark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \
                          -Dspark.hadoop.fs.s3a.path.style.access=true \
                          -Dspark.hadoop.fs.s3a.connection.ssl.enabled=false"

# ✅ Verificación final
echo "✅ Variables de entorno configuradas correctamente."
printenv | grep S3A

# 🛠 Iniciar Airflow correctamente en el puerto 8082
if [ "$1" = "webserver" ]; then
    echo "🔹 Iniciando el servidor web de Airflow en el puerto 8082..."
    exec airflow webserver --port 8082  # ✅ Se fuerza el uso del puerto correcto
elif [ "$1" = "scheduler" ]; then
    echo "🔹 Iniciando el scheduler de Airflow..."
    exec airflow scheduler
else
    exec "$@"
fi
