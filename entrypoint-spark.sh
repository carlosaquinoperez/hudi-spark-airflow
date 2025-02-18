#!/bin/bash

# Configurar variables de entorno de Java y Spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

# Validar el parámetro de rol (master/worker)
ROLE=${1:-master}  # Si no se pasa argumento, usa 'master' por defecto

# 🛠 Obtener la IP pública desde la variable de entorno
EC2_PUBLIC_IP=${EC2_PUBLIC_IP:-"127.0.0.1"}   # 🚀 Usa 127.0.0.1 si no está definida

echo "🌍 La IP pública de la instancia es: $EC2_PUBLIC_IP"

# Configurar la URL de MinIO
export MINIO_ENDPOINT="http://$EC2_PUBLIC_IP:9000"
export SPARK_HADOOP_OPTS="-Dspark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT"

# ✅ Verificar la configuración
echo "🔹 MinIO Endpoint configurado en: $MINIO_ENDPOINT"

# 🛠 Iniciar Spark con el rol correcto
if [ "$SPARK_MODE" = "master" ]; then
    echo "🚀 Iniciando Spark Master..."
    exec /opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.master.Master
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "🚀 Iniciando Spark Worker..."
    exec /opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
else
    echo "❌ Modo de Spark desconocido: $SPARK_MODE"
    exit 1
fi
