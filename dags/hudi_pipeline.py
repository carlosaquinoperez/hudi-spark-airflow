from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import requests

# Obtener la IP pública de la instancia EC2 con manejo de errores
try:
    EC2_PUBLIC_IP = requests.get("http://169.254.169.254/latest/meta-data/public-ipv4", timeout=5).text
except requests.RequestException:
    EC2_PUBLIC_IP = "localhost"  # Fallback si no se puede obtener la IP

MINIO_ENDPOINT = f"http://{EC2_PUBLIC_IP}:9000"

# 📌 Configuración de valores por defecto del DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 13),  # Fecha de inicio del DAG
    "retries": 1,  # Número de intentos en caso de fallo
}

# 📌 Definir el DAG para procesamiento con Apache Hudi
dag = DAG(
    "hudi_pipeline",  # Nombre del DAG en Airflow
    default_args=default_args,
    description="Pipeline para procesamiento con Apache Hudi en Spark",
    schedule_interval=None,  # Se ejecuta manualmente
    catchup=False,  # Evita la ejecución de tareas pasadas
)

# ✅ Configuración común de Spark (Evita duplicación de código)
spark_conf = {
    "spark.master": "spark://spark-master:7077",  # Dirección del Spark Master
    "spark.executor.memory": "2g",  # Memoria del ejecutor
    "spark.driver.memory": "1g",  # Memoria del driver
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,  # Conexión a MinIO
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password",
    "spark.hadoop.fs.s3a.path.style.access": "true",  # Usa rutas estilo S3
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}

# 📌 Tarea 1: Convertir archivos CSV a formato Parquet
convert_csv_to_parquet = SparkSubmitOperator(
    task_id="convert_csv_to_parquet",  # Nombre de la tarea en Airflow
    application="/opt/airflow/scripts/convert_csv_to_parquet.py",  # Ruta del script a ejecutar
    conn_id="spark_default",  # Conexión con Spark
    conf=spark_conf,  # Configuración de Spark
    packages="org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hadoop:hadoop-aws:3.3.4",  # Librerías necesarias
    dag=dag,  # Asigna la tarea al DAG
)

# 📌 Tarea 2: Cargar los datos Parquet en Apache Hudi
load_parquet_to_hudi = SparkSubmitOperator(
    task_id="load_parquet_to_hudi",
    application="/opt/airflow/scripts/load_parquet_to_hudi.py",
    conn_id="spark_default",
    conf={
        **spark_conf,  # Reutiliza la configuración de Spark
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    },
    packages="org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.2.5",
    dag=dag,
)

# ✅ Definir dependencias entre tareas
convert_csv_to_parquet >> load_parquet_to_hudi  # Primero convierte CSV, luego carga a Hudi
