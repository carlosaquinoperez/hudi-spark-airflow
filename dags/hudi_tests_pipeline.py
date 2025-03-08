from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import requests
from airflow.operators.dummy_operator import DummyOperator

# ✅ Obtener la IP pública de la instancia EC2 con manejo de errores
try:
    EC2_PUBLIC_IP = requests.get("http://169.254.169.254/latest/meta-data/public-ipv4", timeout=5).text
except requests.RequestException:
    EC2_PUBLIC_IP = "localhost"  # Fallback en caso de error

# ✅ Definir el endpoint de MinIO con la IP pública obtenida
MINIO_ENDPOINT = f"http://{EC2_PUBLIC_IP}:9000"

# 📌 Configuración de valores por defecto del DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 13),  # Fecha de inicio del DAG
    "retries": 1,  # Número de intentos en caso de fallo
}

# 📌 Definir el DAG para pruebas con Apache Hudi
dag = DAG(
    "hudi_tests_pipeline",  # Nombre del DAG en Airflow
    default_args=default_args,
    description="Pipeline para pruebas con Apache Hudi en Spark",
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

# 📌 Prueba 1: UPSERT en Hudi
upsert_sales_hudi = SparkSubmitOperator(
    task_id="upsert_sales_hudi",
    application="/opt/airflow/scripts/upsert_sales_hudi.py",  # Script de UPSERT en Hudi
    conn_id="spark_default",  # Conexión con Spark
    conf=spark_conf,  # Configuración de Spark
    packages="org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.2.5",
    dag=dag,
)

# 📌 Prueba 2: DELETE en Hudi
delete_sales_hudi = SparkSubmitOperator(
    task_id="delete_sales_hudi",
    application="/opt/airflow/scripts/delete_sales_hudi.py",  # Script de DELETE en Hudi
    conn_id="spark_default",
    conf=spark_conf,  # Configuración de Spark
    packages="org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.2.5",
    dag=dag,
)

# ✅ Definir dependencias entre tareas
# 🔗 Primero se ejecuta UPSERT, luego DELETE
upsert_sales_hudi >> delete_sales_hudi
