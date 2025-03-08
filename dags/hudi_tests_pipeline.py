from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import requests
from airflow.operators.dummy_operator import DummyOperator

# Obtener la IP pública de la instancia EC2 con manejo de errores
try:
    EC2_PUBLIC_IP = requests.get("http://169.254.169.254/latest/meta-data/public-ipv4", timeout=5).text
except requests.RequestException:
    EC2_PUBLIC_IP = "localhost"  # Fallback si no se puede obtener la IP

MINIO_ENDPOINT = f"http://{EC2_PUBLIC_IP}:9000"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 13),
    "retries": 1,
}

dag = DAG(
    "hudi_tests_pipeline",
    default_args=default_args,
    description="Pipeline para pruebas con Apache Hudi en Spark",
    schedule_interval=None,
    catchup=False,
)

# ✅ Prueba 1: UPSERT en Hudi
upsert_sales_hudi = DummyOperator(
    task_id="upsert_sales_hudi",
    dag=dag,
)

""" upsert_sales_hudi = SparkSubmitOperator(
    task_id="upsert_sales_hudi",
    application="/opt/airflow/scripts/upsert_sales_hudi.py",
    conn_id="spark_default",
    conf={
        "spark.master": "spark://spark-master:7077",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "1g",
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": "admin",
        "spark.hadoop.fs.s3a.secret.key": "password",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    },
    packages="org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.2.5",
    dag=dag,
) """

# ✅ Prueba 2: DELETE en Hudi
""" delete_sales_hudi = DummyOperator(
    task_id="delete_sales_hudi",
    dag=dag,
) """
delete_sales_hudi = SparkSubmitOperator(
    task_id="delete_sales_hudi",
    application="/opt/airflow/scripts/delete_sales_hudi.py",
    conn_id="spark_default",
    conf={
        "spark.master": "spark://spark-master:7077",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "1g",
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": "admin",
        "spark.hadoop.fs.s3a.secret.key": "password",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    },
    packages="org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.2.5",
    dag=dag,
)

# 🔗 Dependencias entre tareas
upsert_sales_hudi >> delete_sales_hudi
