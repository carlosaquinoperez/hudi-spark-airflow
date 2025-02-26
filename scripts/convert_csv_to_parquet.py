from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import requests

# Obtener la IP pública de la instancia EC2 con manejo de errores
try:
    EC2_PUBLIC_IP = requests.get("http://169.254.169.254/latest/meta-data/public-ipv4", timeout=5).text
except requests.RequestException:
    EC2_PUBLIC_IP = "localhost"  # Fallback si no se puede obtener la IP

MINIO_ENDPOINT = f"http://{EC2_PUBLIC_IP}:9000"

# Iniciar sesión de Spark con configuración para MinIO
spark = SparkSession.builder \
    .appName("CSV_to_Parquet") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# Definir rutas en MinIO
csv_folder_path = "s3a://hudi-raw-data/csv/"
parquet_output_path = "s3a://hudi-processed-data/parquet/"

# Lista de los archivos CSV a convertir
months = ["2012-01", "2012-02", "2012-03", "2012-04", "2012-05"]

for month in months:
    csv_path = f"{csv_folder_path}walmart_sales_{month}.csv"
    parquet_path = f"{parquet_output_path}walmart_sales_{month}.parquet"

    # Leer el CSV con Spark
    df_spark = spark.read.option("header", "true").csv(csv_path)

    # Guardar en formato Parquet en MinIO
    df_spark.write.mode("overwrite").parquet(parquet_path)

# Cerrar la sesión de Spark
spark.stop()
