from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import requests

# Obtener la IP pública de la instancia EC2 con manejo de errores
try:
    EC2_PUBLIC_IP = requests.get("http://169.254.169.254/latest/meta-data/public-ipv4", timeout=5).text
except requests.RequestException:
    EC2_PUBLIC_IP = "localhost"  # Fallback si no se puede obtener la IP

MINIO_ENDPOINT = f"http://{EC2_PUBLIC_IP}:9000"

# Inicializar Spark con soporte para Hudi
spark = SparkSession.builder \
    .appName("Load Parquet to Hudi") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.region", "us-east-1") \
    .getOrCreate()

# Leer Parquet desde MinIO
parquet_path = "s3a://hudi-processed-data/parquet/"
df = spark.read.option("recursiveFileLookup", "true").parquet(parquet_path)

# Escribir en Hudi
hudi_table_path = "s3a://hudi-tables/walmart_sales_hudi/"
hudi_options = {
    "hoodie.table.name": "walmart_sales_hudi",
    "hoodie.datasource.write.recordkey.field": "Store,Date",
    "hoodie.datasource.write.precombine.field": "Date",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    # Habilitar la sincronización con Hive Metastore en modo HMS
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "walmart_sales_hudi",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
    "hoodie.datasource.hive_sync.username": "airflow",
    "hoodie.datasource.hive_sync.password": "airflow",
    # Si tu tabla está particionada, especifica los campos de partición (opcional)
    # "hoodie.datasource.hive_sync.partition_fields": "partition_column1,partition_column2",
}

df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_table_path)

print("✅ Datos cargados en Apache Hudi con éxito")
spark.stop()
