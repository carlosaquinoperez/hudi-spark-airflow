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

# 🔍 Verificar esquema antes de escribir en Hudi
print("✅ Schema final antes de cargar en Hudi:")
df.printSchema()

# Escribir en Hudi
hudi_table_path = "s3a://hudi-tables/walmart_sales_hudi/"
hudi_options = {
    "hoodie.table.name": "walmart_sales_hudi",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",

    # Claves:
    "hoodie.datasource.write.recordkey.field": "Store,Date",
    "hoodie.datasource.write.precombine.field": "Date",
    # Necesitas un key generator que soporte 'Store,Date'
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    # Columna de partición (una sola):
    "hoodie.datasource.write.partitionpath.field": "month",
    # Sincronización con Hive usando HMS
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.auto_create_table": "true",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "walmart_sales_hudi",
    "hoodie.datasource.hive_sync.partition_fields": "month",
    # Para 1 sola partición, mejor SinglePartPartitionPathExtractor
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.SinglePartPartitionValueExtractor",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
    "hoodie.datasource.hive_sync.username": "airflow",
    "hoodie.datasource.hive_sync.password": "airflow",
    # Opcional para asegurarte de que ponga las rutas como month=2012-01
    "hoodie.datasource.write.hive_style_partitioning": "false"
}


df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_table_path)

print("✅ Datos cargados en Apache Hudi con éxito")
spark.stop()
