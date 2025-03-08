from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import requests

# ✅ Obtener la IP pública de la instancia EC2 con manejo de errores
try:
    EC2_PUBLIC_IP = requests.get("http://169.254.169.254/latest/meta-data/public-ipv4", timeout=5).text
except requests.RequestException:
    EC2_PUBLIC_IP = "localhost"  # Fallback en caso de error

# ✅ Definir el endpoint de MinIO con la IP pública obtenida
MINIO_ENDPOINT = f"http://{EC2_PUBLIC_IP}:9000"

# ✅ Iniciar sesión de Spark con soporte para Hudi y MinIO
spark = SparkSession.builder \
    .appName("Load Parquet to Hudi") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.region", "us-east-1") \
    .getOrCreate()

# 📌 Ruta de datos en MinIO
parquet_path = "s3a://hudi-processed-data/parquet/"

# ✅ Leer todos los archivos Parquet de la carpeta (recursivamente)
df = spark.read.option("recursiveFileLookup", "true").parquet(parquet_path)

# 🔍 Verificar el esquema antes de escribir en Hudi
print("✅ Schema final antes de cargar en Hudi:")
df.printSchema()

# 📌 Ruta destino en Hudi
hudi_table_path = "s3a://hudi-tables/walmart_sales_hudi/"

# ✅ Configuración de escritura en Apache Hudi
hudi_options = {
    "hoodie.table.name": "walmart_sales_hudi",
    "hoodie.datasource.write.operation": "upsert",  # Upsert para mantener los datos actualizados
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",  # Tipo de tabla en Hudi

    # 🔑 Configuración de claves
    "hoodie.datasource.write.recordkey.field": "Store,Date",  # Claves compuestas
    "hoodie.datasource.write.precombine.field": "Date",  # Campo de resolución de conflictos
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",  # Generador de claves

    # 📌 Configuración de particiones
    "hoodie.datasource.write.partitionpath.field": "month",  # Particionamiento por mes
    "hoodie.datasource.write.hive_style_partitioning": "false",  # Evita rutas en formato `month=202401`

    # 🔗 Sincronización con Hive Metastore
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",  # Usar Hive Metastore (HMS)
    "hoodie.datasource.hive_sync.auto_create_table": "true",  # Crear la tabla si no existe
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "walmart_sales_hudi",
    "hoodie.datasource.hive_sync.partition_fields": "month",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.SinglePartPartitionValueExtractor",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
    "hoodie.datasource.hive_sync.username": "airflow",
    "hoodie.datasource.hive_sync.password": "airflow",
}

# ✅ Escribir los datos en Hudi
df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_table_path)

print("✅ Datos cargados en Apache Hudi con éxito 🚀")

# 🚀 Cerrar sesión de Spark
spark.stop()
print("🎯 Proceso finalizado correctamente.")
