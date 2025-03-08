from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests

# Obtener la IP pública de la instancia EC2 con manejo de errores
try:
    EC2_PUBLIC_IP = requests.get("http://169.254.169.254/latest/meta-data/public-ipv4", timeout=5).text
except requests.RequestException:
    EC2_PUBLIC_IP = "localhost"  # Fallback si no se puede obtener la IP

MINIO_ENDPOINT = f"http://{EC2_PUBLIC_IP}:9000"

# Inicializar Spark
spark = SparkSession.builder \
    .appName("Hudi Delete Test") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# Ruta de la tabla Hudi
hudi_table_path = "s3a://hudi-tables/walmart_sales_hudi/"

# Leer la tabla Hudi antes del DELETE
df_hudi = spark.read.format("hudi").load(hudi_table_path)
print("🔍 Datos antes del DELETE:")
df_hudi.show(truncate=False)

# Crear un DataFrame con los registros a eliminar (CAST CORRECTO)
data_delete = [
    (1, "2012-03-23", 201203),
    (1, "2012-03-30", 201203)
]

columns = ["Store", "Date", "month"]
df_delete = spark.createDataFrame(data_delete, columns)

df_delete = df_delete \
    .withColumn("Store", col("Store").cast("int")) \
    .withColumn("Date", col("Date").cast("date")) \
    .withColumn("month", col("month").cast("int"))

print("🔍 Registros a eliminar:")
df_delete.show(truncate=False)

# Configurar opciones de escritura en Hudi (DELETE)
hudi_options = {
    "hoodie.table.name": "walmart_sales_hudi",
    "hoodie.datasource.write.operation": "delete",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "Store,Date",
    "hoodie.datasource.write.precombine.field": "Date",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.partitionpath.field": "month",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.auto_create_table": "true",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "walmart_sales_hudi",
    "hoodie.datasource.hive_sync.partition_fields": "month",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.SinglePartPartitionValueExtractor",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
    "hoodie.datasource.hive_sync.username": "airflow",
    "hoodie.datasource.hive_sync.password": "airflow",
    "hoodie.datasource.write.hive_style_partitioning": "false"
}

# 📌 Configuraciones para evitar errores de retención de commits
hudi_options.update({
    "hoodie.clean.automatic": "false",  # Deshabilitar la limpieza automática
    "hoodie.keep.min.commits": "12",  # Aumentado a 12 para que sea mayor que cleaner.commits.retained
    "hoodie.keep.max.commits": "15",  # Máximo número de commits antes de limpiar
    "hoodie.cleaner.commits.retained": "10",  # Se retendrán 10 commits antes de eliminar versiones antiguas
    "hoodie.clean.retain_commits": "10",  # Asegura retención suficiente para consultas incrementales
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS"  # Mantiene los commits más recientes
})

# Ejecutar el DELETE en Hudi
df_delete.write.format("hudi").options(**hudi_options).mode("append").save(hudi_table_path)

print("✅ DELETE ejecutado en Apache Hudi")

# Verificar que los datos eliminados no aparecen
df_hudi_post_delete = spark.read.format("hudi").load(hudi_table_path)
print("🔍 Datos después del DELETE:")
df_hudi_post_delete.show(truncate=False)

# Finalizar sesión de Spark
spark.stop()
