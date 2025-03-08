from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import requests

# ✅ Obtener la IP pública de la instancia EC2 con manejo de errores
try:
    EC2_PUBLIC_IP = requests.get("http://169.254.169.254/latest/meta-data/public-ipv4", timeout=5).text
except requests.RequestException:
    EC2_PUBLIC_IP = "localhost"  # Fallback en caso de error

# ✅ Definir el endpoint de MinIO con la IP pública obtenida
MINIO_ENDPOINT = f"http://{EC2_PUBLIC_IP}:9000"

# ✅ Inicializar Spark con soporte para Hudi y MinIO
spark = SparkSession.builder \
    .appName("Hudi Upsert Test") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.region", "us-east-1") \
    .getOrCreate()

# 📌 Ruta de la tabla Hudi en MinIO
hudi_table_path = "s3a://hudi-tables/walmart_sales_hudi/"

# ✅ Leer la tabla Hudi actual para verificar el esquema
df_hudi = spark.read.format("hudi").load(hudi_table_path)
print("🔍 Esquema de la tabla Hudi antes del UPSERT:")
df_hudi.printSchema()

# ✅ Datos para UPSERT (modificación e inserción)
data = [
    (1, "2012-02-03", 2000000.00, 0, 35.5, 3.20, 220.0, 7.8, 201202),  # Actualización de Store 1
    (29, "2012-02-10", 600000.00, 1, 33.8, 3.40, 222.5, 7.5, 201202),  # Actualización de Store 29
    (50, "2012-02-18", 300000.00, 0, 32.5, 3.50, 210.0, 7.2, 201202)   # Nueva inserción (Store 50)
]

columns = ["Store", "Date", "Sales", "Holiday_Flag", "Temperature", "Fuel_Price", "CPI", "Unemployment", "month"]
df_upsert = spark.createDataFrame(data, columns)

# 🔍 Mostrar DataFrame antes del UPSERT
print("🔍 Datos antes del UPSERT:")
df_upsert.show(truncate=False)

# ✅ Convertir tipos de datos correctamente
df_upsert = df_upsert \
    .withColumn("Store", col("Store").cast("int")) \
    .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd")) \
    .withColumn("Sales", col("Sales").cast("double")) \
    .withColumn("Holiday_Flag", col("Holiday_Flag").cast("int")) \
    .withColumn("Temperature", col("Temperature").cast("double")) \
    .withColumn("Fuel_Price", col("Fuel_Price").cast("double")) \
    .withColumn("CPI", col("CPI").cast("double")) \
    .withColumn("Unemployment", col("Unemployment").cast("double")) \
    .withColumn("month", col("month").cast("int"))

# ✅ Configuración de escritura en Apache Hudi (UPSERT)
hudi_options = {
    "hoodie.table.name": "walmart_sales_hudi",
    "hoodie.datasource.write.operation": "upsert",  # Modo UPSERT
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",  # Formato de tabla Hudi

    # 🔑 Configuración de claves
    "hoodie.datasource.write.recordkey.field": "Store,Date",  # Claves compuestas
    "hoodie.datasource.write.precombine.field": "Date",  # Campo de resolución de conflictos
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",  # Generador de claves

    # 📌 Configuración de particiones
    "hoodie.datasource.write.partitionpath.field": "month",  # Particionamiento por mes
    "hoodie.datasource.write.hive_style_partitioning": "false",  # Evita rutas en formato `month=2012-01`

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

# 📌 Configuraciones para evitar que los commits se borren rápidamente
hudi_options.update({
    "hoodie.clean.automatic": "false",  # Deshabilitar la limpieza automática
    "hoodie.keep.min.commits": "5",  # Mínimo número de commits a retener
    "hoodie.keep.max.commits": "10",  # Máximo número de commits antes de limpieza
    "hoodie.clean.retain_commits": "10",  # Retención de commits
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS"  # Mantener los commits más recientes
})

# ✅ Escribir los datos en Hudi
df_upsert.write.format("hudi").options(**hudi_options).mode("append").save(hudi_table_path)

print("✅ Upsert realizado con éxito en Apache Hudi 🚀")

# 🔍 Verificar la tabla después del UPSERT
df_hudi_post_upsert = spark.read.format("hudi").load(hudi_table_path)
print("🔍 Datos después del UPSERT:")
df_hudi_post_upsert.show(truncate=False)

# 🚀 Cerrar sesión de Spark
spark.stop()
print("🎯 Proceso finalizado correctamente.")
