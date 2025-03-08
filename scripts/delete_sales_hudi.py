from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
    .appName("Hudi Delete Test") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# 📌 Ruta de la tabla Hudi en MinIO
hudi_table_path = "s3a://hudi-tables/walmart_sales_hudi/"

# ✅ Leer la tabla Hudi antes del DELETE
df_hudi = spark.read.format("hudi").load(hudi_table_path)
print("🔍 Datos antes del DELETE:")
df_hudi.show(truncate=False)

# ✅ Definir los registros a eliminar (ejemplo: eliminar registros específicos de Store 1 en marzo 2012)
data_delete = [
    (1, "2012-03-23", 201203),  # Eliminar Store 1, Fecha: 2012-03-23
    (1, "2012-03-30", 201203)   # Eliminar Store 1, Fecha: 2012-03-30
]

columns = ["Store", "Date", "month"]
df_delete = spark.createDataFrame(data_delete, columns)

# ✅ Convertir los tipos de datos correctamente
df_delete = df_delete \
    .withColumn("Store", col("Store").cast("int")) \
    .withColumn("Date", col("Date").cast("date")) \
    .withColumn("month", col("month").cast("int"))

# 🔍 Mostrar registros antes de eliminar
print("🔍 Registros a eliminar:")
df_delete.show(truncate=False)

# ✅ Configurar opciones de escritura en Apache Hudi (DELETE)
hudi_options = {
    "hoodie.table.name": "walmart_sales_hudi",
    "hoodie.datasource.write.operation": "delete",  # Modo DELETE
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

# 📌 Configuraciones para evitar errores de retención de commits
hudi_options.update({
    "hoodie.clean.automatic": "false",  # Deshabilitar la limpieza automática
    "hoodie.keep.min.commits": "12",  # Asegurar un mínimo de commits antes de limpieza
    "hoodie.keep.max.commits": "15",  # Máximo número de commits antes de limpiar
    "hoodie.cleaner.commits.retained": "10",  # Retener los últimos 10 commits
    "hoodie.clean.retain_commits": "10",  # Asegura retención suficiente para consultas incrementales
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS"  # Mantiene los commits más recientes
})

# ✅ Ejecutar el DELETE en Apache Hudi
df_delete.write.format("hudi").options(**hudi_options).mode("append").save(hudi_table_path)

print("✅ DELETE ejecutado en Apache Hudi 🚀")

# ✅ Verificar la tabla después del DELETE
df_hudi_post_delete = spark.read.format("hudi").load(hudi_table_path)
print("🔍 Datos después del DELETE:")
df_hudi_post_delete.show(truncate=False)

# 🚀 Cerrar sesión de Spark
spark.stop()
print("🎯 Proceso finalizado correctamente.")
