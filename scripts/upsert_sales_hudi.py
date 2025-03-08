from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Inicializar Spark
spark = SparkSession.builder \
    .appName("Hudi Upsert Test") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.region", "us-east-1") \
    .getOrCreate()

df_hudi = spark.read.format("hudi").load("s3a://hudi-tables/walmart_sales_hudi/")
df_hudi.printSchema()

# Datos para UPSERT (modificación e inserción)
data = [
    (1, "2012-02-03", 2000000.00, 0, 35.5, 3.20, 220.0, 7.8, 201202),  # Actualización
    (29, "2012-02-10", 600000.00, 1, 33.8, 3.40, 222.5, 7.5, 201202),  # Actualización
    (50, "2012-02-18", 300000.00, 0, 32.5, 3.50, 210.0, 7.2, 201202)  # Nueva venta
]

columns = ["Store", "Date", "Sales", "Holiday_Flag", "Temperature", "Fuel_Price", "CPI", "Unemployment", "month"]
df_upsert = spark.createDataFrame(data, columns)

print("🔍 DataFrame antes del upsert:")
df_upsert.show(truncate=False)

# **Corrección de tipos de datos**
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

# Configurar opciones de escritura en Hudi
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
    # Opcional para asegurarte de que no ponga las rutas como month=2012-01
    "hoodie.datasource.write.hive_style_partitioning": "false"
}

# 📌 Configuraciones para evitar que los commits se borren rápidamente
hudi_options.update({
    "hoodie.clean.automatic": "false",  # Deshabilitar la limpieza automática
    "hoodie.keep.min.commits": "5",  # Mínimo número de commits a retener
    "hoodie.keep.max.commits": "10", # Máximo número de commits antes de que se activen las políticas de limpieza
    "hoodie.clean.retain_commits": "10", # Número de commits a retener antes de eliminar los más antiguos
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS" # Mantiene los commits más recientes
})

# Escribir los datos en Hudi
df_upsert.write.format("hudi").options(**hudi_options).mode("append").save(hudi_table_path)

print("✅ Upsert realizado con éxito en Apache Hudi")

# Finalizar sesión de Spark
spark.stop()
