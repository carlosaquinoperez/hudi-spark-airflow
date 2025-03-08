from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, to_date, date_format
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

    # 🔄 Convertir tipos de datos correctamente
    df_spark = df_spark.withColumn("Store", col("Store").cast("int"))
    df_spark = df_spark.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))  # Convertir a tipo fecha
    df_spark = df_spark.withColumn("Sales", col("Sales").cast("double"))
    df_spark = df_spark.withColumn("Holiday_Flag", col("Holiday_Flag").cast("int"))
    df_spark = df_spark.withColumn("Temperature", col("Temperature").cast("double"))
    df_spark = df_spark.withColumn("Fuel_Price", col("Fuel_Price").cast("double"))
    df_spark = df_spark.withColumn("CPI", col("CPI").cast("double"))
    df_spark = df_spark.withColumn("Unemployment", col("Unemployment").cast("double"))

    # 🔹 Convertir `month` a `yyyyMM` y guardarlo como `int`
    df_spark = df_spark.withColumn("month", date_format(col("Date"), "yyyyMM").cast("int"))
    
    # 🔍 Mostrar esquema actualizado
    print(f"🔍 Schema después de conversión para {month}:")
    df_spark.printSchema()

    # Guardar en formato Parquet en MinIO
    df_spark.write.mode("overwrite").parquet(parquet_path)

# Cerrar la sesión de Spark
spark.stop()
