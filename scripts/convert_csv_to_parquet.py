from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, to_date, date_format
import requests

# Obtener la IP pública de la instancia EC2 con manejo de errores
try:
    EC2_PUBLIC_IP = requests.get("http://169.254.169.254/latest/meta-data/public-ipv4", timeout=5).text
except requests.RequestException:
    EC2_PUBLIC_IP = "localhost"  # Fallback en caso de error

# ✅ Definir el endpoint de MinIO con la IP pública obtenida
MINIO_ENDPOINT = f"http://{EC2_PUBLIC_IP}:9000"

# ✅ Iniciar sesión de Spark con configuración para MinIO
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

# 📌 Rutas en MinIO para la carga y almacenamiento de datos
csv_folder_path = "s3a://hudi-raw-data/csv/"  # Ruta de archivos CSV en MinIO
parquet_output_path = "s3a://hudi-processed-data/parquet/"  # Ruta destino de Parquet en MinIO

# Lista de los archivos CSV a convertir
months = ["2012-01", "2012-02", "2012-03", "2012-04", "2012-05"]

# 🔄 Iterar sobre los archivos mensuales y convertirlos a Parquet
for month in months:
    csv_path = f"{csv_folder_path}walmart_sales_{month}.csv"
    parquet_path = f"{parquet_output_path}walmart_sales_{month}.parquet"

    print(f"🚀 Procesando archivo: {csv_path}")

    # Leer el CSV con Spark
    df_spark = spark.read.option("header", "true").csv(csv_path)

    # 🔄 Convertir tipos de datos correctamente
    df_spark = df_spark.withColumns({
        "Store": col("Store").cast("int"),
        "Date": to_date(col("Date"), "yyyy-MM-dd"),
        "Sales": col("Sales").cast("double"),
        "Holiday_Flag": col("Holiday_Flag").cast("int"),
        "Temperature": col("Temperature").cast("double"),
        "Fuel_Price": col("Fuel_Price").cast("double"),
        "CPI": col("CPI").cast("double"),
        "Unemployment": col("Unemployment").cast("double"),
        "month": date_format(col("Date"), "yyyyMM").cast("int")  # Agregar campo `month`
    })
    
    # 🔍 Mostrar esquema actualizado después de la conversión
    print(f"🔍 Esquema de datos después de la conversión para {month}:")
    df_spark.printSchema()

    # ✅ Guardar en formato Parquet en MinIO
    df_spark.write.mode("overwrite").parquet(parquet_path)
    print(f"✅ Archivo convertido y almacenado en: {parquet_path}")

# 🚀 Cerrar la sesión de Spark al finalizar el proceso
spark.stop()
print("🎯 Proceso finalizado correctamente.")
