from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import requests

# Obtener la IP pública de la instancia EC2 con manejo de errores
try:
    EC2_PUBLIC_IP = requests.get("http://169.254.169.254/latest/meta-data/public-ipv4", timeout=5).text
except requests.RequestException:
    EC2_PUBLIC_IP = "localhost"  # Fallback si no se puede obtener la IP

MINIO_ENDPOINT = f"http://{EC2_PUBLIC_IP}:9000"

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("HudiSparkJob") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# Datos de prueba
data = [("1", "Carlos", 30), ("2", "Maria", 25), ("3", "Juan", 35)]
columns = ["id", "name", "age"]

# Crear DataFrame y agregar columna 'timestamp'
df = spark.createDataFrame(data, columns).withColumn("timestamp", current_timestamp())

# Imprimir el esquema del DataFrame
df.printSchema()

# Escribir en Hudi
df.write.format("hudi") \
    .option("hoodie.table.name", "hudi_table") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.precombine.field", "timestamp") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator") \
    .mode("overwrite") \
    .save("s3a://hudi-data/hudi_table")

print("✅ Proceso completado con éxito")

# Detener Spark
spark.stop()
