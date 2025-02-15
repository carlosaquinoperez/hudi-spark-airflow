from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("HudiSparkJob") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
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
