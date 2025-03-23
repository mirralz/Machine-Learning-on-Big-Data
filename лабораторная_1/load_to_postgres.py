from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0,org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

df = spark.read.format("parquet") \
    .option("recursiveFileLookup", "true") \
    .load("./spark_orders/clean_parquet")

df = df.withColumn("lat", col("lat").cast("double")) \
       .withColumn("lng", col("lng").cast("double")) \
       .withColumn("price", col("price").cast("double")) \
       .withColumn("datetime", col("datetime").cast("timestamp"))


df.write.mode("overwrite").jdbc(
    url="jdbc:postgresql://localhost:5432/spark_orders",
    table="all_orders",
    properties={
        "user": "mirralz",
        "driver": "org.postgresql.Driver"
    }
)

print("✅ Всё! Данные загружены в таблицу all_orders.")
spark.stop()