from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

spark = SparkSession.builder \
    .appName("OrderDataProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
    .getOrCreate()
    
input_path = "./spark_orders/orders.csv"
df = spark.read.option("header", "true").csv(input_path)

df = df.withColumn("lat", col("lat").cast("double")) \
       .withColumn("lng", col("lng").cast("double")) \
       .withColumn("price", col("price").cast("double"))

df = df.withColumn("date", split(col("datetime"), " ").getItem(0))

deadletter_path = "./spark_orders/deadletter"
clean_path_avro = "./spark_orders/clean_avro"
clean_path_parquet = "./spark_orders/clean_parquet"

deadletter_df = df.filter((col("lat") == -999.999) | (col("lng") == -999.999) | col("menu_item").isNull())
deadletter_df.write.mode("overwrite").csv(deadletter_path)

clean_df = df.filter((col("lat") != -999.999) & (col("lng") != -999.999) & col("menu_item").isNotNull())

clean_df.write.mode("overwrite").partitionBy("date").format("avro").save(clean_path_avro)
clean_df.write.mode("overwrite").partitionBy("date").parquet(clean_path_parquet)

print("✅ Фильтрация завершена. Данные сохранены в Avro и Parquet.")
spark.stop()