from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct, sum as _sum

spark = SparkSession.builder \
    .appName("OrdersToMongoFormat") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/spark.orders") \
    .getOrCreate()

df = spark.read.format("parquet") \
    .option("recursiveFileLookup", "true") \
    .load("./spark_orders/clean_parquet")

df = df.withColumn("price", col("price").cast("double"))

orders_df = df.groupBy("Order_id", "datetime", "facility_name", "city", "country") \
    .agg(
        collect_list(struct("menu_item", "price")).alias("items"),
        _sum("price").alias("total_price")
    )

orders_df.write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .save()

print("✅ Всё! Данные успешно сохранены в MongoDB.")
spark.stop()