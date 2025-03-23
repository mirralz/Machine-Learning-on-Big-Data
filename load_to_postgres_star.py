from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

spark = SparkSession.builder \
    .appName("WriteStarSchemaToPostgres") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0,org.postgresql:postgresql:42.7.1") \
    .getOrCreate()


df = spark.read.format("parquet") \
    .option("recursiveFileLookup", "true") \
    .load("./spark_orders/clean_parquet")

# кастим

df = df.withColumn("lat", col("lat").cast("double")) \
       .withColumn("lng", col("lng").cast("double")) \
       .withColumn("price", col("price").cast("double")) \
       .withColumn("datetime", col("datetime").cast("timestamp"))

# таблицы-измерения с ID

cities_dim = df.select("city", "country").distinct() \
    .withColumn("city_id", monotonically_increasing_id())

facilities_dim = df.select("facility_name", "lat", "lng", "city").distinct() \
    .join(cities_dim, on="city") \
    .withColumn("facility_id", monotonically_increasing_id())

menu_items_dim = df.select("menu_item", "price").distinct() \
    .withColumn("menu_item_id", monotonically_increasing_id())

# факт-таблица с ID-шками

facts = df.select("Order_id", "datetime", "facility_name", "menu_item", "price") \
    .join(facilities_dim.select("facility_name", "facility_id"), on="facility_name") \
    .join(menu_items_dim.select("menu_item", "menu_item_id"), on="menu_item") \
    .select(col("Order_id").alias("order_id"), "datetime", "facility_id", "menu_item_id", "price")


url = "jdbc:postgresql://localhost:5432/spark_orders"
properties = {
    "user": "mirralz",
    "driver": "org.postgresql.Driver"
}

cities_dim.select("city_id", "city", "country") \
    .write.mode("overwrite").jdbc(url=url, table="cities_star", properties=properties)

facilities_dim.select("facility_id", "facility_name", "lat", "lng", "city_id") \
    .write.mode("overwrite").jdbc(url=url, table="facilities_star", properties=properties)

menu_items_dim.select("menu_item_id", "menu_item", "price") \
    .write.mode("overwrite").jdbc(url=url, table="menu_items_star", properties=properties)

facts.write.mode("overwrite").jdbc(url=url, table="order_facts", properties=properties)

print("✅ Звёздная схема успешно загружена в PostgreSQL")
spark.stop()
