from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
import os
import sys

# Environment variables
HDFS_URI = os.environ.get("HDFS_URI", "hdfs://hadoop-namenode:9000")
POSTGRES__USERNAME = os.environ.get("POSTGRES__USERNAME", "quanda")
POSTGRES__PASSWORD = os.environ.get("POSTGRES__PASSWORD", "quanda")
POSTGRES__DATABASE = os.environ.get("POSTGRES__DATABASE", "ecommerce_analytics")
POSTGRES__URI = os.environ.get("POSTGRES__URI", "jdbc:postgresql://postgres-db:5432")
POSTGRES_URL = f"{POSTGRES__URI}/{POSTGRES__DATABASE}"

postgres_properties = {
    "user": POSTGRES__USERNAME,
    "password": POSTGRES__PASSWORD,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

if len(sys.argv) < 3:
    print("Usage: python script.py <year> <month>")
    sys.exit(1)

input_year = int(sys.argv[1])
input_month = int(sys.argv[2])

spark = SparkSession.builder.appName("LoadTransform").getOrCreate()

# Read cleaned data from HDFS
input_path = f"{HDFS_URI}/cleaned_data/year={input_year}/month={input_month}/"
df = spark.read.parquet(input_path)

# === DIM TABLES ===
# dim_time
dim_date = df.select(
    to_date(col("event_time")).alias("date_id"),
    month("event_time").alias("month"),
    dayofmonth("event_time").alias("day"),
    quarter("event_time").alias("quarter"),
    dayofweek("event_time").alias("day_of_week"),
    date_format(col("date_id"), "EEEE").alias("day_name")
).distinct()

# dim_event_type
dim_event_type = spark.createDataFrame([
    ("view", 1),
    ("cart", 2),
    ("purchase", 3)
], ["event_type_name", "event_type_id"])

# dim_product
dim_product = df.select(
    "product_id", "category_id", "brand", "price"
).distinct().groupBy("product_id", "category_id", "brand").agg(
    round(avg("price"), 2).alias("price")
).distinct()

window = Window.partitionBy("product_id", "category_id").orderBy("brand")
dim_product = dim_product.withColumn("row_number", row_number().over(window)) \
    .filter("row_number = 1")\
    .drop("row_number")\
    .distinct()

# dim_category
dim_category = df.select(
    "category_id", "category_level_1", "category_level_2", "category_level_3", "category_level_4"
).distinct()

# === FACT TABLE: fact_events ===
fact_events = df.withColumn("event_id", monotonically_increasing_id())\
    .withColumn("date_id", to_date(col("event_time"))) \
    .withColumn("event_type_id",
                when(col("event_type") == "view", 1)
                .when(col("event_type") == "cart", 2)
                .when(col("event_type") == "purchase", 3)
                .otherwise(None)) \
    .withColumn("revenue", when(col("event_type") == "purchase", col("price")).otherwise(0)) \
    .withColumn("quantity", lit(1)) \
    .select("event_id", "date_id", "user_id", "user_session", "product_id", "event_type_id", "revenue", "quantity")

# fact summary
df_event_counts = df.groupBy("product_id", "event_type").count() \
    .groupBy("product_id") \
    .pivot("event_type", ["view", "cart", "purchase"]) \
    .sum("count") \
    .fillna(0)

df_event_counts = df_event_counts.withColumn(
    "total_events", col("view") + col("cart") + col("purchase")
).withColumn(
    "purchase_conversion", round((col("purchase") / (col("view") + col("cart"))) * 100, 2)
)

df_revenue = df.filter(col("event_type") == "purchase") \
    .groupBy("product_id") \
    .agg(sum("price").alias("total_revenue"))

fact_summary = df_event_counts.join(df_revenue, on="product_id", how="left") \
    .fillna({"total_revenue": 0})

# === FUNCTION TO WRITE TO POSTGRES ===
def write_to_postgres(df, table_name, mode="overwrite"):
    try:
        df.write.jdbc(url=POSTGRES_URL, table=table_name, mode=mode, properties=postgres_properties)
        print(f"Wrote {df.count()} records to table: {table_name}")
    except Exception as e:
        print(f"Error writing {table_name}: {str(e)}")
        raise

# === WRITE ALL TABLES ===
write_to_postgres(dim_date, "dim_date")
write_to_postgres(dim_category, "dim_category")
write_to_postgres(dim_product, "dim_product")
write_to_postgres(dim_event_type, "dim_event_type")
write_to_postgres(fact_events, "fact_events")
write_to_postgres(fact_summary, "fact_summary")

spark.stop()
