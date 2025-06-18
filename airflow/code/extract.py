from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import sys
import calendar

HDFS__URI = os.environ.get("HDFS__URI", "hdfs://hadoop-namenode:9000")

if len(sys.argv) < 3:
    sys.exit(1)

input_year = int(sys.argv[1])
input_month = int(sys.argv[2])
month_name_short = calendar.month_abbr[input_month]

schema = StructType([
    StructField("event_time", TimestampType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("category_id", StringType(), nullable=False),
    StructField("category_code", StringType(), nullable=True),
    StructField("brand", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("user_session", StringType(), nullable=False),
])


spark = SparkSession.builder \
    .appName("Extract and Clean Data") \
    .getOrCreate()


df_raw = spark.read.format("csv").option("header", "true").schema(schema).load(f"file:///tmp/data/{input_year}-{month_name_short}.csv")


df_filtered = df_raw.dropDuplicates() \
    .dropna(subset=["event_time", "product_id", "category_id", "price", "user_id", "user_session", "brand", "category_code"])\
    .filter(month("event_time") == input_month)


split_col = split(col("category_code"), "\\.")
df_transformed = df_filtered \
    .withColumn("category_level_1", split_col.getItem(0)) \
    .withColumn("category_level_2", split_col.getItem(1)) \
    .withColumn("category_level_3", split_col.getItem(2)) \
    .withColumn("category_level_4", split_col.getItem(3)) \
    .drop("category_code") \
    .withColumn("year", year("event_time")) \
    .withColumn("month", month("event_time")) \
    .withColumn("day", day("event_time"))

df_repartitioned = df_transformed.repartition(12, "year", "month")

df_repartitioned.write \
    .partitionBy("year", "month", "day") \
    .mode("append") \
    .parquet(f"{HDFS__URI}/cleaned_data/")

spark.stop()