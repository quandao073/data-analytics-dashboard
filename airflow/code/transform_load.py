from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import calendar
import os
import sys

# Environment variables
HDFS_URI = os.environ.get("HDFS_URI", "hdfs://hadoop-namenode:9000")
POSTGRES__USERNAME = os.environ.get("POSTGRES__USERNAME", "quanda")
POSTGRES__PASSWORD = os.environ.get("POSTGRES__PASSWORD", "quanda")
POSTGRES__DATABASE = os.environ.get("POSTGRES__DATABASE", "ecommerce_analytics")
POSTGRES__URI = os.environ.get("POSTGRES__URI", "jdbc:postgresql://postgres-db:5432")

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

spark = SparkSession.builder.appName("TransformLoad").getOrCreate()

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

dim_time = df.select(
    hour(col("event_time")).alias("hour")
).distinct()\
.withColumn("hour_label", format_string("%02d:00", col("hour")))\
.withColumn("is_morning", col("hour").between(6, 11))\
.withColumn("is_afternoon", col("hour").between(12, 17))\
.withColumn("is_evening", col("hour").between(18, 21))\
.withColumn("is_night", (col("hour") >= 22) | (col("hour") <= 5))\
.withColumn("hour_group", when(col("hour").between(6,11), "Morning")
                         .when(col("hour").between(12,17), "Afternoon")
                         .when(col("hour").between(18,21), "Evening")
                         .otherwise("Night"))


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
    .withColumn("hour", hour(col("event_time"))) \
    .withColumn("event_type_id",
                when(col("event_type") == "view", 1)
                .when(col("event_type") == "cart", 2)
                .when(col("event_type") == "purchase", 3)
                .otherwise(None)) \
    .withColumn("revenue", when(col("event_type") == "purchase", col("price")).otherwise(0)) \
    .withColumn("quantity", lit(1)) \
    .select("event_id", "date_id", "hour", "user_id", "user_session", "product_id", "event_type_id", "revenue", "quantity")

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

# === Predict Revenue for next month ===
df_daily = fact_events.groupBy("date_id")\
    .agg(sum("revenue").alias("total_revenue"))\
    .withColumn("day_index", dayofmonth("date_id"))\
    .withColumn("day_of_week", dayofweek("date_id"))\
    .withColumn("is_weekend", col("day_of_week").isin(1, 7).cast("int"))


# Huấn luyện Linear Regression
feature_cols = ["day_index", "day_of_week", "is_weekend"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_features = assembler.transform(df_daily).select("features", "total_revenue")

lr = LinearRegression(featuresCol="features", labelCol="total_revenue")
model = lr.fit(df_features)

next_month = input_month + 1 if input_month < 12 else 1
next_year = input_year if input_month < 12 else input_year + 1
days_in_next_month = calendar.monthrange(next_year, next_month)[1]

# Tạo dataframe day_index cho tháng kế tiếp
future_days = spark.range(1, days_in_next_month + 1).toDF("day_index")
future_days = future_days.withColumn(
    "forecast_date",
    to_date(concat_ws("-", lit(next_year), lpad(lit(next_month), 2, "0"), lpad(col("day_index"), 2, "0")))
)
future_days = future_days.withColumn("day_of_week", dayofweek("forecast_date"))\
    .withColumn("is_weekend", col("day_of_week").isin(1, 7).cast("int"))

df_future = assembler.transform(future_days)
predictions = model.transform(df_future)
revenue_predictions = predictions.select(
    "forecast_date", "day_of_week", "is_weekend", 
    col("prediction").alias("predicted_revenue"),
    col("day_index").alias("day")
)

# === FUNCTION TO WRITE TO POSTGRES ===
def write_to_postgres(df, table_name, mode="overwrite"):
    try:
        df.write.jdbc(url=f"{POSTGRES__URI}/{POSTGRES__DATABASE}", table=table_name, mode=mode, properties=postgres_properties)
        print(f"Wrote {df.count()} records to table: {table_name}")
    except Exception as e:
        print(f"Error writing {table_name}: {str(e)}")
        raise

# === WRITE ALL TABLES ===
write_to_postgres(dim_date, "dim_date")
write_to_postgres(dim_time, "dim_time")
write_to_postgres(dim_category, "dim_category")
write_to_postgres(dim_product, "dim_product")
write_to_postgres(dim_event_type, "dim_event_type")
write_to_postgres(fact_events, "fact_events")
write_to_postgres(fact_summary, "fact_summary")
write_to_postgres(revenue_predictions, "predicted_revenue")

spark.stop()
