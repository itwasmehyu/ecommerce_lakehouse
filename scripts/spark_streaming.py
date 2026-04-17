from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Schema cho dữ liệu order
schema = StructType([
    StructField("order_id", StringType()),
    StructField("product_name", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("timestamp", StringType())
])

spark = SparkSession.builder \
    .appName("EcommerceRealTime") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ecommerce-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .getOrCreate()

# 1. Đọc luồng từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .load()

# 2. Parse dữ liệu JSON từ cột 'value'
orders_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 3. Ghi luồng vào MinIO (Lớp Silver)
query = orders_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://ecommerce-raw/silver/streaming_orders") \
    .option("checkpointLocation", "s3a://ecommerce-raw/checkpoints/streaming") \
    .outputMode("append") \
    .start()

print("⚡ Spark Streaming đang hoạt động... Đang chờ dữ liệu từ Kafka.")
query.awaitTermination()