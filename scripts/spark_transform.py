from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Cấu hình Spark để "nói chuyện" với MinIO (S3)
spark = SparkSession.builder \
    .appName("EcommerceTransform") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ecommerce-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 1. Đọc toàn bộ file JSON từ lớp Bronze
print("📂 Đang đọc dữ liệu từ Bronze Lake...")
df_raw = spark.read.json("s3a://ecommerce-raw/orders/*.json")

# 2. Transform: Làm sạch và thêm cột quản lý
print("✨ Đang làm sạch dữ liệu...")
df_cleaned = df_raw.select(
    col("order_id").alias("order_uuid"),
    col("customer_id").cast("int"),
    col("product_name"),
    col("price").cast("double"),
    col("status"),
    col("created_at").cast("timestamp")
).withColumn("processed_at", current_timestamp())

# 3. Lưu vào lớp Silver dưới dạng Parquet
# Parquet giúp nén dữ liệu cực tốt và truy vấn nhanh gấp chục lần JSON
print("💾 Đang lưu dữ liệu vào Silver Layer (Parquet)...")
df_cleaned.write \
    .mode("overwrite") \
    .parquet("s3a://ecommerce-raw/silver/orders_cleaned")

print("✅ Hoàn thành! Dữ liệu đã sẵn sàng để phân tích.")
spark.stop()