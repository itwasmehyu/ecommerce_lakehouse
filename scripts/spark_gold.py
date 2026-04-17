from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count

spark = SparkSession.builder \
    .appName("EcommerceGold") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ecommerce-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Đọc từ Silver
df_silver = spark.read.parquet("s3a://ecommerce-raw/silver/orders_cleaned")

# Tính toán: Doanh thu theo sản phẩm
df_gold = df_silver.groupBy("product_name") \
    .agg(sum("price").alias("total_revenue"), 
         count("order_uuid").alias("total_orders"))

# Lưu vào lớp Gold
df_gold.write.mode("overwrite").parquet("s3a://ecommerce-raw/gold/revenue_report")

print("🏆 Đã hoàn thành báo cáo lớp Gold!")
spark.stop()