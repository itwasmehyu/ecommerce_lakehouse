from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckGoldData") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ecommerce-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("\n" + "="*50)
print("📊 BÁO CÁO DOANH THU TỪ LỚP GOLD")
print("="*50)

# Đọc và hiển thị dữ liệu
df = spark.read.parquet("s3a://ecommerce-raw/gold/revenue_report")
df.orderBy("total_revenue", ascending=False).show()

print("="*50 + "\n")
spark.stop()