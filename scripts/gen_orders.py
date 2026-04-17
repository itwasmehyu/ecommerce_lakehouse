import json
import random
import time
from datetime import datetime
from faker import Faker
import boto3
from io import BytesIO

fake = Faker()

# Cấu hình kết nối MinIO
s3 = boto3.client(
    's3',
    endpoint_url='http://ecommerce-minio:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password123'
)

BUCKET_NAME = 'ecommerce-raw'

def generate_order():
    return {
        "order_id": fake.uuid4(),
        "customer_id": random.randint(1000, 9999),
        "product_name": random.choice(["Iphone 15", "Modern 14 Laptop", "Cartier Santos", "Mechanical Keyboard", "AirPods"]),
        "price": round(random.uniform(100, 2000), 2),
        "status": random.choice(["COMPLETED", "PENDING", "CANCELLED"]),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def upload_to_minio(data):
    file_name = f"order_{int(time.time())}.json"
    json_data = json.dumps(data).encode('utf-8')
    
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"orders/{file_name}",
        Body=json_data
    )
    print(f"✅ Đã đẩy {file_name} lên MinIO!")

if __name__ == "__main__":
    print("🚀 Bắt đầu bơm dữ liệu vào Data Lake...")
    # Thử tạo 10 đơn hàng
    for i in range(10):
        order = generate_order()
        upload_to_minio(order)
        time.sleep(1) # Nghỉ 1 giây cho giống thực tế