import json
import time
import random
from kafka import KafkaProducer
from faker import Faker

fake = Faker()
# Kết nối đến Kafka trong Docker
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ['Cartier Santos', 'Iphone 15', 'Modern 14 Laptop', 'Mechanical Keyboard', 'AirPods']

print("🚀 Đang bắt đầu bắn đơn hàng vào Kafka... Nhấn Ctrl+C để dừng.")

while True:
    order = {
        'order_id': fake.uuid4(),
        'product_name': random.choice(products),
        'price': round(random.uniform(500, 3000), 2),
        'quantity': random.randint(1, 5),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    producer.send('orders', value=order)
    print(f"✅ Đã bắn: {order['product_name']} - {order['price']}$")
    time.sleep(2) # Bắn mỗi 2 giây