# Real-time E-commerce Data Lakehouse Pipeline

Dự án xây dựng hệ thống xử lý dữ liệu thời gian thực (End-to-End) theo kiến trúc **Medallion Architecture**. Hệ thống mô phỏng luồng giao dịch thương mại điện tử từ lúc phát sinh đơn hàng cho đến khi tổng hợp báo cáo doanh thu.

## Kiến trúc hệ thống
1. **Data Source**: Python script sử dụng `Faker` để tạo ra hàng ngàn đơn hàng ảo.
2. **Ingestion (Bronze)**: Dữ liệu được đẩy vào **Apache Kafka** để đảm bảo khả năng mở rộng và chịu lỗi.
3. **Processing (Silver)**: **Spark Structured Streaming** tiêu thụ dữ liệu từ Kafka, làm sạch và lưu trữ dưới dạng **Parquet** vào **MinIO (S3 compatible)**.
4. **Orchestration**: Toàn bộ luồng dữ liệu được giám sát và điều phối bởi **Apache Airflow**.

## Tech Stack
* **Language**: Python (PySpark)
* **Processing**: Apache Spark
* **Message Broker**: Apache Kafka
* **Storage**: MinIO (Object Storage)
* **Orchestration**: Apache Airflow
* **Infrastructure**: Docker & Docker Compose

## Hướng dẫn khởi chạy
1. Khởi động Docker Desktop và các container:
   ```bash
   docker compose up -d
   ```
2. Cấp quyền cho Docker socket:
   ```bash
   sudo ln -sf /home/huynguyen/.docker/desktop/docker.sock /var/run/docker.sock
   sudo chmod 666 /var/run/docker.sock
   ```
3. Khởi chạy luồng Streaming:

* Terminal 1: Chạy Spark Streaming để hứng dữ liệu.
  ```bash
  docker exec -it ecommerce-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp" \
    --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
    /opt/spark/scripts/spark_streaming.py
  ```
* Terminal 2: Chạy Python Producer để bắn đơn hàng.
  ```bash
  docker exec -it ecommerce-airflow python3 /opt/airflow/scripts/gen_orders_streaming.py
  ```
## Kết quả đạt được
* Xử lý dữ liệu với độ trễ thấp (Real-time).
* Lưu trữ dữ liệu tối ưu với định dạng Parquet.
* Hệ thống có khả năng tự phục hồi nhờ Checkpointing.
