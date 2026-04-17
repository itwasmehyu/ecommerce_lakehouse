from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'huynguyen',
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_full_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2026, 4, 1),
    catchup=False
) as dag:

    # 1. Sinh dữ liệu giả (Chạy trên máy Airflow)
    generate_data = BashOperator(
        task_id='generate_data',
        bash_command='python3 /opt/airflow/scripts/gen_orders.py'
    )

    # 2. Chạy Spark Transform (Gọi lệnh submit sang Spark Master)
    spark_transform = BashOperator(
        task_id='spark_transform',
        bash_command="""
        docker exec ecommerce-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp" \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        /opt/spark/scripts/spark_transform.py
        """
    )

    # 3. Chạy Spark Gold
    spark_gold = BashOperator(
        task_id='spark_gold',
        bash_command="""
        docker exec ecommerce-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp" \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        /opt/spark/scripts/spark_gold.py
        """
    )

    generate_data >> spark_transform >> spark_gold