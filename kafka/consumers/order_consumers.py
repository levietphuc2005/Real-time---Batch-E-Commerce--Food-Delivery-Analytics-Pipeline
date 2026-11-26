import json
from confluent_kafka import Consumer, KafkaError # type: ignore
import psycopg2 # type: ignore
from dotenv import load_dotenv
import os

load_dotenv()

# Cấu hình kết nối PostgreSQL
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "postgres"),     # Thay đổi user
    "password": os.getenv("DB_PASSWORD"), # Thay đổi mật khẩu
    "host": os.getenv("DB_HOST", "172.29.16.1"),
    "port": int(os.getenv("DB_PORT", 5432))
}

# Cấu hình Kafka Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-db-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
TOPIC_NAME = 'order_events'
consumer.subscribe([TOPIC_NAME])

def save_to_postgres(order_data):
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO orders (order_id, customer_id, item_name, price, quantity, total_amount, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """
        
        cursor.execute(insert_query, (
            order_data["order_id"],
            order_data["customer_id"],
            order_data["item_name"],
            order_data["price"],
            order_data["quantity"],
            order_data["total_amount"],
            order_data["status"],
            order_data["created_at"]
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Đã lưu đơn hàng vào DB: {order_data['order_id']}")
    except Exception as e:
        print(f"Lỗi ghi Postgres: {e}")

if __name__ == "__main__":
    print(f"Consumer đang lắng nghe dữ liệu từ topic '{TOPIC_NAME}' trên WSL...\n")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Lỗi Kafka: {msg.error()}")
                    break
            
            # Đọc message JSON và gọi hàm lưu vào DB
            order_data = json.loads(msg.value().decode('utf-8'))
            save_to_postgres(order_data)
            
    except KeyboardInterrupt:
        print("\nĐang dừng Consumer...")
    finally:
        consumer.close()