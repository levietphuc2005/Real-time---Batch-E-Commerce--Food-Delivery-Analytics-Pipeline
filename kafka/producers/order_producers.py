import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer # type: ignore

# Cấu hình kết nối Kafka Broker
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'order-producer-client'
}

producer = Producer(conf)
TOPIC_NAME = 'order_events'

#fake data
FOOD_ITEMS = [
    {"item": "Bánh mì Sài Gòn", "price": 25000},
    {"item": "Bún bò Huế", "price": 45000},
    {"item": "Cà phê sữa đá", "price": 30000},
    {"item": "Trà sữa chân trâu", "price": 35000},
    {"item": "Cơm tấm sườn nướng", "price": 50000}
]

STATUSES = ["CREATED", "PROCESSING", "DELIVERED", "CANCELLED"]

def delivery_report(err, msg):
    
    if err is not None:
        print(f"Gửi message thất bại: {err}")
    else:
        print(f"Đã gửi thành công tới Topic: {msg.topic()} [Partition: {msg.partition()}] @ Offset {msg.offset()}")

def generate_order():
    #tạo random đơn hàng
    food = random.choice(FOOD_ITEMS)
    quantity = random.randint(1, 3)
    order_data = {
        "order_id": f"ORD_{int(time.time() * 1000)}_{random.randint(100, 999)}",
        "customer_id": f"CUST_{random.randint(1000, 9999)}",
        "item_name": food["item"],
        "price": food["price"],
        "quantity": quantity,
        "total_amount": food["price"] * quantity,
        "status": random.choice(STATUSES),
        "created_at": datetime.now().isoformat()
    }
    return order_data

if __name__ == "__main__":
    print(f"Bắt đầu gửi data tới Kafka Topic '{TOPIC_NAME}'... Nhấn Ctrl+C để dừng.\n")
    try:
        while True:
            order = generate_order()
            payload = json.dumps(order)
            
            # Gửi message vào Kafka
            producer.produce(
                topic=TOPIC_NAME,
                key=order["customer_id"],
                value=payload,
                callback=delivery_report
            )
            
            # Giai phong bo nho dem
            producer.poll(0)
            
            # Giả lập thời gian giãn cách giữa các đơn hàng
            time.sleep(random.uniform(1.0, 3.0))

    except KeyboardInterrupt:
        print("\nĐang dừng Producer...")
    finally:
        producer.flush()
        print("Producer đã thoát an toàn!")