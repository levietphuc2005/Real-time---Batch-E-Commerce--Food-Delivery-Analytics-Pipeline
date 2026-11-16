CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    item_name VARCHAR(100),
    price NUMERIC,
    quantity INT,
    total_amount NUMERIC,
    status VARCHAR(50),
    created_at TIMESTAMP
);