CREATE TABLE Orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    car_id INT,
    user_id INT,
    order_status VARCHAR(255),
    payment_status VARCHAR(255),
    total_value DECIMAL(10, 2),
    order_discount DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
