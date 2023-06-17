CREATE TABLE IF NOT EXISTS general_mart_order (
  order_id UInt32,
  car_id UInt32,
  user_id UInt32,
  order_status String,
  payment_status String,
  total_value Float64,
  order_discount Float64,
  first_name String,
  last_name String,
  cellphone String,
  username String,
  email String,
  gender String,
  model String,
  fuel String,
  manufacturer String,
  color String,
  created_at DateTime,
  updated_at DateTime
) ENGINE = MergeTree()
ORDER BY (order_id);
