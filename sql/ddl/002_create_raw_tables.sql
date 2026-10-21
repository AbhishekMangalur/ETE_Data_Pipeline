
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id     VARCHAR(50) PRIMARY KEY,
    customer_name   VARCHAR(150),
    email           VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(100),
    signup_date     DATE,
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id      VARCHAR(50) PRIMARY KEY,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    unit_price      NUMERIC(12, 2),
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id        VARCHAR(50) PRIMARY KEY,
    customer_id     VARCHAR(50),
    order_date      TIMESTAMP,
    order_status    VARCHAR(30),
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id   VARCHAR(60) PRIMARY KEY,
    order_id        VARCHAR(50),
    product_id      VARCHAR(50),
    quantity        INTEGER,
    unit_price      NUMERIC(12, 2),
    freight_value   NUMERIC(12, 2),
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id      VARCHAR(50) PRIMARY KEY,
    order_id        VARCHAR(50),
    payment_type    VARCHAR(50),
    payment_value   NUMERIC(12, 2),
    payment_status  VARCHAR(30),
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

