-- %%
SHOW DATABASES;

-- %%
USE BRAZILIAN_ECOMMERCE;

-- %%
-- RAW Customers
CREATE OR REPLACE TABLE raw_customers (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
);

-- RAW Geolocation
CREATE OR REPLACE TABLE raw_geolocation (
    geolocation_zip_code_prefix STRING,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city STRING,
    geolocation_state STRING
);

-- RAW Order Items
CREATE OR REPLACE TABLE raw_order_items (
    order_id STRING,
    order_item_id NUMBER,
    product_id STRING,
    seller_id STRING,
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT
);

-- RAW Order Payments
CREATE OR REPLACE TABLE raw_order_payments (
    order_id STRING,
    payment_sequential NUMBER,
    payment_type STRING,
    payment_installments NUMBER,
    payment_value FLOAT
);

-- RAW Order Reviews
CREATE OR REPLACE TABLE raw_order_reviews (
    review_id STRING,
    order_id STRING,
    review_score NUMBER,
    review_comment_title STRING,
    review_comment_message STRING,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- RAW Orders
CREATE OR REPLACE TABLE raw_orders (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- RAW Products
CREATE OR REPLACE TABLE raw_products (
    product_id STRING,
    product_category_name STRING,
    product_name_lenght NUMBER,
    product_description_lenght NUMBER,
    product_photos_qty NUMBER,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT
);

-- RAW Sellers
CREATE OR REPLACE TABLE raw_sellers (
    seller_id STRING,
    seller_zip_code_prefix STRING,
    seller_city STRING,
    seller_state STRING
);

-- RAW Product Category Translation
CREATE OR REPLACE TABLE raw_product_category_name_translation (
    product_category_name STRING,
    product_category_name_english STRING
);


-- %%
SHOW TABLES;

-- %%
-- RAW Customers
COPY INTO raw_customers
FROM @olist_stage/olist_customers_dataset.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    NULL_IF = ('NULL','null')
);

-- RAW Geolocation
COPY INTO raw_geolocation
FROM @olist_stage/olist_geolocation_dataset.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    NULL_IF = ('NULL','null')
);

-- RAW Order Items
COPY INTO raw_order_items
FROM @olist_stage/olist_order_items_dataset.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    NULL_IF = ('NULL','null')
);

-- RAW Order Payments
COPY INTO raw_order_payments
FROM @olist_stage/olist_order_payments_dataset.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    NULL_IF = ('NULL','null')
);

-- RAW Order Reviews
COPY INTO raw_order_reviews
FROM @olist_stage/olist_order_reviews_dataset.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    NULL_IF = ('NULL','null')
);

-- RAW Orders
COPY INTO raw_orders
FROM @olist_stage/olist_orders_dataset.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    NULL_IF = ('NULL','null')
);

-- RAW Products
COPY INTO raw_products
FROM @olist_stage/olist_products_dataset.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    NULL_IF = ('NULL','null')
);

-- RAW Sellers
COPY INTO raw_sellers
FROM @olist_stage/olist_sellers_dataset.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    NULL_IF = ('NULL','null')
);

-- RAW Product Category Translation
COPY INTO raw_product_category_name_translation
FROM @olist_stage/product_category_name_translation.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    NULL_IF = ('NULL','null')
);


-- %%
SELECT * FROM raw_customers;

-- %%
-- DIM DATE
CREATE OR REPLACE TABLE dim_date (
    date_id INT AUTOINCREMENT PRIMARY KEY,
    full_date DATE,
    day_of_month SMALLINT,
    day_of_week VARCHAR,
    week_of_year SMALLINT,
    month SMALLINT,
    quarter SMALLINT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE
);

INSERT INTO dim_date (full_date, day_of_month, day_of_week, week_of_year, month, quarter, year, is_weekend)
SELECT DISTINCT
    CAST(DATE(ts) AS DATE),
    EXTRACT(DAY FROM ts),
    TO_VARCHAR(DATE(ts), 'DY'),
    EXTRACT(WEEK FROM ts),
    EXTRACT(MONTH FROM ts),
    EXTRACT(QUARTER FROM ts),
    EXTRACT(YEAR FROM ts),
    CASE WHEN EXTRACT(DOW FROM ts) IN (0,6) THEN TRUE ELSE FALSE END
FROM (
    SELECT order_purchase_timestamp AS ts FROM raw_orders
    UNION SELECT order_approved_at FROM raw_orders
    UNION SELECT order_delivered_carrier_date FROM raw_orders
    UNION SELECT order_delivered_customer_date FROM raw_orders
    UNION SELECT order_estimated_delivery_date FROM raw_orders
    UNION SELECT review_creation_date FROM raw_order_reviews
    UNION SELECT review_answer_timestamp FROM raw_order_reviews
    UNION SELECT shipping_limit_date FROM raw_order_items
) t
WHERE ts IS NOT NULL;

-- DIM TIME
CREATE OR REPLACE TABLE dim_time (
    time_id INT AUTOINCREMENT PRIMARY KEY,
    hour SMALLINT,
    minute SMALLINT,
    second SMALLINT
);

INSERT INTO dim_time (hour, minute, second)
SELECT DISTINCT
    EXTRACT(HOUR FROM ts),
    EXTRACT(MINUTE FROM ts),
    EXTRACT(SECOND FROM ts)
FROM (
    SELECT order_purchase_timestamp AS ts FROM raw_orders
    UNION SELECT order_approved_at FROM raw_orders
    UNION SELECT order_delivered_carrier_date FROM raw_orders
    UNION SELECT order_delivered_customer_date FROM raw_orders
    UNION SELECT order_estimated_delivery_date FROM raw_orders
    UNION SELECT review_creation_date FROM raw_order_reviews
    UNION SELECT review_answer_timestamp FROM raw_order_reviews
    UNION SELECT shipping_limit_date FROM raw_order_items
) t
WHERE ts IS NOT NULL;

-- DIM CUSTOMER
CREATE OR REPLACE TABLE dim_customer (
    customer_id VARCHAR PRIMARY KEY,
    customer_unique_id VARCHAR
);

INSERT INTO dim_customer
SELECT DISTINCT customer_id, customer_unique_id
FROM raw_customers;

-- DIM GEOLOCATION
CREATE OR REPLACE TABLE dim_geolocation (
    geolocation_id INT AUTOINCREMENT PRIMARY KEY,
    geolocation_zip_code_prefix INT,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR
);

INSERT INTO dim_geolocation (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city)
SELECT DISTINCT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city
FROM raw_geolocation;

-- DIM PRODUCT
CREATE OR REPLACE TABLE dim_product (
    product_id VARCHAR PRIMARY KEY,
    product_category_name VARCHAR,
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT
);

INSERT INTO dim_product
SELECT DISTINCT
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM raw_products;

-- DIM SELLER
CREATE OR REPLACE TABLE dim_seller (
    seller_id VARCHAR PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR,
    seller_state VARCHAR
);

INSERT INTO dim_seller
SELECT DISTINCT seller_id, seller_zip_code_prefix, seller_city, seller_state
FROM raw_sellers;


-- %%
-- FACT ORDERS
CREATE OR REPLACE TABLE fact_orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    geolocation_id INT,
    review_id VARCHAR,
    purchase_date_id INT,
    purchase_time_id INT,
    approved_date_id INT,
    approved_time_id INT,
    delivered_carrier_date_id INT,
    delivered_carrier_time_id INT,
    delivered_customer_date_id INT,
    delivered_customer_time_id INT,
    estimated_delivery_date_id INT,
    estimated_delivery_time_id INT
);

INSERT INTO fact_orders
SELECT
    o.order_id,
    o.customer_id,
    g.geolocation_id,
    NULL AS review_id,
    d1.date_id, t1.time_id,
    d2.date_id, t2.time_id,
    d3.date_id, t3.time_id,
    d4.date_id, t4.time_id,
    d5.date_id, t5.time_id
FROM raw_orders o
LEFT JOIN raw_customers c ON c.customer_id = o.customer_id
LEFT JOIN dim_geolocation g ON g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
LEFT JOIN dim_date d1 ON d1.full_date = DATE(o.order_purchase_timestamp)
LEFT JOIN dim_time t1 ON t1.hour = EXTRACT(HOUR FROM o.order_purchase_timestamp)
                      AND t1.minute = EXTRACT(MINUTE FROM o.order_purchase_timestamp)
                      AND t1.second = EXTRACT(SECOND FROM o.order_purchase_timestamp)
LEFT JOIN dim_date d2 ON d2.full_date = DATE(o.order_approved_at)
LEFT JOIN dim_time t2 ON t2.hour = EXTRACT(HOUR FROM o.order_approved_at)
                      AND t2.minute = EXTRACT(MINUTE FROM o.order_approved_at)
                      AND t2.second = EXTRACT(SECOND FROM o.order_approved_at)
LEFT JOIN dim_date d3 ON d3.full_date = DATE(o.order_delivered_carrier_date)
LEFT JOIN dim_time t3 ON t3.hour = EXTRACT(HOUR FROM o.order_delivered_carrier_date)
                      AND t3.minute = EXTRACT(MINUTE FROM o.order_delivered_carrier_date)
                      AND t3.second = EXTRACT(SECOND FROM o.order_delivered_carrier_date)
LEFT JOIN dim_date d4 ON d4.full_date = DATE(o.order_delivered_customer_date)
LEFT JOIN dim_time t4 ON t4.hour = EXTRACT(HOUR FROM o.order_delivered_customer_date)
                      AND t4.minute = EXTRACT(MINUTE FROM o.order_delivered_customer_date)
                      AND t4.second = EXTRACT(SECOND FROM o.order_delivered_customer_date)
LEFT JOIN dim_date d5 ON d5.full_date = DATE(o.order_estimated_delivery_date)
LEFT JOIN dim_time t5 ON t5.hour = EXTRACT(HOUR FROM o.order_estimated_delivery_date)
                      AND t5.minute = EXTRACT(MINUTE FROM o.order_estimated_delivery_date)
                      AND t5.second = EXTRACT(SECOND FROM o.order_estimated_delivery_date);


-- FACT ORDER ITEMS
CREATE OR REPLACE TABLE fact_order_items (
    order_item_id INT AUTOINCREMENT PRIMARY KEY,
    order_id VARCHAR,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date_id INT,
    shipping_limit_time_id INT,
    price FLOAT,
    freight_value FLOAT
);

INSERT INTO fact_order_items (order_id, product_id, seller_id, shipping_limit_date_id, shipping_limit_time_id, price, freight_value)
SELECT
    oi.order_id,
    oi.product_id,
    oi.seller_id,
    d.date_id,
    t.time_id,
    oi.price,
    oi.freight_value
FROM raw_order_items oi
LEFT JOIN dim_date d ON d.full_date = DATE(oi.shipping_limit_date)
LEFT JOIN dim_time t ON t.hour = EXTRACT(HOUR FROM oi.shipping_limit_date)
                     AND t.minute = EXTRACT(MINUTE FROM oi.shipping_limit_date)
                     AND t.second = EXTRACT(SECOND FROM oi.shipping_limit_date);


-- FACT PAYMENT
CREATE OR REPLACE TABLE fact_payment (
    payment_id INT AUTOINCREMENT PRIMARY KEY,
    order_id VARCHAR,
    customer_id VARCHAR,
    geolocation_id INT,
    payment_sequential INT,
    payment_type VARCHAR,
    payment_installments INT,
    payment_value FLOAT
);

INSERT INTO fact_payment (order_id, customer_id, geolocation_id, payment_sequential, payment_type, payment_installments, payment_value)
SELECT
    p.order_id,
    o.customer_id,
    g.geolocation_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value
FROM raw_order_payments p
JOIN raw_orders o ON o.order_id = p.order_id
JOIN raw_customers c ON c.customer_id = o.customer_id
LEFT JOIN dim_geolocation g ON g.geolocation_zip_code_prefix = c.customer_zip_code_prefix;


-- FACT REVIEW
CREATE OR REPLACE TABLE fact_review (
    review_id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    customer_id VARCHAR,
    geolocation_id INT,
    review_creation_date_id INT,
    review_creation_time_id INT,
    review_answer_date_id INT,
    review_answer_time_id INT,
    review_score SMALLINT,
    review_comment_title TEXT,
    review_comment_message TEXT
);

INSERT INTO fact_review
SELECT
    r.review_id,
    r.order_id,
    o.customer_id,
    g.geolocation_id,
    d1.date_id, t1.time_id,
    d2.date_id, t2.time_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message
FROM raw_order_reviews r
JOIN raw_orders o ON o.order_id = r.order_id
JOIN raw_customers c ON c.customer_id = o.customer_id
LEFT JOIN dim_geolocation g ON g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
LEFT JOIN dim_date d1 ON d1.full_date = DATE(r.review_creation_date)
LEFT JOIN dim_time t1 ON t1.hour = EXTRACT(HOUR FROM r.review_creation_date)
                      AND t1.minute = EXTRACT(MINUTE FROM r.review_creation_date)
                      AND t1.second = EXTRACT(SECOND FROM r.review_creation_date)
LEFT JOIN dim_date d2 ON d2.full_date = DATE(r.review_answer_timestamp)
LEFT JOIN dim_time t2 ON t2.hour = EXTRACT(HOUR FROM r.review_answer_timestamp)
                      AND t2.minute = EXTRACT(MINUTE FROM r.review_answer_timestamp)
                      AND t2.second = EXTRACT(SECOND FROM r.review_answer_timestamp);


-- %%
SELECT *
FROM fact_orders
LIMIT 10;

-- %%
-- ========================
-- DIMENSIONS (split files)
-- ========================
COPY INTO @KIMBALL_STAGE/dim_date/
FROM dim_date
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = NONE)
OVERWRITE = TRUE;

COPY INTO @KIMBALL_STAGE/dim_time/
FROM dim_time
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = NONE)
OVERWRITE = TRUE;

COPY INTO @KIMBALL_STAGE/dim_customer/
FROM dim_customer
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = NONE)
OVERWRITE = TRUE;

COPY INTO @KIMBALL_STAGE/dim_product/
FROM dim_product
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = NONE)
OVERWRITE = TRUE;

COPY INTO @KIMBALL_STAGE/dim_seller/
FROM dim_seller
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = NONE)
OVERWRITE = TRUE;

COPY INTO @KIMBALL_STAGE/dim_geolocation/
FROM dim_geolocation
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = NONE)
OVERWRITE = TRUE;


-- ===================
-- FACTS (split files)
-- ===================
COPY INTO @KIMBALL_STAGE/fact_orders/
FROM fact_orders
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = NONE)
OVERWRITE = TRUE;

COPY INTO @KIMBALL_STAGE/fact_order_items/
FROM fact_order_items
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = NONE)
OVERWRITE = TRUE;

COPY INTO @KIMBALL_STAGE/fact_payment/
FROM fact_payment
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = NONE)
OVERWRITE = TRUE;

COPY INTO @KIMBALL_STAGE/fact_review/
FROM fact_review
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = NONE)
OVERWRITE = TRUE;

-- Drop RAW tables, stage files and stage
DROP TABLE IF EXISTS raw_customers;
DROP TABLE IF EXISTS raw_geolocation;
DROP TABLE IF EXISTS raw_order_items;
DROP TABLE IF EXISTS raw_order_payments;
DROP TABLE IF EXISTS raw_order_reviews;
DROP TABLE IF EXISTS raw_orders;
DROP TABLE IF EXISTS raw_products;
DROP TABLE IF EXISTS raw_sellers;
DROP TABLE IF EXISTS raw_product_category_name_translation;
REMOVE @olist_stage PATTERN='.*';
DROP STAGE IF EXISTS olist_stage;