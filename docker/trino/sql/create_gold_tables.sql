CREATE SCHEMA IF NOT EXISTS hive.gold
WITH (location = 's3://warehouse/gold');

CREATE TABLE IF NOT EXISTS hive.gold.dim_users (
    user_id varchar,
    name varchar,
    email varchar,
    gender varchar,
    city varchar,
    signup_date date,
    etl_loaded_at timestamp(3),
    etl_run_date date,
    etl_run_id varchar
)
WITH (
    external_location = 's3://warehouse/gold/dim_users',
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.gold.dim_products (
    product_id varchar,
    product_name varchar,
    category varchar,
    brand varchar,
    price double,
    rating double,
    etl_loaded_at timestamp(3),
    etl_run_date date,
    etl_run_id varchar
)
WITH (
    external_location = 's3://warehouse/gold/dim_products',
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.gold.fact_orders (
    order_id varchar,
    user_id varchar,
    order_date timestamp(3),
    order_status varchar,
    total_amount double,
    user_city varchar,
    user_gender varchar,
    etl_loaded_at timestamp(3),
    etl_run_date date,
    etl_run_id varchar,
    order_day date
)
WITH (
    external_location = 's3://warehouse/gold/fact_orders',
    format = 'PARQUET',
    partitioned_by = ARRAY['order_day']
);

CREATE TABLE IF NOT EXISTS hive.gold.fact_order_items (
    order_item_id varchar,
    order_id varchar,
    product_id varchar,
    user_id varchar,
    order_date timestamp(3),
    order_status varchar,
    quantity integer,
    item_price double,
    item_total double,
    category varchar,
    brand varchar,
    etl_loaded_at timestamp(3),
    etl_run_date date,
    etl_run_id varchar,
    order_day date
)
WITH (
    external_location = 's3://warehouse/gold/fact_order_items',
    format = 'PARQUET',
    partitioned_by = ARRAY['order_day']
);

CREATE TABLE IF NOT EXISTS hive.gold.fact_events (
    event_id varchar,
    user_id varchar,
    product_id varchar,
    event_type varchar,
    event_timestamp timestamp(3),
    user_city varchar,
    category varchar,
    brand varchar,
    etl_loaded_at timestamp(3),
    etl_run_date date,
    etl_run_id varchar,
    event_date date
)
WITH (
    external_location = 's3://warehouse/gold/fact_events',
    format = 'PARQUET',
    partitioned_by = ARRAY['event_date']
);

CREATE TABLE IF NOT EXISTS hive.gold.daily_sales (
    order_count bigint,
    order_item_count bigint,
    etl_loaded_at timestamp(3),
    etl_run_date date,
    etl_run_id varchar,
    order_day date
)
WITH (
    external_location = 's3://warehouse/gold/daily_sales',
    format = 'PARQUET',
    partitioned_by = ARRAY['order_day']
);

CREATE TABLE IF NOT EXISTS hive.gold.product_performance (
    product_id varchar,
    product_name varchar,
    category varchar,
    brand varchar,
    price double,
    catalog_rating double,
    units_sold bigint,
    gross_sales double,
    order_count bigint,
    event_count bigint,
    active_user_count bigint,
    review_count bigint,
    avg_review_rating double,
    etl_loaded_at timestamp(3),
    etl_run_date date,
    etl_run_id varchar
)
WITH (
    external_location = 's3://warehouse/gold/product_performance',
    format = 'PARQUET'
);
