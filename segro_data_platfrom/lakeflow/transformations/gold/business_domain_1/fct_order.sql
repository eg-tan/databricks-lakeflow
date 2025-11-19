CREATE OR REFRESH STREAMING TABLE gold.fact_order (
    order_key INT,
    customer_surrogate_key BIGINT,
    order_date_key INT,
    order_status STRING,
    order_priority STRING,
    clerk_name STRING,
    ship_priority INT,
    total_price DECIMAL(15,2),
    -- total_quantity DECIMAL(15,2),
    -- total_discount DECIMAL(15,2),
    -- total_tax DECIMAL(15,2),
    -- total_extended_price DECIMAL(15,2),
    -- line_item_count BIGINT,
    order_comment STRING,
    order_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITIONED BY (order_date_key)
AS

SELECT
    o.o_orderkey as order_key,
    
    -- Dimension Surrogate Key
    COALESCE(c.customer_surrogate_key, -1) as customer_surrogate_key,
    
    -- Date Key
    cast(date_format(o.o_orderdate, 'yyyyMMdd') as int) as order_date_key,
    
    -- Degenerate Dimensions (order attributes)
    o.o_orderstatus as order_status,
    o.o_orderpriority as order_priority,
    o.o_clerk as clerk_name,
    o.o_shippriority as ship_priority,
    
    -- Measures
    cast(o.o_totalprice as decimal(15,2)) as total_price,
    -- COALESCE(agg.total_quantity, 0) as total_quantity,
    -- COALESCE(agg.total_discount, 0) as total_discount,
    -- COALESCE(agg.total_tax, 0) as total_tax,
    -- COALESCE(agg.total_extended_price, 0) as total_extended_price,
    -- COALESCE(agg.line_item_count, 0) as line_item_count,
    
    -- Additional attributes
    o.o_comment as order_comment

FROM STREAM(silver.slv_order_stream) o
LEFT JOIN gold.dim_customer c
    ON o.o_custkey = c.customer_key
