--CREATE OR REFRESH LIVE TABLE gold.dim_customer
CREATE OR REPLACE MATERIALIZED VIEW gold.dim_customer
AS
WITH customer_data AS (
    SELECT
        c.c_custkey AS customer_key,
        c.c_name AS customer_name,
        c.c_address AS customer_address,
        c.c_phone AS customer_phone,
        c.c_acctbal AS customer_account_balance,
        c.c_mktsegment AS customer_market_segment,
        c.c_comment AS customer_comment,
        n.n_name AS nation_name,
        r.r_name AS region_name,
        n.n_nationkey AS nation_key,
        r.r_regionkey AS region_key
    FROM silver.slv_customer c
    LEFT JOIN silver.slv_nation n ON c.c_nationkey = n.n_nationkey
    LEFT JOIN silver.slv_region r ON n.n_regionkey = r.r_regionkey
)

SELECT
    XXHASH64(CONCAT_WS('-', customer_key, nation_key, region_key)) AS customer_surrogate_key,
    *
FROM customer_data

UNION ALL

SELECT
    -1 AS customer_surrogate_key,
    -1 AS customer_key,
    'Unknown' AS customer_name,
    'Unknown' AS customer_address,
    'Unknown' AS customer_phone,
    0.00 AS customer_account_balance,
    'Unknown' AS customer_market_segment,
    'Unknown' AS customer_comment,
    'Unknown' AS nation_name,
    'Unknown' AS region_name,
    -1 AS nation_key,
    -1 AS region_key
;