CREATE OR REPLACE MATERIALIZED VIEW gold.dim_part

SELECT
    XXHASH64(p_partkey) as part_surrogate_key,
    p_partkey as part_key,
    p_name as part_name,
    p_mfgr as part_manufacturer,
    p_brand as part_brand,
    p_type as part_type,
    p_size as part_size,
    p_container as part_container,
    cast(p_retailprice as decimal(15,2)) as part_retail_price,
    p_comment as part_comment
FROM silver.slv_part

UNION ALL

SELECT
    -1 as part_surrogate_key,
    -1 as part_key,
    'Unknown' as part_name,
    'Unknown' as part_manufacturer,
    'Unknown' as part_brand,
    'Unknown' as part_type,
    0 as part_size,
    'Unknown' as part_container,
    cast(0.00 as decimal(15,2)) as part_retail_price,
    'Unknown' as part_comment