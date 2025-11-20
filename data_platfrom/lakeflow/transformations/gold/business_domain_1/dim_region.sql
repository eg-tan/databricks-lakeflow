CREATE OR REPLACE MATERIALIZED VIEW gold.dim_region

SELECT
    XXHASH64(r_regionkey) as region_surrogate_key,
    r_regionkey as region_key,
    r_name as region_name,
    r_comment as region_comment
FROM silver.slv_region

UNION ALL

SELECT
    -1 as region_surrogate_key,
    -1 as region_key,
    'Unknown' as region_name,
    'Unknown' as region_comment