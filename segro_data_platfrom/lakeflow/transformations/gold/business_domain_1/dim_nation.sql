CREATE OR REPLACE MATERIALIZED VIEW gold.dim_nation

SELECT
    XXHASH64(n.n_nationkey) as nation_surrogate_key,
    n.n_nationkey as nation_key,
    n.n_name as nation_name,
    n.n_regionkey as region_key,
    r.r_name as region_name,
    n.n_comment as nation_comment
FROM silver.slv_nation n
LEFT JOIN silver.slv_region r
    ON n.n_regionkey = r.r_regionkey

UNION ALL

SELECT
    -1 as nation_surrogate_key,
    -1 as nation_key,
    'Unknown' as nation_name,
    -1 as region_key,
    'Unknown' as region_name,
    'Unknown' as nation_comment

