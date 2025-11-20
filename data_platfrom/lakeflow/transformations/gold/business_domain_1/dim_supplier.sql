
CREATE OR REPLACE MATERIALIZED VIEW gold.dim_supplier

SELECT
    XXHASH64(s.s_suppkey) as supplier_surrogate_key,
    s.s_suppkey as supplier_key,
    s.s_name as supplier_name,
    s.s_address as supplier_address,
    s.s_nationkey as nation_key,
    n.n_name as nation_name,
    n.n_regionkey as region_key,
    r.r_name as region_name,
    s.s_phone as supplier_phone,
    s.s_acctbal  as supplier_account_balance,
    s.s_comment as supplier_comment
FROM silver.slv_supplier s
LEFT JOIN silver.slv_nation n
    ON s.s_nationkey = n.n_nationkey
LEFT JOIN silver.slv_region r
    ON n.n_regionkey = r.r_regionkey

UNION ALL

SELECT
    -1 as supplier_surrogate_key,
    -1 as supplier_key,
    'Unknown' as supplier_name,
    'Unknown' as supplier_address,
    -1 as nation_key,
    'Unknown' as nation_name,
    -1 as region_key,
    'Unknown' as region_name,
    'Unknown' as supplier_phone,
    cast(0.00 as decimal(15,2)) as supplier_account_balance,
    'Unknown' as supplier_comment