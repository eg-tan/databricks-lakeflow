CREATE OR REPLACE MATERIALIZED VIEW gold.dim_part_supplier

SELECT
    XXHASH64(CONCAT_WS('-', ps_partkey, ps_suppkey)) as part_supplier_key,
    ps_partkey as part_key,
    ps_suppkey as supplier_key,
    ps_availqty as available_quantity,
    cast(ps_supplycost as decimal(15,2)) as supply_cost,
    ps_comment as comment
FROM silver.slv_partsupp

UNION ALL

SELECT
    -1 as part_supplier_key,
    -1 as part_key,
    -1 as supplier_key,
    0 as available_quantity,
    cast(0.00 as decimal(15,2)) as supply_cost,
    'Unknown' as comment