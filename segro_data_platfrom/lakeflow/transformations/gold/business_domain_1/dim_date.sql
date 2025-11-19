
CREATE OR REPLACE MATERIALIZED VIEW gold.dim_date

WITH date_spine AS (
    SELECT explode(sequence(
        to_date('2000-01-01'),
        to_date('2030-12-31'),
        interval 1 day
    )) as full_date
)

SELECT
    cast(date_format(full_date, 'yyyyMMdd') as int) as date_key,
    full_date,
    dayofweek(full_date) as day_of_week,
    date_format(full_date, 'EEEE') as day_name,
    dayofmonth(full_date) as day_of_month,
    dayofyear(full_date) as day_of_year,
    weekofyear(full_date) as week_of_year,
    month(full_date) as month_number,
    date_format(full_date, 'MMMM') as month_name,
    quarter(full_date) as quarter,
    year(full_date) as year,
    CASE WHEN dayofweek(full_date) IN (1, 7) THEN true ELSE false END as is_weekend,
    false as is_holiday,
    year(full_date) as fiscal_year,
    quarter(full_date) as fiscal_quarter,
    month(full_date) as fiscal_period
FROM date_spine

UNION ALL

SELECT
    -1 as date_key,
    cast('1900-01-01' as date) as full_date,
    0 as day_of_week,
    'Unknown' as day_name,
    0 as day_of_month,
    0 as day_of_year,
    0 as week_of_year,
    0 as month_number,
    'Unknown' as month_name,
    0 as quarter,
    1900 as year,
    false as is_weekend,
    false as is_holiday,
    1900 as fiscal_year,
    0 as fiscal_quarter,
    0 as fiscal_period