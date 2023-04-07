{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q22_100G_result.parquet') }}

WITH date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
inventory AS (
    select * from {{ source('external_source', 'inventory') }}
),
item AS (
    select * from {{ source('external_source', 'item') }}
)
SELECT i_product_name ,
       i_brand ,
       i_class ,
       i_category ,
       avg(inv_quantity_on_hand) qoh
FROM inventory ,
     date_dim ,
     item
WHERE inv_date_sk=d_date_sk
  AND inv_item_sk=i_item_sk
  AND d_month_seq BETWEEN 1200 AND 1200 + 11
GROUP BY rollup(i_product_name ,i_brand ,i_class ,i_category)
ORDER BY qoh NULLS FIRST,
         i_product_name NULLS FIRST,
         i_brand NULLS FIRST,
         i_class NULLS FIRST,
         i_category NULLS FIRST
    LIMIT 100