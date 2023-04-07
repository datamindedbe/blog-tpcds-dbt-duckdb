{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q82_100G_result.parquet') }}

WITH store_sales AS (
    select * from {{ source('external_source', 'store_sales') }}
),
inventory AS (
    select * from {{ source('external_source', 'inventory') }}
),
date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
item AS (
    select * from {{ source('external_source', 'item') }}
)

SELECT it.i_item_id ,
       it.i_item_desc ,
       it.i_current_price
FROM item it,
     inventory inv,
     date_dim d1,
     store_sales ss
WHERE it.i_current_price BETWEEN 62 AND 62+30
  AND inv.inv_item_sk = it.i_item_sk
  AND d1.d_date_sk=inv.inv_date_sk
  AND d1.d_date BETWEEN cast('2000-05-25' AS date) AND cast('2000-07-24' AS date)
  AND it.i_manufact_id IN (129,
                        270,
                        821,
                        423)
  AND inv.inv_quantity_on_hand BETWEEN 100 AND 500
  AND ss.ss_item_sk = it.i_item_sk
GROUP BY it.i_item_id,
         it.i_item_desc,
         it.i_current_price
ORDER BY it.i_item_id
    LIMIT 100