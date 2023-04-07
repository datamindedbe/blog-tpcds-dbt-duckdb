{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q03_100G_result.parquet') }}
WITH store_sales AS (
    select * from {{ source('external_source', 'store_sales') }}
),
date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
item AS (
    select * from {{ source('external_source', 'item') }}
)
SELECT dt.d_year,
       i.i_brand_id brand_id,
       i.i_brand brand,
       sum(ss_ext_sales_price) sum_agg
FROM date_dim dt,
     store_sales ss,
     item i
WHERE dt.d_date_sk = ss.ss_sold_date_sk
  AND ss.ss_item_sk = i.i_item_sk
  AND i.i_manufact_id = 128
  AND dt.d_moy=11
GROUP BY dt.d_year,
         i.i_brand,
         i.i_brand_id
ORDER BY dt.d_year,
         sum_agg DESC,
         brand_id
    LIMIT 100
