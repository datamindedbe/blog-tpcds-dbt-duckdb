{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q67_100G_result.parquet') }}

WITH catalog_sales AS (
    select * from {{ source('external_source', 'catalog_sales') }}
),
store_sales AS (
    select * from {{ source('external_source', 'store_sales') }}
),
date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
store AS (
    select * from {{ source('external_source', 'store') }}
),
item AS (
    select * from {{ source('external_source', 'item') }}
)
SELECT *
FROM
    (SELECT i_category,
            i_class,
            i_brand,
            i_product_name,
            d_year,
            d_qoy,
            d_moy,
            s_store_id,
            sumsales,
            rank() OVER (PARTITION BY i_category
                       ORDER BY sumsales DESC) rk
     FROM
         (SELECT i_category,
                 i_class,
                 i_brand,
                 i_product_name,
                 d_year,
                 d_qoy,
                 d_moy,
                 s_store_id,
                 sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
          FROM store_sales,
               date_dim,
               store,
               item
          WHERE ss_sold_date_sk=d_date_sk
            AND ss_item_sk=i_item_sk
            AND ss_store_sk = s_store_sk
            AND d_month_seq BETWEEN 1200 AND 1200+11
          GROUP BY rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
WHERE rk <= 100
ORDER BY i_category NULLS FIRST,
         i_class NULLS FIRST,
         i_brand NULLS FIRST,
         i_product_name NULLS FIRST,
         d_year NULLS FIRST,
         d_qoy NULLS FIRST,
         d_moy NULLS FIRST,
         s_store_id NULLS FIRST,
         sumsales NULLS FIRST,
         rk NULLS FIRST
    LIMIT 100