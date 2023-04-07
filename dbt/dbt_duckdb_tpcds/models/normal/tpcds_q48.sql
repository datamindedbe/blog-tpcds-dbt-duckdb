{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q48_100G_result.parquet') }}

WITH store_sales AS (
    select * from {{ source('external_source', 'store_sales') }}
),
date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
store AS (
    select * from {{ source('external_source', 'store') }}
),
customer AS (
    select * from {{ source('external_source', 'customer') }}
),
customer_address AS (
    select * from {{ source('external_source', 'customer_address') }}
),
customer_demographics AS (
    select * from {{ source('external_source', 'customer_demographics') }}
)
SELECT SUM (ss_quantity)
FROM store_sales,
     store,
     customer_demographics,
     customer_address,
     date_dim
WHERE s_store_sk = ss_store_sk
  AND ss_sold_date_sk = d_date_sk
  AND d_year = 2000
  AND ((cd_demo_sk = ss_cdemo_sk
    AND cd_marital_status = 'M'
    AND cd_education_status = '4 yr Degree'
    AND ss_sales_price BETWEEN 100.00 AND 150.00)
    OR (cd_demo_sk = ss_cdemo_sk
        AND cd_marital_status = 'D'
        AND cd_education_status = '2 yr Degree'
        AND ss_sales_price BETWEEN 50.00 AND 100.00)
    OR (cd_demo_sk = ss_cdemo_sk
        AND cd_marital_status = 'S'
        AND cd_education_status = 'College'
        AND ss_sales_price BETWEEN 150.00 AND 200.00))
  AND ((ss_addr_sk = ca_address_sk
    AND ca_country = 'United States'
    AND ca_state IN ('CO',
                     'OH',
                     'TX')
    AND ss_net_profit BETWEEN 0 AND 2000)
    OR (ss_addr_sk = ca_address_sk
        AND ca_country = 'United States'
        AND ca_state IN ('OR',
                         'MN',
                         'KY')
        AND ss_net_profit BETWEEN 150 AND 3000)
    OR (ss_addr_sk = ca_address_sk
        AND ca_country = 'United States'
        AND ca_state IN ('VA',
                         'CA',
                         'MS')
        AND ss_net_profit BETWEEN 50 AND 25000))