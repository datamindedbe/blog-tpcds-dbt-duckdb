{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q93_100G_result.parquet') }}

WITH store_sales AS (
    select * from {{ source('external_source', 'store_sales') }}
),
store_returns AS (
    select * from {{ source('external_source', 'store_returns') }}
),
date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
store AS (
    select * from {{ source('external_source', 'store') }}
),
reason AS (
    select * from {{ source('external_source', 'reason') }}
)
SELECT ss_customer_sk,
       sum(act_sales) sumsales
FROM
    (SELECT ss_item_sk,
            ss_ticket_number,
            ss_customer_sk,
            CASE
                WHEN sr_return_quantity IS NOT NULL THEN (ss_quantity-sr_return_quantity)*ss_sales_price
                ELSE (ss_quantity*ss_sales_price)
                END act_sales
     FROM store_sales
              LEFT OUTER JOIN store_returns ON (sr_item_sk = ss_item_sk
         AND sr_ticket_number = ss_ticket_number) ,reason
     WHERE sr_reason_sk = r_reason_sk
       AND r_reason_desc = 'reason 28') t
GROUP BY ss_customer_sk
ORDER BY sumsales NULLS FIRST,
         ss_customer_sk NULLS FIRST
    LIMIT 100