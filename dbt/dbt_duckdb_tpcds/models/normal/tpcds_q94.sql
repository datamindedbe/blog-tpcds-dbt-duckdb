{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q94_100G_result.parquet') }}

WITH date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
customer AS (
    select * from {{ source('external_source', 'customer') }}
),
customer_address AS (
    select * from {{ source('external_source', 'customer_address') }}
),
item AS (
    select * from {{ source('external_source', 'item') }}
),
web_sales AS (
    select * from {{ source('external_source', 'web_sales') }}
),
web_returns AS (
    select * from {{ source('external_source', 'web_returns') }}
),
web_site AS (
    select * from {{ source('external_source', 'web_site') }}
)
SELECT count(DISTINCT ws_order_number) AS "order count" ,
       sum(ws_ext_ship_cost) AS "total shipping cost" ,
       sum(ws_net_profit) AS "total net profit"
FROM web_sales ws1 ,
     date_dim ,
     customer_address ,
     web_site
WHERE d_date BETWEEN '1999-02-01' AND cast('1999-04-02' AS date)
  AND ws1.ws_ship_date_sk = d_date_sk
  AND ws1.ws_ship_addr_sk = ca_address_sk
  AND ca_state = 'IL'
  AND ws1.ws_web_site_sk = web_site_sk
  AND web_company_name = 'pri'
  AND EXISTS
    (SELECT *
     FROM web_sales ws2
     WHERE ws1.ws_order_number = ws2.ws_order_number
       AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
  AND NOT exists
    (SELECT *
     FROM web_returns wr1
     WHERE ws1.ws_order_number = wr1.wr_order_number)
ORDER BY count(DISTINCT ws_order_number)
    LIMIT 100