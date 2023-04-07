{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q46_100G_result.parquet') }}

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
),
household_demographics AS (
    select * from {{ source('external_source', 'household_demographics') }}
)
SELECT c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       amt,
       profit
FROM
    (SELECT ss_ticket_number,
            ss_customer_sk,
            ca_city bought_city,
            sum(ss_coupon_amt) amt,
            sum(ss_net_profit) profit
     FROM store_sales,
          date_dim,
          store,
          household_demographics,
          customer_address
     WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
       AND store_sales.ss_store_sk = store.s_store_sk
       AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
       AND store_sales.ss_addr_sk = customer_address.ca_address_sk
       AND (household_demographics.hd_dep_count = 4
         OR household_demographics.hd_vehicle_count= 3)
       AND date_dim.d_dow IN (6,
                              0)
       AND date_dim.d_year IN (1999,
                               1999+1,
                               1999+2)
       AND store.s_city IN ('Fairview',
                            'Midway')
     GROUP BY ss_ticket_number,
              ss_customer_sk,
              ss_addr_sk,
              ca_city) dn,
    customer,
    customer_address current_addr
WHERE ss_customer_sk = c_customer_sk
  AND customer.c_current_addr_sk = current_addr.ca_address_sk
  AND current_addr.ca_city <> bought_city
ORDER BY c_last_name NULLS FIRST,
         c_first_name NULLS FIRST,
         ca_city NULLS FIRST,
         bought_city NULLS FIRST,
         ss_ticket_number NULLS FIRST
    LIMIT 100