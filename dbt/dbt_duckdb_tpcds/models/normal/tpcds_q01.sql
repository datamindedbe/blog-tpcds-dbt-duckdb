{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q01_100G_result.parquet') }}
WITH catalog_returns AS (
    select * from {{ source('external_source', 'catalog_returns') }}
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
customer AS (
    select * from {{ source('external_source', 'customer') }}
), customer_total_return AS
         (SELECT sr_customer_sk AS ctr_customer_sk,
                 sr_store_sk AS ctr_store_sk,
                 sum(sr_return_amt) AS ctr_total_return
          FROM store_returns sr,
               date_dim d
          WHERE sr.sr_returned_date_sk = d.d_date_sk
            AND d.d_year = 2000
          GROUP BY sr.sr_customer_sk,
                   sr.sr_store_sk)

SELECT c_customer_id
FROM customer_total_return ctr1,
     store s,
     customer c
WHERE ctr1.ctr_total_return >
      (SELECT avg(ctr_total_return)*1.2
       FROM customer_total_return ctr2
       WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s.s_store_sk = ctr1.ctr_store_sk
  AND s.s_state = 'TN'
  AND ctr1.ctr_customer_sk = c.c_customer_sk
ORDER BY c.c_customer_id
    LIMIT 100