

WITH store_returns AS (
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
),
customer_address AS (
    select * from {{ source('external_source', 'customer_address') }}
),
customer_demographics AS (
    select * from {{ source('external_source', 'customer_demographics') }}
),
household_demographics AS (
    select * from {{ source('external_source', 'household_demographics') }}
),
income_band AS (
    select * from {{ source('external_source', 'income_band') }}
)
SELECT c_customer_id AS customer_id ,
       concat(concat(coalesce(c_last_name, '') , ', '), coalesce(c_first_name, '')) AS customername
FROM customer ,
     customer_address ,
     customer_demographics ,
     household_demographics ,
     income_band ,
     store_returns
WHERE ca_city = 'Edgewood'
  AND c_current_addr_sk = ca_address_sk
  AND ib_lower_bound >= 38128
  AND ib_upper_bound <= 38128 + 50000
  AND ib_income_band_sk = hd_income_band_sk
  AND cd_demo_sk = c_current_cdemo_sk
  AND hd_demo_sk = c_current_hdemo_sk
  AND sr_cdemo_sk = cd_demo_sk
ORDER BY c_customer_id NULLS FIRST
    LIMIT 100