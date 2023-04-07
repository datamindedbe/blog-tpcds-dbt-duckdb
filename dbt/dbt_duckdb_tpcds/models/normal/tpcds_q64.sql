{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q64_100G_result.parquet') }}

WITH catalog_sales AS (
    select * from {{ source('external_source', 'catalog_sales') }}
),
catalog_returns AS (
    select * from {{ source('external_source', 'catalog_returns') }}
),
store_sales AS (
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
customer AS (
    select * from {{ source('external_source', 'customer') }}
),
customer_demographics AS (
    select * from {{ source('external_source', 'customer_demographics') }}
),
promotion AS (
    select * from {{ source('external_source', 'promotion') }}
),
promotion_demographics AS (
    select * from {{ source('external_source', 'promotion_demographics') }}
),
household_demographics AS (
    select * from {{ source('external_source', 'household_demographics') }}
),
customer_address AS (
    select * from {{ source('external_source', 'customer_address') }}
),
income_band AS (
    select * from {{ source('external_source', 'income_band') }}
),
item AS (
    select * from {{ source('external_source', 'item') }}
),
cs_ui AS (SELECT cs.cs_item_sk,
          sum(cs.cs_ext_list_price) AS sale,
          sum(cr.cr_refunded_cash+cr_reversed_charge+cr.cr_store_credit) AS refund
   FROM catalog_sales cs,
        catalog_returns cr
   WHERE cs.cs_item_sk = cr.cr_item_sk
     AND cs.cs_order_number = cr.cr_order_number
   GROUP BY cs.cs_item_sk
   HAVING sum(cs.cs_ext_list_price)>2*sum(cr.cr_refunded_cash+cr.cr_reversed_charge+cr.cr_store_credit)
),

cross_sales AS
  (SELECT i.i_product_name product_name,
          i.i_item_sk item_sk,
          s.s_store_name store_name,
          s.s_zip store_zip,
          ad1.ca_street_number b_street_number,
          ad1.ca_street_name b_street_name,
          ad1.ca_city b_city,
          ad1.ca_zip b_zip,
          ad2.ca_street_number c_street_number,
          ad2.ca_street_name c_street_name,
          ad2.ca_city c_city,
          ad2.ca_zip c_zip,
          d1.d_year AS syear,
          d2.d_year AS fsyear,
          d3.d_year s2year,
          count(*) cnt,
          sum(ss.ss_wholesale_cost) s1,
          sum(ss.ss_list_price) s2,
          sum(ss.ss_coupon_amt) s3
   FROM store_sales ss,
        store_returns sr,
        cs_ui,
        date_dim d1,
        date_dim d2,
        date_dim d3,
        store s,
        customer cu,
        customer_demographics cd1,
        customer_demographics cd2,
        promotion p,
        household_demographics hd1,
        household_demographics hd2,
        customer_address ad1,
        customer_address ad2,
        income_band ib1,
        income_band ib2,
        item i
   WHERE ss.ss_store_sk = s.s_store_sk
     AND ss.ss_sold_date_sk = d1.d_date_sk
     AND ss.ss_customer_sk = cu.c_customer_sk
     AND ss.ss_cdemo_sk= cd1.cd_demo_sk
     AND ss.ss_hdemo_sk = hd1.hd_demo_sk
     AND ss.ss_addr_sk = ad1.ca_address_sk
     AND ss.ss_item_sk = i.i_item_sk
     AND ss.ss_item_sk = sr.sr_item_sk
     AND ss.ss_ticket_number = sr.sr_ticket_number
     AND ss.ss_item_sk = cs_ui.cs_item_sk
     AND cu.c_current_cdemo_sk = cd2.cd_demo_sk
     AND cu.c_current_hdemo_sk = hd2.hd_demo_sk
     AND cu.c_current_addr_sk = ad2.ca_address_sk
     AND cu.c_first_sales_date_sk = d2.d_date_sk
     AND cu.c_first_shipto_date_sk = d3.d_date_sk
     AND ss.ss_promo_sk = p.p_promo_sk
     AND hd1.hd_income_band_sk = ib1.ib_income_band_sk
     AND hd2.hd_income_band_sk = ib2.ib_income_band_sk
     AND cd1.cd_marital_status <> cd2.cd_marital_status
     AND i.i_color IN ('purple',
                     'burlywood',
                     'indian',
                     'spring',
                     'floral',
                     'medium')
     AND i.i_current_price BETWEEN 64 AND 64 + 10
     AND i.i_current_price BETWEEN 64 + 1 AND 64 + 15
   GROUP BY i.i_product_name,
            i.i_item_sk,
            s.s_store_name,
            s.s_zip,
            ad1.ca_street_number,
            ad1.ca_street_name,
            ad1.ca_city,
            ad1.ca_zip,
            ad2.ca_street_number,
            ad2.ca_street_name,
            ad2.ca_city,
            ad2.ca_zip,
            d1.d_year,
            d2.d_year,
            d3.d_year)


SELECT cs1.product_name,
       cs1.store_name,
       cs1.store_zip,
       cs1.b_street_number,
       cs1.b_street_name,
       cs1.b_city,
       cs1.b_zip,
       cs1.c_street_number,
       cs1.c_street_name,
       cs1.c_city,
       cs1.c_zip,
       cs1.syear cs1syear,
       cs1.cnt cs1cnt,
       cs1.s1 AS s11,
       cs1.s2 AS s21,
       cs1.s3 AS s31,
       cs2.s1 AS s12,
       cs2.s2 AS s22,
       cs2.s3 AS s32,
       cs2.syear,
       cs2.cnt
FROM cross_sales cs1,
     cross_sales cs2
WHERE cs1.item_sk=cs2.item_sk
  AND cs1.syear = 1999
  AND cs2.syear = 1999 + 1
  AND cs2.cnt <= cs1.cnt
  AND cs1.store_name = cs2.store_name
  AND cs1.store_zip = cs2.store_zip
ORDER BY cs1.product_name,
         cs1.store_name,
         cs2.cnt,
         cs1.s1,
         cs2.s1