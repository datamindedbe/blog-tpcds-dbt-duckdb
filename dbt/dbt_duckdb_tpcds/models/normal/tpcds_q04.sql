{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q04_100G_result.parquet') }}
WITH catalog_sales AS (
    select * from {{ source('external_source', 'catalog_sales') }}
),
store_sales AS (
    select * from {{ source('external_source', 'store_sales') }}
),
date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
customer AS (
    select * from {{ source('external_source', 'customer') }}
),
item AS (
    select * from {{ source('external_source', 'item') }}
),
web_sales AS (
    select * from {{ source('external_source', 'web_sales') }}
),
year_total AS
         (SELECT c.c_customer_id customer_id,
                 c.c_first_name customer_first_name,
                 c.c_last_name customer_last_name,
                 c.c_preferred_cust_flag customer_preferred_cust_flag,
                 c.c_birth_country customer_birth_country,
                 c.c_login customer_login,
                 c.c_email_address customer_email_address,
                 d.d_year dyear,
                 sum(((ss.ss_ext_list_price-ss.ss_ext_wholesale_cost-ss.ss_ext_discount_amt)+ss.ss_ext_sales_price)/2) year_total,
                 's' sale_type
          FROM customer c,
               store_sales ss,
               date_dim d
          WHERE c.c_customer_sk = ss.ss_customer_sk
            AND ss.ss_sold_date_sk = d.d_date_sk
          GROUP BY c.c_customer_id,
                   c.c_first_name,
                   c.c_last_name,
                   c.c_preferred_cust_flag,
                   c.c_birth_country,
                   c.c_login,
                   c.c_email_address,
                   d.d_year
          UNION ALL SELECT c.c_customer_id customer_id,
                           c.c_first_name customer_first_name,
                           c.c_last_name customer_last_name,
                           c.c_preferred_cust_flag customer_preferred_cust_flag,
                           c.c_birth_country customer_birth_country,
                           c.c_login customer_login,
                           c.c_email_address customer_email_address,
                           d.d_year dyear,
                           sum((((cs.cs_ext_list_price-cs.cs_ext_wholesale_cost-cs.cs_ext_discount_amt)+cs.cs_ext_sales_price)/2)) year_total,
                           'c' sale_type
          FROM customer c,
               catalog_sales cs,
               date_dim d
          WHERE c.c_customer_sk = cs.cs_bill_customer_sk
            AND cs.cs_sold_date_sk = d.d_date_sk
          GROUP BY c.c_customer_id,
                   c.c_first_name,
                   c.c_last_name,
                   c.c_preferred_cust_flag,
                   c.c_birth_country,
                   c.c_login,
                   c.c_email_address,
                   d.d_year
          UNION ALL SELECT c.c_customer_id customer_id,
                           c.c_first_name customer_first_name,
                           c.c_last_name customer_last_name,
                           c.c_preferred_cust_flag customer_preferred_cust_flag,
                           c.c_birth_country customer_birth_country,
                           c.c_login customer_login,
                           c.c_email_address customer_email_address,
                           d.d_year dyear,
                           sum((((ws.ws_ext_list_price-ws.ws_ext_wholesale_cost-ws.ws_ext_discount_amt)+ws.ws_ext_sales_price)/2)) year_total,
                           'w' sale_type
          FROM customer c,
               web_sales ws,
               date_dim d
          WHERE c.c_customer_sk = ws.ws_bill_customer_sk
            AND ws.ws_sold_date_sk = d.d_date_sk
          GROUP BY c.c_customer_id,
                   c.c_first_name,
                   c.c_last_name,
                   c.c_preferred_cust_flag,
                   c.c_birth_country,
                   c.c_login,
                   c.c_email_address,
                   d.d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_preferred_cust_flag
FROM year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_c_firstyear,
     year_total t_c_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_c_secyear.customer_id
  AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_c_firstyear.sale_type = 'c'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_c_secyear.sale_type = 'c'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.dyear = 2001
  AND t_s_secyear.dyear = 2001+1
  AND t_c_firstyear.dyear = 2001
  AND t_c_secyear.dyear = 2001+1
  AND t_w_firstyear.dyear = 2001
  AND t_w_secyear.dyear = 2001+1
  AND t_s_firstyear.year_total > 0
  AND t_c_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
          END > CASE
                    WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                    ELSE NULL
          END
  AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
          END > CASE
                    WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
                    ELSE NULL
          END
ORDER BY t_s_secyear.customer_id NULLS FIRST,
         t_s_secyear.customer_first_name NULLS FIRST,
         t_s_secyear.customer_last_name NULLS FIRST,
         t_s_secyear.customer_preferred_cust_flag NULLS FIRST
    LIMIT 100
