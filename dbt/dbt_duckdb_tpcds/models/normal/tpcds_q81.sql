

WITH catalog_returns AS (
    select * from {{ source('external_source', 'catalog_returns') }}
),
inventory AS (
    select * from {{ source('external_source', 'inventory') }}
),
date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
customer AS (
    select * from {{ source('external_source', 'customer') }}
),
customer_address AS (
    select * from {{ source('external_source', 'customer_address') }}
),
customer_total_return AS (
    SELECT cr.cr_returning_customer_sk AS ctr_customer_sk ,
          ca.ca_state AS ctr_state,
          sum(cr.cr_return_amt_inc_tax) AS ctr_total_return
   FROM catalog_returns cr,
        date_dim dt,
        customer_address ca
   WHERE cr.cr_returned_date_sk = dt.d_date_sk
     AND dt.d_year = 2000
     AND cr.cr_returning_addr_sk = ca.ca_address_sk
   GROUP BY cr.cr_returning_customer_sk ,
            ca.ca_state
)

SELECT cu.c_customer_id,
       cu.c_salutation,
       cu.c_first_name,
       cu.c_last_name,
       ca.ca_street_number,
       ca.ca_street_name ,
       ca.ca_street_type,
       ca.ca_suite_number,
       ca.ca_city,
       ca.ca_county,
       ca.ca_state,
       ca.ca_zip,
       ca.ca_country,
       ca.ca_gmt_offset ,
       ca.ca_location_type,
       ctr_total_return
FROM customer_total_return ctr1 ,
     customer_address ca,
     customer cu
WHERE ctr1.ctr_total_return >
      (SELECT avg(ctr_total_return)*1.2
       FROM customer_total_return ctr2
       WHERE ctr1.ctr_state = ctr2.ctr_state)
  AND ca.ca_address_sk = cu.c_current_addr_sk
  AND ca.ca_state = 'GA'
  AND ctr1.ctr_customer_sk = cu.c_customer_sk
ORDER BY cu.c_customer_id,
         cu.c_salutation,
         cu.c_first_name,
         cu.c_last_name,
         ca.ca_street_number,
         ca.ca_street_name ,
         ca.ca_street_type,
         ca.ca_suite_number,
         ca.ca_city,
         ca.ca_county,
         ca.ca_state,
         ca.ca_zip,
         ca.ca_country,
         ca.ca_gmt_offset ,
         ca.ca_location_type,
         ctr_total_return
    LIMIT 100