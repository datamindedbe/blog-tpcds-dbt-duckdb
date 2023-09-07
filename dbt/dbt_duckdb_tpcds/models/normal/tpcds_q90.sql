

WITH catalog_sales AS (
    select * from {{ source('external_source', 'catalog_sales') }}
),
store_sales AS (
    select * from {{ source('external_source', 'store_sales') }}
),
date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
time_dim AS (
    select * from {{ source('external_source', 'time_dim') }}
),
store AS (
    select * from {{ source('external_source', 'store') }}
),
household_demographics AS (
    select * from {{ source('external_source', 'household_demographics') }}
),
item AS (
    select * from {{ source('external_source', 'item') }}
),
web_page AS (
    select * from {{ source('external_source', 'web_page') }}
),
web_sales AS (
    select * from {{ source('external_source', 'web_sales') }}
)
SELECT case when pmc=0 then null else cast(amc AS decimal(15,4))/cast(pmc AS decimal(15,4)) end am_pm_ratio
FROM
    (SELECT count(*) amc
     FROM web_sales,
          household_demographics,
          time_dim,
          web_page
     WHERE ws_sold_time_sk = time_dim.t_time_sk
       AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
       AND ws_web_page_sk = web_page.wp_web_page_sk
       AND time_dim.t_hour BETWEEN 8 AND 8+1
       AND household_demographics.hd_dep_count = 6
       AND web_page.wp_char_count BETWEEN 5000 AND 5200) AT,
  (SELECT count(*) pmc
   FROM web_sales,
        household_demographics,
        time_dim,
        web_page
   WHERE ws_sold_time_sk = time_dim.t_time_sk
     AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
     AND ws_web_page_sk = web_page.wp_web_page_sk
     AND time_dim.t_hour BETWEEN 19 AND 19+1
     AND household_demographics.hd_dep_count = 6
     AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt
ORDER BY am_pm_ratio
    LIMIT 100