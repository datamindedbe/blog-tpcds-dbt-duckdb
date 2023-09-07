

WITH catalog_sales AS (
    select * from {{ source('external_source', 'catalog_sales') }}
),
catalog_returns AS (
    select * from {{ source('external_source', 'catalog_returns') }}
),
warehouse AS (
    select * from {{ source('external_source', 'warehouse') }}
),
date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
item AS (
    select * from {{ source('external_source', 'item') }}
)

SELECT w.w_state,
       i.i_item_id,
       sum(CASE
               WHEN (cast(dt.d_date AS date) < CAST ('2000-03-11' AS date)) THEN cs.cs_sales_price - coalesce(cr.cr_refunded_cash,0)
               ELSE 0
           END) AS sales_before,
       sum(CASE
               WHEN (cast(dt.d_date AS date) >= CAST ('2000-03-11' AS date)) THEN cs.cs_sales_price - coalesce(cr.cr_refunded_cash,0)
               ELSE 0
           END) AS sales_after
FROM catalog_sales cs
         LEFT OUTER JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number
    AND cs.cs_item_sk = cr.cr_item_sk) ,
     warehouse w,
     item i,
     date_dim dt
WHERE i.i_current_price BETWEEN 0.99 AND 1.49
  AND I.i_item_sk = cs.cs_item_sk
  AND cs.cs_warehouse_sk = w.w_warehouse_sk
  AND cs.cs_sold_date_sk = dt.d_date_sk
  AND dt.d_date BETWEEN CAST ('2000-02-10' AS date) AND CAST ('2000-04-10' AS date)
GROUP BY w.w_state,
         i.i_item_id
ORDER BY w.w_state,
         i.i_item_id
    LIMIT 100