


WITH store_sales AS (
         select * from {{ source('external_source', 'store_sales') }}
     ),
     date_dim AS (
         select * from {{ source('external_source', 'date_dim') }}
     ),
     store AS (
         select * from {{ source('external_source', 'store') }}
     )

SELECT sum(ss.ss_net_profit) AS total_sum,
       s.s_state,
       s.s_county,
       grouping(s.s_state)+grouping(s.s_county) AS lochierarchy,
       rank() OVER (PARTITION BY grouping(s.s_state)+grouping(s.s_county),
                                 CASE
                                     WHEN grouping(s.s_county) = 0 THEN s.s_state
                                 END
                    ORDER BY sum(ss.ss_net_profit) DESC) AS rank_within_parent
FROM store_sales ss,
     date_dim d1,
     store s
WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
  AND d1.d_date_sk = ss.ss_sold_date_sk
  AND s.s_store_sk = ss.ss_store_sk
  AND s.s_state IN
      (SELECT s.s_state
       FROM
           (SELECT s.s_state AS s_state,
                   rank() OVER (PARTITION BY s.s_state
                            ORDER BY sum(ss.ss_net_profit) DESC) AS ranking
            FROM store_sales ss,
                 store s,
                 date_dim d1
            WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
              AND d1.d_date_sk = ss.ss_sold_date_sk
              AND s.s_store_sk = ss.ss_store_sk
            GROUP BY s.s_state) tmp1
       WHERE ranking <= 5 )
GROUP BY rollup(s.s_state,s.s_county)
ORDER BY lochierarchy DESC ,
         CASE
             WHEN lochierarchy = 0 THEN s.s_state
             END ,
         rank_within_parent
    LIMIT 100