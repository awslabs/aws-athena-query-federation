SELECT count(*)
FROM (
       SELECT DISTINCT
         c_last_name,
         c_first_name,
         d_date
       FROM store_sales[ TABLE_SUFFIX ], date_dim[ TABLE_SUFFIX ], customer[ TABLE_SUFFIX ]
       WHERE store_sales[ TABLE_SUFFIX ].ss_sold_date_sk = date_dim[ TABLE_SUFFIX ].d_date_sk
         AND store_sales[ TABLE_SUFFIX ].ss_customer_sk = customer[ TABLE_SUFFIX ].c_customer_sk
         AND d_month_seq BETWEEN 1200 AND 1200 + 11
       INTERSECT
       SELECT DISTINCT
         c_last_name,
         c_first_name,
         d_date
       FROM catalog_sales[ TABLE_SUFFIX ], date_dim[ TABLE_SUFFIX ], customer[ TABLE_SUFFIX ]
       WHERE catalog_sales[ TABLE_SUFFIX ].cs_sold_date_sk = date_dim[ TABLE_SUFFIX ].d_date_sk
         AND catalog_sales[ TABLE_SUFFIX ].cs_bill_customer_sk = customer[ TABLE_SUFFIX ].c_customer_sk
         AND d_month_seq BETWEEN 1200 AND 1200 + 11
       INTERSECT
       SELECT DISTINCT
         c_last_name,
         c_first_name,
         d_date
       FROM web_sales[ TABLE_SUFFIX ], date_dim[ TABLE_SUFFIX ], customer[ TABLE_SUFFIX ]
       WHERE web_sales[ TABLE_SUFFIX ].ws_sold_date_sk = date_dim[ TABLE_SUFFIX ].d_date_sk
         AND web_sales[ TABLE_SUFFIX ].ws_bill_customer_sk = customer[ TABLE_SUFFIX ].c_customer_sk
         AND d_month_seq BETWEEN 1200 AND 1200 + 11
     ) hot_cust
LIMIT 100