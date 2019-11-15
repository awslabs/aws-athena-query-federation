SELECT
  dt.d_year,
  item[ TABLE_SUFFIX ].i_category_id,
  item[ TABLE_SUFFIX ].i_category,
  sum(ss_ext_sales_price)
FROM date_dim[ TABLE_SUFFIX ] dt, store_sales[ TABLE_SUFFIX ], item[ TABLE_SUFFIX ]
WHERE dt.d_date_sk = store_sales[ TABLE_SUFFIX ].ss_sold_date_sk
  AND store_sales[ TABLE_SUFFIX ].ss_item_sk = item[ TABLE_SUFFIX ].i_item_sk
  AND item[ TABLE_SUFFIX ].i_manager_id = 1
  AND dt.d_moy = 11
  AND dt.d_year = 2000
GROUP BY dt.d_year
  , item[ TABLE_SUFFIX ].i_category_id
  , item[ TABLE_SUFFIX ].i_category
ORDER BY sum(ss_ext_sales_price) DESC, dt.d_year
  , item[ TABLE_SUFFIX ].i_category_id
  , item[ TABLE_SUFFIX ].i_category
LIMIT 100