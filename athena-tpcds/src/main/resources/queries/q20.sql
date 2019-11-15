SELECT
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  sum(cs_ext_sales_price) AS itemrevenue,
  sum(cs_ext_sales_price) * 100 / sum(sum(cs_ext_sales_price))
  OVER
  (PARTITION BY i_class) AS revenueratio
FROM catalog_sales[ TABLE_SUFFIX ], item[ TABLE_SUFFIX ], date_dim[ TABLE_SUFFIX ]
WHERE cs_item_sk = i_item_sk
  AND i_category IN ('Sports', 'Books', 'Home')
  AND cs_sold_date_sk = d_date_sk
  AND CAST(d_date as DATE) BETWEEN cast('1999-02-22' AS DATE)
AND (cast('1999-02-22' AS DATE) + INTERVAL '30' day)
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100
