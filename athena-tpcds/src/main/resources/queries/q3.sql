SELECT
  dt.d_year,
  it.i_brand_id brand_id,
  it.i_brand brand,
  SUM(ss_ext_sales_price) sum_agg
FROM date_dim[ TABLE_SUFFIX ] dt, store_sales[ TABLE_SUFFIX ] ss, item[ TABLE_SUFFIX ] it
WHERE dt.d_date_sk = ss.ss_sold_date_sk
  AND ss.ss_item_sk = it.i_item_sk
  AND it.i_manufact_id = 128
  AND dt.d_moy = 11
GROUP BY dt.d_year, it.i_brand, it.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id
LIMIT 100