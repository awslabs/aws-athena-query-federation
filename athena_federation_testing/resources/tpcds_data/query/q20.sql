-- start query 20 in stream 0 using template query20.tpl
SELECT
         i_item_id ,
         i_item_desc ,
         i_category ,
         i_class ,
         i_current_price ,
         Sum(cs_ext_sales_price)                                                              AS itemrevenue ,
         Sum(cs_ext_sales_price)*100/Sum(Sum(cs_ext_sales_price)) OVER (partition BY i_class) AS revenueratio
FROM     {catalog_place_holder}.{database_place_holder}.{table_prefix}catalog_sales ,
         {catalog_place_holder}.{database_place_holder}.{table_prefix}item ,
         {catalog_place_holder}.{database_place_holder}.{table_prefix}date_dim
WHERE    cs_item_sk = i_item_sk
AND      i_category IN ('Children',
                        'Women',
                        'Electronics')
AND      cs_sold_date_sk = d_date_sk
AND      d_date BETWEEN Cast('2001-02-03' AS DATE) AND      (
                  Cast('2001-02-03' AS DATE) + INTERVAL '30' day)
GROUP BY i_item_id ,
         i_item_desc ,
         i_category ,
         i_class ,
         i_current_price
ORDER BY i_category ,
         i_class ,
         i_item_id ,
         i_item_desc ,
         revenueratio
LIMIT 100;
