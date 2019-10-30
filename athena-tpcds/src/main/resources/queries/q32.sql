SELECT 1 AS "excess discount amount "
FROM
  catalog_sales, item, date_dim
WHERE
  i_manufact_id = 977
    AND i_item_sk = cs_item_sk
    AND CAST(d_date as DATE) BETWEEN cast('2000-01-27' AS DATE) AND (cast('2000-01-27' AS DATE) + interval '90' day)
    AND d_date_sk = cs_sold_date_sk
    AND cs_ext_discount_amt > (
    SELECT 1.3 * avg(cs_ext_discount_amt)
    FROM catalog_sales, date_dim
    WHERE cs_item_sk = i_item_sk
      AND CAST(d_date as DATE) BETWEEN cast('2000-01-27' AS DATE) AND (cast('2000-01-27' AS DATE) + interval '90' day)
      AND d_date_sk = cs_sold_date_sk)
LIMIT 100
