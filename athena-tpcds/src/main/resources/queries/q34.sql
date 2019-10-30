SELECT
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag,
  ss_ticket_number,
  cnt
FROM
  (SELECT
    ss_ticket_number,
    ss_customer_sk,
    count(*) cnt
  FROM store_sales[ TABLE_SUFFIX ], date_dim[ TABLE_SUFFIX ], store[ TABLE_SUFFIX ], household_demographics[ TABLE_SUFFIX ]
  WHERE store_sales[ TABLE_SUFFIX ].ss_sold_date_sk = date_dim[ TABLE_SUFFIX ].d_date_sk
    AND store_sales[ TABLE_SUFFIX ].ss_store_sk = store[ TABLE_SUFFIX ].s_store_sk
    AND store_sales[ TABLE_SUFFIX ].ss_hdemo_sk = household_demographics[ TABLE_SUFFIX ].hd_demo_sk
    AND (date_dim[ TABLE_SUFFIX ].d_dom BETWEEN 1 AND 3 OR date_dim[ TABLE_SUFFIX ].d_dom BETWEEN 25 AND 28)
    AND (household_demographics[ TABLE_SUFFIX ].hd_buy_potential = '>10000' OR
    household_demographics[ TABLE_SUFFIX ].hd_buy_potential = 'unknown')
    AND household_demographics[ TABLE_SUFFIX ].hd_vehicle_count > 0
    AND (CASE WHEN household_demographics[ TABLE_SUFFIX ].hd_vehicle_count > 0
    THEN household_demographics[ TABLE_SUFFIX ].hd_dep_count / household_demographics[ TABLE_SUFFIX ].hd_vehicle_count
         ELSE NULL
         END) > 1.2
    AND date_dim[ TABLE_SUFFIX ].d_year IN (1999, 1999 + 1, 1999 + 2)
    AND store[ TABLE_SUFFIX ].s_county IN
    ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County',
     'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County')
  GROUP BY ss_ticket_number, ss_customer_sk) dn, customer
WHERE ss_customer_sk = c_customer_sk
  AND cnt BETWEEN 15 AND 20
ORDER BY c_last_name, c_first_name, c_salutation, c_preferred_cust_flag DESC
