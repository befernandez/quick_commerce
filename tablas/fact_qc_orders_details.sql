
INSERT INTO `peya-bi-tools-pro.il_qcommerce.fact_qc_orders_details` WITH xselling_transactions AS(

SELECT DISTINCT
  session_id,
  global_entity_id,
  client_id,
  partner_id,
  upselling_product_id,
  upsellingTransactions
FROM `peya-bi-tools-pro.il_sessions.perseus_fact_upselling`
LEFT JOIN UNNEST (split(upselling_product_click_transaction_id,',')) upsellingTransactions
WHERE date BETWEEN DATE('2023-01-01') AND DATE('2023-12-25')
  AND partner_business_name NOT IN ('Restaurant', 'Coffee')
  AND upsellingTransactions IS NOT NULL
  AND upselling_location = 'cart'

)

, categories AS(

SELECT DISTINCT
  remote_vendor_id AS partner_id,
  vendor_name AS partner_name,
  remote_product_id AS product_id,
  sku,
  product_name,
  parent_category_name[OFFSET(0)] AS Level_1,
  category_name[OFFSET(0)] AS Level_2
FROM
  `peya-bi-tools-pro.il_qcommerce.dim_vendor_product`
  CROSS JOIN UNNEST(parent_category_name) AS parent_category WITH OFFSET
    JOIN UNNEST(category_name) AS category WITH OFFSET
    USING(OFFSET)

)

SELECT o.*
 ,MAX(CASE WHEN pu.user_id IS NOT NULL THEN 1 ELSE 0 END) AS plus_user,
 CURRENT_TIMESTAMP() AS _ingested_at
FROM(
SELECT
  o.order_id,
  o.registered_date,
  o.user.id AS user_id,
  o.restaurant.id AS partner_id,
  hp.business_name AS business_type,
  hp.is_darkstore AS dmart,
  o.country.country_id,
  o.country.country_name,
  o.shipping_amount,
  o.shipping_amount/ex.rate_eu AS shipping_amount_eu,
  us.user_segment,
  CASE WHEN o.shipping_amount > 0 THEN 1 ELSE 0 END AS has_delivery_fee,
  o.application AS platform,
  hp.min_delivery_amount AS MOV_lc,
  hp.min_delivery_amount/ex.rate_eu AS MOV_eu,
  m.mission_type AS mission_type,
  MAX(CASE WHEN ut.upsellingTransactions IS NOT NULL THEN 1 ELSE 0 END) AS xselling_order,
  COUNT(DISTINCT det.product.product_id) AS distinct_products_order,
  SUM(det.quantity) AS total_items_order,
  COUNT(DISTINCT CASE WHEN upselling_product_id = det.product.product_id THEN det.product.product_id ELSE NULL END) AS distinct_products_xselling_order,
  SUM(CASE WHEN upselling_product_id = det.product.product_id THEN det.quantity ELSE NULL END) AS xselling_items,
  SUM(CASE WHEN upselling_product_id = det.product.product_id THEN det.product.price ELSE NULL END) AS xselling_product_price_lc,
  SUM(CASE WHEN upselling_product_id = det.product.product_id THEN det.product.price*det.quantity ELSE NULL END)/ex.rate_eu AS xselling_product_price_eu,
  total_amount + o.shipping_amount + IFNULL(serviceFee.service_fee_amount,0) AS GMV_LC,
  (total_amount + o.shipping_amount + IFNULL(serviceFee.service_fee_amount,0))/ex.rate_eu AS GMV_eu,
  total_amount AS items_price_lc,
  total_amount/ex.rate_eu AS items_price_eu,
  amount_no_discount AS items_no_discount_lc,
  amount_no_discount/ex.rate_eu AS items_no_discount_eu,
  count(DISTINCT CASE WHEN Level_1 = 'Root' OR Level_1 IS NULL THEN Level_2 ELSE Level_1 END) AS total_categories,

FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
LEFT JOIN UNNEST(details) det
INNER JOIN `peya-bi-tools-pro.il_core.dim_historical_partners` AS hp
  ON hp.restaurant_id = o.restaurant.id
  AND DATE(hp.full_date) = DATE(o.registered_date)
LEFT JOIN xselling_transactions AS ut
  ON safe_cast(ut.upsellingTransactions AS int) = o.order_id
  AND ut.upselling_product_id = det.product.product_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS country
  ON country.country_id = o.country_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_currency_exchange` AS ex
  ON ex.currency_id = country.currency_id
  AND ex.currency_exchange_date = DATE_TRUNC(o.registered_date, MONTH)
LEFT JOIN categories AS c
  ON safe_cast(c.partner_id AS int) = o.restaurant.id
  AND safe_cast(c.product_id AS int)= det.product.product_id
LEFT JOIN `peya-bi-tools-pro.il_qcommerce.fact_user_segmentation_snapshot` AS us
  ON us.user_id = o.user.id
  AND us.snapshot_date BETWEEN DATE_SUB('2026-01-08', INTERVAL 7 DAY) AND DATE('2026-01-08')
LEFT JOIN `peya-bi-tools-pro.il_qcommerce.fact_groceries_shopping_missions` AS m
  ON m.order_id = o.order_id
WHERE
  UPPER(o.order_status) = 'CONFIRMED'
  AND DATE(o.registered_date) BETWEEN DATE_SUB('2026-01-08', INTERVAL 7 DAY) AND DATE('2026-01-08')
  AND DATE(m.registered_date) BETWEEN DATE_SUB('2026-01-08', INTERVAL 7 DAY) AND DATE('2026-01-08')
  AND hp.business_name NOT IN ('Restaurant', 'Courier', 'Courier Business', 'Coffee')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, total_amount, o.shipping_amount ,serviceFee.service_fee_amount, ex.rate_eu, amount_no_discount
) AS o
LEFT JOIN `peya-data-origins-pro.cl_loyalty.loyalty_subscription_historical` AS pu
  ON pu.user_id = o.user_id
  AND date(start_date) <= o.registered_date
  AND (date(cancellation_date) IS NULL OR date(cancellation_date) >= o.registered_date)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30