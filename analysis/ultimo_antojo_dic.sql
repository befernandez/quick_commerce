DECLARE from_date DATE DEFAULT DATE '2025-11-01';
DECLARE to_date   DATE DEFAULT DATE '2025-11-30';

-- 1. Eventos de impresión y extracción de SKUs sugeridos
WITH ImpressionEvents AS 
(
  SELECT DISTINCT 
    CONCAT(sessionId, global_entity_id) AS sessionId,
    t1.userId,
    t1.global_entity_id,
    CONCAT(sessionId, global_entity_id, hitMatchId) AS hitMatchId,
    t1.shopId,
    SPLIT(STRING(t1.eventVariablesJson.productsSuggested), ':')[SAFE_OFFSET(1)] AS suggested_ids_string
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` t1
  WHERE TRUE
    AND t1.partition_date BETWEEN from_date AND to_date
    AND t1.eventAction = 'last_aisle.loaded'
    AND STRING(t1.eventVariablesJson.screenName) = 'LastAisle'
    AND NOT STARTS_WITH(STRING(t1.eventVariablesJson.productsSuggested), '0:')
),

-- 2. Unnest de SKUs sugeridos
UnnestedImpressions AS 
(
  SELECT
    t1.sessionId,
    t1.userId,
    t1.global_entity_id, 
    t1.shopId, 
    suggested_sku,
    hitMatchId
  FROM ImpressionEvents t1,
  UNNEST(SPLIT(t1.suggested_ids_string, ',')) AS suggested_sku
),

-- 3. Eventos de clic
ClickEvents AS 
(
  SELECT DISTINCT 
    CONCAT(sessionId, global_entity_id) AS sessionId,
    CONCAT(sessionId, global_entity_id, hitMatchId) AS hitMatchId,
    t2.userId,
    t2.global_entity_id, 
    t2.shopId,
    STRING(t2.eventVariablesJson.productSku) AS product_sku
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` t2
  WHERE TRUE
    AND t2.partition_date BETWEEN from_date AND to_date
    AND t2.eventAction = 'product.clicked'
    AND STRING(t2.eventVariablesJson.screenName) = 'LastAisle'
),

-- 4. Transacciones
trx AS 
(
  SELECT DISTINCT 
    CONCAT(sessionId, global_entity_id) AS sessionId,
    t2.userId,
    t2.global_entity_id,  
    STRING(t2.eventVariablesJson.orderId) AS orderId,
    STRING(t2.eventVariablesJson.shopId) AS shopId
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` t2
  WHERE TRUE
    AND t2.partition_date BETWEEN from_date AND to_date
    AND t2.eventAction = 'transaction'
),

-- 5. Fact orders
fact_orders AS 
(
  SELECT DISTINCT 
    o.order_id,
    user.id AS user_id,
    d.product_name,
    d.product.product_id
  FROM `peya-bi-tools-pro.il_core.fact_orders` AS o 
  LEFT JOIN UNNEST(details) d
  WHERE TRUE
    AND o.registered_date BETWEEN from_date AND to_date
),

-- 6. Relación sesión-producto-orden
session_prod_order AS 
(
  SELECT DISTINCT
    sessionId,
    userId,
    shopId,
    order_id,
    product_id
  FROM trx
  LEFT JOIN fact_orders 
    ON fact_orders.order_id = SAFE_CAST(trx.orderId AS INT64)
  WHERE order_id IS NOT NULL 
),

currency_exchange AS
(
  SELECT 
     dc.country_code
    ,dce.currency_exchange_date
    ,dce.rate_eu
    ,dce.currency_iso
    ,dce.currency_id
    , dc.country_id
  FROM `peya-bi-tools-pro.il_core.dim_country` dc
  LEFT JOIN `peya-bi-tools-pro.il_core.dim_currency_exchange` dce  ON dc.currency_id = dce.currency_id
  where dce.currency_exchange_date ='2025-12-01'
)

, product_price as
( 
  SELECT distinct  
  id as product_id
  , d.country_id
  , name
  , price AS local_price
  , rate_eu
  , ROUND(SAFE_DIVIDE(price, rate_eu),2)  as price_eu
  FROM `peya-bi-tools-pro.il_core.dim_product` d
  left join currency_exchange on currency_exchange.country_id = d.country_id
)

-- 7. Base final
, base_final AS 
(
  SELECT DISTINCT 
    imp.sessionId,
    imp.userId,
    imp.global_entity_id,
    imp.shopId,
    imp.suggested_sku,
    imp.hitMatchId,
    p.product_name,
    cli.sessionId AS product_clicked_sessions,
    cli.hitMatchId AS product_clicked_hitMatchId,
    cli.userId AS product_clicked_userId,
    so.sessionId AS product_trx,
    local_price,
    price_eu
  FROM UnnestedImpressions imp
  LEFT JOIN ClickEvents cli 
    ON imp.userId = cli.userId
    AND imp.shopId = cli.shopId
    AND imp.suggested_sku = cli.product_sku
    AND imp.sessionId = cli.sessionId
  INNER JOIN `peya-bi-tools-pro.il_qcommerce.dim_vendor_product` p
    ON imp.suggested_sku = CAST(p.remote_product_id AS STRING)
  LEFT JOIN session_prod_order so 
    ON cli.userId = SAFE_CAST(so.userId AS STRING)
    AND cli.shopId = SAFE_CAST(so.shopId AS STRING)
    AND cli.product_sku = SAFE_CAST(so.product_id AS STRING)
    AND cli.sessionId = so.sessionId
  LEFT JOIN product_price ON SAFE_CAST(product_price.product_id AS STRING)=  imp.suggested_sku
)

 
-- 8. Resultado final
SELECT
  global_entity_id,
  base_final.product_name, 
  COUNT(DISTINCT hitMatchId) AS impressions_hits,
  COUNT(DISTINCT product_clicked_hitMatchId) AS clicks_hits,
  COUNT(DISTINCT product_trx) AS trx_products
FROM base_final 
GROUP BY
  global_entity_id,
  product_name 