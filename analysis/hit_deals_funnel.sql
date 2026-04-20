WITH users_ab as 
(
  SELECT DISTINCT
    platform,
    country,
    experimentId,
    experimentVariation,
    userId, 
    MIN(eventTimestamp) as first_event_timestamp
  FROM `fulfillment-dwh-production.curated_data_shared_experimentation.experiment_assignment_pedidosya`
  WHERE partition_date >= '2026-03-25'
    AND dh_platform = 'pedidosya'
    AND experimentId = 'qc-hit-deals-component'
  GROUP BY 1,2,3,4,5
) 

, user_history AS 
( ----si queres ver la primera orden de alguien en qc, deberias ver trials de qc + acqs cuya orden sea en qc
  SELECT 
    o.user_id,
    COUNT(DISTINCT o.order_id) as orders_before_exp
  FROM `peya-datamarts-pro.dm_onboarding.orders_objectives` o
  INNER JOIN users_ab ab ON SAFE_CAST(ab.userId AS INT64) = o.user_id
  WHERE o.fecha_at_utc < ab.first_event_timestamp  
    AND o.business NOT IN ('RESTAURANT')
  GROUP BY 1
)

-- Paso 1: Sesiones que VIERON el swimlane del test

, initial_impression AS 
(
  SELECT DISTINCT 
    sessionId,
    screenName AS screen_origin,
    CASE WHEN h.orders_before_exp IS NULL OR h.orders_before_exp = 0 THEN 'Prospect' ELSE 'Existing' END AS user_type_at_assignment,
    MIN(payload_timestamp) as first_impression_time
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` p
  LEFT JOIN user_history h on safe_cast(p.userId as INT64) = h.user_id
  WHERE 
    TRUE
    AND partition_date >=  '2026-03-26'
    AND JSON_VALUE(eventVariablesJson.swimlaneTitle) IN ('Precios imperdibles en el mercado' , 'Precios imperdibles en el súper')
    AND eventAction = 'swimlane.shown'
  GROUP BY 1, 2, 3
)

-- Paso 2: Clicks directos en el swimlane (con action)

, target_clicks AS 
(
  SELECT 
    p.sessionId,
    --imm.screen_origin,
    screenName AS screen_origin,
    --JSON_VALUE(p.eventVariablesJson.action) as click_action,
    MAX(CASE WHEN lower(JSON_VALUE(p.eventVariablesJson.action)) = 'see_more' THEN p.sessionId END) AS click_see_more,
    MAX(CASE WHEN lower(JSON_VALUE(p.eventVariablesJson.action)) = 'add_button' THEN p.sessionId END) AS click_add_button,
    MAX(CASE WHEN lower(JSON_VALUE(p.eventVariablesJson.action)) = 'item' THEN p.sessionId END) AS click_item,
    MIN(CASE WHEN lower(JSON_VALUE(p.eventVariablesJson.action)) = 'see_more' THEN p.payload_timestamp END) AS click_see_more_timestamp,
    MIN(CASE WHEN lower(JSON_VALUE(p.eventVariablesJson.action)) = 'add_button' THEN p.payload_timestamp END) AS click_add_button_timestamp,
    MIN(CASE WHEN lower(JSON_VALUE(p.eventVariablesJson.action)) = 'item' THEN p.payload_timestamp END) AS click_item_timestamp,
    MIN(p.payload_timestamp) as first_click_time
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` p
  INNER JOIN initial_impression imm ON p.sessionId = imm.sessionId
  WHERE 
    TRUE
    AND p.partition_date >=  '2026-03-26'
    AND p.eventAction = 'swimlane.clicked'
    AND JSON_VALUE(p.eventVariablesJson.swimlaneTitle) IN ('Precios imperdibles en el mercado' , 'Precios imperdibles en el súper')
    AND p.payload_timestamp >= imm.first_impression_time
  GROUP BY ALL
)

-- Paso 3: Add to cart que TIENEN el título (Filtro directo)

, first_swimlane_clic_tmp AS 
( 
  SELECT 
    p.sessionId, 
    STRING(eventVariablesJson.shopId) as shopId,
    MIN(p.payload_timestamp) as first_swimlane_clic,
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` p
  INNER JOIN initial_impression imm ON p.sessionId = imm.sessionId
  WHERE p.partition_date >=  '2026-03-26'
    AND p.eventAction = 'swimlane.clicked'
    AND JSON_VALUE(p.eventVariablesJson.swimlaneTitle) IN ('Precios imperdibles en el mercado' , 'Precios imperdibles en el súper')
    AND p.payload_timestamp >= imm.first_impression_time
  GROUP BY 1, 2
)

, first_swimlane_clic AS 
( 
  SELECT 
    p.sessionId, 
    screenName AS screen_origin,
    STRING(eventVariablesJson.shopId) as shopId,
     p.payload_timestamp as first_swimlane_clic,
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` p
  INNER JOIN first_swimlane_clic_tmp imm ON p.sessionId = imm.sessionId
  WHERE p.partition_date >=  '2026-03-26'
    AND p.eventAction = 'swimlane.clicked'
    AND JSON_VALUE(p.eventVariablesJson.swimlaneTitle) IN ('Precios imperdibles en el mercado' , 'Precios imperdibles en el súper')
    AND p.payload_timestamp = imm.first_swimlane_clic
)

, add_to_cart_events AS 
( 
  SELECT DISTINCT
    p.sessionId,
    tmp.screen_origin as screen_origin,
    STRING(eventVariablesJson.shopId) as shopId,
    first_swimlane_clic
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` p
  INNER JOIN first_swimlane_clic tmp ON p.sessionId = tmp.sessionId AND p.shopId = tmp.shopId
  WHERE p.partition_date >=  '2026-03-26'
    AND p.eventAction = 'add_cart.clicked'
   -- AND JSON_VALUE(p.eventVariablesJson.swimlaneTitle) IN ('Precios imperdibles en el mercado' , 'Precios imperdibles en el súper')
    AND p.payload_timestamp >= tmp.first_swimlane_clic
)

-- Paso 4: Transacciones que ocurren DESPUÉS de un Add to Cart de ese swimlane

, cart AS 
(
  SELECT 
    p.sessionId,
    --screenName as screen_origin,
    atc.screen_origin,
    STRING(eventVariablesJson.shopId) as shopId, 
    max(payload_timestamp) as max_cart_loaded
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` p
  INNER JOIN add_to_cart_events atc ON p.sessionId = atc.sessionId AND ATC.shopId = p.shopId  
  WHERE p.partition_date >=  '2026-03-26'
    AND p.eventAction in ('cart.loaded')
    AND p.payload_timestamp > atc.first_swimlane_clic
  GROUP BY 1, 2, 3
)

-- Paso 4: Transacciones que ocurren DESPUÉS de un Add to Cart de ese swimlane

, last_cart_status AS 
(
  SELECT DISTINCT  
    p.sessionId,
    cart.screen_origin,
    STRING(eventVariablesJson.shopId) as shopId, 
    CASE WHEN STRING(eventVariablesJson.cartStatus) = 'hard_ok_soft_not_reached' THEN p.sessionId END AS hard_ok_soft_not_reached,
    CASE WHEN STRING(eventVariablesJson.cartStatus) = 'no_hard_mov_reached' THEN p.sessionId END AS no_hard_mov_reached,
    CASE WHEN STRING(eventVariablesJson.cartStatus) = 'completed' THEN p.sessionId END AS completed,
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` p
  INNER JOIN cart  ON p.sessionId = cart.sessionId AND cart.shopId = p.shopId
  WHERE p.partition_date >=  '2026-03-26'
    AND p.eventAction in ('cart.loaded')
    AND p.payload_timestamp = cart.max_cart_loaded 
)

, checkout_loaded AS 
(
  SELECT 
    p.sessionId,
    atc.screen_origin,
    STRING(eventVariablesJson.shopId) as shopId,
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` p
  INNER JOIN add_to_cart_events atc ON p.sessionId = atc.sessionId AND ATC.shopId = p.shopId
  WHERE p.partition_date >=  '2026-03-26'
    AND p.eventAction in ('checkout.loaded')
    AND p.payload_timestamp > atc.first_swimlane_clic
  GROUP BY 1, 2, 3
)


-- Paso 4: Transacciones que ocurren DESPUÉS de un Add to Cart de ese swimlane

, transaction_c AS 
(
  SELECT 
    p.sessionId,
    atc.screen_origin,
    STRING(eventVariablesJson.shopId) as shopId,
    COUNT(DISTINCT p.event_id) as transacciones
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` p
  INNER JOIN add_to_cart_events atc ON p.sessionId = atc.sessionId AND ATC.shopId = p.shopId
  WHERE p.partition_date >=  '2026-03-26'
    AND p.eventAction in ('transaction.clicked')
    AND p.payload_timestamp > atc.first_swimlane_clic
  GROUP BY 1, 2, 3
)

  -- Paso 4: Transacciones que ocurren DESPUÉS de un Add to Cart de ese swimlane

, transactions AS 
(
  SELECT 
    p.sessionId,
    atc.screen_origin,
    STRING(eventVariablesJson.shopId) as shopId,
    COUNT(DISTINCT p.event_id) as transacciones
  FROM `peya-data-origins-pro.cl_sessions.perseus_events` p
  INNER JOIN add_to_cart_events atc ON p.sessionId = atc.sessionId
  WHERE p.partition_date >=  '2026-03-26'
    AND p.eventAction in ('transaction')
    AND p.payload_timestamp > atc.first_swimlane_clic
    AND ATC.shopId = p.shopId -- Secuencialidad
  GROUP BY 1, 2, 3
)

-- Paso 5: Reporte Final
SELECT --i.*,
    --i.sessionId,
    i.screen_origin AS flujo_origen,
    i.user_type_at_assignment,
   -- CASE WHEN c.sessionid IS NULL THEN 1 END as no_click,
  --  CASE WHEN click_see_more IS NOT NULL OR click_item IS NOT NULL THEN 1 END as session_has_click_see_more_item,
  --  CASE WHEN click_add_button IS NOT NULL AND click_see_more IS NULL AND  click_item is null THEN 1 END as session_only_add_cart,
    COUNT(DISTINCT i.sessionId) AS total_sesiones_shown,
    COUNT(DISTINCT c.sessionId) AS sesiones_con_click,
    COUNT(DISTINCT click_see_more) AS click_see_more,
    COUNT(DISTINCT click_add_button) AS click_add_button,
    COUNT(DISTINCT click_item) AS click_item, 
    
    COUNT(DISTINCT atc.sessionId) AS sesiones_con_atc,
    COUNT(DISTINCT ca.sessionId) AS cart_loaded,
    COUNT(DISTINCT CASE WHEN ca.sessionId IS NULL AND atc.sessionId IS NOT NULL THEN i.sessionId END ) AS no_cart_loaded,
    COUNT(DISTINCT CASE WHEN no_hard_mov_reached IS NOT NULL THEN cs.sessionId END ) AS no_hard_mov_reached,
    COUNT(DISTINCT CASE WHEN hard_ok_soft_not_reached IS NOT NULL THEN cs.sessionId END ) AS hard_ok_soft_not_reached,
    
    COUNT(DISTINCT CASE WHEN completed IS NOT NULL THEN cs.sessionId END ) AS completed,
    COUNT(DISTINCT cl.sessionId) AS sesiones_con_checkout_loaded,
    COUNT(DISTINCT ct.sessionId) AS sesiones_con_transaction_clicked,
    COUNT(DISTINCT t.sessionId) AS sesiones_con_transaccion,
    
    -- Métricas
    SAFE_DIVIDE(COUNT(DISTINCT c.sessionId), COUNT(DISTINCT i.sessionId)) AS CTR,
    SAFE_DIVIDE(COUNT(DISTINCT t.sessionId), COUNT(DISTINCT i.sessionId)) AS CVR_Total,

    
FROM initial_impression i
LEFT JOIN target_clicks c ON i.sessionId = c.sessionId AND i.screen_origin = c.screen_origin
LEFT JOIN add_to_cart_events atc ON i.sessionId = atc.sessionId AND i.screen_origin = atc.screen_origin
LEFT JOIN cart ca ON i.sessionId = ca.sessionId AND i.screen_origin = ca.screen_origin
LEFT JOIN last_cart_status cs ON i.sessionId = cs.sessionId AND i.screen_origin = cs.screen_origin 
LEFT JOIN checkout_loaded cl ON i.sessionId = cl.sessionId AND i.screen_origin = cl.screen_origin 
LEFT JOIN transactions t ON i.sessionId = t.sessionId AND i.screen_origin = t.screen_origin 
LEFT JOIN transaction_c ct ON i.sessionId = ct.sessionId AND i.screen_origin = ct.screen_origin 
 --where completed is   null and cl.sessionId is not null 
 --where i.sessionId  = '1776270442478.4923642970.iktduwqhuj'
--where atc.sessionId is not null
GROUP BY ALL
--ORDER BY sessionId DESC
limit 1000
