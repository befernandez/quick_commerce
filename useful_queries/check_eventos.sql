SELECT 
    date_local, 
    EXTRACT(HOUR FROM timestamp_local) AS hour_local,
    platform,
    app_version,
    -- COUNT(DISTINCT IF(platform = "iOS", CONCAT(sessionId, global_entity_id, hitMatchId), NULL)) as count_events_ios,
    COUNT(DISTINCT IF(eventAction = "shop_details.loaded", CONCAT(sessionId, global_entity_id, hitMatchId), NULL)) as count_events_shop_details,
    COUNT(DISTINCT IF(eventAction = "cart.started", CONCAT(sessionId, global_entity_id, hitMatchId), NULL)) as count_events_cart_start,
    SAFE_DIVIDE( COUNT(DISTINCT IF(eventAction = "cart.started", CONCAT(sessionId, global_entity_id, hitMatchId), NULL)), COUNT(DISTINCT IF(eventAction = "shop_details.loaded", CONCAT(sessionId, global_entity_id, hitMatchId), NULL))) AS event_ratio

FROM (
SELECT 
    pe.partition_date AS date,
    DATE(pe.payload_timestamp_local) as date_local,
    pe.payload_timestamp_local as timestamp_local,
    pe.global_entity_id,
    pe.platform,
    pe.appVersionCode AS app_version,
    pe.sessionId,
    pe.userId,
    pe.eventAction,
    pe.hit_number,
    pe.hitMatchId,
    -- eventVariablesJson,

FROM `peya-data-origins-pro.cl_sessions.perseus_events` pe --, UNNEST(eventVariables) as e
WHERE (pe.eventAction IN ("cart.started") OR pe.eventAction IN ("shop_details.loaded") AND LOWER(pe.screenName) IN ('shop_details', 'shop_detail', 'shopdetails', 'shopdetail')) 
  AND LOWER(pe.global_entity_id) IN ("py_uy") AND pe.platform IN ("Android")
  AND ((DATE(pe.partition_date) BETWEEN "2026-01-14" AND "2026-01-16") OR (DATE(pe.partition_date) BETWEEN "2026-01-21" AND "2026-01-23"))
  AND ((DATE(pe.payload_timestamp_local) = "2026-01-15") OR (DATE(pe.payload_timestamp_local) = "2026-01-22"))
ORDER BY pe.sessionId, pe.payload_timestamp_local ASC
)
GROUP BY 1, 2, 3, 4
ORDER BY 3, 4, 1, 2 ASC
