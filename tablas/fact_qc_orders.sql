DECLARE start_date date; 		
DECLARE end_date date; 		

SET start_date = "2026-03-01";
SET end_date = "2026-03-10";


WITH sessionid AS 
(
  SELECT
    o.order_id AS order_id,
    session_id,
    session_start_timestamp_utc
  FROM
    `peya-bi-tools-pro.il_sessions.fact_perseus_sessions` p
  LEFT JOIN
    UNNEST(orders) AS o
  WHERE
    p.partition_date >= start_date
    AND o.order_id IS NOT NULL
  QUALIFY
    ROW_NUMBER() OVER(PARTITION BY o.order_id ORDER BY session_start_timestamp_utc ASC) = 1
) 


, double_ya AS 
(
  SELECT DISTINCT 
  order_details.id.secondary as order_id
  FROM 
    `peya-bi-tools-pro.il_qcommerce.fact_orders_double_ya` 
  WHERE 
    TRUE
    AND order_type="DOUBLE_YA"
  AND date >= start_date
)

, shopping_missions AS
(
  SELECT 
    order_id, 
    mission_type
  FROM 
    `peya-bi-tools-pro.il_qcommerce.fact_groceries_shopping_missions` sm
  WHERE 
    TRUE
    AND registered_date>=start_date
)

, non_seamless AS 
(
  SELECT DISTINCT
    platform_order_code,
    CASE WHEN non_seamless_order IS TRUE THEN 1 ELSE 0 END AS is_non_seamless_order,
    is_slow_order,
    is_late_order,
    is_session_order,
    is_modified_order,
    actual_delivery_time
  FROM 
    `peya-datamarts-pro.dm_fulfillment.non_seamless_delivery_order_level` 
  WHERE 
    TRUE
    AND created_date_local >=start_date
)

, comp_and_ref AS 
(
  SELECT DISTINCT 
    original_order_id
    , max(CASE WHEN care = 'Compensation' THEN 1 END) AS has_compensation
    , MAX(CASE WHEN care = 'Refunds' THEN 1 END)      AS has_refund
  FROM 
    `peya-bi-tools-pro.il_compensations.fact_compensations_and_refunds_care`
  LEFT JOIN UNNEST (cor) as cor
  WHERE 
    TRUE
    AND date(created_date) >= start_date
  GROUP BY 1
)

, fact_orders_profitability AS 
(
  SELECT  
    order_id,
    gpo.gpo as gpo  
  FROM 
    `peya-datamarts-pro.dm_order_profitability.fact_order_profitability`
  WHERE 
    registered_date >= start_date
)
 
, discount_products AS 
(
  SELECT
    o.order_id,
    CASE WHEN app.applies_to = 'PRODUCT' THEN app.product_id END as product_with_promo,
    COUNT(DISTINCT CASE WHEN app.applies_to = 'PRODUCT' THEN app.product_id END ) AS qty_prod_with_disc
    /* app.product_id as product_id_with_discount,
    app.product_name, 
    di.discount_id,
    di.discount_type,
    app.amount AS amount_discount,*/
  FROM
    `peya-bi-tools-pro.il_core.fact_orders` AS o
    LEFT JOIN UNNEST(o.discounts_v2) AS di
    LEFT JOIN  UNNEST(di.discounts_application) AS app
  WHERE
    TRUE
    AND o.registered_date >= start_date
    GROUP BY 1,2
)

, tmp_prod_order AS 
( 
  SELECT 
    o.order_id,
    CASE WHEN gc.master_category_names.level_one IN ('Ready To Consume', 'Dairy / Chilled / Eggs', 'Bread / Bakery') THEN 1 ELSE 0 END AS  has_fresh_products,
    CASE WHEN gc.master_category_names.level_one IN ( 'Produce','Meat / Seafood') THEN 1 ELSE 0 END AS  has_ultra_fresh_products,
    CASE WHEN gc.master_category_names.level_one IN ( 'Produce') THEN 1 ELSE 0 END AS  has_produce_products,
    CASE WHEN gc.master_category_names.level_one IN ( 'Meat / Seafood') THEN 1 ELSE 0 END AS  has_meat_seafood_products,
    d.quantity ,
    d.total as gmv,
    d.product.product_id,
    d.product.name,
    product_with_promo,
    CASE WHEN product_with_promo IS NOT NULL THEN  d.quantity END AS qty_prod_with_promo,
    CASE WHEN product_with_promo IS NOT NULL THEN  d.total END AS gmv_prod_with_promo,
    gc.master_category_names.level_one
    FROM
      `peya-bi-tools-pro.il_core.fact_orders` AS o,
      UNNEST(o.details) AS d
    LEFT JOIN
      `peya-bi-tools-pro.il_qcommerce.dim_vendor_product` AS gc
      ON gc.remote_product_id = d.product.product_id
    LEFT JOIN 
      discount_products dp
        on dp.order_id = o.order_id
        AND dp.product_with_promo = d.product.product_id
    WHERE
      o.registered_date >= start_date
)
 , products as 
 (
  SELECT DISTINCT
    order_id,
    MAX(has_fresh_products) as has_fresh_products,
    MAX(has_ultra_fresh_products) as has_ultra_fresh_products,
    MAX(has_produce_products) as has_produce_products,
    MAX(has_meat_seafood_products) as has_meat_seafood_products,
    COUNT(DISTINCT level_one) AS qty_distinct_cat_level_one,   
    SUM(quantity) AS basket_size,
    COUNT(DISTINCT product_id) AS qty_distinct_products,
    SUM(qty_prod_with_promo) AS qty_products_with_promo,
    COUNT(DISTINCT product_with_promo) AS qty_distinct_products_with_promo,
    SUM(gmv) AS total_gmv,
    SUM(gmv_prod_with_promo) AS gmv_products_with_promo,
    SAFE_DIVIDE(SUM(qty_prod_with_promo) , SUM(quantity)) as share_products_with_promo,
    SAFE_DIVIDE(SUM(gmv_prod_with_promo) , SUM(gmv)) as share_gmv_products_with_promo
  FROM  
      tmp_prod_order
    GROUP BY 1
 )
 
, events AS 
(
  SELECT
    e.platform,
    e.userId,
    e.partition_date,
    e.payload_timestamp_local,
    CONCAT(e.sessionId, e.global_entity_id) as session_id,
    e.eventAction,
    e.eventVariablesJson
  FROM 
    `peya-data-origins-pro.cl_sessions.perseus_events` e
  WHERE 
    TRUE
    AND e.partition_date >= start_date
    AND DATE(e.payload_timestamp_local) >= start_date
    AND e.platform in ("Android","iOS")
    AND CONCAT(e.sessionId, e.global_entity_id) IS NOT NULL 
    AND 
    ( 
      (e.eventAction IN ('transaction') AND (LOWER(STRING(eventVariablesJson.checkoutType)) IN ("groceries")))
      OR (e.eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.businessType)) NOT IN ("restaurant"))
      OR (e.eventAction IN ('shop_details.loaded') 
        AND LOWER(STRING(eventVariablesJson.businessType)) NOT IN ("restaurant") 
        AND COALESCE(LOWER(STRING(eventVariablesJson.eventOrigin)),LOWER(STRING(eventVariablesJson.origin)))  IN ("entry-point-service", "custom-entry-points-service"))
    )
)


-- Identificamos las transacciones
, transactions AS 
(
  SELECT * 
  FROM 
  (
    SELECT
    session_id
    , CASE WHEN STRING(eventVariablesJson.sequence) IS NULL THEN 'single_order'
         WHEN STRING(eventVariablesJson.sequence) = "2" THEN 'double_ya'
         ELSE NULL END AS order_type
    , STRING(eventVariablesJson.transactionId) as transactionId
    , CASE WHEN STRING(eventVariablesJson.sequence) IS NULL THEN STRING(eventVariablesJson.businessType)
         WHEN STRING(eventVariablesJson.sequence) = "2" THEN STRING(eventVariablesJson.businessTypeSecondary)
         ELSE NULL END AS businessType
    , CASE WHEN STRING(eventVariablesJson.sequence) IS NULL THEN STRING(eventVariablesJson.shopId)
         WHEN STRING(eventVariablesJson.sequence) = "2" THEN STRING(eventVariablesJson.shopIdSecondary)
         ELSE NULL END AS shopId
     , ROW_NUMBER() OVER (PARTITION BY STRING(eventVariablesJson.transactionId) ORDER BY payload_timestamp_local ASC) as row_num
    FROM 
      events
    WHERE 
      TRUE
      AND eventAction IN ('transaction') --AND (STRING(eventVariablesJson.sequence) IS NULL OR STRING(eventVariablesJson.sequence) = "2")
  ) 
  WHERE 
    TRUE
    AND row_num = 1
)
-- identificamos el primer shop.clicked
, shop_clicked AS 
(
  SELECT DISTINCT 
    session_id
    , STRING(eventVariablesJson.shopId) AS shopId
    , STRING(eventVariablesJson.businessType) AS businessType
    , STRING(eventVariablesJson.shopCategorySelected) AS categorySelected
    , CASE WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('home_screen') THEN "Home"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('campaign_landing', 'incentives_voucher_landing', 'incentives_pm_landing') THEN "Home"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('verticalhome') THEN "QC Shop List"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('homesearchdetails') THEN "Home Presearch"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('verticalsearchdetails') THEN "QC Presearch"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('searchresults') AND REGEXP_CONTAINS(LOWER(STRING(eventVariablesJson.shopSponsoringPlacement)), 'home') THEN "Home Partner Search"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('searchresults') AND NOT REGEXP_CONTAINS(LOWER(STRING(eventVariablesJson.shopSponsoringPlacement)), 'home') THEN "QC Partner Search"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('nestedsearchresults') AND REGEXP_CONTAINS(LOWER(STRING(eventVariablesJson.shopSponsoringPlacement)), 'home') THEN "Home Product Search"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('nestedsearchresults') AND NOT REGEXP_CONTAINS(LOWER(STRING(eventVariablesJson.shopSponsoringPlacement)), 'home') THEN "QC Product Search"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('nullverticalsearchresults') AND REGEXP_CONTAINS(LOWER(STRING(eventVariablesJson.shopSponsoringPlacement)), 'home') THEN "Home Null Search"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('nullverticalsearchresults') AND NOT REGEXP_CONTAINS(LOWER(STRING(eventVariablesJson.shopSponsoringPlacement)), 'home') THEN "QC Null Search" 
          WHEN eventAction IN ('shop.clicked') AND REGEXP_CONTAINS(LOWER(STRING(eventVariablesJson.screenName)), 'otp') THEN "OTP"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('promodetails') THEN "QC Promo Details"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('promotions-section-home') THEN "Promo Section"
          WHEN eventAction IN ('shop.clicked') AND LOWER(STRING(eventVariablesJson.screenName)) IN ('promotions-section-qc') THEN "Promo Section QC"
          WHEN eventAction IN ('shop.clicked') AND REGEXP_CONTAINS(LOWER(STRING(eventVariablesJson.screenName)), 'cart') THEN "Cart"
          WHEN eventAction IN ('shop_details.loaded') THEN "Home Entry Point"
          ELSE NULL 
      END AS order_attribution
  FROM 
    events
  WHERE 
    eventAction IN ('shop.clicked', 'shop_details.loaded')
  QUALIFY ROW_NUMBER() OVER(PARTITION BY session_id, STRING(eventVariablesJson.shopId) ORDER BY payload_timestamp_local ASC) = 1
) 
, order_att AS 
(
  SELECT 
    t.session_id,
    t.order_type,
    t.transactionId as transaction_id,
    t.shopId as shop_id,
    t.businessType as business_type,
    sc.categorySelected as category_selected,
    CASE 
      WHEN t.order_type IN ('single_order') THEN sc.order_attribution
      WHEN t.order_type IN ('double_ya') THEN "DoubleYa" 
      ELSE NULL 
    END AS order_attribution,
  FROM
    transactions t
  LEFT JOIN 
    shop_clicked sc
    ON t.session_id = sc.session_id 
    AND t.shopId = sc.shopId
)

 

SELECT DISTINCT 
   o.registered_date,
   o.order_id,
   o.user.id as user_id,
   s.session_id,
   o.country.country_name,
   o.city.city_name,
   o.business_type.business_type_name,
   d.partner_id,
   partner_name,
   d.franchise.franchise_name as franchise,
   CASE
    WHEN REGEXP_CONTAINS(LOWER(partner_name), r"pedidosya|pya") THEN 'DMart'
    WHEN is_aaa is true THEN 'AAA'
    ELSE 'Non-AAA' 
   END as vendor_type ,
   CASE 
    WHEN o.business_type.business_type_name in ('Market','Kiosks','Drinks','Pets','Pharmacy','Shop') THEN 1 
    ELSE 0 
   END AS qc_order,
   CASE 
    WHEN o.business_type.business_type_name in ('Market','Kiosks','Drinks','Pets','Pharmacy','Shop') AND (is_aaa is true or  REGEXP_CONTAINS(LOWER(partner_name), r"pedidosya|pya")) then 1 
    ELSE 0 
   END AS groceries_order,
    mission_type,
    o.is_pre_order,
    o.order_status,  
    CASE WHEN dy.order_id IS NOT NULL THEN 1 ELSE 0 END AS is_double_ya,
    o.promisedDeliveryTime.minMinutes AS min_pdt,
    o.promisedDeliveryTime.maxMinutes AS max_pdt, 
    ns.actual_delivery_time,
    CASE WHEN ns.is_non_seamless_order = 1 THEN 1 ELSE 0 END AS is_non_seamless_order,
    CASE WHEN ns.is_slow_order = 1 THEN 1 ELSE 0 END AS is_slow_order,
    CASE WHEN ns.is_late_order = 1 THEN 1 ELSE 0 END AS is_late_order,
    CASE WHEN ns.is_session_order = 1 THEN 1 ELSE 0 END AS is_session_order,
    CASE WHEN ns.is_modified_order = 1 THEN 1 ELSE 0 END AS is_modified_order,
    CASE WHEN has_compensation = 1 THEN 1 ELSE 0 END AS has_compensation,
    CASE WHEN has_refund = 1 THEN 1 ELSE 0 END AS has_refund,
    ROUND(fop.gpo,3) AS gpo,
    ROUND(fop.gpo/rate_eu,3) as gpo_eu,
    ROUND(p.total_gmv,3) AS total_gmv,
    ROUND(p.total_gmv/rate_eu,3) AS total_gmv_eu,
    ROUND(CASE WHEN p.gmv_products_with_promo IS NOT NULL THEN p.gmv_products_with_promo ELSE 0 END,3) AS gmv_products_with_promo,
    ROUND(CASE WHEN p.gmv_products_with_promo IS NOT NULL THEN p.gmv_products_with_promo/rate_eu ELSE 0 END,3) AS gmv_products_with_promo_eu,
    p.basket_size,
    p.qty_distinct_products,
    CASE WHEN p.qty_products_with_promo IS NOT NULL THEN p.qty_products_with_promo ELSE 0 END AS qty_products_with_promo,
    CASE WHEN p.qty_distinct_products_with_promo IS NOT NULL THEN p.qty_distinct_products_with_promo ELSE 0 END AS qty_distinct_products_with_promo,
    ROUND(CASE WHEN p.share_products_with_promo IS NOT NULL THEN p.share_products_with_promo ELSE 0 END,3) AS share_products_with_promo,
    ROUND(CASE WHEN p.share_gmv_products_with_promo IS NOT NULL THEN p.share_gmv_products_with_promo ELSE 0 END,3) AS share_gmv_products_with_promo,
    p.qty_distinct_cat_level_one,
    p.has_fresh_products,
    p.has_ultra_fresh_products,
    p.has_produce_products,
    p.has_meat_seafood_products,  
    att.order_attribution
  FROM 
   `peya-bi-tools-pro.il_core.fact_orders` o
  LEFT JOIN 
   `peya-bi-tools-pro.il_core.dim_partner` AS d 
    ON d.partner_id = restaurant.id
  LEFT JOIN 
    `peya-bi-tools-pro.il_core.dim_currency_exchange` AS ce
    ON ce.currency_exchange_date = DATE_TRUNC(o.registered_date,MONTH) 
    AND ce.currency_id = o.restaurant.country.currency.id
  LEFT JOIN 
    sessionid AS s 
    ON s.order_id = o.order_id
  LEFT JOIN 
    double_ya AS dy 
    ON dy.order_id = o.order_id
  LEFT JOIN 
    shopping_missions AS sm 
    ON sm.order_id = o.order_id
  LEFT JOIN 
    non_seamless AS ns 
    ON ns.platform_order_code = o.order_id
  LEFT JOIN 
    comp_and_ref AS cr 
    ON cr.original_order_id = o.order_id
  LEFT JOIN 
    fact_orders_profitability AS fop 
    ON fop.order_id = o.order_id
  LEFT JOIN 
    products AS p 
    ON p.order_id = o.order_id
  LEFT JOIN 
    order_att AS att
    ON SAFE_CAST(att.transaction_id AS INT64)= o.order_id
  WHERE
    TRUE
    AND o.registered_date >= start_date
    AND o.business_type.business_type_name in ('Market','Kiosks','Drinks','Pets','Pharmacy','Shop')
 
