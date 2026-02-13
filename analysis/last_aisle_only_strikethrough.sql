WITH   EXT AS 
(
  SELECT
    CONCAT(product_id, partner_id) AS clave,
    1 AS score
  FROM `peya-data-origins-pro.raw_qcommerce.gsheet_extraordinary_aisle`
)

, BASE AS 
(
  SELECT 
    ua.city,
    dv.remote_vendor_id,
    dv.remote_product_id,
    ua.product_name,
    dv.sku,
    IFNULL(e.score, ua.product_score) AS product_score,
    vendor_name,
    vertical,
    CASE
    WHEN REGEXP_CONTAINS(LOWER(partner_name), r"pedidosya|pya") THEN 'DMart'
    WHEN is_aaa is true THEN 'AAA'
    ELSE 'Local Store'
    END as vendor_type ,
  FROM `peya-bi-tools-pro.il_qcommerce.last_aisle_scoring` ua
  INNER JOIN `peya-bi-tools-pro.il_qcommerce.dim_vendor_product` dv
    ON ua.barcode = dv.barcodes
  INNER JOIN `peya-bi-tools-pro.il_core.dim_partner` dp 
    ON dv.remote_vendor_id = dp.partner_id 
    AND ua.city = dp.city.name
  LEFT JOIN EXT AS e 
    ON CONCAT(dv.vendor_product_id, dv.remote_vendor_id) = e.clave
  WHERE ua.score_type = 'City Score'
    AND ua.city IS NOT NULL
)

, promos_tmp AS 
(
  SELECT	
    create_date,
    partner_id,	
    catalog_global_vendor_id,	
    partner_name,	
    sku,	
    global_product_id,	
    product_name,	
    incentive_type,	
    campaign_type,	
    campaign_subtype,	
    product_discount_type,	
    start_timestamp,	
    expire_timestamp,	
    is_active,	
    original_price_lc,	
    discounted_price_lc	,
    campaign_id, 
    CASE WHEN c.discounted_amount_lc >= 0 THEN c.discounted_amount_lc
          WHEN c.discounted_amount_lc < 0  THEN -c.discounted_amount_lc
          ELSE NULL END AS discounted_amount_lc    
    --c.discounted_price_lc
    ,CASE WHEN c.min_trigger_qty_product_discount >= 0 THEN c.min_trigger_qty_product_discount
          WHEN c.min_trigger_qty_product_discount < 0  THEN -c.min_trigger_qty_product_discount
          ELSE NULL END AS min_trigger_qty_product_discount
    ,CASE 
        WHEN c.campaign_type = "Strikethrough" 
          THEN SAFE_CAST(ROUND(SAFE_DIVIDE(c.discounted_amount_lc, original_price_lc), 3) AS FLOAT64)
        WHEN c.campaign_type = "SameItemBundle" OR c.campaign_type = "MixAndMatch" AND NOT REGEXP_CONTAINS(c.campaign_subtype, "Free") 
          THEN SAFE_CAST(ROUND(SAFE_DIVIDE(c.discounted_amount_lc, original_price_lc*c.min_trigger_qty_product_discount), 3) AS FLOAT64)
        WHEN c.campaign_type = "SameItemBundle" OR c.campaign_type = "MixAndMatch" AND REGEXP_CONTAINS(c.campaign_subtype, "Free") 
          THEN SAFE_CAST(ROUND(SAFE_DIVIDE(c.discounted_amount_lc, original_price_lc*c.min_trigger_qty_product_discount), 3) AS FLOAT64)
        ELSE NULL 
      END AS implicit_unit_discount_percentage
  FROM	
    `peya-bi-tools-pro.il_qcommerce.promo_tool_campaigns_v2`	 c
  WHERE	
    TRUE	
    AND create_date >= ('2026-01-01')	
    AND CURRENT_TIMESTAMP BETWEEN (start_timestamp) and (expire_timestamp)
    AND  c.original_price_lc > 0 AND c.original_price_lc > c.discounted_amount_lc  AND discounted_amount_lc != 0
    AND is_active IS TRUE ---Strikethrough
)


, promos_Strikethrough AS 
(
  SELECT DISTINCT
    partner_id,
    partner_name,
    sku AS promo_sku,
    product_name AS promo_product_name,
    max(implicit_unit_discount_percentage) as max_implicit_unit_discount_percentage
  FROM 
    promos_tmp
  WHERE 
    TRUE
    AND implicit_unit_discount_percentage > 0
    AND campaign_type= 'Strikethrough'
  GROUP BY 1,2,3,4
)

, promos AS 
(
  SELECT DISTINCT
    partner_id,
    partner_name,
    sku AS promo_sku,
    product_name AS promo_product_name,
    max(implicit_unit_discount_percentage) as max_implicit_unit_discount_percentage
  FROM 
    promos_tmp
  WHERE 
    TRUE
    AND implicit_unit_discount_percentage > 0
  GROUP BY 1,2,3,4
)

, new_score_calc as 
(
select *  , 

---- v1 no cambia nada
/*  CASE 
    WHEN max_implicit_unit_discount_percentage IS NULL THEN product_score
    ELSE product_score * (1+ max_implicit_unit_discount_percentage) 
  END AS new_score, */
   product_score * 0.5 + ( coalesce(ps.max_implicit_unit_discount_percentage,0) * 0.5)  AS new_score, 
  CASE WHEN ps.max_implicit_unit_discount_percentage IS NOT NULL THEN 1 ELSE 0 END AS has_promo_Strikethrough,
  CASE WHEN p.max_implicit_unit_discount_percentage IS NOT NULL THEN 1 ELSE 0 END AS has_promo 
from BASE b 
LEFT JOIN promos_Strikethrough ps ON ps.partner_id = b.remote_vendor_id and ps.promo_sku = b.sku
LEFT JOIN promos p on p.partner_id = b.remote_vendor_id and p.promo_sku = b.sku
)

, new_rank AS 
(
  SELECT * ,
  ROW_NUMBER() OVER (PARTITION BY  remote_vendor_id ORDER BY (new_score) DESC) AS ranking
  FROM new_score_calc
)


SELECT DISTINCT 
  remote_vendor_id,
  vendor_name,
  vertical,
  vendor_type,
  COUNT(DISTINCT remote_product_id)remote_product_id,
  SUM(has_promo_Strikethrough) has_promo_Strikethrough,
  SUM(has_promo)has_promo,
FROM
(
SELECT 
  city,
  vendor_name,
  vertical,
  vendor_type,
  remote_vendor_id,
  remote_product_id,
  product_name,
  product_score,
  new_score,
  has_promo,
  has_promo_Strikethrough
FROM new_rank
WHERE 
  TRUE
  AND ranking <= 50
  AND vendor_type IN ('DMart' , 'AAA')
  AND vertical = 'Market'

--and remote_vendor_id = 393273
--and remote_vendor_id = 513066
ORDER BY remote_vendor_id, product_score DESC
) GROUP BY 1 , 2 , 3,4
 
--ROW_NUMBER() OVER (PARTITION BY dv.remote_vendor_id ORDER BY IFNULL(e.score, ua.product_score) DESC) AS ranking
--where p.promo_sku is not null
 
--216419006 mayor a cero
--198.836
  --  LIMIT 1000
