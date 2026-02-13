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
  SELECT DISTINCT
    create_date,
    vendor_id,
    start_timestamp,
    expire_timestamp,
    is_active,
    calculate.max_discount_percentage,
    p.name,
    p.product_sku,
    p.remote_product_id
  FROM 
    `peya-data-origins-pro.airflow_it_tribe_verticals.promo_refinery` , unnest(products) p
  WHERE
    TRUE
    --AND vendor_id = '286797'
    AND CURRENT_TIMESTAMP() BETWEEN start_timestamp AND expire_timestamp
    AND is_active IS TRUE
    AND active IS TRUE 
)

, promos AS 
(
  SELECT DISTINCT
    SAFE_CAST(vendor_id AS INT64) AS partner_id,
    --partner_name,
    product_sku AS promo_sku,
    name AS promo_product_name,
    max(max_discount_percentage)/100 as max_implicit_unit_discount_percentage
  FROM 
    promos_tmp
  WHERE 
    TRUE
  GROUP BY 1,2,3
)

, new_score_calc AS 
(
  SELECT *  , 
  ---- v1 no cambia nada
    /*
      CASE 
        WHEN max_implicit_unit_discount_percentage IS NULL THEN product_score
        ELSE product_score * (1+ max_implicit_unit_discount_percentage) 
      END AS new_score, 
    */
    product_score * 0.5 + ( coalesce(max_implicit_unit_discount_percentage,0) * 0.5)  AS new_score, 
    CASE WHEN max_implicit_unit_discount_percentage IS NOT NULL THEN 1 ELSE 0 END AS has_promo
  FROM 
    BASE b
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
  SUM(has_promo)has_promo,
FROM (
 
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
  has_promo
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
