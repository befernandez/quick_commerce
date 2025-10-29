 WITH
-- Paso 1: Unificar órdenes confirmadas con sus reseñas correspondientes.
product_sales_with_reviews AS (
  SELECT
  fs.id AS product_id_tabla_survey,
  fo.restaurant.id AS partner_id,
  partner_name,
  vp.product_name,
  CASE
    WHEN dp.is_darkstore THEN 'DMart' --REGEXP_CONTAINS(LOWER(partner_name), r"pedidosya|pya")
    WHEN dp.is_aaa THEN 'AAA'
    ELSE 'Local Store'
    END AS vendor_type,
  dp.shopper_type,
  fo.country.country_code,
  fo.order_id,
  d.product.product_id,
  d.subtotal,
  fo.restaurant.country.currency.id AS currency_id,
  DATE_TRUNC(fo.registered_date, MONTH) AS registered_month,
  CAST(fs.option_id AS NUMERIC) AS rating
  FROM `peya-bi-tools-pro.il_core.fact_orders` AS fo, UNNEST(fo.details) AS d
  LEFT JOIN `peya-bi-tools-pro.il_qcommerce.feedback_survey_extended` AS fs ON CAST(fo.order_id AS STRING) = fs.order_id AND CAST(d.product.product_id AS STRING) = fs.id
  AND fs.question_category IN ("stars-ultra_fresh")
  AND fs.option_selected = TRUE
JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON fo.restaurant.id = dp.partner_id
JOIN `peya-bi-tools-pro.il_qcommerce.dim_vendor_product` AS vp ON d.product.product_id = vp.remote_product_id
WHERE
  TRUE
  AND DATE(fo.registered_date) >= "2025-07-07"
  --AND fo.business_type_id NOT IN (1, 7, 9, 11)
  --AND LOWER(fo.order_status) = "confirmed"
  AND d.subtotal > 0
  AND REGEXP_CONTAINS(LOWER(partner_name), r"pedidosya|pya")
  ---AND LOWER(vp.master_category_names.level_one) IN ('produce', 'meat / seafood')
  AND (dp.is_aaa OR dp.is_darkstore)
  --AND dp.shopper_type <> "NO_SHOPPER"
  AND fs.question_category IN ("stars-ultra_fresh")
  AND fs.option_selected = TRUE 
) 

SELECT DISTINCT 
product_id
, product_name
, partner_name
, vendor_type
, round(AVG(rating),2) AS rating
, COUNT(*) AS total_ratings
, SUM(CASE WHEN rating = 1 THEN 1 END ) AS qty_ratings_1_star
, SUM(CASE WHEN rating = 2 THEN 1 END ) AS qty_ratings_2_star
, SUM(CASE WHEN rating = 3 THEN 1 END ) AS qty_ratings_3_star
, SUM(CASE WHEN rating = 4 THEN 1 END ) AS qty_ratings_4_star
, SUM(CASE WHEN rating = 5 THEN 1 END ) AS qty_ratings_5_star
 from product_sales_with_reviews
 GROUP BY 1,2,3,4
