WITH tmp_sv AS 
(
  SELECT
  cast(remote_product_id as int64) product_id_sv,
  t.bucket_ventas as bucket_ventas_tmp
  --concat(remote_product_id,";",t.bucket_ventas) results,
  FROM 
    `peya-bi-tools-pro.il_qcommerce.social_validation_fresh_ultrafresh` AS t,
  UNNEST(SPLIT(t.ids_producto_remoto, ';')) AS remote_product_id
  WHERE
    TRUE
    AND remote_product_id IS NOT NULL 
    AND remote_product_id != '' 
    AND supera_umbral = TRUE
)

, tmp_rating AS 
(
  SELECT DISTINCT
  product_id AS product_id_r
  , rating as rating_tmp
  , total_ratings as total_ratings_tmp
  FROM `peya-food-and-groceries.automated_tables_reports.rating_product_level`
  WHERE 
    TRUE
    AND REGEXP_CONTAINS(LOWER(partner_name), r"pedidosya|pya")
    AND rating >= 2.0
    AND total_ratings >= 5
)

, base_join AS 
(
  SELECT * 
  , CASE WHEN product_id_r IS NULL THEN product_id_sv ELSE product_id_r END AS product_id
  , CASE WHEN bucket_ventas_tmp IS NULL THEN 0 ELSE bucket_ventas_tmp END AS bucket_ventas
  , CASE WHEN rating_tmp IS NULL THEN 0 ELSE rating_tmp END AS rating
  , CASE WHEN total_ratings_tmp IS NULL THEN 0 ELSE total_ratings_tmp END AS total_ratings
  FROM tmp_rating
  FULL OUTER JOIN tmp_sv ON tmp_sv.product_id_sv = tmp_rating.product_id_r
)

SELECT DISTINCT  
  product_id
 , concat(product_id,";",bucket_ventas,";",rating,";",total_ratings) results
FROM base_join 

