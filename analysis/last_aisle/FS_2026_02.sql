WITH base AS 
(
  SELECT
    SAFE_CAST(remote_vendor_id AS INT64) AS vendor_id,
    p.remote_product_id, 
    product_score,
    CONCAT(CAST(remote_product_id AS STRING), ';', CAST(product_score AS STRING)) AS product_id_score
  FROM  
    `peya-food-and-groceries.automated_tables_reports.prueba_last_aisle` , UNNEST(top_50_products) AS p
  WHERE
    TRUE
    AND SAFE_CAST(remote_vendor_id AS INT64) IS NOT NULL
)

SELECT 
  vendor_id,
  STRING_AGG(product_id_score, ',') AS product_id_score
  FROM 
    base
  GROUP BY 1
