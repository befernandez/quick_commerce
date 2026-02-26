ITH EXT AS 
 
(
  SELECT 
    SAFE_CAST(partner_id AS INT64) as vendor_id,
    SAFE_CAST(product_id AS INT64)  AS remote_product_id, 
    1 - (SAFE_CAST(position AS INT64) * 0.0001) AS product_score 
    FROM `peya-data-origins-pro.cl_qcommerce.extraordinary_aisle` --  `peya-delivery-and-support.automated_tables_reports.tmp_ul_ant`
)

---- me quedo con un rand()*0.98 del score para que nunca sea mayor a los productos de EXT
, tmp_base_raw AS 
(
  SELECT
    SAFE_CAST(remote_vendor_id AS INT64) AS vendor_id,
    p.remote_product_id, 
    RAND()*0.98  AS product_score
  FROM `peya-bi-tools-pro.il_qcommerce.last_aisle_scoring_by_partner`, UNNEST(top_50_products) AS p
  WHERE
    SAFE_CAST(remote_vendor_id AS INT64) IS NOT NULL
 
)

----filtro los productos para que si están en EXT o queden duplicados
, tmp_base AS (
  SELECT b.*
  FROM tmp_base_raw b
  LEFT JOIN EXT e
    ON b.vendor_id = e.vendor_id
   AND b.remote_product_id = e.remote_product_id
  WHERE e.remote_product_id IS NULL   
)

, base_EXT AS 
( 
  SELECT *
  FROM tmp_base
  UNION ALL
  SELECT * FROM EXT
  ORDER BY 3 DESC 
)

, base_final_tmp as 
(
  SELECT * 
  , ROW_NUMBER() OVER ( PARTITION BY vendor_id ORDER BY product_score DESC) AS rank_by_vendor
   FROM base_EXT
)

, base_final AS 
(
  SELECT DISTINCT 
  vendor_id
  , remote_product_id
  , product_score 
  , CONCAT(CAST(remote_product_id AS STRING), ';', CAST(product_score AS STRING)) AS product_id_score_rand
  FROM base_final_tmp
  WHERE rank_by_vendor <= 50
  ORDER BY 1,2,3
)

SELECT
  vendor_id,
 -- STRING_AGG(product_id_score_tmp, ',') AS product_id_score, --
  STRING_AGG(product_id_score_rand, ',') AS product_id_score
FROM base_final 
 GROUP BY vendor_id
