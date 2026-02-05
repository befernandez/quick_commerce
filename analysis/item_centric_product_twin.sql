
WITH dim_partner AS
(
  SELECT DISTINCT
    partner_id,
    business_type.business_type_description,
    partner_name,
    CASE
    WHEN REGEXP_CONTAINS(LOWER(partner_name), r"pedidosya|pya") THEN 'DMart'
    WHEN is_aaa is true THEN 'AAA'
    ELSE 'Local Store'
    END as vendor_type ,
    franchise.franchise_name
  FROM 
    `peya-bi-tools-pro.il_core.dim_partner`
  WHERE
    TRUE
    AND business_type.business_type_description != 'Restaurant'
)

, dim_vendor_prod AS
  (
  SELECT DISTINCT
    master_code
    , categoria_nivel_uno
    , CASE WHEN categoria_nivel_uno IN ('Produce', 'Meat / Seafood', 'Ready To Consume') THEN 1 else 0 end as is_uf
  FROM
  (
  SELECT DISTINCT
    master_code
    , MAX(gc.master_category_names.level_one) AS categoria_nivel_uno
    , MAX(gc.master_category_names.level_two) AS categoria_nivel_dos
  FROM 
    `peya-bi-tools-pro.il_qcommerce.dim_vendor_product` AS gc
  GROUP BY 1
  )
)

, fresh_uf_orders AS
(
SELECT DISTINCT
  peya_order_id
  , MAX(is_uf) AS is_uf
FROM
  (
  SELECT
    dh_cart_id
    , peya_order_id
    , gc.categoria_nivel_uno
    , is_uf
    , item.master_code
  FROM 
    `peya-data-origins-pro.cl_qcommerce.item_first_cart` , UNNEST(items) AS item
  LEFT JOIN dim_vendor_prod AS gc ON gc.master_code = item.master_code
  WHERE
    TRUE
    AND partition_date between '2026-01-01' and '2026-01-15'
    AND message_type = 'ORIGINAL_CART'
) GROUP BY 1
)

, fact_orders AS
  (
  SELECT distinct
  order_id
  , country.country_name
  , city.city_name
  , is_user_plus
  , restaurant.id as partner_id
  , partner_name
  , business_type_description
  , vendor_type
  , franchise_name
  , is_uf
  , business_type.business_type_name
  FROM 
    `peya-bi-tools-pro.il_core.fact_orders`
  LEFT JOIN dim_partner ON dim_partner.partner_id = restaurant.id
  LEFT JOIN fresh_uf_orders ON SAFE_CAST(fresh_uf_orders.peya_order_id AS INT64) = order_id
  WHERE
    TRUE
    AND registered_date >= "2026-01-01"
)

,  ORIGINAL_CART AS 
(
SELECT DISTINCT
  dh_cart_id,
  peya_order_id, 
  country_name,
  franchise_name,
  business_type_name,
  is_uf,
  vendor_id,
  partner_name,
  item.name,
  item.master_code,
  item.remote_product_id,
  item.sku
FROM 
  `peya-data-origins-pro.cl_qcommerce.item_first_cart` , UNNEST(items) AS item  
  LEFT JOIN fact_orders ON fact_orders.order_id  =SAFE_CAST(peya_order_id AS INT64)
WHERE
  TRUE
  AND partition_date between '2026-01-01' and '2026-01-15'
  AND message_type = 'ORIGINAL_CART' 
) 

, COMPARED_CART AS 
(
  SELECT DISTINCT
  original_dh_cart_id,
  message_type,
  vendor_id,
  partner_name,
  franchise_name,
  date_comparison,
  item.name,
  item.master_code,
  item.remote_product_id,
  item.sku
FROM 
  `peya-data-origins-pro.cl_qcommerce.item_first_cart` , UNNEST(items) AS item 
LEFT JOIN dim_partner ON dim_partner.partner_id = SAFE_CAST(vendor_id AS INT64)
WHERE 
  TRUE
  AND partition_date between '2026-01-01' and '2026-01-15'
  AND message_type = 'COMPARED_CART' 
)

, original_items_count AS
(
  SELECT DISTINCT
  dh_cart_id,
  peya_order_id, 
  COUNT(DISTINCT master_code) AS total_items_order
  FROM ORIGINAL_CART
  GROUP BY 1,2
)

, base_vendors_order_original AS 
(
SELECT DISTINCT 
  peya_order_id,
  dh_cart_id,
  country_name,
  is_uf,
  business_type_name,
  original_cart.vendor_id as original_vendor_id,
  original_cart.partner_name as original_partner_name ,
  original_cart.franchise_name as original_franchise_name,
  original_cart.name as original_name,
  original_cart.master_code as original_master_code,
  dc.twin as original_twin,
  original_cart.remote_product_id as original_remote_product_id,
  original_cart.sku as original_sku,
  original_dh_cart_id, 
  compared_cart.vendor_id as compared_vendor_id,
  compared_cart.partner_name as compared_partner_name ,
  compared_cart.franchise_name as compared_franchise_name,
 FROM  
  ORIGINAL_CART
 LEFT JOIN 
   COMPARED_CART ON COMPARED_CART.original_dh_cart_id = ORIGINAL_CART.dh_cart_id 
LEFT JOIN  `peya-food-and-groceries.automated_tables_reports.dedup_catalogo` dc on dc.remote_vendor_id =  SAFE_CAST(original_cart.vendor_id AS INT64) AND original_cart.master_code = dc.master_code
)

, productos_que_dh_encontro AS 
(
SELECT DISTINCT 
  peya_order_id, 
  original_cart.vendor_id   AS original_vendor_id, 
  original_cart.master_code AS original_master_code,  
  compared_cart.vendor_id   AS compared_vendor_id,
  compared_cart.master_code AS compared_master_code, 
 FROM  
  ORIGINAL_CART
 LEFT JOIN 
   COMPARED_CART ON COMPARED_CART.original_dh_cart_id = ORIGINAL_CART.dh_cart_id and  COMPARED_CART.master_code = ORIGINAL_CART.master_code 
   order by 1,COMPARED_CART.vendor_id
)

, base_tmp as 
(
SELECT  
  o.* 
  , CASE WHEN original_franchise_name = compared_franchise_name THEN 1 ELSE 0 END AS is_same_franchise
  , d.master_code as twin_master_code_compared --Tabla twin
  , d.twin as twin_compared --Tabla twin
  , compared_master_code as dh_compared_master_code  --comparados de carritos
  , CASE WHEN dh.compared_master_code IS NULL AND d.master_code = o.original_master_code THEN o.original_master_code END AS same_master_code_not_found_dh 
  , CASE WHEN dh.compared_master_code IS NULL AND d.twin IS NOT NULL AND d.master_code != o.original_master_code  THEN o.original_master_code END AS twin_celaned 
  , CASE 
      WHEN dh.compared_master_code IS NOT NULL THEN  o.original_master_code
      WHEN dh.compared_master_code IS NULL AND d.twin IS NOT NULL AND d.master_code != o.original_master_code THEN o.original_master_code 
    END AS found_prod_twin_dh 
FROM 
  base_vendors_order_original o
LEFT JOIN 
  `peya-food-and-groceries.automated_tables_reports.dedup_catalogo` d on SAFE_CAST(o.compared_vendor_id AS INT64) = d.remote_vendor_id AND original_twin = d.twin
LEFT JOIN 
  productos_que_dh_encontro dh ON dh.peya_order_id = o.peya_order_id AND dh.compared_vendor_id = o.compared_vendor_id AND dh.compared_master_code = o.original_master_code
--where o.peya_order_id =  '1853096021' -- '1862785035'
)
 
 /*SELECT * 
 FROM  base_final
 where peya_order_id IN('1853096021' ,'1862785035')
 */
, base_final as (
SELECT  DISTINCT
  country_name,
  peya_order_id,
  is_same_franchise,
  is_uf,
  compared_vendor_id,
  COUNT(DISTINCT original_master_code) as total_items_order,
  COUNT(DISTINCT dh_compared_master_code) AS qty_encontrados_DH,
  COUNT(DISTINCT found_prod_twin_dh)found_prod_twin_dh
FROM  base_tmp
WHERE 
  TRUE
  AND country_name = 'Argentina'
  AND is_uf = 0
  AND business_type_name in ('Market')
  GROUP BY 1,2,3,4,5
)
 -- AND peya_order_id IN ('1853096021' , '1862785035')

SELECT DISTINCT
CASE WHEN total_items_order <= 10 THEN total_items_order ELSE 11 END AS total_items_order_red
, COUNT(DISTINCT peya_order_id) AS peya_order_id
, COUNT(DISTINCT CASE WHEN qty_encontrados_DH = total_items_order AND is_same_franchise = 0 THEN peya_order_id END) AS carritos_completados_full_distinct_fr
, COUNT(DISTINCT CASE WHEN found_prod_twin_dh = total_items_order AND is_same_franchise = 0 THEN peya_order_id END) AS carritos_completados_found_prod_twin_dh_distinct_fr
FROM Base_final
group by 1
order by 1
