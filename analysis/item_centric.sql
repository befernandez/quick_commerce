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
FROM `peya-bi-tools-pro.il_core.dim_partner`
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
FROM `peya-bi-tools-pro.il_qcommerce.dim_vendor_product` AS gc
GROUP BY 1
)
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
FROM `peya-bi-tools-pro.il_core.fact_orders`
LEFT JOIN dim_partner ON dim_partner.partner_id = restaurant.id
WHERE
TRUE
AND registered_date >= "2026-01-01"
)

, original_items_count AS
(
SELECT
dh_cart_id
, peya_order_id
, country_name
, city_name
, is_user_plus
, partner_name as original_partner_name
, business_type_description as original_business_type_description
, vendor_type as original_vendor_type
, franchise_name as original_franchise_name
, COUNT(DISTINCT item.sku) as total_items_order
, MAX(cart_resume.total) AS amount_original_cart
FROM `peya-data-origins-pro.cl_qcommerce.item_first_cart` , UNNEST(items) AS item
LEFT JOIN fact_orders fo ON SAFE_CAST(fo.order_id AS STRING) = peya_order_id
WHERE
TRUE
AND partition_date between '2026-01-01' and '2026-01-15'
AND message_type = 'ORIGINAL_CART'
GROUP BY 1,2,3,4,5,6,7,8,9
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
FROM `peya-data-origins-pro.cl_qcommerce.item_first_cart` , UNNEST(items) AS item
LEFT JOIN dim_vendor_prod AS gc ON gc.master_code = item.master_code
WHERE
TRUE
AND partition_date between '2026-01-01' and '2026-01-15'
AND message_type = 'ORIGINAL_CART'
) GROUP BY 1
)

, vendor_items_count AS
(
SELECT
original_dh_cart_id
, vendor_id as compared_vendor_id
, partner_name as compared_partner_name
, business_type_description as compared_business_type_description
, vendor_type as compared_vendor_type
, franchise_name as compared_franchise_name
, COUNT(DISTINCT item.sku) as items_encontrados
, MAX(cart_resume.total) AS amount_compared_cart
FROM `peya-data-origins-pro.cl_qcommerce.item_first_cart` , UNNEST(items) AS item
LEFT JOIN dim_partner ON SAFE_CAST(dim_partner.partner_id AS STRING) = vendor_id
WHERE
TRUE
AND partition_date between '2026-01-01' and '2026-01-15'
AND message_type = 'COMPARED_CART'
GROUP BY 1, 2,3,4,5,6
)

, tmp AS
(
SELECT o.*
, CASE
WHEN is_uf = 0 THEN 'NO'
WHEN is_uf = 1 THEN 'YES'
END AS is_uf
, compared_vendor_id
, compared_partner_name
, compared_vendor_type
, compared_franchise_name
, items_encontrados
, amount_compared_cart
, CASE WHEN compared_franchise_name = original_franchise_name THEN 'Yes' ELSE 'No' END as is_same_franchise
, CASE WHEN total_items_order = items_encontrados THEN amount_original_cart - amount_compared_cart END AS price_diff
, ROUND((items_encontrados / total_items_order) * 100,2) AS share_fulfill
, CASE WHEN items_encontrados + 1 = total_items_order THEN o.peya_order_id END AS falta_1_item
, CASE WHEN items_encontrados + 2 = total_items_order THEN o.peya_order_id END AS falta_2_item
, CASE WHEN items_encontrados + 3 = total_items_order THEN o.peya_order_id END AS falta_3_item
FROM original_items_count o
LEFT JOIN vendor_items_count v ON v.original_dh_cart_id = o.dh_cart_id
LEFT JOIN fresh_uf_orders on fresh_uf_orders.peya_order_id = o.peya_order_id
)
, base AS
(
SELECT DISTINCT
peya_order_id,
is_uf,
is_user_plus,
--total_compared_cart,
amount_original_cart,
--compared_vendor_id,
--share_fulfill,
--items_encontrados,
--original_dh_cart_id,
total_items_order,
original_business_type_description,
original_vendor_type,

city_name
, COUNT( DISTINCT CASE WHEN share_fulfill = 100 THEN peya_order_id END) AS carritos_completados_full
, COUNT( DISTINCT CASE WHEN share_fulfill = 100 AND is_same_franchise = 'Yes' THEN peya_order_id END) AS carritos_completados_full_same_franchise
, COUNT( DISTINCT CASE WHEN share_fulfill = 100 AND is_same_franchise = 'No' THEN peya_order_id END) AS carritos_completados_full_distinct_franchise

, COUNT( CASE WHEN share_fulfill = 100 THEN peya_order_id END) AS vendors_100
, COUNT( CASE WHEN share_fulfill = 100 AND is_same_franchise = 'Yes' THEN peya_order_id END) AS vendors_100_same_franchise
, COUNT( CASE WHEN share_fulfill = 100 AND is_same_franchise = 'No' THEN peya_order_id END) AS vendors_100_distinct_franchise
, COUNT(DISTINCT CASE WHEN is_same_franchise = 'No' THEN falta_1_item END ) AS falta_1_item
, COUNT(DISTINCT CASE WHEN is_same_franchise = 'No' THEN falta_2_item END ) AS falta_2_item

, SUM(price_diff) AS price_diff
, SUM(CASE WHEN is_same_franchise = 'Yes' THEN price_diff END) AS price_diff_same_franchise
, SUM(CASE WHEN is_same_franchise = 'No' THEN price_diff END) AS price_diff_distinct_franchise
, COUNT( CASE WHEN share_fulfill is null THEN peya_order_id END) AS vendors_null
FROM tmp
GROUP BY all
)
select distinct
CASE WHEN total_items_order <= 10 THEN total_items_order ELSE 11 END AS total_items_order_red
, is_uf
, original_vendor_type
, original_business_type_description
, city_name
, COUNT(DISTINCT peya_order_id)peya_order_id
, sum(carritos_completados_full) carritos_completados_full
, sum(vendors_100)vendors_100
, COUNT(DISTINCT CASE WHEN vendors_100 = vendors_100_same_franchise and vendors_100 > 0 THEN peya_order_id END) AS only_match_same_franchise
, sum(carritos_completados_full_same_franchise)carritos_completados_full_same_franchise
, sum(carritos_completados_full_distinct_franchise)carritos_completados_full_distinct_franchise
, SUM(vendors_100_distinct_franchise)vendors_100_distinct_franchise
, sum(price_diff)price_diff
, sum(price_diff_same_franchise)price_diff_same_franchise
, sum(price_diff_distinct_franchise)price_diff_distinct_franchise
, SUM(CASE WHEN carritos_completados_full = 0 THEN falta_1_item END ) AS falta_1_item
, SUM(CASE WHEN carritos_completados_full = 0 THEN falta_2_item END ) AS falta_2_item
FROM base
GROUP BY 1,2,3,4,5
ORDER BY 1,2



































































































































































































































































































































































































































































































































































































































































































































































































































