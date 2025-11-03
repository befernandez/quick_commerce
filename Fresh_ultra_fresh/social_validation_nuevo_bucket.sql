WITH
  ventas_agregadas AS (
    SELECT
      gc.master_category_names.level_one AS categoria_nivel_uno,
      gc.master_category_names.level_two AS categoria_nivel_dos,
      gc.country_code AS pais,
      gc.chain_name AS cadena,
      gc.sku,
      SUM(d.quantity) AS ventas_unidades,
      COUNT(DISTINCT o.order_id) AS ordenes_unicas,
      SUM(d.total) AS gmv,
      STRING_AGG(DISTINCT CAST(gc.remote_product_id AS STRING), ';') AS ids_producto_remoto,
      STRING_AGG(DISTINCT o.restaurant.name, ';') AS tiendas,
      STRING_AGG(DISTINCT CAST(o.restaurant.id AS STRING), ';') AS ids_tienda
    FROM
      `peya-bi-tools-pro.il_core.fact_orders` AS o,
      UNNEST(o.details) AS d
    LEFT JOIN
      `peya-bi-tools-pro.il_qcommerce.dim_vendor_product` AS gc
      ON gc.remote_product_id = d.product.product_id
    WHERE
      o.registered_date > DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 MONTH)
      AND o.confirmed_order = 1
      AND o.business_type.business_type_name IN ('Market', 'Drinks', 'Shop', 'Pharmacy', 'Kiosks', 'Pets')
      AND gc.master_code IS NOT NULL
      AND gc.master_category_names.level_one IN ('Produce', 'Meat / Seafood')
    GROUP BY
      1, 2, 3, 4, 5 
  ),

  ventas_con_acumulados AS (
    SELECT
      *,
      SUM(ventas_unidades) OVER (PARTITION BY pais, cadena, sku) AS ventas_por_cadena_sku,
      SUM(ordenes_unicas) OVER (PARTITION BY pais) AS ordenes_por_pais,
      SUM(gmv) OVER (PARTITION BY pais) AS gmv_por_pais
    FROM ventas_agregadas
  )

 
SELECT
  *,
  CASE
    WHEN pais = 'ar' THEN
      CASE
        WHEN ventas_por_cadena_sku BETWEEN   250 AND   399 THEN   250
        WHEN ventas_por_cadena_sku BETWEEN   400 AND   649 THEN   400
        WHEN ventas_por_cadena_sku BETWEEN   650 AND   999 THEN   650
        WHEN ventas_por_cadena_sku BETWEEN  1000 AND  1999 THEN  1000 
        WHEN ventas_por_cadena_sku BETWEEN  2000 AND  2999 THEN  2000
        WHEN ventas_por_cadena_sku BETWEEN  3000 AND  4999 THEN  3000 
        WHEN ventas_por_cadena_sku BETWEEN  5000 AND  7999 THEN  5000
        WHEN ventas_por_cadena_sku BETWEEN  8000 AND 12999 THEN  8000
        WHEN ventas_por_cadena_sku BETWEEN 13000 AND 20999 THEN 13000
        WHEN ventas_por_cadena_sku BETWEEN 21000 AND 33999 THEN 21000 
        WHEN ventas_por_cadena_sku >= 34000 THEN 34000
        ELSE CAST(FLOOR(ventas_por_cadena_sku / 100) * 100 AS INT64)
      END
    ELSE 
      CASE
        WHEN ventas_por_cadena_sku BETWEEN   100 AND   199 THEN   100
        WHEN ventas_por_cadena_sku BETWEEN   200 AND   299 THEN   200
        WHEN ventas_por_cadena_sku BETWEEN   300 AND   499 THEN   300
        WHEN ventas_por_cadena_sku BETWEEN   500 AND   799 THEN   500
        WHEN ventas_por_cadena_sku BETWEEN   800 AND   999 THEN   800
        WHEN ventas_por_cadena_sku BETWEEN  1000 AND  1999 THEN  1000
        WHEN ventas_por_cadena_sku BETWEEN  2000 AND  2999 THEN  2000
        WHEN ventas_por_cadena_sku BETWEEN  3000 AND  4999 THEN  3000 
        WHEN ventas_por_cadena_sku BETWEEN  5000 AND  7999 THEN  5000
        WHEN ventas_por_cadena_sku BETWEEN  8000 AND 12999 THEN  8000
        WHEN ventas_por_cadena_sku BETWEEN 13000 AND 20999 THEN 13000 
        WHEN ventas_por_cadena_sku >= 21000 THEN 21000
        ELSE CAST(FLOOR(ventas_por_cadena_sku / 100) * 100 AS INT64)
      END
  END AS bucket_ventas,
  (
    (pais = 'ar' AND ventas_por_cadena_sku >= 250)
    OR (pais <> 'ar' AND ventas_por_cadena_sku >= 100)
  ) AS supera_umbral
FROM ventas_con_acumulados
