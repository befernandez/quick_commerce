WITH   users_universe AS 
(
  SELECT  
    du.country_id,
    du.country.country_name,
    du.user_id,
    CASE 
      WHEN ci.income IN ('HIGH', 'MID-HIGH') THEN 'High'
      WHEN ci.income IN ('MID') THEN 'Mid'
      WHEN ci.income IN ('LOW', 'MID-LOW') THEN 'Low'
      WHEN ci.income = 'UNKNOWN' OR ci.income IS NULL THEN 'Other'
      ELSE ci.income 
    END AS user_income,
    CASE 
      WHEN (ci.age < 20 AND ci.age IS NOT NULL) THEN 'Teen'
      WHEN (ci.age >= 20 AND ci.age < 25 ) OR ((ci.age >= 20 OR ci.age IS NULL) AND ci.age_group_merged = 'U25') THEN 'Young 20s'
      WHEN (ci.age >= 25 AND ci.age IS NOT NULL AND ci.age < 30) THEN "Old 20s"
      WHEN (((ci.age >= 30 AND ci.age IS NOT NULL) OR ci.age IS NULL) AND ci.age_group_merged = 'U35') THEN 'Young Adult'
      WHEN (((ci.age > 35 AND ci.age IS NOT NULL) OR ci.age IS NULL) AND ci.age_group_merged = 'U45')  THEN 'Adult'
      WHEN (((ci.age > 45 AND ci.age IS NOT NULL) OR ci.age IS NULL) AND ci.age_group_merged = 'O45')  THEN 'Senior'
      ELSE ci.age_group_merged
    END AS user_age_group,
    CASE 
      WHEN ci.gender_merged IN ("FEMALE", "MALE") THEN INITCAP(ci.gender_merged) 
      ELSE NULL 
    END AS user_gender,
  
  FROM `peya-bi-tools-pro.il_core.dim_user` du
  
  INNER JOIN `peya-bi-tools-pro.il_core.dim_country` AS dc 
    ON (du.country_id = dc.country_id AND dc.active)
  
  LEFT JOIN `peya-bi-tools-pro.il_growth.user_income` AS ci 
    ON du.user_id = ci.user_id
  
  GROUP BY 1, 2, 3, 4, 5, 6
  )

, user_segment_qc AS 
(
  select *
  ,
    CASE 
    WHEN u.user_income IN ('High', "Mid", "Low") AND u.user_age_group = 'Teen' THEN 'Future Core (15 a 19)'
    WHEN u.user_income IN ('High', "Mid", "Low") AND u.user_age_group = 'Young 20s' THEN 'Future Core (20 a 24)'
    WHEN u.user_income IN ('High', "Mid") AND u.user_age_group = 'Old 20s' THEN 'Future Core (25 a 29)'
    WHEN u.user_income = 'High' AND u.user_age_group IN ("Young Adult", 'Adult', "Senior") THEN 'Core'
    WHEN u.user_income = 'Mid' AND u.user_age_group IN ("Young Adult", 'Adult', "Senior") THEN 'Complementary'
    WHEN u.user_income = 'Low' AND u.user_age_group IN ("Young Adult", 'Adult', "Senior", "Old 20s") THEN 'Not Core'
    ELSE 'Other' 
  END AS subsegment
  FROM users_universe u
)


, food_orders AS 
(
   SELECT DISTINCT
   user_id,
   MIN(CASE WHEN (nro_order_confirmed_restaurant >= 1 OR nro_order_confirmed_coffee >= 1) THEN registered_date_partition ELSE NULL END) AS date_first_food_order,
   MAX(registered_date_partition) AS date_last_food_order,
   FROM `peya-bi-tools-pro.il_core.fact_peya_orders_by_customers` c 
  GROUP BY 1
)


, plus_users AS ---usuario susciptos a plus el 31/7/25
(
  SELECT DISTINCT
  user_id
  FROM `peya-data-origins-pro.cl_loyalty.loyalty_subscription_historical`
  WHERE start_date <= '2025-07-31'AND (inactive_date > '2025-07-31' OR inactive_date IS NULL)
)


, dim_partner AS
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
)

, user_base_onb as 
(
  SELECT DISTINCT
  user_id
  , fecha as first_order_date_qc
  , order_id
  , vendor_type 
  from  `peya-datamarts-pro.dm_onboarding.orders_objectives` o
  left join dim_partner p on  p.partner_id = o.partner_id
  where (qc_trial is true or acq is true )
  and fecha between '2025-07-01' and '2025-07-31'
  and vendor_type in ('DMart', 'AAA')
)

, base_users_tmp AS (
  SELECT DISTINCT
  b.user_id
  , first_order_date_qc
  , b.order_id as first_order_id
  , CASE WHEN date_first_food_order < b.first_order_date_qc THEN 'food user' ELSE 'no food user' END as is_food_user
  , CASE WHEN plus_users.user_id IS NOT NULL THEN 'is_plus' ELSE 'no_plus' END AS is_plus_user
  FROM user_base_onb b
  left join food_orders on food_orders.user_id = b.user_id
  left join plus_users on plus_users.user_id = b.user_id
)

, base_users_final AS 
(
  select  *
  ,  CASE 
    WHEN is_food_user = 'food user' AND is_plus_user = 'is_plus' THEN  'a.Food user & plus'
    WHEN is_food_user = 'food user' AND is_plus_user = 'no_plus' THEN  'b.Food user & No plus'
    WHEN is_food_user = 'no food user' THEN 'c. New PeYa user'
  END AS user_type
  from base_users_tmp
)

, dvp AS
(
  SELECT
  snapshot_date,
  remote_vendor_id as partner_id,
  remote_product_id as product_id,
   master_category_names.level_one,
  CASE WHEN master_category_names.level_one IN ('Produce', 'Meat / Seafood') THEN TRUE ELSE FALSE END AS ultra_fresh,
  CASE WHEN master_category_names.level_one IN ('Meat / Seafood') THEN TRUE ELSE FALSE END AS meat,
  CASE WHEN master_category_names.level_one IN ('Ready To Consume', 'Dairy / Chilled / Eggs', 'Bread / Bakery') THEN TRUE ELSE FALSE END AS fresh,
  CASE WHEN master_category_names.level_one IN ('BWS') THEN TRUE ELSE FALSE END AS BWS,
  CASE WHEN master_category_names.level_one IN ('Smoking / Tobacco') THEN TRUE ELSE FALSE END AS Smoking_Tobacco,
  CASE WHEN master_category_names.level_one IN ('Personal Care / Baby / Health') THEN TRUE ELSE FALSE END AS Personal_Care_Baby_health,
  CASE WHEN master_category_names.level_one IN ('Dairy / Chilled / Eggs') THEN TRUE ELSE FALSE END AS Dairy_Chilled_Eggs,
  CASE WHEN master_category_names.level_one IN ('Snacks') THEN TRUE ELSE FALSE END AS Snacks,
  CASE WHEN master_category_names.level_one IN ('Produce') THEN TRUE ELSE FALSE END AS Produce,
  CASE WHEN master_category_names.level_one IN ('Frozen') THEN TRUE ELSE FALSE END AS Frozen,
  CASE WHEN master_category_names.level_one IN ('General Merchandise') THEN TRUE ELSE FALSE END AS General_Merchandise,
  CASE WHEN master_category_names.level_one IN ('Bread / Bakery') THEN TRUE ELSE FALSE END AS Bread_Bakery,
  CASE WHEN master_category_names.level_one IN ('Beverages') THEN TRUE ELSE FALSE END AS Beverages,
  CASE WHEN master_category_names.level_one IN ('Meat / Seafood') THEN TRUE ELSE FALSE END AS Meat_Seafood,
  CASE WHEN master_category_names.level_one IN ('Home / Pet') THEN TRUE ELSE FALSE END AS Home_Pet,
  CASE WHEN master_category_names.level_one IN ('Packaged Foods') THEN TRUE ELSE FALSE END AS Packaged_Foods,
  CASE WHEN master_category_names.level_one IN ('Ready To Consume') THEN TRUE ELSE FALSE END AS Ready_Consume,
  CASE WHEN master_category_names.level_one IN ('BWS') THEN TRUE ELSE FALSE END AS BWS,
  FROM `peya-bi-tools-pro.il_qcommerce.dim_vendor_product_snapshot`
  WHERE DATE(snapshot_date) BETWEEN '2025-07-01' and '2025-07-31'      
 )

 , fact_orders AS 
 (
  SELECT 
  user.id as user_id,
    fo.order_id,
    fo.registered_date,
    fo.restaurant.id as partner_id,
    d.product.product_id as product_id,
    business_type.business_type_name,
    total_amount, --lo que pagó el usuario
    discount_amount, --total de descuentoa
    amount_no_discount,
    d.product_name, 
    qty_total_products,
    subsegment,
    user_type
    FROM `peya-bi-tools-pro.il_core.fact_orders` fo      
    LEFT JOIN UNNEST(details) d    
    INNER JOIN user_base_onb onb on onb.order_id =fo.order_id 
    LEFT JOIN user_segment_qc ON user_segment_qc.user_id = fo.user.id
    LEFT JOIN base_users_final ON base_users_final.user_id = fo.user.id
    WHERE 
      TRUE
      AND DATE(fo.registered_date) BETWEEN '2025-07-01' and '2025-07-31'
      AND fo.business_type_id IN (2 , 3 , 4 , 5 , 6 , 8 , 12) 
 )
 
 
, base_ordenes_categorias aS (
 select *
 from fact_orders fo
   LEFT JOIN dvp 
      ON fo.product_id = dvp.product_id 
    AND fo.partner_id = dvp.partner_id 
      AND fo.registered_date = dvp.snapshot_date ---sacar bolsa
      ANd product_name !='bolsa'
order by 2
)
select distinct 
user_type
, level_one
, count(*)
 from base_ordenes_categorias
group by 1,2
 

--
