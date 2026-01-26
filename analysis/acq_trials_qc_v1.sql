 
with users_universe AS 
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


, users AS 
(
  SELECT DISTINCT 
  USER_ID
  , MIN(first_purchase_date_dmarts) AS first_purchase_date_dmarts
  , MIN(first_purchase_date_aaa) AS first_purchase_date_aaa
  FROM `peya-bi-tools-pro.il_core.fact_peya_orders_by_customers` c
  GROUP BY 1
)

, base_tmp AS 
(
  SELECT *
  FROM users
  WHERE
  ( first_purchase_date_dmarts BETWEEN DATE '2025-07-01' AND DATE '2025-07-31' AND (first_purchase_date_aaa IS NULL OR first_purchase_date_dmarts <= first_purchase_date_aaa))
  OR
  (first_purchase_date_aaa BETWEEN DATE '2025-07-01' AND DATE '2025-07-31' AND (first_purchase_date_dmarts IS NULL OR first_purchase_date_aaa < first_purchase_date_dmarts))
)

, users_base AS 
(
 SELECT DISTINCT 
 user_id 
 , CASE 
    WHEN first_purchase_date_dmarts is null then first_purchase_date_aaa
    WHEN first_purchase_date_aaa is null then first_purchase_date_dmarts
    WHEN first_purchase_date_aaa IS NOT NULL AND first_purchase_date_dmarts IS NOT NULL THEN LEAST(first_purchase_date_aaa, first_purchase_date_dmarts)
  END AS first_order_date_qc 
 FROM base_tmp
)

, first_order_id AS
(
  SELECT 
  b.user_id
  , first_order_date_qc
  , MIN(order_id) AS order_id
  FROM users_base b
  LEFT JOIN `peya-bi-tools-pro.il_core.fact_peya_orders_by_customers` c 
    ON b.USER_ID = c.USER_ID
    AND first_order_date_qc = registered_date_partition
    AND business_type_id = 2
  GROUP BY 1,2 
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

, vouchers_mkt AS
(
  SELECT DISTINCT
   created_date
   , created_at_local
   , user_id
   , id
   , campaign_title
   , title
   , voucher_code
   , redeemed_date
   , redeemed_order_id
   FROM `peya-bi-tools-pro.il_compensations.fact_care_voucher` c 
   where  created_date between '2025-01-01' and  '2025-07-31' 
   and is_care_campaign is false and is_care_processed is false
   and redeemed_order_id is not null 
)

, new_orders_base AS 
(
  SELECT DISTINCT
  first_order_date_qc,
  first_order_id.order_id,
  fo.registered_date,
  fo.order_id,
  user.id as user_id ,
  date_diff(registered_date ,first_order_date_qc, day) as days_from_first_order
  , COUNT(DISTINCT CASE WHEN  business_type.business_type_name = 'Restaurant' THEN fo.ORDER_ID END) AS new_food_orders
  , COUNT(DISTINCT CASE WHEN  business_type.business_type_name = 'Market' THEN fo.ORDER_ID END) AS new_market_orders
  , count(distinct fo.order_id) AS total_new_orders 
  FROM first_order_id 
  LEFT JOIN `peya-bi-tools-pro.il_core.fact_orders` AS fo ON first_order_id.user_id = fo.user.id AND fo.order_id > first_order_id.order_id
  WHERE
    TRUE
    AND DATE(fo.registered_date) BETWEEN "2025-07-01" AND "2025-12-31" 
    GROUP BY all
)

, new_orders AS 
(
  SELECT DISTINCT  
  user_id
  , SUM(new_food_orders)new_food_orders
  , SUM(new_market_orders)new_market_orders
  , SUM(total_new_orders)total_new_orders
  , MIN(days_from_first_order)days_from_first_order
  FROM new_orders_base
  GROUP BY 1
)

, currency_exchange AS
(
  SELECT 
     dc.country_code
    ,dce.currency_exchange_date
    ,dce.rate_eu
    ,dce.currency_iso
    ,dce.currency_id
  FROM `peya-bi-tools-pro.il_core.dim_country` dc
  LEFT JOIN `peya-bi-tools-pro.il_core.dim_currency_exchange` dce  ON dc.currency_id = dce.currency_id
)

, original_orders_info AS 
(
  SELECT 
  user.id as user_id,
  fo.order_id,
  fo.registered_date,
  fo.restaurant.id as partner_id,
  business_type.business_type_name,
  total_amount, --lo que pagó el usuario
  discount_amount, --total de descuentoa
  amount_no_discount, 
  qty_total_products, 
  SAFE_DIVIDE(total_amount, rate_eu)  as total_amount_EU,
  SAFE_DIVIDE(discount_amount, rate_eu)  as discount_amount_EU,
  SAFE_DIVIDE(amount_no_discount, rate_eu)  as amount_no_discount_EU
  FROM `peya-bi-tools-pro.il_core.fact_orders` fo
  INNER JOIN first_order_id ON first_order_id.order_id = fo.order_id
  LEFT JOIN currency_exchange ce on ce.country_code = fo.country.country_code and ce.currency_exchange_date = date_trunc(date(fo.registered_date),month)
  WHERE DATE(fo.registered_date) BETWEEN '2025-07-01' and '2025-07-31'    
 )


 , INCENTIVES AS (
   select distinct o.order_id 
 , MAX(CASE WHEN discount_subtype IN ('MARKETING_CENTRAL', 'MARKETING_CRM') THEN 1 END) AS mkt_voucher
 , MAX(case when discount_type='QC_CAMPAIGN' THEN 1 END) AS QC_CAMPAIGN
 , MAX(case when discount_subtype = 'DELIVERY_FEE_DFS' then 1 end) as DELIVERY_FEE_DFS
from original_orders_info o
left join  `peya-bi-tools-pro.il_core.order_incentives` i on i.order_id = o.order_id
 where i.registered_date >= '2025-07-01'
 GROUP BY ALL
 
 )


, base as 
(
SELECT DISTINCT
users_base.user_id
, users_base.first_order_date_qc
, date_first_food_order
, first_order_id.order_id
, CASE WHEN date_first_food_order < users_base.first_order_date_qc THEN 'food user' ELSE 'no food user' END as is_food_user
, CASE WHEN plus_users.user_id IS NOT NULL THEN 'is_plus' ELSE 'no_plus' END AS is_plus_user
 --, redeemed_order_id
-- , CASE WHEN redeemed_order_id IS NOT NULL THEN 'mkt voucher' END AS first_order_with_mkt_voucher
, user_income 
, country_id
, user_age_group
, user_gender
, subsegment
, country_name
, COALESCE(new_food_orders, 0) AS new_food_orders
, COALESCE(new_market_orders, 0) AS new_market_orders
, COALESCE(total_new_orders, 0) AS total_new_orders
, days_from_first_order
, total_amount_EU
, discount_amount_EU
, amount_no_discount_EU
, total_amount
, discount_amount
, amount_no_discount
, qty_total_products
, mkt_voucher
,  QC_CAMPAIGN
 ,  DELIVERY_FEE_DFS
from users_base 
left join food_orders on food_orders.user_id = users_base.user_id
left join plus_users on plus_users.user_id = users_base.user_id
LEFT JOIN first_order_id ON first_order_id.user_id = users_base.user_id 
--LEFT JOIN vouchers_mkt ON first_order_id.order_id = vouchers_mkt.redeemed_order_id 
LEFT JOIN user_segment_qc ON user_segment_qc.user_id = users_base.user_id
LEFT JOIN new_orders ON new_orders.user_id = users_base.user_id
LEFT JOIN  original_orders_info ON original_orders_info.order_id = first_order_id.order_id
LEFT JOIN INCENTIVES ON INCENTIVES.order_id = first_order_id.order_id
 where first_order_id.order_id is not null 
)

, base_2 as (
  select * 
  ,   CASE 
    WHEN is_food_user = 'food user' AND is_plus_user = 'is_plus' THEN  'a.Food user & plus'
    WHEN is_food_user = 'food user' AND is_plus_user = 'no_plus' THEN  'b.Food user & No plus'
    WHEN is_food_user = 'no food user' THEN 'c. New PeYa user'
  END AS user_type
  from base

)

select * from base
limit 1000

/*
select distinct
user_type
--, PERCENTILE_CONT(qty_total_products, 0.5) OVER (PARTITION BY user_type) AS mediana
, avg( total_amount_EU)
, avg( discount_amount_EU)
, avg( amount_no_discount_EU)

FROM base_2
group by 1
limit 100 */
 
 /*
SELECT DISTINCT 
 
user_type
  , subsegment
  , country_name
  , mkt_voucher
,  QC_CAMPAIGN
 ,  DELIVERY_FEE_DFS
  , CASE 
      WHEN total_new_orders = 0 THEN 'a. no new orders'
      WHEN new_food_orders = 0 AND new_market_orders > 0 THEN 'b. only market new orders'
      WHEN new_market_orders = 0 AND new_food_orders > 0 THEN 'c. only food new orders'
      WHEN new_market_orders > 0 AND new_food_orders > 0 THEN 'd. market & food new orders'
    END AS new_orders
  , COUNT(DISTINCT USER_ID)
  , SUM(total_amount_EU)total_amount_EU
  , SUM(discount_amount_EU)discount_amount_EU
  , SUM(amount_no_discount_EU)amount_no_discount_EU
  , SUM(qty_total_products)qty_total_products
  --, COUNT(*)
FROM base_2 
GROUP BY ALL 
 */
 
-- , first_order_with_mkt_voucher
 /*, CASE WHEN total_new_orders = 0 THEN 'a. no new orders'
        WHEN total_new_orders >=1 and  total_new_orders <= 5 THEN 'b.1 to 5 new orders'
        WHEN total_new_orders >=6 THEN 'c.more than 6 new orders'
    END AS total_new_orders_group
  , CASE WHEN new_market_orders = 0 THEN 'a. no new orders'
        WHEN new_market_orders >=1 and  new_market_orders <= 5 THEN 'b.1 to 5 new orders'
        WHEN new_market_orders >=6 THEN 'c.more than 6 new orders'
    END AS new_market_orders_group
    , CASE WHEN new_food_orders = 0 THEN 'a. no new orders'
        WHEN new_food_orders >=1 and  new_food_orders <= 5 THEN 'b.1 to 5 new orders'
        WHEN new_food_orders >=6 THEN 'c.more than 6 new orders'
    END AS new_food_orders_group*/
