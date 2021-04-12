/* ----- params start -----
database : access_aus_bi
table_name : cbo_report_order
if_exist : replace
schedule_interval : 5 8 * * *
   ----- params end ----- */

-- @@ v3 order
-- add seller count
-- @@@ Use this

WITH  t_main AS
(
  SELECT t_order.created_at,
		 LEFT(t_order.created_at,10) AS `datetime_day`,
		 CONCAT(LEFT(t_order.created_at,7),'-01') AS `datetime_month`,
		 CONCAT(LEFT(t_order.created_at,4),'-01-01') AS `datetime_year`,
         CASE
           WHEN LEFT(t_order.created_at,10) <= t_user_old_new.first_pmt_date THEN 'New User'
           ELSE 'Old User'
         END AS is_new_buyer,
         CASE
           WHEN t_user_dim.user_area = '澳新区' THEN 'ANZ'
           WHEN t_user_dim.user_area = '中国区' THEN 'CN'
           ELSE 'Other'
         END AS user_region,
         t_order.brand_id,
         t_order.brand_name,
       CASE WHEN brand_type = 1 THEN 'ACG Brands' WHEN brand_type = 2 THEN 'F3 Brands' ELSE 'Other Brands' END AS brand_type_desc_en,
         t_order.brand_type,
         t_order.gmv,
		 t_order.grow_or_sup_id_code_bo ,
         t_order.user_id
  FROM access_cdm.dwd_trade_order_goods_dd_f AS t_order
    LEFT JOIN access_cdm.dws_trade_new_old_buy_user_dd_f AS t_user_old_new
           ON t_user_old_new.user_id = t_order.user_id
    LEFT JOIN access_cdm.dwt_seller_topic_dd_f AS t_user_dim
           ON t_user_dim.id_code = t_order.id_code 
  WHERE t_user_dim.dt = to_date (NOW () - INTERVAL 1 DAY)
    AND t_user_old_new.dt = to_date (NOW () - INTERVAL 1 DAY)
	AND   t_order.dt = to_date(NOW() - INTERVAL 1 DAY)
  	AND   t_order.created_at > '2020'/*-- longest date backed 2019 --*/ 
  	AND   t_order.is_free_goods = 'N'
  	AND   t_order.is_zero_amount = 'N'
  	AND   t_order.paid_at IS NOT NULL
  	)
 -- @@ ----------------- main ------------------
 -- ------------------------------------ each brand ----------------------------------------------
SELECT 'day' AS date_dim_level,
       'brand' AS prod_dim_level,
       datetime_day AS `date`,
       brand_id,
       brand_type,
       is_new_buyer,
       user_region,
       -- --------------- dim above ---------------
       max(brand_name) as brand_name,
       sum(gmv) as gmv,
       COUNT(DISTINCT user_id) AS buyer_count,
       COUNT(DISTINCT grow_or_sup_id_code_bo) AS seller_count
FROM t_main
GROUP BY 1, 2, 3, 4, 5, 6, 7
UNION ALL
SELECT 'month' AS date_dim_level,
       'brand' AS prod_dim_level,
       datetime_month AS `date`,
       brand_id,
       brand_type,
       is_new_buyer,
       user_region,
       -- --------------- dim above ---------------
       max(brand_name) as brand_name,
       NULL as gmv,
       COUNT(DISTINCT user_id) AS buyer_count,
       COUNT(DISTINCT grow_or_sup_id_code_bo) AS seller_count
FROM t_main
GROUP BY 1, 2, 3, 4, 5, 6, 7
UNION ALL
SELECT 'year' AS date_dim_level /*date_dim_change*/,
       'brand' AS prod_dim_level,
       datetime_year AS `date` /*date_dim_change*/,
       brand_id,
       brand_type,
       is_new_buyer,
       user_region,
       -- --------------- dim above ---------------
       max(brand_name) as brand_name,
       null as gmv,
       COUNT(DISTINCT user_id) AS buyer_count,
       COUNT(DISTINCT grow_or_sup_id_code_bo) AS seller_count
FROM t_main
GROUP BY 1, 2, 3, 4, 5, 6, 7
UNION ALL
 -- ------------------------------------ brand groups ----------------------------------------------
SELECT 'day' AS date_dim_level /*date_dim_change*/,
       'brand group' AS prod_dim_level /*product_dim_change*/,
       datetime_day AS `date` /*date_dim_change*/,
       0 as brand_id,
       brand_type,
       is_new_buyer,
       user_region,
       -- --------------- dim above ---------------
       max(brand_type_desc_en) as brand_name,
       null as gmv,
       COUNT(DISTINCT user_id) AS buyer_count,
       COUNT(DISTINCT grow_or_sup_id_code_bo) AS seller_count
FROM t_main
GROUP BY 1, 2, 3, 4, 5, 6, 7
UNION ALL
SELECT 'month' AS date_dim_level /*date_dim_change*/,
       'brand group' AS prod_dim_level /*product_dim_change*/,
       datetime_month AS `date` /*date_dim_change*/,
       0 as brand_id,
       brand_type,
       is_new_buyer,
       user_region,
       -- --------------- dim above ---------------
       max(brand_type_desc_en) as brand_name,
       null as gmv,
       COUNT(DISTINCT user_id) AS buyer_count,
       COUNT(DISTINCT grow_or_sup_id_code_bo) AS seller_count
FROM t_main
GROUP BY 1, 2, 3, 4, 5, 6, 7
UNION ALL
SELECT 'year' AS date_dim_level /*date_dim_change*/,
       'brand group' AS prod_dim_level /*product_dim_change*/,
       datetime_year AS `date` /*date_dim_change*/,
       0 as brand_id,
       brand_type,
       is_new_buyer,
       user_region,
       -- --------------- dim above ---------------
       max(brand_type_desc_en) as brand_name,
       null as gmv,
       COUNT(DISTINCT user_id) AS buyer_count,
       COUNT(DISTINCT grow_or_sup_id_code_bo) AS seller_count
FROM t_main
GROUP BY 1, 2, 3, 4, 5, 6, 7
UNION ALL
-- ------------------------------------ all brands ----------------------------------------------
SELECT 'day' AS date_dim_level /*date_dim_change*/,
       'all brands' AS prod_dim_level /*product_dim_change*/,
       datetime_day AS `date` /*date_dim_change*/,
       0 as brand_id,
       0 as brand_type,
       is_new_buyer,
       user_region,
       -- --------------- dim above ---------------
       '(all brands)' as brand_name,
       null as gmv,
       COUNT(DISTINCT user_id) AS buyer_count,
       COUNT(DISTINCT grow_or_sup_id_code_bo) AS seller_count
FROM t_main
GROUP BY 1, 2, 3, 4, 5, 6, 7
UNION ALL
SELECT 'month' AS date_dim_level /*date_dim_change*/,
       'all brands' AS prod_dim_level /*product_dim_change*/,
       datetime_month AS `date` /*date_dim_change*/,
       0 as brand_id,
       brand_type,
       is_new_buyer,
       user_region,
       -- --------------- dim above ---------------
       '(all brands)' as brand_name,
       null as gmv,
       COUNT(DISTINCT user_id) AS buyer_count,
       COUNT(DISTINCT grow_or_sup_id_code_bo) AS seller_count
FROM t_main
GROUP BY 1, 2, 3, 4, 5, 6, 7
UNION ALL
SELECT 'year' AS date_dim_level /*date_dim_change*/,
       'all brands' AS prod_dim_level /*product_dim_change*/,
       datetime_year AS `date` /*date_dim_change*/,
       0 as brand_id,
       0 as brand_type,
       is_new_buyer,
       user_region,
       -- --------------- dim above ---------------
       '(all brands)' as brand_name,
       null as gmv,
       COUNT(DISTINCT user_id) AS buyer_count,
       COUNT(DISTINCT grow_or_sup_id_code_bo) AS seller_count
FROM t_main
GROUP BY 1, 2, 3, 4, 5, 6, 7
