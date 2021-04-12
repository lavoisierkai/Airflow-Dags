/* ----- params start -----
database : access_aus_bi
table_name : cbo_report_uv
if_exist : replace
schedule_interval : 1 8 * * *
   ----- params end ----- */
WITH t_brand_dim AS
(
  SELECT brand_id, brand_name, brand_type
  FROM access_cdm.dim_brand_dd_f
  WHERE dt = to_date(NOW() - INTERVAL 1 DAY)
),
t_trk_platform AS
(
  SELECT t_trk.event_time,
		 CONCAT(LEFT(t_trk.event_time,13),':00') AS `datetime_hour`,
		 CONCAT(t_trk.dt,' 00:00') AS `datetime_day`,
		 CONCAT(LEFT(t_trk.dt,7),'-01 00:00') AS `datetime_month`,
		 CONCAT(LEFT(t_trk.dt,4),'-01-01 00:00') AS `datetime_year`,
		 user_id
  FROM access_cdm.dwd_user_event_trk_dc_dd_f AS t_trk /* product_dim_change*/ /*use this table only for platform visit*/ 
  WHERE t_trk.dt > '2020'
),
t_trk_brand AS
(
  SELECT t_trk.event_time,
		 CONCAT(LEFT(t_trk.event_time,13),':00') AS `datetime_hour`,
		 CONCAT(LEFT(t_trk.event_time,10),' 00:00') AS `datetime_day`,
		 CONCAT(LEFT(t_trk.event_time,7),'-01 00:00') AS `datetime_month`,
		 CONCAT(LEFT(t_trk.event_time,4),'-01-01 00:00') AS `datetime_year`,
		 t_trk.user_id,
		 t_brand_dim.brand_id,
		 t_brand_dim.brand_name,
		 t_brand_dim.brand_type, 
		 CASE
		   WHEN (t_brand_dim.brand_type) = 1 THEN 'ACG Brands'
		   WHEN (t_brand_dim.brand_type) = 2 THEN 'F3 Brands'
		   ELSE 'Other Brands'
		 END AS brand_type_desc_en
  FROM access_cdm.dwd_flow_goods_trk_vtn_dd_i AS t_trk /* product_dim_change*/ /*use this table only for product details visit*/ 
	LEFT JOIN t_brand_dim ON t_brand_dim.brand_id = CAST (t_trk.brand_id AS INT)
  WHERE  t_trk.dt > '2020' AND t_trk.brand_id IS NOT NULL
)
-- @@ UV -- ## each brand
SELECT 'year' AS date_dim_level /*date_dim_change*/,
	   'brand' AS prod_dim_level,
	   datetime_year AS `datetime` /*date_dim_change*/,
	   brand_id,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_brand
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT 'month' AS date_dim_level /*date_dim_change*/,
	   'brand' AS prod_dim_level,
	   datetime_month AS `datetime` /*date_dim_change*/,
	   brand_id,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_brand
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT 'day' AS date_dim_level /*date_dim_change*/,
       'brand' AS prod_dim_level,
	   datetime_day AS `datetime` /*date_dim_change*/,
	   brand_id,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_brand
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT 'hour' AS date_dim_level /*date_dim_change*/,
	   'brand' AS prod_dim_level,
	   datetime_hour AS `datetime` /*date_dim_change*/,
	   brand_id,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_brand
where event_time > to_date(now() - interval 3 day)
GROUP BY 1, 2, 3, 4
UNION ALL
-- ## each Brand Group 
SELECT 'year' AS date_dim_level /*date_dim_change*/,
	   'brand group' AS prod_dim_level /*product_dim_change*/,
	   datetime_year AS `datetime` /*date_dim_change*/,
		cast(null as int) AS brand_id /*product_dim_change*/,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_brand
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT 'month' AS date_dim_level /*date_dim_change*/,
	   'brand group' AS prod_dim_level /*product_dim_change*/,
	   datetime_month AS `datetime` /*date_dim_change*/,
		cast(null as int) AS brand_id /*product_dim_change*/,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_brand
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT 'day' AS date_dim_level /*date_dim_change*/,
	   'brand group' AS prod_dim_level /*product_dim_change*/,
	   datetime_day AS `datetime` /*date_dim_change*/,
		cast(null as int) AS brand_id /*product_dim_change*/,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_brand
where event_time > '2020'
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT 'hour' AS date_dim_level /*date_dim_change*/,
	   'brand group' AS prod_dim_level /*product_dim_change*/,
	   datetime_hour AS `datetime` /*date_dim_change*/,
		cast(null as int) AS brand_id /*product_dim_change*/,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_brand
where event_time > to_date(now() - interval 10 day)
GROUP BY 1, 2, 3, 4
UNION ALL
-- ## all brand      a.k.a platform
SELECT 'year' AS date_dim_level /*date_dim_change*/,
	   'all brands' AS prod_dim_level /*product_dim_change*/,
	   datetime_year AS `datetime` /*date_dim_change*/,
		cast(null as int) AS brand_id /*product_dim_change*/,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_platform
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT 'month' AS date_dim_level /*date_dim_change*/,
	   'all brands' AS prod_dim_level /*product_dim_change*/,
	   datetime_month AS `datetime` /*date_dim_change*/,
		cast(null as int) AS brand_id /*product_dim_change*/,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_platform
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT 'day' AS date_dim_level /*date_dim_change*/,
       'all brands' AS prod_dim_level /*product_dim_change*/,
	   datetime_day AS `datetime` /*date_dim_change*/,
		cast(null as int) AS brand_id /*product_dim_change*/,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_platform
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT 'hour' AS date_dim_level /*date_dim_change*/,
	   'all brands' AS prod_dim_level /*product_dim_change*/,
	   datetime_hour AS `datetime` /*date_dim_change*/,
	   cast(null as int) AS brand_id /*product_dim_change*/,
	   ------------- -- dim above ---------------
	   COUNT(DISTINCT user_id) AS uv
FROM t_trk_platform
where event_time > to_date(now() - interval 10 day)
GROUP BY 1, 2, 3, 4
;				
