/* ----- params start -----
# YAML format, only `table_name` is mandatory
database : access_aus_bi
table_name : score_card_2021_Q1
if_exist : replace # enums {‘fail’, ‘replace’, ‘append’}, default ‘replace’
schedule_interval : "@once"  # check https://crontab.guru/ to verify your schedule interval
   ----- params end ----- */

WITH t_barcode AS
(
  SELECT bar_code AS _bar_code,
         MAX(brand_id) AS _brand_id,
         MAX(gtm_date) AS gtm_date,
         SUM(gmv) AS gmv
  FROM access_cdm.dwd_trade_order_goods_dd_f AS t_order
    LEFT JOIN (SELECT bar_code AS existing_bar_code,
                      MIN(created_at) AS gtm_date
               FROM access_cdm.dwd_trade_order_goods_dd_f AS t_order
               WHERE dt = '2021-03-31'
               AND   t_order.is_free_goods = 'N'
               AND   t_order.is_zero_amount = 'N'
               AND   t_order.paid_at IS NOT NULL
               AND   t_order.brand_type = 1
               GROUP BY 1) AS t_2020 ON bar_code = existing_bar_code
  WHERE dt = '2021-03-31'
  AND   QUARTER(t_order.created_at) = 1
  AND   t_order.created_at > '2021'
  AND   t_order.is_free_goods = 'N'
  AND   t_order.is_zero_amount = 'N'
  AND   t_order.paid_at IS NOT NULL
  AND   brand_type = 1
  GROUP BY 1
),
t_brand AS
(
  SELECT t_order.brand_id,
         MAX(t_order.brand_name) AS brand_name,
         COUNT(DISTINCT t_order.user_id) AS buyer_count,
         COUNT(DISTINCT CASE WHEN is_first_order = 'Y' THEN t_order.user_id ELSE NULL END) AS buyer_count_new,
         ROUND(SUM(t_order.gmv) /COUNT(DISTINCT t_order.user_id),2) AS acs,
         SUM(t_order.gmv) AS gmv
  FROM access_cdm.dwd_trade_order_goods_dd_f AS t_order
  WHERE dt = '2021-03-31'
  AND   QUARTER(t_order.created_at) = 1
  AND   t_order.created_at > '2021'
  AND   t_order.is_free_goods = 'N'
  AND   t_order.is_zero_amount = 'N'
  AND   t_order.paid_at IS NOT NULL
  AND   brand_type = 1
  GROUP BY 1
),
t_brand_uv AS
(
  SELECT CAST(brand_id AS INT) AS _brand_id,
         COUNT(DISTINCT user_id) AS uv
  FROM access_cdm.dwd_flow_goods_trk_vtn_dd_i
  WHERE dt > '2021'
  AND   dt <= '2021-03-31'
  AND   event_type = 'Browse'
  GROUP BY 1
),
t_penetration AS
(
  SELECT brand_id AS _brand_id,
         pene_rate AS penetration
  FROM access_ads.ads_brand_penetration_rate_dd_i
  WHERE dt = '2021-03-31'
  AND   pene_type = 0
),
t_repurchase AS
(
  SELECT brand_id AS _brand_id,
         AVG(rp_rate) AS repurchase
  FROM access_ads.ads_brand_re_purchase_mm_i
  WHERE dt BETWEEN '2020-10' AND '2021'
  AND   rp_type = 2 /* 3 Month return window */ 
  GROUP BY 1
),
t_forecast AS
(
  SELECT brand_id AS _brand_id,
         SUM(gmv) AS gmv_forecast
  FROM (SELECT CAST(brand_id AS BIGINT) brand_id,
               SUBSTR(predict_time,1,7) AS predict_month,
               SUBSTR(dt,1,7) AS dt,
               RANK() OVER (PARTITION BY dt ORDER BY predict_time) AS rk,
               gmv
        FROM access_aus_bi.access_aus_bi_finance_rolling_forecast_i_dd_f
        WHERE predict_time > dt) AS t_forecast
  WHERE rk = 1
  AND   predict_month BETWEEN '2021-01' AND '2021-03'
  GROUP BY 1
)
SELECT t_brand.brand_id,
       COUNT(t_barcode._bar_code) AS SKU_ALL,
       MAX(acs) AS acs,
       MAX(uv) AS uv,
       MAX(gmv_forecast) AS gmv_forecast,
       MAX(t_brand.gmv) AS gmv,
       MAX(penetration) AS penetration,
       MAX(repurchase) AS repurchase,
       MAX(buyer_count) AS buyer_count,
       MAX(buyer_count_new) AS buyer_count_new,
       MAX(buyer_count) /MAX(uv) AS conversion_rate,
       SUM(CASE WHEN gtm_date > '2021' THEN 1 ELSE NULL END) AS SKU_NPD,
       SUM(CASE WHEN gtm_date > '2021' AND t_barcode.gmv > 10000000 THEN 1 ELSE NULL END) AS SKU_NPD_10M,
       SUM(CASE WHEN t_barcode.gmv >= 1000000000 THEN 1 ELSE NULL END) AS SKU_1B,
       SUM(CASE WHEN t_barcode.gmv < 1000000000 and  t_barcode.gmv >= 500000000 THEN 1 ELSE NULL END) AS SKU_500M,
       SUM(CASE WHEN t_barcode.gmv < 500000000 AND t_barcode.gmv >= 100000000 THEN 1 ELSE NULL END) AS SKU_100M,
       SUM(CASE WHEN t_barcode.gmv < 100000000 AND t_barcode.gmv >= 10000000 THEN 1 ELSE NULL END) AS SKU_10M
FROM t_brand
  LEFT JOIN t_barcode ON t_brand.brand_id = t_barcode._brand_id
  LEFT JOIN t_brand_uv ON t_brand.brand_id = t_brand_uv._brand_id
  LEFT JOIN t_penetration ON t_brand.brand_id = t_penetration._brand_id
  LEFT JOIN t_repurchase ON t_brand.brand_id = t_repurchase._brand_id
  LEFT JOIN t_forecast ON t_brand.brand_id = t_forecast._brand_id
GROUP BY 1

