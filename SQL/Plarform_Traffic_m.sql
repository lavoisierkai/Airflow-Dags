/* ----- params start -----
database : access_ads
table_name : ads_goods_sku_penetration_rate_dd_i
if_exist : replace
schedule_interval : 1 8 * * *
   ----- params end ----- */
select dt as `date`,
	platform,
	CASE
	when `level` = '99' then '0'
	else `level` 
	end as `level`,
	user_visit_cnt as uv,
	goods_user_visit_cnt as goods_uv,
	addcart_user_cnt ,
	addbuy_user_cnt ,
	buynow_user_cnt ,
	pay_suc_cnt 
from access_ads.ads_platform_vtn_user_visit_level_month_dd_i	
