/* ----- params start -----
database : access_ads
table_name : ads_goods_sku_penetration_rate_dd_i
if_exist : replace
schedule_interval : 1 8 * * *
   ----- params end ----- */
select 
	DISTINCT 
	dt as `date`,
	bar_code ,
	pene_type as `level`,
	user_count as total_user_cnt,
	payment_user_count as user_reach,
	pene_rate 
from access_ads.ads_goods_sku_penetration_rate_dd_i
where day(date_add(dt,1))=1
	and bar_code is not null
	and bar_code <> ""
	and pene_rate >0