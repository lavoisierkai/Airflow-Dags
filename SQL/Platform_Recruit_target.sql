/* ----- params start -----
database : access_ads
table_name : ads_kpi_month_platform_target
if_exist : replace
schedule_interval : 1 8 * * *
   ----- params end ----- */
select `month` as `date`,
	high_person as hk_target,
	middle_person as svip_target,
	vvip_person as vvip_target,
	primary_person as vip_target
from access_ads.ads_kpi_month_platform_target