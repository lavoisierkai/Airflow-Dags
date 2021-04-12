/* ----- params start -----
database : access_ads
table_name : ads_seller_report_overview_md_f
if_exist : replace
schedule_interval : 1 8 * * *
   ----- params end ----- */
select
	dt as `date`,
	`level`,
	seller_add_cnt_mtd as mtd_new_member_cnt,
	seller_cnt_htd as cumulative_member_cnt
from
	access_ads.ads_seller_report_overview_md_f
order by
	dt DESC