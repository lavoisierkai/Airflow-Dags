/* ----- params start -----
database : analysis
table_name : ads_bi_platform_brand_repurchase_active_mm_i
if_exist : replace
schedule_interval : 1 8 * * *
   ----- params end ----- */
select
	dt as `date`,
	level_by_order ,
	brand_id ,
	goods_type ,
	`cycle`,
	pmt_user_cnt_htd as user_pool,
	repu_user_cnt_mtd as ret_user_cnt,
	repu_active_ratio_mtd as ret_rate,
	repu_user_cnt_all_mtd as ret_user_cnt_all,
	repu_active_ratio_all_mtd as ret_rate_all
from analysis.ads_bi_platform_brand_repurchase_active_mm_i
where order_env = '0'
	and brand_type_desc = '自有品牌'
	and goods_type not in ('21','22','23')
