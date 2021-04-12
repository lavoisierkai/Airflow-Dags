/* ----- params start -----
database : analysis
table_name : ads_brand_re_purchase_mm_i
if_exist : replace
schedule_interval : 1 8 * * *
   ----- params end ----- */
SELECT
	dt as `date`,
	brand_id,
	level_by_order ,
	pmt_usr_cnt_mtd as user_cnt,
	rp1_usr_cnt_mtd as m_rep_user_cnt,
	rp3_usr_cnt_mtd as q_rep_user_cnt,
	rp6_usr_cnt_mtd as hy_rep_user_cnt,
	rp12_usr_cnt_mtd as y_rep_user_cnt,
	rp1_usr_cnt_mtd / pmt_usr_cnt_mtd as m_rep_rate,
	rp3_usr_cnt_mtd / pmt_usr_cnt_mtd as q_rep_rate,
	rp6_usr_cnt_mtd / pmt_usr_cnt_mtd as hy_rep_rate,
	rp12_usr_cnt_mtd / pmt_usr_cnt_mtd as y_rep_rate
	--from_timestamp(add_months(concat(dt, '-01'),1),'yyyy-MM') as rp_month
from
	analysis.ads_brand_re_purchase_mm_i
where
--	level_by_order = '0'
	brand_type = '自有品牌'
	and is_new_buy_mtd = '合计'
	and order_env = '0'
	and goods_type_desc = '合计'
order by brand_id,
	dt,
	level_by_order
