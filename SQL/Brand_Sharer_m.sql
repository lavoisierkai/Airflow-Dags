/* ----- params start -----
database : access_ads
table_name : ads_seller_report_share_brand_md_i
if_exist : replace
schedule_interval : 1 8 * * *
   ----- params end ----- */
select
	dt as month,
	brand_id,
	sale_goods_type ,
	sharer_cnt_htd as total_sharer,
	share_succ_cnt_1m as active_sharer,
	share_succ_pmt_user_cnt_1m as success_shared,
	share_succ_pmt_gmv_cnt_1m as success_shared_gmv,
	share_succ_pmt_order_cnt_1m as success_shared_ords,
	share_succ_ratio_1m as active_sharer_ratio,
	share_succ_user_avg_1m as avg_success_shared,
	sharer_income_amt_1m as sharer_income,
	sharer_income_avg_1m as sharer_avg_income
from
	access_ads.ads_seller_report_share_brand_md_i
where  brand_id not in( SELECT DISTINCT brand_id
					FROM access_cdm.dim_brand_property_dd_f
					WHERE dt=to_date(date_sub(now(), 1) )
        				AND brand_type_desc <>'自有品牌'
        				)
order by brand_id