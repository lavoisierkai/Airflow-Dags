/* ----- params start -----
# YAML format, only `table_name` is mandatory
#
# ### Sample params #####
# database : access_aus_bi
# table_name : impala_connection_test
# if_exist : append # enums {‘fail’, ‘replace’, ‘append’}, default ‘replace’
# start_date : 2021-03-31T00:00:00+11:00 # optional, default is the fine modification time
# schedule_interval : 30 8 * * * # check https://crontab.guru/ to verify your schedule interval

database : access_aus_bi
table_name : aubi_vtn_mtd_ytd_dd_f
if_exist : replace 
schedule_interval : 15 8 * * * 

   ----- params end ----- */


with od as (
	select brand_type 
		,bar_code 
		,brand_id 
		,order_id 
		,user_id 
		,gmv 
		,order_goods_amount as amount 
		,count_coll  as quantity 
		,year(created_at) as stat_year
		,if(from_timestamp(created_at,'MM-dd')>='01-01' and from_timestamp(created_at,'MM-dd')< from_timestamp(current_date(),'MM-dd'),1,0) as is_ytd 
		,if(from_timestamp(created_at,'MM-dd')>=from_timestamp(trunc(current_date(),'MM'),'MM-dd') and from_timestamp(created_at,'MM-dd')< from_timestamp(current_date(),'MM-dd'),1,0) as is_mtd
	from access_cdm.dwd_trade_order_goods_dd_f 
	where dt=date_sub(current_date(),1)
		and paid_at  is not null 
		and is_free_goods ='N' and is_zero_amount ='N'
		and brand_id <>130
),
smr as (
	select date_sub(current_date(),1) as stat_date 
		,brand_type 
		,stat_year
		,sum(gmv) as gmv_y
		,sum(amount) as pay_amount_y
		,sum(quantity) as quantity_y
		,count(distinct user_id ) as buyer_cnt_y
		,count(distinct order_id ) as order_cnt_y
		
		,sum(gmv*is_ytd) as gmv_ytd
		,sum(amount*is_ytd) as pay_amount_ytd
		,sum(quantity*is_ytd) as quantity_ytd
		,count(distinct if(is_ytd=1, user_id,null)) as buyer_cnt_ytd
		,count(distinct if(is_ytd=1, order_id,null)) as order_cnt_ytd
		
		,sum(gmv*is_mtd) as gmv_mtd
	    ,sum(amount*is_mtd) as pay_amount_mtd
	    ,sum(quantity*is_mtd) as quantity_mtd
	    ,count(distinct if(is_mtd=1, user_id,null)) as buyer_cnt_mtd
	    ,count(distinct if(is_mtd=1, order_id,null)) as order_cnt_mtd
	from od 
	group by brand_type,  stat_year
	
	union all 
	select date_sub(current_date(),1) as stat_date 
		,0 as brand_type 
		,stat_year
		,sum(gmv) as gmv_y
		,sum(amount) as pay_amount_y
		,sum(quantity) as quantity_y
		,count(distinct user_id ) as buyer_cnt_y
		,count(distinct order_id ) as order_cnt_y
		
		,sum(gmv*is_ytd) as gmv_ytd
		,sum(amount*is_ytd) as pay_amount_ytd
		,sum(quantity*is_ytd) as quantity_ytd
		,count(distinct if(is_ytd=1, user_id,null)) as buyer_cnt_ytd
		,count(distinct if(is_ytd=1, order_id,null)) as order_cnt_ytd
		
		,sum(gmv*is_mtd) as gmv_mtd
	    ,sum(amount*is_mtd) as pay_amount_mtd
	    ,sum(quantity*is_mtd) as quantity_mtd
	    ,count(distinct if(is_mtd=1, user_id,null)) as buyer_cnt_mtd
	    ,count(distinct if(is_mtd=1, order_id,null)) as order_cnt_mtd
	from od 
	group by   stat_year
)
select *
from smr 
order by brand_type,  stat_year
;
 

 
;
 

 