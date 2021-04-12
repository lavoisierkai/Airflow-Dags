/* ----- params start -----
database : access_aus_bi
table_name : aubi_barcode_repurchase_gap
if_exist : replace
schedule_interval : 1 8 1 * *
   ----- params end ----- */

 

WITH w_brand as (
    select distinct id   
        ,name  as brand_name 
    from access_ods.ods_w_brand_dh_f 
    where dt=to_date(date_sub(current_date(), 1) )
    --ORDER BY id desc
),
tb AS(
    SELECT  user_id, bar_code
        , count(DISTINCT order_id) as order_cnt
        , to_date(created_at) as create_date
        , sum(order_goods_amount) as paid_amount
        , avg(order_goods_amount) as avg_paid_amount
        , sum(gmv) as gmv
        , sum(count_coll) as quantity
    FROM access_cdm.dwd_trade_order_goods_dd_f
    WHERE dt = to_date(date_sub(current_date(),1))
        and paid_at IS NOT NULL 
        and created_at>=date_sub(current_date(),366)
		AND is_free_goods ='N' and is_zero_amount ='N'
        and brand_type=1
    group by  user_id, bar_code, to_date(created_at)
),
pre AS(
    SELECT user_id, bar_code
        ------ recency
        ,min(create_date) as min_create_date
        ,max(create_date) as max_create_date
        ,datediff(max(create_date),min(create_date)) as time_gap
        ,datediff(current_date(),max(create_date)) as recency
        ------ frequency
        ,sum(order_cnt) as order_cnt
        ,sum(quantity) as quantity 
        --,sum(if(create_date>date_sub(current_date(),365),order_cnt,0)) as order_cnt_365d
        ,sum(if(create_date>date_sub(current_date(),180),order_cnt,0)) as order_cnt_180d
        ,count(DISTINCT create_date) as order_days 
        --,count(DISTINCT if(create_date>date_sub(current_date(),365),create_date,null)) as order_days_365d
        ,count(DISTINCT if(create_date>date_sub(current_date(),180),create_date,null)) as order_days_180d
        ------ monetary
        ,sum(paid_amount) as sum_paid_amount
        --,sum(if(create_date>date_sub(current_date(),365),paid_amount,0)) as sum_paid_amount_365d
        ,avg(paid_amount) as avg_day_paid_amount
        ,avg(avg_paid_amount) as avg_brand_order_paid_amount
        --,rank() OVER(PARTITION BY bar_code order by rand()) as sample_rank
    FROM tb 
    WHERE create_date < current_date()
    group by user_id, bar_code 
)
SELECT date_sub(current_date(),180) as start_date
	,current_date() as end_date
	,bar_code
	,sum(1) as rp_byr_cnt
	,avg(time_gap/(order_days-1)) as avg_rp_window
	,avg(quantity/order_days) as avg_rp_quantity
FROM  pre 
WHERE order_days >1 
GROUP BY bar_code
HAVING sum(1)>100