/* ----- params start -----
database : access_aus_bi
table_name : aubi_barcode_cust_gd_mm_f
if_exist : replace
schedule_interval : 30 8 * * *
   ----- params end ----- */
   
  select goods_bar_code as bar_code
        ,from_timestamp(paid_at,'yyyy-MM') as stat_date
        ,coalesce(gd_cate_level1_name,'Others') as gd_cate_level1_name
        --,gd_cate_level2_name
        --,gd_cate_level3_name 
        ,coalesce(gd_cate_level4_name,'Others') as gd_cate_level4_name
        ,sum(1) as ticket_count
        --,count(distinct customer_id) as customer_count
        ,count(distinct order_sn) as order_count
        ,sum(tickets_goods_count) as tickets_goods_count --商品数量（申请售后商品数量）
    from access_cdm.dmd_cust_gd_order_goods_dd_f
    where dt=date_sub(current_date(),1) 
        and paid_at>='2020-06'
        and brand_type=1
        and   gd_cate_level1_id = 557 -- 品质相关
    group by goods_bar_code 
        ,from_timestamp(paid_at,'yyyy-MM')
        ,gd_cate_level1_name
		--,gd_cate_level2_name
        --,gd_cate_level3_name 
        ,coalesce(gd_cate_level4_name,'Others')