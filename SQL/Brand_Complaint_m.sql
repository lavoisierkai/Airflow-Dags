/* ----- params start -----
database : access_cdm
table_name : dmd_cust_gd_order_goods_dd_f
if_exist : replace
schedule_interval : 1 8 * * *
   ----- params end ----- */
with 
cust as (
    select tickets_goods_sku_id as sku_id
        ,from_timestamp(paid_at,'yyyy-MM') as stat_date
        ,coalesce(gd_cate_level1_name,'Others') as gd_cate_level1_name
        --,gd_cate_level2_name
        --,gd_cate_level3_name 
        ,sum(1) as ticket_count
        --,count(distinct customer_id) as customer_count
        ,count(distinct order_sn) as order_count
        ,sum(tickets_goods_count) as tickets_goods_count --商品数量（申请售后商品数量）
    from access_cdm.dmd_cust_gd_order_goods_dd_f
    where dt in (select max(dt) from access_cdm.dmd_cust_gd_order_goods_dd_f where dt >= date_sub(current_date(),3)) 
        and paid_at>='2020-06'
    group by tickets_goods_sku_id 
        ,from_timestamp(paid_at,'yyyy-MM')
        ,gd_cate_level1_name
        --,gd_cate_level2_name
        --,gd_cate_level3_name 
),
sku as (
    select distinct sku_id as id
        , spu_id 
    from access_cdm.dim_item_goods_sku_dh_f
    where dt=date_sub(current_date(),1) 
    
),
spu as (
    select distinct spu_id as id
        , goods_name
        ,goods_name_en
        , brand_id,brand_name
        ,brand_type
        ,cate_id
        ,coalesce(cate_name,"Unsorted")cate_name
        ,cate_level1_id,cate_level1_name
    from access_cdm.dim_item_goods_sku_dh_f
    where dt=date_sub(current_date(),1)
),
sku2 as (
    select distinct sku.id
        ,spu_id
        , goods_name
        ,goods_name_en
        , brand_id,brand_name
        ,brand_type
        ,cate_id,cate_name
        ,cate_level1_id,cate_level1_name
    from sku left outer join spu on sku.spu_id=spu.id
),
base as (
    select  stat_date
        ,cate_level1_id,cate_level1_name
        ,cate_id,cate_name
        , brand_id,brand_name
        ,brand_type
        ,gd_cate_level1_name
        ,sum(ticket_count) as ticket_count
        ,sum(order_count) as order_count
        ,sum(tickets_goods_count) as tickets_goods_count
    from(
        select *
        from cust 
        left outer join sku2 on cust.sku_id=sku2.id
    )t0
    group by  stat_date
        , brand_id,brand_name
        ,brand_type
        ,cate_id,cate_name
        ,cate_level1_id,cate_level1_name
        ,gd_cate_level1_name
)
select *
from base
where    brand_type=1  --品牌类型：0-其他，1-自有品牌，2-福三
;