#coding=utf-8
from ImpalatoCSV import *
from CSVtoMysql import *
from ComparetoDT import *
from ICheckTableExists import *
from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
import pendulum
from schedule_timetable import *

local_tz = pendulum.timezone("Australia/Sydney")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 3, 10, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # 'schedule_interval': '@daily',
}

datebase = 'access_cdm'
tableName = "dmd_cust_gd_order_goods_dd_f"
mysql_method = 'replace' #{‘fail’, ‘replace’, ‘append’}, default ‘fail’
# dt=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
dt=dt

dag = DAG(
    "{datebase}.{tableName}".format(datebase=datebase,tableName=tableName),
    description="{datebase}.{tableName}_AGG".format(datebase=datebase,tableName=tableName),
    schedule_interval=schedule_interval1,
    default_args=default_args,
    # catchup=False,
)

querry = '''
WITH cust as (
    select tickets_goods_sku_id as sku_id
        ,from_timestamp(paid_at,'yyyy-MM') as stat_date
        ,coalesce(gd_cate_level1_name,'Others') as gd_cate_level1_name
        --,gd_cate_level2_name
        --,gd_cate_level3_name 
        ,sum(1) as ticket_count
        --,count(distinct customer_id) as customer_count
        ,count(distinct order_sn) as order_count
        ,sum(tickets_goods_count) as tickets_goods_count --商品数量（申请售后商品数量）
    from {datebase}.{tableName}
    where dt in (select max(dt) from {datebase}.{tableName} where dt >= date_sub(current_date(),3)) 
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
where brand_type=1  --品牌类型：0-其他，1-自有品牌，2-福三
;
'''.format(dt=dt,datebase=datebase,tableName=tableName)

get_impala = Get_impala()
csv_to_mysql = CSVtoMysql()
compare_to_dt = ComparetoDT()
checktableexists = ICheckTableExists()

check_table_exists = BranchPythonOperator(
    task_id="check_table_exists", 
    python_callable=checktableexists.icheck_table_exists, 
    op_kwargs={'tableName': tableName}, 
    provide_context=True, 
    dag=dag)

# compare_to_dt = BranchPythonOperator(
#     task_id="compare_to_dt", 
#     python_callable=compare_to_dt.compare_to_dt, 
#     op_kwargs={'dt':dt, 'datebase': datebase, 'tableName': tableName}, 
#     provide_context=True, 
#     # depends_on_past=True,
#     trigger_rule = "none_failed",
#     # depends_on_past=True,
#     dag=dag)

No_need_to_update = DummyOperator(
    task_id = "No_need_to_update",
    trigger_rule = "none_failed",
    dag=dag
    )

get_impala = PythonOperator(
    task_id="get_impala", 
    python_callable=get_impala.get_impala, 
    op_kwargs={'querry': querry, 'dt': dt, 'datebase': datebase, 'tableName': tableName}, 
    provide_context=True, 
    # depends_on_past=True,
    trigger_rule = "none_failed",
    dag=dag)

csv_to_mysql = PythonOperator(
    task_id="csv_to_mysql", 
    python_callable=csv_to_mysql.csv_to_mysql, 
    op_kwargs={'dt': dt, 'datebase': datebase, 'tableName': tableName, 'mysql_method':mysql_method}, 
    provide_context=True, 
    # trigger_rule = "all_success",
    dag=dag)

check_table_exists >> [No_need_to_update, get_impala]
# compare_to_dt >> [get_impala, No_need_to_update]
get_impala >> csv_to_mysql