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
    "start_date": datetime(2021, 3, 23, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    'schedule_interval': '@weekly',
}

datebase = 'access_aus_bi'
tableName = "aubi_barcode_sale_newold_rp_mm_f"
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
with od as (
	select *
		,if((DENSE_RANK() OVER(PARTITION by order_id order by order_goods_amount desc))=1 and is_first_order=1,1,0) as is_first_barcode_order_strict
	from( 
	    select order_id 
	        ,id_code -- userid
	        , bar_code 
	        ,gmv
	        ,order_goods_amount
	        ,count_coll
	        ,is_buy_refund_same_month
	        ,brand_type
	        ,created_at
	        --,if(is_first_order='Y',1,0) as is_first_order 
	        ,IF(created_at = (min(created_at) OVER(PARTITION BY  id_code)),1,0) AS is_first_order
	        --,if((DENSE_RANK() OVER(PARTITION by order_id order by order_goods_amount desc))=1 and is_first_order='Y',1,0) as is_first_barcode_order_strict
	        --,(min(created_at) over(PARTITION BY bar_code,id_code)) as first_barcode_order
	        ,IF(created_at = (min(created_at) OVER(PARTITION BY bar_code,id_code)),1,0) AS is_first_barcode_order
	        --,if(is_first_barcode_order='Y',1,0) as is_first_sku_order
	    from access_cdm.dwd_trade_order_goods_dd_f  
	    where  dt =date_sub(current_date(),1) --and t1.dt=date_sub(current_date(),1)
	        and  paid_at is not null and  is_free_goods='N' and  is_zero_amount='N' and  brand_id not in (7,130) 
      )tmp
),
user_base as (
    select from_timestamp(created_at,'yyyy-MM')  as stat_date
        ,from_timestamp(add_months(created_at,1),'yyyy-MM')  as rp_month
        ,id_code
        ,bar_code
        --,cate_id
        --,brand_id
        ,max(is_first_order) as is_first_order -- first vtn order by barcode
        ,max(is_first_barcode_order) as is_first_barcode_order
        ,max(is_first_barcode_order_strict) as is_first_barcode_order_strict
        ,count(distinct order_id) as order_cnt
        ,sum(gmv) as gmv
        ,sum(count_coll) as quantity
        ,sum(order_goods_amount) as paid_amount
        ,sum(if(is_buy_refund_same_month='Y',gmv,0)) as refund_gmv
    from od 
    where created_at>'2019-12' and  brand_type = 1 
    group by from_timestamp(created_at,'yyyy-MM') 
        ,from_timestamp(add_months(created_at,1),'yyyy-MM') 
        ,id_code
        ,bar_code
        --,cate_id
        --,brand_id
),
vtn_u_base as (
	select DISTINCT id_code, stat_date --, 1 as vtn_re_purchase
	from user_base
),
vtn_new as (
	select stat_date,count(distinct id_code) as vtn_new_byer
	from user_base 
	where is_first_order=1
	group by stat_date
),
user_smr as(
    select t1.* 
        ,if(t3.id_code is not null,1,0) as vtn_re_purchase
        ,if(t2.id_code is not null,1,0) as re_purchase
        ,t2.gmv AS rp_gmv
        ,t2.order_cnt AS rp_order_cnt
        ,t2.quantity AS rp_quantity
        ,t2.paid_amount AS rp_paid_amount
    from user_base t1
    left outer join user_base t2
        on t1.id_code=t2.id_code
        and t1.bar_code=t2.bar_code
        --and t1.cate_id=t2.cate_id
        --and t1.brand_id=t2.brand_id
        and t1.rp_month=t2.stat_date
    left outer join vtn_u_base t3 
    	on t1.id_code=t3.id_code
    	and t1.rp_month=t3.stat_date
),
smr as (
    select stat_date,rp_month
        ,bar_code
        --,cate_id 
        --,brand_id
        ,sum(gmv) as gmv
        ,sum(1) as buyer_cnt
        ,sum(order_cnt) as order_cnt
        ,sum(quantity) as quantity
        ,sum(paid_amount) as paid_amount
        ,sum(refund_gmv) as refund_gmv
		
        ,sum(gmv*is_first_barcode_order) as first_byr_gmv
        ,sum(is_first_barcode_order) as first_byr_cnt
        ,sum(is_first_order) as vtn_first_byr_cnt
        ,sum(is_first_barcode_order_strict) as vtn_new_mostly_led_by_barcode
        
        ,sum(vtn_re_purchase) as vtn_rp_buyer_cnt
        ,sum(rp_gmv*re_purchase) as rp_gmv
        ,sum(re_purchase) as rp_buyer_cnt
        ,sum(rp_order_cnt*re_purchase) as rp_order_cnt
        ,sum(rp_quantity*re_purchase) as rp_quantity
        ,sum(rp_paid_amount*re_purchase) as rp_paid_amount 
    from user_smr 
    group by stat_date,rp_month , bar_code
        --,cate_id 
        --,brand_id
)
select t1.*  
    ,rp_buyer_cnt/buyer_cnt as rp_rate
    ,t4.vtn_new_byer 
from smr t1
left outer join vtn_new as t4 on t1.stat_date=t4.stat_date
where t1.stat_date>='2020'   
'''.format(datebase=datebase,tableName=tableName)

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