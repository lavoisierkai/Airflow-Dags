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
    "start_date": datetime(2021, 3, 25, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    'schedule_interval': '@daily',
}

datebase = 'access_aus_bi'
tableName = "aubi_barcode_mtd_ytd_sale"
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
)
select date_sub(current_date(),1) as stat_date 
	,brand_type, bar_code 
	,brand_id 
	,stat_year
    
	,sum(gmv) as gmv_y
	,sum(amount) as pay_amount_y
	,sum(quantity) as quantity_y
	,count(distinct  user_id ) as buyer_cnt_y
	,count(distinct  order_id ) as order_cnt_y
    
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
group by brand_type, brand_id, bar_code,stat_year
order by brand_type, brand_id, bar_code,stat_year
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