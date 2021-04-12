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
    'schedule_interval': '@daily',
}

datebase = 'access_ads'
tableName = "ads_brand_user_vtn_visit_month_dd_f"
mysql_method = 'replace' #{‘fail’, ‘replace’, ‘append’}, default ‘fail’
dt=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
# print(datetime.today().strftime("%Y-%m-%d"))

dag = DAG(
    "{datebase}.xxxxxx".format(datebase=datebase),
    description="{datebase}.{tableName}_AGG".format(datebase=datebase,tableName=tableName),
    schedule_interval=schedule_interval1,
    default_args=default_args,
    # catchup=False,
)

querry = '''
SELECT
    dt as `date`,
    platform_type,
    CASE
    when login_type = 'all' then '0'
    when login_type = '0' then '2'
    else login_type 
    end as is_new,
    CASE
    when `level` = 99 then 0
    else `level` 
    end as `level` ,
    brand_id,
    SUM(goods_uv) as uv,
    SUM(addcart_user_cnt) as addcart_user_cnt,
    SUM(addbuy_user_cnt) as addbuy_user_cnt,
    SUM(buynow_user_cnt) as buynow_user_cnt,
    SUM(pay_suc_cnt) as pay_suc_cnt 
FROM
    {datebase}.{tableName}
WHERE
    brand_id in (SELECT DISTINCT brand_id
FROM access_cdm.dim_brand_property_dd_f
WHERE dt=to_date(date_sub(now(), 1) )
        AND brand_type_desc ='自有品牌' )
GROUP BY
    dt,
    platform_type,
    brand_id,
    is_new,
    `level`
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