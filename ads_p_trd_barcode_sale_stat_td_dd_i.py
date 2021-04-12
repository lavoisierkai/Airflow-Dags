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

datebase = 'access_ads'
tableName = "ads_p_trd_barcode_sale_stat_td_dd_i"
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
SELECT
    bs.dt as `date`,
    dd.week_of_year_long_cn as `week`,
    brand_id,
    brand_name,
    barcode,
    bg.goods_name as product,
    bg.english_name as product_en,
    goods_type_desc as goods_type,
    level_by_order_desc as `level`,
    CASE
        when is_day_platform_env_new_buy = 1 then 'New'
        else 'Repurchase' end as customer_type,
    pmt_gmv_td as gmv,
    pmt_goods_cnt_coll_td as unit_sold,
    pmt_user_cnt_td as customer_cnt,
    pmt_order_cnt_td as order_cnt,
    pmt_goods_cnt_coll_td as sku_cnt,
    pct_td as ACS,
    pupo_td as ACO,
    pupg_td as AUS,
    pot_td as AOV,
    pgt_td as AGV
FROM
    {datebase}.{tableName} bs
LEFT JOIN access_cdm.dim_date_new dd on
    bs.dt = dd.date_formatted
LEFT JOIN access_ods.ods_tb_base_goods_dh_f bg on
    bg.goods_code = bs.barcode
where
    bs.dt >= to_date(trunc(now(),"Y"))
    and bg.dt = to_date(date_sub(now(),1))
    and bg.del_flag = 0
    and brand_id != 0
    and order_platform = 0
    and order_env = 0
    and is_day_platform_env_new_buy != 0
    and brand_type = 1
    and goods_type not in (0,
    2)
    and level_by_order != 0
    and barcode in ('9354693000443',
    '20200909001',
    '0784672193900',
    '9351813001083',
    '9312628060047',
    '9351909000297',
    '9312628013432',
    '9319629102691',
    '0784672193610',
    '638845989299',
    '9421031781286',
    '4044518510015',
    '9319629105722',
    '9354731000015',
    '9351813001137',
    '8436559992954',
    '9319629743566',
    '4044518510022',
    '8436547722846',
    '9351813001151',
    '9421031781187',
    '9310297016808',
    '9421031780166',
    '9322111143140',
    '638845989244',
    '9421031781088',
    '9351909000006',
    '9314057011898',
    '638845989282',
    '9421031780982',
    '9319629742439',
    '9353801000733',
    '9354731000107')
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