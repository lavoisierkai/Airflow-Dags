#coding=utf-8
from ImpalatoCSV import *
from CSVtoMysql import *
from ComparetoDT import *
from CheckTableExists import *
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

datebase = 'analysis'
tableName = "promotion_activities"
mysql_method = 'replace' #{‘fail’, ‘replace’, ‘append’}, default ‘fail’
dt=dt

dag = DAG(
    "{datebase}.{tableName}".format(datebase=datebase,tableName=tableName),
    description="{datebase}.{tableName}_FULL".format(datebase=datebase,tableName=tableName),
    schedule_interval=schedule_interval1,
    default_args=default_args,
    # catchup=False,
)

querry = '''
SELECT * FROM {datebase}.{tableName}
'''.format(datebase=datebase,tableName=tableName)


get_impala = Get_impala()
csv_to_mysql = CSVtoMysql()
compare_to_dt = ComparetoDT()
checktableexistswithoutdt = CheckTableExists()

check_table_exists = BranchPythonOperator(
    task_id="check_table_exists", 
    python_callable=checktableexistswithoutdt.check_table_exists, 
    op_kwargs={'tableName': tableName}, 
    provide_context=True, 
    dag=dag)

"""
For table has no dt column, need to skip compare dt part, and the document will replace the table in MySQL directly
"""
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