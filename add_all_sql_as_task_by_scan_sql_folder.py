# coding=utf-8
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
import os
import yaml
from schedule_timetable import dt
import logging

# print will be logged as INFO by airflow
print('*******************************************************************************************')
print('********************************* Start scanning SQL folder *******************************')
print('*******************************************************************************************')

local_tz = pendulum.timezone("Australia/Sydney")
sql_path = '/root/airflow/dags/SQL/'

for sql_filename in os.listdir(sql_path):
    try:
        sql_file_full_path = os.path.join(sql_path, sql_filename)
        print(f'---------------- Start adding {sql_file_full_path} into DAGs ---------------------------')
        with open(sql_file_full_path, 'r') as f:
            sql = f.read()
        mdate = datetime.fromtimestamp(os.path.getmtime(sql_file_full_path))
        params_string = sql.split("/* ----- params start -----")[1]
        params_string = params_string.split("----- params end ----- */")[0]
        # print(params_string)
        params = yaml.load(params_string)
        default_args = {
            "owner": "airflow",
            "depends_on_past": False,
            # use sql modify date as start date default value
            "start_date": params.get('start_date', mdate).replace(tzinfo=local_tz),
            "retries": 1,
            "retry_delay": timedelta(minutes=2),
            # 'schedule_interval': '@daily',
        }
        datebase = params.get('database', 'access_aus_bi')
        tableName = params['table_name']
        mysql_method = params.get('if_exist', 'replace')
        dag_id = f"{datebase}.{tableName}"
        # {‘fail’, ‘replace’, ‘append’}, default ‘fail’
        dag = DAG(
            dag_id,
            description=f"{datebase}.{tableName}_ACG scanning SQL",
            schedule_interval=params.get('schedule_interval1', "*/30 8 * * *"),
            default_args=default_args
            # catchup=False,
        )

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

        No_need_to_update = DummyOperator(
            task_id="No_need_to_update",
            trigger_rule="none_failed",
            dag=dag
        )

        get_impala = PythonOperator(
            task_id="get_impala",
            python_callable=get_impala.get_impala,
            op_kwargs={'querry': sql, 'dt': dt,
                    'datebase': datebase, 'tableName': tableName},
            provide_context=True,
            # depends_on_past=True,
            trigger_rule="none_failed",
            dag=dag)

        csv_to_mysql = PythonOperator(
            task_id="csv_to_mysql",
            python_callable=csv_to_mysql.csv_to_mysql,
            op_kwargs={'dt': dt, 'datebase': datebase,
                    'tableName': tableName, 'mysql_method': mysql_method},
            provide_context=True,
            # trigger_rule = "all_success",
            dag=dag)

        check_table_exists >> [No_need_to_update, get_impala]
        # compare_to_dt >> [get_impala, No_need_to_update]
        get_impala >> csv_to_mysql
        globals()[dag_id] = dag
    except Exception:
        logging.exception(f"Exception during add {sql_file_full_path}:")
        print(f'---------------- Failed in adding {sql_file_full_path} into DAGs ---------------------------')
        continue
    else:
        print(f'---------------- Successfull add {sql_file_full_path} into DAGs ---------------------------')
