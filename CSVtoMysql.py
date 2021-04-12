# coding=utf-8
'''offical excutor, donnot delete '''
import pymysql
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
type_conversion = {'bar_code': str,
                   'barcode': str}


class CSVtoMysql(object):
    def __init__(self):
        None

    def csv_to_mysql(self, dt, datebase, tableName, mysql_method):
        value = pd.read_csv("/root/airflow/results/{dt}/{datebase}.{tableName}.csv".format(
            dt=dt, datebase=datebase, tableName=tableName), encoding="utf-8", dtype=type_conversion)
        sqlEngine = create_engine('mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_datebase}'.format(
            mysql_user="root", mysql_password='*******', mysql_host='localhost', mysql_datebase=datebase), pool_recycle=3600)
        dbConnection = sqlEngine.connect()
        try:
            value.to_sql(tableName, dbConnection,
                         if_exists=mysql_method, index=False)
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
        else:
            print("Table %s created successfully." % tableName)
