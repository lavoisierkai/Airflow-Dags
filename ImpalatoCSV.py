# coding=utf-8
'''offical excutor, donnot delete '''
import os
import impala
import pandas as pd
import numpy as np
from impala.dbapi import connect
from pandas.io.sql import pandasSQL_builder  # replacement of read_sql


class Get_impala(object):
    def __init__(self):
        None

    def get_impala(self, querry, dt, datebase, tableName):
        with connect(host='localhost', database='*******',
                     port=21050, user='******', password='******',
                     auth_mechanism='NOSASL') as conn:
            with conn.cursor(user='*******') as cur:
                # this will get authorization without password!!!!
                # try change user='hufenggang' !!!
                pandas_sql = pandasSQL_builder(cur, is_cursor=True)
                df = pandas_sql.read_query(querry)
        path = f"/root/airflow/results/{dt}"
        os.makedirs(path, exist_ok=True)
        df.to_csv(f"{path}/{datebase}.{tableName}.csv",
                  encoding="utf-8-sig", index=False, mode='w+')
        print("csv exported")
        # return df
