#coding=utf-8
'''offical excutor, donnot delete '''
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta, date


class ComparetoDT(object):
	def __init__(self):
		None

	def compare_to_dt(self, dt, datebase, tableName):
		mydb = mysql.connector.connect(
		  host="localhost",
		  user="root",
		  password="*******"
		)
		mycursor = mydb.cursor()
		mycursor.execute("SELECT max(dt) FROM {datebase}.{tableName}".format(datebase=datebase,tableName=tableName))
		myresult = mycursor.fetchall()
		mysql_dt = myresult[0][0]

		#mysql dt
		print(mysql_dt)

		#log result dt
		# value = pd.read_csv("/root/airflow/results/{dt}*{datebase}.{tableName}.csv".format(dt=dt,datebase=datebase,tableName=tableName), encoding = "utf-8")
		# csv_dt = value["dt"].max()

		# print(csv_dt)
		# print(value["dt"].max() == mysql_dt)


		#correct dt
		dt=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
		print(dt)

		if dt > mysql_dt:
			print("Missing dt from MySQL_dt for {days}".format(days=abs((datetime.strptime(dt, "%Y-%m-%d")-datetime.strptime(mysql_dt, "%Y-%m-%d"))).days))
			# print("Missing dt from csv_dt for {days}".format(days=abs((datetime.strptime(dt, "%Y-%m-%d")-datetime.strptime(csv_dt, "%Y-%m-%d"))).days))
			return "get_impala"
		else:
			print("Already the updated dt {dt}".format(dt=mysql_dt))
			return "No_need_to_update"
