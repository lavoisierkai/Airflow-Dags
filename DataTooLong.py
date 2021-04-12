#coding=utf-8
'''offical excutor, donnot delete '''
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta, date


class datatolong(object):
	def __init__(self):
		None

	def data_too_long(self, dt, datebase, tableName):
		mydb = mysql.connector.connect(
		  host="localhost",
		  user="root",
		  password="Acn@2021!"
		)
		mycursor = mydb.cursor()
		mycursor.execute("SELECT max(dt) FROM {datebase}.{tableName}".format(datebase=datebase,tableName=tableName))
		myresult = mycursor.fetchall()
		mysql_dt = myresult[0][0]