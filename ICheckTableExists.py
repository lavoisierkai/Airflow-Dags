#coding=utf-8
'''offical excutor, donnot delete '''
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta, date


class ICheckTableExists(object):
	def __init__(self):
		None

	def icheck_table_exists(tableName):
		print("-----------------------tableName-----------------")
		print(tableName)
		mydb = mysql.connector.connect(
		  host="localhost",
		  user="root",
		  password="*******"
		)
		mycursor = mydb.cursor()
		querry = '''
		SELECT * FROM information_schema.tables where TABLE_NAME = '{tableName}'
		'''.format(tableName=tableName)
		mycursor.execute(querry)
		myresult = mycursor.fetchall()
		if len(myresult) < 1:
			print("Table not exists, need to create a new table")
			return 'get_impala'
		elif len(myresult) == 1:
			print("Table exists, need to compare with mysql dt")
			return 'No_need_to_update'