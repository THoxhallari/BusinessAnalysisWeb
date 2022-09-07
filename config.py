import psycopg2
import psycopg2.extras
from pyodbc import OperationalError
import pyodbc
import pandas as pd
import streamlit as st


class DBPostgres():
	def __init__(self):
		self.config = None
		self.connection = None

	def initialize(self, config):
		self.config = config
		self.connect()

	def connect(self):
		try:

			self.connection = pyodbc.connect(
				"DRIVER={ODBC Driver 17 for SQL Server};SERVER="
				+ self.config['host']
				+ ";DATABASE="
				+ self.config['db_name']
				+ ";UID="
				+ self.config['user']
				+ ";PWD="
				+ self.config['password']
			)

			return {
				"message": "Connected succesfully to {}".format(self.config['host']),
				"status": True
			}
		except OperationalError as err:
			return {
				"message": err.args[0],
				"status": False
			}

	def get_data(self, query):
		df = pd.read_sql(query, self.connection)
		return df

	def close(self):
		self.connection.close()

	def execute(self, query, values):
		cursor = self.connection.cursor()
		lens = len(values[0])
		args_str = ','.join(cursor.mogrify(
			"(" + ",".join(["%s"] * lens) + ")", x).decode("utf-8") for x in values)
		args_str = args_str.replace("''", "NULL").replace("'NaN'", "NULL").replace("'NaT'", "NULL")
		try:
			print("Inserting data...")
			cursor.execute(query + args_str)
			self.connection.commit()
			print("All right")
		except Exception as e:
			print("Something went wrong")
			print(e)
