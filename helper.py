import mysql.connector
from mysql.connector.constants import ClientFlag

class MySqlHelper(object):
	_username 	= ""
	_password 	= ""
	_host 		= ""
	_database 	= ""

	def __init__(self, username, password, host, database):
		self._username = username
		self._password = password
		self._host = host
		self._database = database

	def select_query(self, query):
		# Executes a query against the current database and returns the results
		cursor = self._cnx.cursor()
		cursor.execute(query)
		results	= []
		for row in cursor:
			results.append(row)

		self._cnx.commit()
		cursor.close()

		return results

	def execute_query(self, query, data=''):
		self._cnx.autocommit = False
		
		self._cursor = self._cnx.cursor()
		self._cursor.execute(query, data)
		
		return self._cursor

	def connect(self):
		flags = [ClientFlag.MULTI_STATEMENTS]
		self._cnx = mysql.connector.connect(user=self._username,  password=self._password, host=self._host, database=self._database, client_flags=flags)
		return self._cnx

	def commit(self):
			self._cnx.commit()
			self._cursor.close()
			self._cnx.close()   
			
	def close(self):
			self._cnx.close() 