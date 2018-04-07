import sqlite3
from .medooBuilder import Dialect, Table, Builder
from .medooBase import MedooBase, Box

class DialectSqlite(Dialect):
	
	pass

class MedooSqlite(MedooBase):

	def __init__(self, *args, **kwargs):
		super(MedooSqlite, self).__init__(*args, **kwargs)
		self.connection.row_factory = lambda cursor, row: Box({
			k[0]:row[i] for i, k in enumerate(cursor.description)
		})
		self.cursor = self.connection.cursor()
		self.sql    = Builder(dialect = DialectSqlite)
	
	def _connect(self, *args, **kwargs):
		arguments = {
			'database_file'    : ':memory:',
			'timeout'          : 5.0,
			'detect_types'     : 0,
			'isolation_level'  : None,
			'check_same_thread': False,
			#'factory'          : [str][0],
			'cached_statements': 100
		}
		arguments.update(kwargs)
		arguments['database'] = arguments['database_file']
		del arguments['database_file']
		return sqlite3.connect(**arguments)
		
	def tableExists(self, table):
		return self.has('sqlite_master', 'name', {'type': 'table', 'name': table})
		
	def drop(self, table, commit = True):
		table = Table(table)
		return self.query('DROP TABLE IF EXISTS %s' % table, commit)

	def create(self, table, fields, ifnotexists = True, suffix = '', commit = True):
		self.sql.create(table, fields, ifnotexists, suffix)
		sql = self.sql.sql()
		self.sql.clear()
		return self.query(sql, commit)
		
		