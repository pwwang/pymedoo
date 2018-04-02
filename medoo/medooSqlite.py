import sqlite3
from pypika import Table
from .medooBase import MedooBase, MedooRecords

class MedooSqliteRecord(dict):
	
	def __init__(self, record):
		for key in record.keys():
			self[key] = record[key]
			
	def __getattr__(self, key):
		return self[key]
		
	def __setattr__(self, key, val):
		self[key] = val

class MedooSqlite(MedooBase):

	def __init__(self, *args, **kwargs):
		super(MedooSqlite, self).__init__(*args, **kwargs)
		self.connection.row_factory = lambda cursor, row: MedooSqliteRecord({
			k[0]:row[i] for i, k in enumerate(cursor.description)
		})
		self.cursor = self.connection.cursor()
	
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
		
	def tableExists(self, table, schema = None):
		return self.has('sqlite_master', None, 'name', {'type': 'table', 'name': table}, schema)
		
	def dropTable(self, table, commit = True, schema = None):
		table = Table(table, schema = schema)
		return self.query('DROP TABLE IF EXISTS %s' % table, commit)

	def createTable(self, table, fields, drop = True, suffix = '', commit = True, schema = None):
		if drop and self.tableExists(table):
			self.dropTable(table, schema)
		
		table = Table(table, schema = schema)
		fieldstr = ', '.join([
			'"%s" %s' % (k, v) for k,v in fields.items()
		])
		sql = 'CREATE TABLE IF NOT EXISTS %s (%s) %s' % (table, fieldstr, suffix)
		return self.query(sql, commit)
		
		