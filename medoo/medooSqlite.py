import sqlite3
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
		
	def tableExists(self, table):
		return self.has('sqlite_master', None, 'name', {'type': 'table', 'name': table})
		
	def dropTable(self, table):
		return self.query('DROP TABLE IF EXISTS "%s"' % table)

	def createTable(self, table, schema, drop = True, suffix = ''):
		if drop and self.tableExists(table):
			self.dropTable()
			
		fields = ', '.join([
			'"%s" %s' % (k, v) for k,v in schema.items()
		])
		sql = 'CREATE TABLE IF NOT EXISTS "%s" (%s) %s' % (table, fields, suffix)
		return self.query(sql)
		
		