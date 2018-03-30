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

		
		
		