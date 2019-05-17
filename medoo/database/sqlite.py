import sqlite3
from ..base import Base
from ..dialect import Dialect

class DialectSqlite(Dialect):
	pass

class Sqlite(Base):

	def __init__(self, *args, **kwargs):
		database = kwargs.get('database', None)
		if database is not None:
			if database.startswith('file://'):
				database.replace('file://', '')
			kwargs['database'] = database
		super(Sqlite, self).__init__(*args, **kwargs)
		self.cursor = self.connection.cursor()
		self.dialect(DialectSqlite)

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
		if 'database' not in arguments:
			arguments['database'] = arguments['database_file']
		del arguments['database_file']

		database = arguments['database']
		if database.startswith('file://'):
			database = database.replace('file://', '')
		arguments['database'] = database

		return sqlite3.connect(**arguments)
