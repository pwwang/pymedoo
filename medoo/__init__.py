# expose Field, so it can be used as value in update set
from .medooBuilder import Builder, Function, Raw, Field, Table
from .medooSqlite import MedooSqlite
from .medooBase import Box

DATABASE_TYPES = {
	'MedooSqlite': ['sqlite', 'sqlite3']
}

class Medoo(object):
	
	def __new__(klass, *args, **kwargs):
		if 'database_type' not in kwargs:
			raise ValueError('No database type specified.')
		
		dbtype = kwargs['database_type']
		del kwargs['database_type']
		if dbtype not in [dbtype for dblist in DATABASE_TYPES.values() for dbtype in dblist]:
			raise ValueError('Database type not supported: %s.' % dbtype)
			
		if dbtype in DATABASE_TYPES['MedooSqlite']:
			return MedooSqlite(*args, **kwargs)