# expose Field, so it can be used as value in update set
from .medooBase import Field 
from .medooSqlite import MedooSqlite
from .medooException import MedooInitializationError, MedooTableParseError, MedooFieldParseError, MedooWhereParseError

DATABASE_TYPES = {
	'MedooSqlite': ['sqlite', 'sqlite3']
}

class Medoo(object):
	
	def __new__(klass, *args, **kwargs):
		if 'database_type' not in kwargs:
			raise MedooInitializationError('No database type specified.')
			
		dbtype = kwargs['database_type']
		del kwargs['database_type']
		if dbtype not in [dbtype for dblist in DATABASE_TYPES.values() for dbtype in dblist]:
			raise MedooInitializationError('Database type not supported: %s.' % dbtype)
			
		if dbtype in DATABASE_TYPES['MedooSqlite']:
			return MedooSqlite(*args, **kwargs)