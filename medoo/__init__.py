VERSION = '0.0.2'

class utils(object):

	@staticmethod
	def alwaysList(x):
		"""
		Always return a list
		"""
		from six import string_types
		return [y.strip() for y in x.split(',')] if isinstance(x, string_types) else list(x)

	@staticmethod
	def reduce_datetimes(row):
		"""
		Receives a row, converts datetimes to strings.
		"""
		row = list(row)

		for i, r in enumerate(row):
			if hasattr(r, 'isoformat'):
				row[i] = r.isoformat()
		return tuple(row)

import importlib
from .builder import Raw, Table, Field
from .dialect import Dialect

DATABASE_TYPES = {
	'Sqlite': ['sqlite', 'sqlite3'],
	'Mysql' : 'mysql',
	'Mssql' : 'mssql',
	'Pgsql' : ['pgsql', 'postgres', 'postgresql'],
}

class Medoo(object):

	def __new__(klass, dbtype, *args, **kwargs):

		for key, val in DATABASE_TYPES.items():
			if not isinstance(val, list):
				val = [val]
			if not dbtype.lower() in val:
				continue

			mod   = importlib.import_module('.database.{}'.format(key.lower()), package = 'medoo')
			klass = getattr(mod, key)
			return klass(*args, **kwargs)

		raise ValueError('Database type not supported: {}.'.format(dbtype))


