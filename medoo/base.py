from .builder import Builder
from .record import Records
from .dialect import Dialect

class Base(object):

	def __init__(self, *args, **kwargs):
		if 'logging' in kwargs:
			self.logging = kwargs['logging']
			del kwargs['logging']
		else:
			self.logging = False

		self._dialect = None
		if 'dialect' in kwargs:
			self._dialect = kwargs['dialect']
			self.dialect(kwargs['dialect'])
			del kwargs['dialect']

		# in case self._connect raises error
		self.connection = None
		self.connection = self._connect(*args, **kwargs)
		self.cursor     = self.connection.cursor()
		self.history    = []
		self.errors     = []
		self.sql        = None

	@property
	def builder(self):
		return Builder(dialect = self._dialect)

	def id (self):
		return self.cursor.lastrowid

	def _connect(self, *args, **kwargs):
		raise NotImplementedError('API not implemented.')

	def close(self):
		self.connection.close()

	def dialect(self, dial = None):
		dial = dial or Dialect
		self._dialect   = dial
		Builder.DIALECT = dial

	def __del__(self):
		if self.connection:
			self.close()

	def last(self):
		return self.history[-1] if self.history else ''

	def log(self):
		return self.history

	def error(self):
		return self.errors

	def commit(self):
		try:
			self.connection.commit()
		except Exception as ex:
			self.connection.rollback()
			raise ex

	# If data is an ordered dict, then datas could be tuples
	# otherwise, datas also should be dicts
	def insert(self, table, fields, *values, **kwargs):
		sql = self.builder.insert(table, fields, *values)
		return self.query(sql, kwargs.get('commit', True))

	def update(self, table, data, where = None, commit = True):
		sql = self.builder.update(table, data, where)
		return self.query(sql, commit = commit)

	# where required to avoid all data deletion
	def delete(self, table, where, commit = True):
		sql = self.builder.delete(table, where)
		return self.query(sql, commit = commit)

	def select(self, table, columns = '*', where = None, join = None, distinct = False, newtable = None, sub = None, commit = False, readonly = True):
		sql = self.builder.select(table, columns, where, join, distinct, newtable, sub)
		return self.query(sql, commit, readonly)

	def union(self, *queries, **kwargs):
		sql = self.builder.union(*queries)
		return self.query(sql, commit = kwargs.get('commit', False))

	def has(self, table, where = None, join = None, distinct = False):
		rs = self.select(table, '*', where, join)
		return bool(rs.first())

	def get(self, table, columns = '*', where = None, join = None):
		rs = self.select(table, columns, where, join)
		return rs.first()[0]

	def query(self, sql, commit = True, readonly = True):
		self.sql = ('%s' % sql).strip()
		if self.logging:
			self.history.append(self.sql)
		else:
			self.history = [self.sql]
		try:
			self.cursor.execute(self.sql)
			if commit:
				self.commit()
			if self.sql.upper().startswith('SELECT'):
				return Records(self.cursor, readonly)
			else:
				return True
		except Exception as ex:
			self.errors.append(str(ex))
			raise type(ex)(str(ex) + ':\n' + self.sql)
