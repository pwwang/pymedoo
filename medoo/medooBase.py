from collections import OrderedDict
from .medooBuilder import Builder

class Box(OrderedDict):
	def __init__(self, *args, **kwargs):
		super(Box, self).__init__(*args, **kwargs)

	def __repr__(self):
		return 'Box(%s)' % ', '.join(['%s=%s'%(k,v) for k,v in self.items()])

	def __getattr__(self, name):
		if not name.startswith('_OrderedDict'):
			return self[name]
		super(Box, self).__getattr__(name)
		
	def __setattr__(self, name, val):
		if not name.startswith('_OrderedDict'):
			self[name] = val
		else:
			super(Box, self).__setattr__(name, val)

class MedooBase(object):
	
	def __init__(self, *args, **kwargs):
		self.logging = False
		if 'logging' in kwargs:
			self.logging = kwargs['logging']
			del kwargs['logging']
	
		# in case self._connect raises error
		self.connection = None
		self.connection = self._connect(*args, **kwargs)
		self.cursor = self.connection.cursor()
		self.history = []
		self.errors = []
		self.lastid = None
		self.sql = Builder(kwargs['dialect'] if 'dialect' in kwargs else None)
		
	def id (self):
		return self.lastid
		
	def _connect(self, *args, **kwargs):
		raise NotImplementedError('API not implemented.')
		
	def close(self):
		self.connection.close()
		
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
		self.connection.commit()
	
	# If data is an ordered dict, then datas could be tuples
	# otherwise, datas also should be dicts
	def insert(self, table, data, *datas, **kwargs):
		
		values = []
		if isinstance(data, dict):
			self.sql.insert(table, data.keys())
			values.append(data.values())
		else:
			self.sql.insert(table)
			values.append(data)
			
		for data in datas:
			values.append(data.values() if isinstance(data, dict) else data)
		self.sql.values(*values)
		
		sql = self.sql.sql()
		self.sql.clear()
		return self.query(sql, 'commit' in kwargs and kwargs['commit'])
		
			
	def update(self, table, data, where = None, commit = True):
		self.sql.update(table).set(data).where(where)
		sql = self.sql.sql()
		self.sql.clear()
		
		return self.query(sql, commit = commit)
	
	# where required to avoid all data deletion
	def delete(self, table, where, commit = True):
		self.sql.delete(table).where(where)
		sql = self.sql.sql()
		self.sql.clear()
		
		return self.query(sql, commit = commit)		
		
	def select(self, table, columns = '*', where = None, join = None):
		order  = None
		limit  = None
		group  = None
		having = None
		if where and 'ORDER' in where:
			order = where['ORDER']
			del where['ORDER']
		if where and 'LIMIT' in where:
			limit = where['LIMIT']
			del where['LIMIT']
		if where and 'GROUP' in where:
			group = where['GROUP']
			del where['GROUP']
		if where and 'HAVING' in where:
			having = where['HAVING']
			del where['HAVING']
		if join:
			self.sql.select(columns).from_(table).join(join).where(where)
		else:
			self.sql.select(columns).from_(table).where(where)
		if order : self.sql.order(order)
		if limit : self.sql.limit(limit)
		if group : self.sql.group(group)
		if having: self.sql.having(having)
		
		sql = self.sql.sql()
		self.sql.clear()
		
		return self.query(sql)
			
	def tableExists(self, table):
		raise NotImplementedError('API not implemented.')
		
	def create(self, table, fields, drop = True, suffix = ''):
		raise NotImplementedError('API not implemented.')
		
	def drop(self, table):
		raise NotImplementedError('API not implemented.')		
			
	def has(self, table, columns = '*', where = None, join = None):
		rs = self.select(table, columns, where, join)
		if not rs: return False
		return bool(rs.fetchone())
		
	def get(self, table, columns = '*', where = None, join = None):
		rs = self.select(table, columns, where, join)
		if not rs: return None
		return rs.fetchone()
			
	def query(self, sql, commit = True):
		sql = str(sql).strip()
		if self.logging:
			self.history.append(sql)
		else:
			self.history = [sql]
		try:
			self.cursor.execute(sql)
			if commit:
				self.connection.commit()
			if sql.upper().startswith('SELECT'):
				return self.cursor
			else:
				return True
		except Exception as ex:
			self.errors.append(ex)
			return None
			