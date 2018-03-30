import re, json
from pypika import Query, Table, Field, JoinType, functions
from .medooException import MedooNameParseError, MedooWhereParseError, MedooJoinParseError, MedooInsertParseError, MedooUpdateParseError, MedooDeleteParseError, MedooCountColumnParseError

class MedooParser(object):
	
	@staticmethod
	def alias(name):
		if not name.endswith(')'):
			return name.strip(), ''
		m = re.match(r'^\s*([\w*]+)\s*\((\w+)\)\s*$', name)
		if not m: 
			raise MedooNameParseError('Cannot parse alias for "%s"' % name)
		return m.group(1), m.group(2)
		
	@staticmethod
	def whereExpr(table, name, val):
		if val is True:
			m = re.match(r'^([\w.]+)\[(REGEXP|[!><=~]+)\]([\w.]+)$', name)
			if not m:
				raise MedooWhereParseError('Don\'t understand where key: "%s".' % name)
			name = m.group(1)
			rel  = m.group(2)
			val  = m.group(3)
			if '.' in val:
				t, v = val.split('.')
				val = getattr(Table(t), v)
			else:
				val = getattr(table, val)
		elif not name.endswith(']'):
			rel = ''
		else:
			m = re.match(r'^([\w.]+)\[(REGEXP|[!><=~]+)\]$', name)
			if not m:
				raise MedooWhereParseError('Don\'t understand where key: "%s".' % name)
			name = m.group(1)
			rel  = m.group(2)
			
		if '.' in name:
			t, n = name.split('.')
			left = getattr(Table(t), n)
		else:
			left = getattr(table, name)
		if not rel:
			if isinstance(val, (list, tuple)):
				return left.isin(list(val))
			else:
				return left == val
		if rel == '!':
			if isinstance(val, (list, tuple)):
				return left.notin(list(val))
			else:
				return left != val
		if rel == '>':
			return left > val
		if rel == '<':
			return left < val
		if rel == '>=':
			return left >= val
		if rel == '<=':
			return left <= val
		if rel == '<>':
			return left[val[0]:val[1]]
		if rel == '><':
			return (not left[val[0]:val[1]])
		if rel == '~':
			if not val.startswith('%') and not val.endswith('%'):
				val = '%{}%'.format(val)
			return left.like(val)
		if rel == '!~':
			if not val.startswith('%') and not val.endswith('%'):
				val = '%{}%'.format(val)
			return left.not_like(val)
		raise MedooWhereParseError('Do not understand the relation: "%s".' % rel)
		
	@staticmethod
	def where(table, key, value = None):
		if isinstance(key, dict):
			if len(key) == 1:
				return MedooParser.where(table, *key.items()[0])
			else:
				return MedooParser.where(table, 'AND', key)
		elif key.startswith('AND'):
			if not isinstance(value, dict) or len(value) < 2:
				raise MedooWhereParseError('Expect a len > 1 dict for AND clause.')
			k, v = value.items()[0]
			del value[k]
			expr = MedooParser.where(table, k, v)
			for k, v in value.items():
				expr &= MedooParser.where(table, k, v)
			return expr
		elif key.startswith('OR'):
			if not isinstance(value, dict) or len(value) < 2:
				raise MedooWhereParseError('Expect a len > 1 dict for OR clause.')
			k, v = value.items()[0]
			del value[k]
			expr = MedooParser.where(table, k, v)
			for k, v in value.items():
				expr |= MedooParser.where(table, k, v)
			return expr	
		else:
			return MedooParser.whereExpr(table, key, value)
			
	@staticmethod
	def jointable(tablename):
		if not tablename.startswith('['):
			return MedooParser.alias(tablename) + (JoinType.full_outer,)
		m = re.match(r'^\[([<>]+)\](\w+)$', tablename)
		if not m:
			raise MedooJoinParseError('Do not understand join table: "%s"' % tablename)
		jtype  = m.group(1)
		jtable = m.group(2)
		table  = MedooParser.alias(jtable)
		if jtype == '>':
			return table + (JoinType.left, )
		if jtype == '<':
			return table + (JoinType.right, )
		if jtype == '<>':
			return table + (JoinType.outer, )
		if jtype == '><':
			return table + (JoinType.inner, )
		raise MedooJoinParseError('Do not understand join type: "%s"' % jtype)
		
	@staticmethod
	def updateTerm(key, val):
		if not key.endswith(']'):
			return key, val
		m = re.match(r'^(\w+)\[(JSON|[-+*/])\]$', key)
		if not m:
			raise MedooUpdateParseError('Do not understand the key in update data: "%s"' % key)
		field = m.group(1)
		utype = m.group(2)
		if utype == 'JSON':
			return field, json.dumps(val)
		if utype == '+':
			return field, Field(field) + val
		if utype == '-':
			return field, Field(field) - val
		if utype == '*':
			return field, Field(field) * val
		if utype == '/':
			return field, Field(field) / val
		raise MedooUpdateParseError('Do not understand update operation in update data: "%s"' % utype)
		
	@staticmethod
	def countColumns(columns, distinct):
		if isinstance(columns, list):
			if len(columns) > 1:
				raise MedooCountColumnParseError('Multiple columns not allowed for count statement: "%s"' % columns)
			columns = columns[0]
		col, alias = MedooParser.alias(columns)
		if distinct and col == '*':
			raise MedooCountColumnParseError('Need specific column name for distinct: "%s"' % columns)
		ret = functions.Count(Field(col))
		if alias:
			ret = ret.as_(alias)
		if distinct:
			ret = ret.distinct()
		return ret		
	
class MedooRecords(object):
	
	def __init__(self, cursor):
		self.cursor = cursor
		self.recordClass = None
		
	def next(self):
		return self.recordClass(self.cursor.fetchone()) if self.recordClass else self.cursor.fetchone()
		
	def __iter__(self):
		return iter(self)
		
	def all(self):
		return [r for r in self]

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
		self.recordsClass = MedooRecords
		self.sql = '' # last query
		self.history = []
		self.errors = []
		self.lastid = None
		
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
		return self.sql
		
	def error(self):
		return self.errors
		
	def insert(self, table, data, *datas, **kwargs):
		table_org, table_as = MedooParser.alias(table)
		if table_as: 
			raise MedooInsertParseError('No alias allowed for insert statement: "%s"' % table)
		
		data = [data]
		if datas: data += list(datas)
		table = Table(table_org)
		q = Query.into(table)
		values = [tuple(d.values()) if isinstance(d, dict) else d for d in data]
		q = q.columns(*data[0].keys()).insert(*values)
		self.sql = str(q)
		if self.logging:
			self.history.append(self.sql)
		else:
			self.history = [self.sql]
		try:
			self.cursor.execute(self.sql)
			if 'commit' in kwargs and kwargs['commit']: 
				self.connection.commit()
			return True
		except Exception as ex:
			self.errors.append(ex)
			return False
			
	def update(self, table, data, where = None, commit = True):
		table_org, table_as = MedooParser.alias(table)
		if table_as: 
			raise MedooUpdateParseError('No alias allowed for update statement: "%s"' % table)
			
		table = Table(table_org)
		q = Query.update(table)
		for key, val in data.items():
			q = q.set(*MedooParser.updateTerm(key, val))
		if where:
			q = q.where(MedooParser.where(table, where))
		
		self.sql = str(q)
		if self.logging:
			self.history.append(self.sql)
		else:
			self.history = [self.sql]
		try:
			self.cursor.execute(self.sql)
			if commit:
				self.connection.commit()
			return True
		except Exception as ex:
			self.errors.append(ex)
			return False
			
	# where required to avoid all data deletion
	def delete(self, table, where, commit = True):
		table_org, table_as = MedooParser.alias(table)
		if table_as: 
			raise MedooDeleteParseError('No alias allowed for delete statement: "%s"' % table)
		table = Table(table_org)
		q = Query.from_(table).where(MedooParser.where(table, where)).delete()
		
		self.sql = str(q)
		if self.logging:
			self.history.append(self.sql)
		else:
			self.history = [self.sql]
		try:
			self.cursor.execute(self.sql)
			if commit:
				self.connection.commit()
			return True
		except Exception as ex:
			self.errors.append(ex)
			return False
		
		
	def select(self, table, join = None, columns = '*', where = None):
		table_org, table_as = MedooParser.alias(table)
		table = Table(table_org)
		q = Query.from_(table)
		if table_as:
			q = q.as_(table_as)
			
		if join:
			for key, val in join.items():
				jtable, jt_as, jtype = MedooParser.jointable(key)
				joinon = MedooParser.where(val)
				q = q.join(jtable, how = jtype)
				if jt_as: q = q.as_(jt_as)
				q = q.on(joinon)					
		
		if columns == '*':
			cs = [table.star]
		elif isinstance(columns, list):
			cs = [getattr(table, c) for c in columns]
		else:
			cs = [getattr(table, c.strip()) for c in columns.split(',')]
		q = q.select(*cs)
		
		if where:
			q = q.where(MedooParser.where(table, where))
		
		self.sql = str(q)
		if self.logging:
			self.history.append(self.sql)
		else:
			self.history = [self.sql]
		
		try:
			self.cursor.execute(self.sql)
			return self.recordsClass(self.cursor) if self.recordsClass else self.cursor
		except Exception as ex:
			self.errors.append(ex)
			return None
			
	def tableExists(self, table):
		raise NotImplementedError('API not implemented.')
		
	def createTable(self, table, schema, drop = True, suffix = ''):
		raise NotImplementedError('API not implemented.')
		
	def dropTable(self, table):
		raise NotImplementedError('API not implemented.')		
			
	def has(self, table, join = None, columns = '*', where = None):
		rs = self.select(table, join, columns, where)
		return bool(next(rs))
		
	def get(self, table, join = None, columns = '*', where = None):
		rs = self.select(table, join, columns, where)
		if not rs: return None
		r  = next(rs)
		if not r: return None
		return r.values()[0]
		
	def count(self, table, join = None, columns = '*', where = None, distinct = False):
		table_org, table_as = MedooParser.alias(table)
		table = Table(table_org)
		q = Query.from_(table)
		if table_as:
			q = q.as_(table_as)
			
		if join:
			for key, val in join.items():
				jtable, jt_as, jtype = MedooParser.jointable(key)
				joinon = MedooParser.where(val)
				q = q.join(jtable, how = jtype)
				if jt_as: q = q.as_(jt_as)
				q = q.on(joinon)					
		
		q = q.select(MedooParser.countColumns(columns, distinct))
		
		if where:
			q = q.where(MedooParser.where(table, where))
		
		self.sql = str(q)
		if self.logging:
			self.history.append(self.sql)
		else:
			self.history = [self.sql]
		
		try:
			self.cursor.execute(self.sql)
			rs = self.recordsClass(self.cursor) if self.recordsClass else self.cursor
			if not rs: return None
			return next(rs)
		except Exception as ex:
			self.errors.append(ex)
			return None
			
	def query(self, sql, commit = True):
		self.sql = sql.strip()
		if self.logging:
			self.history.append(self.sql)
		else:
			self.history = [self.sql]
		try:
			self.cursor.execute(self.sql)
			if commit:
				self.connection.commit()
			if self.sql.upper().startswith('SELECT'):
				return self.recordsClass(self.cursor) if self.recordsClass else self.cursor
			else:
				return True
		except Exception as ex:
			self.errors.append(ex)
			return None
			