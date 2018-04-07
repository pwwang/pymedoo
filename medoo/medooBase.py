import re, json
from pypika import Query, Table, JoinType, functions, Field as PkField
from pypika.terms import ArithmeticExpression, BasicCriterion
from pypika.enums import Comparator
from .medooException import MedooFieldParseError, MedooWhereParseError, MedooTableParseError

class Raw(str):
	pass

class Matching(Comparator):
	regexp = ' REGEXP '

class Field(PkField):
	def __init__(self, name, alias = None, table = None, schema = None):
		self.name   = name
		self.alias  = alias
		self.table  = table
		self.schema = schema
		
	def init(self):
		parts = self.name.split('.')
		if len(parts) == 1: 
			parts.insert(0, None)
			parts.insert(0, None)
		elif len(parts) == 2:
			parts.insert(0, None)
		schema, table, name = parts
		
		if self.schema and self.table:
			schema = self.schema
			table  = self.table
			name   = self.name
		elif self.schema:
			schema = self.schema
		elif self.table:
			table  = self.table
		
		import gc
		for obj in gc.get_objects():
			if not isinstance(obj, Table): continue
			if obj.schema != schema: continue
			if table == obj.table_name or table == obj.alias:
				table = obj
				break
		if not isinstance(table, Table):
			table = Table(table, schema = schema)
		super(Field, self).__init__(name, self.alias, table)

class MedooParser(object):
	#                      [<>]                   table           (alias)                   # comment
	REGEXP_TABLE  = r'^\s*(?:\[([<>]{1,2})\])?\s*([a-zA-Z0-9_]+)\s*(?:\(([a-zA-Z0-9_]+)\))?\s*(?:#.*)?$'
	#                      table.field(alias)[operation] # comment
	REGEXP_FIELD  = r'^\s*(?:([a-zA-Z0-9_]+)\.)?([a-zA-Z0-9_]+|\*)\s*(?:\(([a-zA-Z0-9_]+)\))?(?:\[(.+?)\])?\s*(?:#.*)?$'

	@staticmethod
	def table(name):
		m = re.match(MedooParser.REGEXP_TABLE, name)
		if not m:
			raise MedooTableParseError('Cannot understand table: "%s"' % name)
		#      join,       table,      alias
		join = m.group(1)
		if join == '>':
			join = JoinType.left
		elif join == '<':
			join = JoinType.right
		elif join == '<>':
			join = JoinType.outer
		elif join == '><':
			# JoinType.inner == ''
			join = JoinType.inner
		elif not join:
			join = None
		else:
			raise MedooTableParseError('Unknown join type: "%s"' % name)
		return join, m.group(2), m.group(3)
			
	@staticmethod
	def field(name):
		m = re.match(MedooParser.REGEXP_FIELD, name)
		if not m:
			raise MedooFieldParseError('Cannot understand field: "%s"' % name)
		operation = m.group(4)
		if operation:
			operation = '__add__' if operation == '+' else \
						'__sub__' if operation == '-' else \
						'__mul__' if operation == '*' else \
						'__div__' if operation == '/' else \
						'__pow__' if operation == '**' else \
						'__mod__' if operation == 'MOD' else \
						operation
		#      table,       field,      alias,    operation		
		return m.group(1), m.group(2), m.group(3), operation
		
	@staticmethod
	def alwaysList(s):
		if isinstance(s, list):
			return s
		elif isinstance(s, tuple):
			return list(s)
		else:
			return [x.strip() for x in s.split(',')]
			
	@staticmethod
	def likeValue(s):
		return '%{}%'.format(s) if not s.startswith('%') and not s.endswith('%') else s
		
	@staticmethod
	def whereExpr(name, val, tables = None):
		t, field, alias, operation = MedooParser.field(name)
		if alias:
			raise MedooFieldParseError('No alias allowed for field in WHERE clause: "%s"' % name)
		ftable = tables[t] if tables and t and t in tables else Table(t) if t else None
		field = PkField(field, table = ftable)
		
		if isinstance(val, Field):
			val.init()
		elif isinstance(val, ArithmeticExpression):
			if isinstance(val.left, Field):
				val.left.init()
			if isinstance(val.right, Field):
				val.left.init()
				
		if not operation:
			if isinstance(val, (list, tuple)):
				return field.isin(list(set(val)))
			else:
				return field == val
		if operation == '!':
			if isinstance(val, (list, tuple)):
				return field.notin(list(set(val)))
			else:
				return field != val
		if operation == '>':
			return field > val
		if operation == '<':
			return field < val
		if operation == '>=':
			return field >= val
		if operation == '<=':
			return field <= val
		if operation == '<>':
			return field[val[0]:val[1]]
		if operation == '><':
			return field[val[0]:val[1]].negate()
		if operation == '~':
			if isinstance(val, list):
				val = list(set(val))
			if isinstance(val, list) and len(val) == 1:
				val = val[0]
			if isinstance(val, list):
				val0 = val.pop(0)
				ret = field.like(MedooParser.likeValue(val0))
				for v in val: 
					ret |= field.like(MedooParser.likeValue(v))
				return ret
			else:
				return field.like(MedooParser.likeValue(val))
		if operation == '!~':
			if isinstance(val, list):
				val = list(set(val))
			if isinstance(val, list) and len(val) == 1:
				val = val[0]
			if isinstance(val, list):
				val0 = val.pop(0)
				ret = field.not_like(MedooParser.likeValue(val0))
				for v in val: 
					ret &= field.not_like(MedooParser.likeValue(v))
				return ret
			else:
				return field.not_like(MedooParser.likeValue(val))
		if operation == '~~':
			if isinstance(val, list):
				val = list(set(val))
			if isinstance(val, list) and len(val) == 1:
				val = val[0]
			if isinstance(val, list):
				val0 = val.pop(0)
				ret = functions.Upper(field).like(MedooParser.likeValue(val0.upper()))
				for v in val: 
					ret |= functions.Upper(field).like(MedooParser.likeValue(v.upper()))
				return ret
			else:
				return functions.Upper(field).like(MedooParser.likeValue(val.upper()))
		if operation == '!~~':
			if isinstance(val, list):
				val = list(set(val))
			if isinstance(val, list) and len(val) == 1:
				val = val[0]
			if isinstance(val, list):
				val0 = val.pop(0)
				ret = functions.Upper(field).not_like(MedooParser.likeValue(val0.upper()))
				for v in val: 
					ret &= functions.Upper(field).not_like(MedooParser.likeValue(v.upper()))
				return ret
			else:
				return functions.Upper(field).not_like(MedooParser.likeValue(val.upper()))
		if operation == 'REGEXP':
			return BasicCriterion(Matching.regexp, field, field._wrap(val), alias = bool(alias))
		raise MedooWhereParseError('Do not understand the relation: "%s".' % operation)
		
	@staticmethod
	def where(key, value = None, tables = None):
		if isinstance(key, dict):
			if len(key) == 1:
				return MedooParser.where(*key.items()[0], tables = tables)
			else:
				return MedooParser.where('AND', key, tables = tables)
		elif key.startswith('AND'):
			#if not isinstance(value, dict) or len(value) < 2:
			#	raise MedooWhereParseError('Expect a len > 1 dict for AND clause.')
			k, v = value.items()[0]
			del value[k]
			expr = MedooParser.where(k, v, tables = tables)
			for k, v in value.items():
				expr &= MedooParser.where(k, v, tables = tables)
			return expr
		elif key.startswith('OR'):
			#if not isinstance(value, dict) or len(value) < 2:
			#	raise MedooWhereParseError('Expect a len > 1 dict for OR clause.')
			k, v = value.items()[0]
			del value[k]
			expr = MedooParser.where(k, v, tables = tables)
			for k, v in value.items():
				expr |= MedooParser.where(k, v, tables = tables)
			return expr	
		else:
			return MedooParser.whereExpr(key, value, tables)
			
	@staticmethod
	def joinOn(field1, field2, table1, table2):
		tname1, fname1, alias1, operation1 = MedooParser.field(field1)
		tname2, fname2, alias2, operation2 = MedooParser.field(field2)
		if operation1:
			raise MedooFieldParseError('No operation allowed for field in JOIN clause: "%s"' % field1)
		if operation2:
			raise MedooFieldParseError('No operation allowed for field in JOIN clause: "%s"' % field2)
		tname1 = tname1 or table1
		tname2 = tname2 or table2
		#print tname1, tname2
		f1 = PkField(fname1, table = tname1, alias = alias1)
		f2 = PkField(fname2, table = tname2, alias = alias2)
		return (f1 == f2)
	
	
class MedooRecords(object):
	
	def __init__(self, cursor):
		self.cursor = cursor
		self.recordClass = None
		
	def next(self):
		return self.recordClass(self.cursor.fetchone()) if self.recordClass else self.cursor.fetchone()
		
	def __iter__(self):
		return iter(self.cursor)
		
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
		tname = table
		if 'schema' in kwargs:
			table = Table(tname, schema = kwargs['schema'])
		else:
			table = Table(tname)
		
		keys   = []
		values = []
		for key, val in data.items():
			t, field, alias, operation = MedooParser.field(key)
			if alias or operation:
				raise MedooFieldParseError('No alias and operstion allowed for insert field: "%s"' % key)
			if t and t != tname:
				raise MedooFieldParseError('Cannot insert with field of a different table: "%s"' % key)
			keys.append(key)
			values.append(val)

		q = Query.into(table).columns(*keys).insert(*values)
		for data in datas:
			if isinstance(data, tuple):
				q = q.insert(*data)
			else:
				values = [data[key] for key in keys]
				q = q.insert(*values)
		
		return self.query(q, 'commit' in kwargs and kwargs['commit'])
			
	def update(self, table, data, where = None, commit = True, schema = None):
		tname = table
		table = Table(tname, schema = schema)
		
		q = Query.update(table)
		raws = []
		for key, val in data.items():
			t, field, alias, operation = MedooParser.field(key)
			if alias:
				raise MedooFieldParseError('No alias allowed for update field: "%s"' % key)
			if t and t != tname:
				raise MedooFieldParseError('Cannot update with field of a different table: "%s"' % key)
			if operation:
				if operation == 'JSON':
					val = json.dumps(val)
				else:
					val = getattr(PkField(field, table = table), operation)(val)
			if isinstance(val, Raw):
				q = q.set(field, '{raw#%s}' % len(raws))
				raws.append(val)
			else:
				q = q.set(field, val)

		if where:
			q = q.where(MedooParser.where(where))
			
		q = str(q)
		# get raw back
		for i, raw in enumerate(raws):
			regex = r'("|\'|`)?\{raw#%s\}(\1)?' % i
			q = re.sub(regex, raw, q)

		return self.query(q, commit)
			
	# where required to avoid all data deletion
	def delete(self, table, where, commit = True, schema = None):
		tname = table
		table = Table(tname, schema = schema)
		
		q = Query.from_(table).where(MedooParser.where(where)).delete()
		
		return self.query(q, commit)
		
		
	def select(self, table, join = None, columns = '*', where = None, schema = None):
		tables = {}
		jsign, tname0, alias = MedooParser.table(table)
		if jsign:
			raise MedooTableParseError('No join sign allowed for primary select table: "%s"' % table)
		table = Table(tname0, alias = alias, schema = schema)
		tables[tname0] = table
		if alias: tables[alias] = table
		q = Query.from_(table)
			
		if join:
			def findKeys(dictionary, value):
				ret = []
				value = str(value)
				for key, val in dictionary.items():
					if str(val) != value: continue
					ret.append(key)
				return ret
				
			# sort join items, since pypika check the tables that not appear before on
			sorts   = {}
			jtables = {}
			jsigns  = {}
			minv    = -1
			maxv    = 999
			for key, val in join.items():
				jsign, tname, alias = MedooParser.table(key)
				jsigns[key] = jsign or JoinType.inner
				jtable = Table(tname, alias = alias, schema = schema)
				tables[tname] = jtable
				if alias: tables[alias] = jtable
				jtables[key] = jtable
				sorts[key] = 500
				if isinstance(val, dict):
					pass
				elif isinstance(val, (tuple, list)):
					join[key] = {v:v for v in val}
				else:
					join[key] = {val:val}
					
			for key, val in join.items():
				deptables = set()
				for k, v in val.items():
					t1, field1, _, _ = MedooParser.field(k)
					t2, field2, _, _ = MedooParser.field(v)
					# not depending on any other tables
					if t1: deptables.add(t1)
					if t2: deptables.add(t2)
				thisjointables = set([str(table), str(jtables[key])])
				if not deptables:
					sorts[key] = minv
				else:
					otherdeptables = deptables - thisjointables
					if not otherdeptables:
						sorts[key] = minv
					else:
						for odtable in otherdeptables:
							keys = findKeys(jtables, odtable)
							sorts[key] = max([sorts[k] for k in keys] + [sorts[key]]) + 1
			
			print sorts
			for key in sorted(join.keys(), key = lambda x: sorts[x]):
				print 'KEY:', key
				val = join[key]
				q = q.join(jtables[key], how = jsigns[key])
				print 'JOINTABLE:', jtables[key]
				print 'JOINFIELDS:', val
				field1, field2 = val.items()[0]
				del val[field1]	
				joinon = MedooParser.joinOn(field1, field2, jtable, table)
				for field1, field2 in val.items():
					joinon &= MedooParser.joinOn(field1, field2, jtable, table)
				print 'JOINON:', str(joinon)
				try:
					q = q.on(joinon)
				except:
					print 'RAISE:', key, val
					raise

		fields = []
		for c in MedooParser.alwaysList(columns):
			tname, field, alias, operation = MedooParser.field(c)
			if operation:
				raise MedooFieldParseError('Operation not allowed for select field: "%s"' % c)
			ftable = tables[tname] if tname and tname in tables else Table(tname) if tname else table
			if field != '*':
				field = PkField(field, alias = alias, table = ftable)
			else:
				field = ftable.star
			fields.append(field.as_(alias) if alias else field)
		
		q = q.select(*fields)

		if where:
			q = q.where(MedooParser.where(where, tables = tables))
		
		return self.query(q)
			
	def tableExists(self, table, schema = None):
		raise NotImplementedError('API not implemented.')
		
	def createTable(self, table, fields, drop = True, suffix = '', schema = None):
		raise NotImplementedError('API not implemented.')
		
	def dropTable(self, table, schema = None):
		raise NotImplementedError('API not implemented.')		
			
	def has(self, table, join = None, columns = '*', where = None, schema = None):
		rs = self.select(table, join, columns, where, schema)
		return bool(next(rs))
		
	def get(self, table, join = None, columns = '*', where = None, schema = None):
		rs = self.select(table, join, columns, where, schema)
		if not rs: return None
		r  = next(rs)
		if not r: return None
		return r.values()[0]
		
	def count(self, table, join = None, columns = '*', where = None, distinct = False, schema = None):
		tables = {}
		jsign, tname0, alias = MedooParser.table(table)
		if jsign:
			raise MedooTableParseError('No join sign allowed for primary select table: "%s"' % table)
		table = Table(tname0, alias = alias, schema = schema)
		tables[tname0] = table
		if alias: tables[alias] = table
		q = Query.from_(table)
			
		if join:
			for key, val in join.items():
				jsign, tname, alias = MedooParser.table(key)
				jtable = Table(tname, alias = alias, schema = schema)
				tables[tname] = jtable
				if alias: tables[alias] = jtable
				q = q.join(jtable, how = jsign or JoinType.inner)
				field1, field2 = val.items()[0]
				del val[field1]				
				joinon = MedooParser.joinOn(field1, field2, table, jtable)
				for field1, field2 in val.items():
					joinon &= MedooParser.joinOn(field1, field2, table, jtable)
				q = q.on(joinon)									
		
		fields = []
		for c in MedooParser.alwaysList(columns):
			tname, field, alias, operation = MedooParser.field(c)
			if operation:
				raise MedooFieldParseError('Operation not allowed for select field: "%s"' % c)
			ftable = tables[tname] if tname and tname in tables else Table(tname) if tname else table
			if field != '*':
				field = PkField(field, alias = alias, table = ftable)
			else:
				field = ftable.star
			field = functions.Count(field)
			if distinct:
				field = field.distinct()
			fields.append(field.as_(alias) if alias else field)
		
		q = q.select(*fields)

		if where:
			q = q.where(MedooParser.where(where, tables = tables))
		
		ret = self.query(q)
		if not ret: return None
		return next(ret)
			
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
				return self.recordsClass(self.cursor) if self.recordsClass else self.cursor
			else:
				return True
		except Exception as ex:
			self.errors.append(ex)
			return None
			