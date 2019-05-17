
from .exception import FieldParseError, TableParseError, WhereParseError, UpdateParseError, JoinParseError, LimitParseError, InsertParseError

from . import utils
from .dialect import Dialect
import re

class Term(object):
	"""
	Any part of a sql statement could be a Term
	"""
	def __str__(self):
		raise NotImplementedError('API not implemented: cannot convert to string.')

	def __eq__(self, other):
		return str(self) == str(other)

	def __ne__(self, other):
		return not self == other

	def __hash__(self):
		return hash(str(self))

class Raw(Term):
	"""
	Raw strings
	"""
	def __init__(self, s):
		self.s = str(s)

	def __str__(self):
		return self.s

class Table(Term):
	"""
	Only table or schema.table
	"""
	# a.c(alias)
	REGEX_FROM = r'^\s*((?:[\w_]+\.)?(?:[\w_]+|\*))\s*(?:\(([\w_]+)\))?\s*$'
	# a.c[=] # comment
	REGEX_JOIN = r'^\s*([\w_]+\.)?([\w_]+)\s*(?:\[(.+?)\])?\s*(?:#.*)?$'

	def __init__(self, table, schema = None):
		parts = table.split('.')
		if len(parts) == 1:
			self.table  = table
			self.schema = schema
		elif len(parts) == 2:
			if schema:
				raise TableParseError('Confusing schema specified in arguments "table" and "schema"')
			self.table = parts[1]
			self.schema = parts[0]
		else:
			raise TableParseError('Additional parts in "table"')

	def __str__(self):
		parts = []
		if self.schema:
			parts.append(Builder.DIALECT.quote(self.schema))
		parts.append(Builder.DIALECT.quote(self.table))
		return '.'.join(parts)

	@staticmethod
	def parse(tablestr, context = None):
		if isinstance(tablestr, Term):
			return tablestr
		else:
			if not context:
				return Table(tablestr)
			elif context == 'from':
				m = re.match(Table.REGEX_FROM, tablestr)
				if not m:
					raise TableParseError('Unrecognized table string.')
				table = m.group(1)
				alias = m.group(2)
				return TableFrom(table, alias = alias)
			elif context in ['insert', 'update', 'delete', 'selectinto']:
				return Table(tablestr)
			else:
				raise TableParseError('Unknown table context: {}'.format(context))

class TableFrom(Term):
	def __init__(self, table, alias = None):
		self.table = Table(table)
		self.alias = alias

	def __str__(self):
		return str(self.table) + (' AS ' + Builder.DIALECT.quote(self.alias) if self.alias else '')

class Field(Term):
	"""
	Only field or table.field or schema.table.field
	"""
	# a.b.c(alias)
	REGEX_SELECT  = r'^\s*((?:[\w_]+\.)?(?:[\w_]+\.)?(?:[\w_]+|\*))\s*(?:\|\s*(\.?[\w_]+))?\s*(?:\(([\w_]+)\))?\s*$'
	# a.b.f1, a.b.f2(func1, func2)[=] # comment
	# REGEX_WHERE   = r'^\s*([\w\s_.]+)\s*(?:\(([\w_,]+)\))?\s*(?:\[(.+?)\])?\s*(?:#.*)?$'

	def __init__(self, field = '*', table = None, schema = None):

		parts = field.split('.')
		if len(parts) == 1:
			self.field  = field
			self.table  = table
			self.schema = schema
		elif len(parts) == 2:
			if table:
				raise FieldParseError('Confusing table specified in arguments "field" and "table"')
			self.field = parts[1]
			self.table = parts[0]
			self.schema = schema
		elif len(parts) == 3:
			if table or schema:
				raise FieldParseError('Confusing table/schema specified in arguments "field" and "table"/"schema"')
			self.field = parts[2]
			self.table = parts[1]
			self.schema = parts[0]
		else:
			raise FieldParseError('Additional parts in "field"')

	def __str__(self):
		parts = []
		if self.schema:
			parts.append(Builder.DIALECT.quote(self.schema))
		if self.table:
			parts.append(Builder.DIALECT.quote(self.table))
		parts.append(Builder.DIALECT.quote(self.field))
		return '.'.join(parts)

	def __add__(self, value):
		return Raw(str(self) + '+' + Builder.DIALECT.value(value))

	def __sub__(self, value):
		return Raw(str(self) + '-' + Builder.DIALECT.value(value))

	def __mul__(self, value):
		return Raw(str(self) + '*' + Builder.DIALECT.value(value))

	def __div__(self, value):
		return Raw(str(self) + '/' + Builder.DIALECT.value(value))

	def __mod__(self, value):
		return Raw(str(self) + '%' + Builder.DIALECT.value(value))

	@staticmethod
	def parse(fieldstr, context = None):
		if isinstance(fieldstr, Term):
			return fieldstr
		else:
			if not context:
				return Field(fieldstr)
			elif context == 'select':
				m = re.match(Field.REGEX_SELECT, fieldstr)
				if not m:
					raise FieldParseError('Unrecognized field string: {}'.format(fieldstr))
				field = m.group(1)
				func  = m.group(2)
				alias = m.group(3)
				return FieldSelect(field, alias = alias, func = func)
			elif context == 'group':
				return Field(fieldstr)
			else:
				raise FieldParseError('Unknown field context: {}'.format(context))

class FieldSelect(Term):

	def __init__(self, field = '*', alias = None, func = None):
		"""
		The fields in SELECT substatements
		@params:
			`field`   : The field that applied
			`alias`    : The alias
		"""
		self.field = Field(field)
		self.alias = alias
		self.distinct = func and func.startswith('.')
		self.func  = func[1:] if func and func.startswith('.') else func

	def __str__(self):
		ret = str(self.field)
		if self.func:
			if hasattr(Builder.DIALECT, self.func.lower()):
				ret = getattr(Builder.DIALECT, self.func.lower())(ret, distinct = self.distinct)
			else:
				ret = '{}({}{})'.format(self.func.upper(), 'DISTINCT ' if self.distinct else '', ret)
		if self.alias:
			ret += ' AS ' + Builder.DIALECT.quote(self.alias)
		return ret

class Where(Term):

	def __init__(self, conditions, root = True):
		self.conditions = conditions
		self.root       = root

	def __str__(self):
		sqlitems = []
		for key, val in self.conditions.items():
			# whatever term it is, the value will be ignored
			if isinstance(key, Term):
				sqlitems.append(str(key))
			elif key.split('#')[0].strip().upper() == 'AND':
				if not isinstance(val, (tuple, list, dict)):
					raise WhereParseError('Expect dict or item list/tuple for conditions to be connected by AND: "%s"' % val)
				val = val.items() if isinstance(val, dict) else [
					v if isinstance(v, tuple) else (v, None) for v in val
				]
				whereterms = [str(Where({k:v}, False)) for k, v in val]
				if len(whereterms) == 1:
					sqlitems.append(whereterms[0])
				else:
					sqlitems.append('(%s)' % (' AND '.join(whereterms)))
			elif key.split('#')[0].strip().upper() == 'OR':
				if not isinstance(val, (tuple, list, dict)):
					raise WhereParseError('Expect dict or item list for conditions to be connected by OR: "%s"' % val)
				val = val.items() if isinstance(val, dict) else [
					v if isinstance(v, tuple) else (v, None) for v in val
				]
				whereterms = [str(Where({k:v}, False)) for k, v in val]
				if len(whereterms) == 1:
					sqlitems.append(whereterms[0])
				else:
					sqlitems.append('(%s)' % (' OR '.join(whereterms)))
			else:
				sqlitems.append(str(WhereTerm(key, val)))

		if self.root and len(sqlitems) == 1 and sqlitems[0][0] == '(' and sqlitems[0][-1] == ')':
			# remove the brackets
			return sqlitems[0][1:-1]
		return ' AND '.join(sqlitems)

class WhereTerm(Term):


	REGEX_KEY = r'^\s*(!)?\s*([\w\s_.]+)\s*(?:\|([\w\s_.]+))?\s*(?:\[(.+?)\])?\s*(?:#.*)?$'

	def __init__(self, key, val):
		self.key = key
		self.val = val

	def __str__(self):
		# whatever term it is for the key, ignore the value
		if isinstance(self.key, Term):
			return str(self.key)

		m = re.match(WhereTerm.REGEX_KEY, self.key)
		if not m:
			raise WhereParseError('Unrecognized key in where conditions.')
		ret   = 'NOT ' if m.group(1) else ''
		field = FieldSelect(m.group(2), func = m.group(3))
		oprt  = m.group(4)

		return ret + Builder.DIALECT._operator(oprt, field, self.val)

class Order(Term):

	def __init__(self, orders):
		self.orders = orders

	def __str__(self):
		return ','.join([str(OrderTerm(key, val)) for key, val in self.orders.items()])

class OrderTerm(Term):

	REGEX_KEY  = r'^\s*((?:[\w_]+\.)?(?:[\w_]+\.)?(?:[\w_]+|\*))\s*(?:\|([\w_.]+))?\s*$'
	def __init__(self, key, val):
		m = re.match(OrderTerm.REGEX_KEY, key)
		if not m:
			raise FieldParseError('Unrecognized field in ORDER BY clause.')
		self.field = FieldSelect(m.group(1), func = m.group(2))
		if val is True or val is None:
			val = 'ASC'
		elif val is False:
			val = 'DESC'
		else:
			val = val.upper()
		self.val = val

	def __str__(self):
		return '{} {}'.format(self.field, self.val)

class Limit(Term):

	def __init__(self, limoff):
		if len(limoff) == 1:
			limit  = limoff[0]
			offset = None
		elif len(limoff) == 2:
			limit, offset = limoff
		else:
			raise LimitParseError('LIMIT requires a two integer tuple/list.')

		lim = Builder.DIALECT.limit(limit, offset)
		if isinstance(lim, tuple):
			self.s, self.pos = lim
		else:
			self.s   = lim
			self.pos = -1

	def __str__(self):
		return self.s

class Set(Term):

	def __init__(self, sets):
		self.sets = sets

	def __str__(self):
		return ','.join([str(SetTerm(key, val)) for key, val in self.sets.items()])

class SetTerm(Term):

	REGEX_KEY = r'^\s*((?:[\w_]+\.)?(?:[\w_]+\.)?(?:[\w_]+|\*))\s*(?:\[(.+?)\])?\s*$'
	def __init__(self, key, val):
		m = re.match(SetTerm.REGEX_KEY, key)
		if not m:
			raise UpdateParseError('Unrecognized field in UPDATE SET.')
		self.field = Field(m.group(1))
		self.oprt  = m.group(2)
		self.val   = val

	def __str__(self):
		return Builder.DIALECT._update(self.oprt, self.field, self.val)

class Join(Term):

	def __init__(self, joins, maintable = None):
		self.joins = joins
		self.maintable = maintable

	def __str__(self):
		return ' '.join([str(JoinTerm(key, val, self.maintable)) for key, val in self.joins.items()])

class JoinTerm(Term):

	REGEX_KEY = r'^\s*(?:\[(.+?)\])?\s*((?:[\w_]+\.)?(?:[\w_]+\.)?(?:[\w_]+|\*))\s*(?:\((.+?)\))?\s*$'
	def __init__(self, key, val, maintable = None):
		m = re.match(JoinTerm.REGEX_KEY, key)
		if not m:
			raise JoinParseError('Unrecognized table in JOIN.')
		self.jointype = m.group(1)
		self.table    = TableFrom(m.group(2), alias = m.group(3))
		self.onfields = []
		fieldtable    = self.table.alias or self.table.table
		if isinstance(maintable, TableFrom):
			maintable = maintable.alias or maintable.table
		elif isinstance(maintable, Builder):
			# subquery
			if maintable._subas in [None, True, False]:
				raise JoinParseError('Require alias for subquery to refer to its fields in JOIN ON.')
			else:
				maintable = maintable._subas

		if isinstance(val, (tuple, list)):
			for v in val:
				f1 = Field(v)
				if f1.table:
					raise JoinParseError('Expected table specified in JOIN ON fields.')
				if not maintable:
					raise JoinParseError('Short format of JOIN fields are not allowed without primary table.')
				f2 = Field(v)
				f1.table = fieldtable
				f2.table = maintable
				self.onfields.append((f1, f2))
		elif isinstance(val, dict): # dict
			for k, v in val.items():
				f1 = Field(k)
				f2 = Field(v)

				if f1.table:
					raise JoinParseError('Unexpected table on JOIN ON left field: {}'.format(f1))

				if maintable and f2.table:
					raise JoinParseError('Unexpected table on JOIN ON right field: {}'.format(f2))

				f1.table = fieldtable
				if maintable:
					f2.table = maintable
				self.onfields.append((f1, f2))
		else:
			f1 = Field(val)
			if f1.table:
				raise JoinParseError('Unexpected table specified in JOIN ON fields.')

			if not maintable:
				raise JoinParseError('Short format of JOIN fields are not allowed without primary table.')

			f2 = Field(val)
			f1.table = fieldtable
			f2.table = maintable
			self.onfields.append((f1, f2))

	def __str__(self):
		return '{} {} ON {}'.format(
			Builder.DIALECT._join(self.jointype),
			self.table,
			' AND '.join(['{}={}'.format(key, val) for key, val in self.onfields])
		)

class Builder(Term):

	DIALECT = Dialect

	def __init__(self, dialect = None):
		Builder.DIALECT = dialect or Dialect

		# for join
		self.table  = None
		self.terms  = []
		self._sql   = None
		self._subas = False

	def _select(self, *fields, **kwargs):
		if not fields: fields = ['*']
		distinct = kwargs.get('distinct', False)
		self.terms.append('SELECT DISTINCT' if distinct else 'SELECT')

		fieldterms = [
			Field.parse(field or '*', 'select')
			for field in fields
		]
		self.terms.append(','.join([str(field) for field in fieldterms]))
		return self

	def _sub(self, alias = None):
		"""
		Give a name for subquery
		"""
		self._subas = alias if alias else True
		return self

	def _from(self, *tables):
		self.terms.append('FROM')
		tableterms = [
			Table.parse(table, 'from')
			for table in tables
		]

		self.table = tableterms[0]._subas if isinstance(tableterms[0], Builder) else tableterms[0]
		self.terms.append(','.join([str(table) for table in tableterms]))
		return self

	def _where(self, conditions):
		if not conditions:
			return self
		self.terms.append('WHERE')
		whereterm = Where(conditions)
		self.terms.append(whereterm)
		return self

	def _order(self, orders):
		self.terms.append('ORDER BY')
		self.terms.append(Order(orders))
		return self

	def _limit(self, limoff):
		if not isinstance(limoff, (tuple, list)):
			limoff = [limoff]
		lim = Limit(limoff)
		if lim.pos == -1:
			self.terms.append(lim)
		elif isinstance(lim.pos, int):
			self.terms.insert(lim.pos, str(lim))
		else: # oracle, add where condition
			# check if where is there:
			if 'WHERE' not in self.terms:
				self.terms.append('WHERE')
				self.terms.append(lim)
			else:
				whereindex = self.terms.index('WHERE')
				# TODO: ' AND ' and ' OR ' could also be in subquery or values
				# it's still OK to have brackets, keep it for now
				if ' AND ' in str(self.terms[whereindex + 1]) or ' OR ' in str(self.terms[whereindex + 1]):
					self.terms[whereindex + 1] = '({}) AND ({})'.format(self.terms[whereindex + 1], lim)
				else:
					self.terms.insert(whereindex+2, 'AND ({})'.format(lim))

		return self

	def _union(self, other, all_ = False):
		self.terms.append('UNION')
		if all_:
			self.terms.append('ALL')
		self.terms.append(other)
		return self

	def _exists(self, query):
		self.terms.append('EXISTS')
		self.terms.append(query)
		return self

	def _group(self, fields):
		self.terms.append('GROUP BY')
		if isinstance(fields, Term):
			fields = [fields]
		else:
			fields = utils.alwaysList(fields)
		fieldterms = [
			Field.parse(field, 'group')
			for field in fields
		]
		self.terms.append(','.join([str(field) for field in fieldterms]))
		return self

	def _having(self, conditions):
		if not conditions:
			return self
		self.terms.append('HAVING')
		self.terms.append(Where(conditions))

		return self

	def _insert(self, table, values, fields = None):
		self.terms.append('INSERT INTO')
		self.terms.append(Table(table))
		if not fields:
			if isinstance(values[0], dict):
				fields = values[0].keys()
				for i, value in enumerate(values):
					if i == 0: continue
					if isinstance(value, dict) and set(fields) != set(value.keys()):
						raise InsertParseError('Inconsistent keys in values for INSERT.')
		if fields:
			fieldterms = [Table.parse(field, 'insert') for field in utils.alwaysList(fields)]
			self.terms.append('({})'.format(','.join([str(field) for field in fieldterms])))

		# support INSERT INTO SELECT ...
		if isinstance(values[0], Builder):
			subquery0 = values.pop(0)
			self.terms.append(subquery0)
			for value in values:
				if isinstance(value, Builder):
					if value._subas:
						self.terms.append('UNION ALL')
						value._subas = None
						self.terms.append(value)
					else:
						self.terms.append('UNION')
						self.terms.append(value)
				else:
					raise InsertParseError('All values should be subqueries in INSERT.')
		else:
			insertvals = []
			for value in values:
				if isinstance(value, (tuple, list)):
					insertvals.append(tuple(value))
				elif isinstance(value, dict):
					if fields:
						insertvals.append(tuple(
							value[field] if field in value else None
							for field in fields
						))
					else:
						insertvals.append(tuple(value.values()))
				else:
					raise InsertParseError('Unsupported type for values in INSERT: {}'.format(type(value)))
			self.terms.append('VALUES')
			valterms = []
			for inval in insertvals:
				valterms.append('(%s)' % (','.join([Builder.DIALECT.value(iv) for iv in inval])))
			self.terms.append(','.join(valterms))
		return self

	def _update(self, table):
		self.terms.append('UPDATE')
		self.terms.append(Table.parse(table, 'update'))
		return self

	def _set(self, sets):
		self.terms.append('SET')
		self.terms.append(Set(sets))
		return self

	def _delete(self, table):
		self.terms.append('DELETE FROM')
		self.terms.append(Table.parse(table, 'delete'))
		return self

	def _join(self, joins):
		self.terms.append(Join(joins, self.table))
		return self

	# support select * into newtable from ...
	def _into(self, table):
		self.terms.append('INTO')
		self.terms.append(Table.parse(table, 'selectinto'))
		return self

	def select(self, table, columns = '*', where = None, join = None, distinct = False, newtable = None, sub = None):
		order  = None
		limit  = None
		group  = None
		having = None
		exists = None
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
		if where and 'EXISTS' in where:
			exists = where['EXISTS']
			del where['EXISTS']
		if isinstance(columns, Term):
			columns = [columns]
		self._select(*utils.alwaysList(columns), distinct = distinct)
		if newtable:
			self._into(newtable)._from(*utils.alwaysList(table))
		else:
			self._from(*utils.alwaysList(table))
		if join:
			self._join(join)._where(where)
		else:
			self._where(where)
		if order : self._order(order)
		if limit : self._limit(limit)
		if group : self._group(group)
		if having: self._having(having)
		if exists: self._exists(exists)
		if sub   : self._sub(sub)
		return self

	def update(self, table, data, where = None):
		self._update(table)._set(data)._where(where)
		return self

	def delete(self, table, where):
		self._delete(table)._where(where)
		return self

	def insert(self, table, fields, *values):
		"""
		data: list will be treated as fields
		"""
		# table, values, fields
		values2 = []
		if isinstance(fields, dict):
			values2.append(list(fields.values()))
			fields = list(fields.keys())
		elif isinstance(fields, tuple):
			values2.append(tuple(fields))
			fields = None
		elif isinstance(fields, list):
			pass
		else: # assuming fields specified as string
			fields = utils.alwaysList(fields)

		values2.extend([
			value if isinstance(value, tuple) else tuple(value.values())
			for value in values
		])
		self._insert(table, values2, fields)
		return self

	def union(self, *queries):
		queries = list(queries)
		if not self.terms:
			query = queries.pop(0)
			subas = query._subas
			query._subas = None # remove brackets
			self.terms.append(str(query))
			query._subas = subas
		for query in queries:
			if query._subas:
				subas = query._subas
				query._subas = None
				self.terms.append('UNION ALL')
				self.terms.append(str(query))
				query._subas = subas
			else:
				self.terms.append('UNION')
				self.terms.append(query)
		return self

	def sql(self):
		if self._sql:
			return self._sql

		ret = ' '.join(['%s' % t for t in self.terms])
		if self._subas is True:
			ret = '({})'.format(ret)
		elif self._subas:
			ret = '({}) AS {}'.format(ret, Builder.DIALECT.quote(self._subas))

		return ret

	def __str__(self):
		return self.sql()
