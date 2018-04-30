import re, six, types, collections

def _alwaysList(s):
	if isinstance(s, list):
		return s
	if isinstance(s, (types.GeneratorType, collections.KeysView)):
		return list(s)
	return [x.strip() for x in s.split(',') if x.strip()]

class TableParseError(Exception):
	pass

class FieldParseError(Exception):
	pass

class ValueParseError(Exception):
	pass

class WhereParseError(Exception):
	pass

class OrderbyParseError(Exception):
	pass

class Term(object):
	def sql(self):
		pass

	def __str__(self):
		return self.sql()

class Raw(str, Term):

	def sql(self):
		return self

class MetaFuncion(type):
	def __getattr__(klass, name):
		return lambda *fields, **kwargs: Function(name, *fields, **kwargs)

class Function(six.with_metaclass(MetaFuncion, Term)):

	def __init__(self, fn, *fields, **kwargs):
		self.fn      = fn
		self.fields  = []
		self.dialect = Dialect
		if 'dialect' in kwargs and kwargs['dialect']:
			self.dialect = kwargs['dialect']
		self.kwargs  = kwargs
		for fieldlist in fields:
			self.fields += Field.parse(fieldlist, self.dialect)

	def sql(self):
		return getattr(self.dialect, self.fn)(*self.fields, **self.kwargs)

	def __hash__(self):
		return hash((self.fn,) + tuple(str(field) for field in self.fields))

	def __eq__(self, other):
		return self.fn == other.fn and self.fields == other.fields

	def __ne__(self, other):
		return self.fn != other.fn or self.fields != other.fields

	def __add__(self, value):
		return Raw(self.sql() + '+' + self.dialect.value(value))

	def __sub__(self, value):
		return Raw(self.sql() + '-' + self.dialect.value(value))

	def __mul__(self, value):
		return Raw(self.sql() + '*' + self.dialect.value(value))

	def __div__(self, value):
		return Raw(self.sql() + '/' + self.dialect.value(value))

class MetaDialect(type):
	def __getattr__(klass, name):
		def func(*fields, **kwargs):
			dialect  = Dialect
			if 'dialect' in kwargs and kwargs['dialect']:
				dialect = kwargs['dialect']

			operator = ''
			realfields = [field for field in fields if isinstance(field, Field)]
			if realfields:
				operator = realfields[0].operator or ''

			return '%s(%s)%s' % (name.upper(), ','.join([Field.stringify(field, False) for field in fields]), operator)
		return func

class Dialect(six.with_metaclass(MetaDialect)):

	@staticmethod
	def quote(item):
		return '"%s"' % item.replace('"', '""')

	@staticmethod
	def alias(item, as_):
		return '%s %s' % (item, as_)

	@staticmethod
	def value(item):
		if isinstance(item, six.integer_types + (float, )):
			return str(item)
		elif isinstance(item, Term):
			return item.sql()
		else:
			return "'{}'".format(str(item).replace("'", "''"))

	@staticmethod
	def distinct(*fields, **kwargs):
		return 'DISTINCT ' + ','.join([Field.stringify(field, False) for field in fields])

	@staticmethod
	def count(*fields, **kwargs):
		if not fields: fields = ['*']
		dialect = Dialect
		if 'dialect' in kwargs and kwargs['dialect']:
			dialect = kwargs['dialect']
		if len(fields) > 1:
			raise FieldParseError('Only 1 field allow for COUNT: "%s"' % fields)

		fieldsql = Field.stringify(fields[0], False)
		if 'alias' in kwargs:
			return dialect.alias('COUNT(%s)' % fieldsql, dialect.quote(kwargs['alias']))
		else:
			return 'COUNT(%s)' % fieldsql

	@staticmethod
	def json(field, value):
		import json
		return field.sql() + '=' + field.dialect.value(json.dumps(value))

	@staticmethod
	def limit(offset, lim):
		return '%s,%s' % (offset, lim)

	@staticmethod
	def join(jointype):
		if jointype == '>':
			return 'LEFT JOIN'
		if jointype == '<':
			return 'RIGHT JOIN'
		if jointype == '<>':
			return 'OUTER JOIN'
		if jointype == '><':
			return 'JOIN'
		raise TableParseError('Unknown join type: "%s"' % jointype)

	@staticmethod
	def likeValue(s):
		return '%{}%'.format(s) if not s.startswith('%') and not s.endswith('%') else s

	@staticmethod
	def operate(operator, left, right, dialect = None):
		dialect = dialect or Dialect
		if isinstance(right, (tuple, list)) and len(right) == 1:
			right = right[0]
		if isinstance(right, (tuple, list)):
			values = [dialect.value(r) for r in right]
			if not operator: operator = 'IN'
			elif operator == '!': operator = 'NOT IN'
			elif operator == '<>':
				if len(values) != 2:
					raise ValueParseError('BETWEEN must have 2 values: "%s"' % values)
				v1, v2 = values
				return '%s BETWEEN %s AND %s' % (left, v1, v2)
			elif operator == '><':
				if len(values) != 2:
					raise ValueParseError('BETWEEN must have 2 values: "%s"' % values)
				v1, v2 = values
				return '%s NOT BETWEEN %s AND %s' % (left, v1, v2)
			elif operator == '~':
				operator = 'LIKE'
				values = [r if isinstance(r, Raw) else dialect.value(dialect.likeValue(r)) for r in right]
				return '(%s)' % ' OR '.join(['%s %s %s' % (left, operator, value) for value in values])
			elif operator == '!~':
				operator = 'NOT LIKE'
				values = [r if isinstance(r, Raw) else dialect.value(dialect.likeValue(r)) for r in right]
				return '(%s)' % ' AND '.join(['%s %s %s' % (left, operator, value) for value in values])
			return '%s %s (%s)' % (left, operator, ','.join(values))
		else:
			value = dialect.value(right)
			if not operator: operator = '='
			elif operator == '!': operator = '!='
			elif operator == '~':
				operator = 'LIKE'
				value = right if isinstance(right, Raw) else dialect.value(dialect.likeValue(right))
			elif operator == '!~':
				operator = 'NOT LIKE'
				value = right if isinstance(right, Raw) else dialect.value(dialect.likeValue(right))
			return '%s %s %s' % (left, operator, value)

class Table(Term):
	#                     [>]    schema.               table              (alias)
	REGEXP_TABLE  = r'^\s*(?:\[([<>]+)\])?\s*(?:([a-zA-Z0-9_]+)\.)?([a-zA-Z0-9_]+)\s*(?:\(([a-zA-Z0-9_]+)\))?\s*(?:#.*)?$'

	def __init__(self, tablestr, dialect = None):
		if isinstance(tablestr, Table):
			self.join    = tablestr.join
			self.schema  = tablestr.schema
			self.table   = tablestr.table
			self.alias   = tablestr.alias
			self.dialect = dialect or tablestr.dialect
		else:
			m = re.match(Table.REGEXP_TABLE, tablestr)
			if not m:
				raise TableParseError('Cannot understand table: "%s"' % tablestr)

			self.join    = m.group(1)
			self.schema  = m.group(2)
			self.table   = m.group(3)
			self.alias   = m.group(4)
			self.dialect = dialect or Dialect

	def sql(self, withAlias = True):
		dialect = self.dialect
		ret = ''
		if self.join:
			ret += dialect.join(self.join) + ' '
		if self.schema:
			ret += dialect.quote(self.schema) + '.'
		ret += dialect.quote(self.table)
		if self.alias and withAlias:
			ret = dialect.alias(ret, dialect.quote(self.alias))
		return ret

	def __hash__(self):
		return hash((self.join, self.schema, self.table, self.alias))

	def __eq__(self, other):
		if not isinstance(other, Table):
			return False
		if self.schema != other.schema:
			return False
		if self.table != other.table:
			return False
		if self.alias != other.alias:
			return False

	def __ne__(self, other):
		return not self.__eq__(other)

	@staticmethod
	def parse(tablelist, dialect = None):
		dialect = dialect or Dialect
		if isinstance(tablelist, (tuple, list)):
			return [table for sublist in [Field.parse(tablestr) for tablestr in tablelist] for field in sublist]
		if isinstance(tablelist, (Raw, Function, Table)):
			return [tablelist]
		else:
			return [Table(tablestr, dialect) for tablestr in _alwaysList(tablelist)]

class Field(Term):

	#                      table.field(alias)[operator] # comment
	REGEXP_FIELD  = r'^\s*(?:([a-zA-Z0-9_]+)\.)?([a-zA-Z0-9_]+|\*)\s*(?:\(([a-zA-Z0-9_]+)\))?(?:\[(.+?)\])?\s*(?:#.*)?$'

	def __init__(self, fieldstr, dialect = None):
		if isinstance(fieldstr, Field):
			self.table    = fieldstr.table
			self.field    = fieldstr.field
			self.alias    = fieldstr.alias
			self.operator = fieldstr.operator
			self.dialect  = dialect or fieldstr.dialect
		else:
			m = re.match(Field.REGEXP_FIELD, fieldstr)
			if not m:
				raise FieldParseError('Cannot understand field: "%s"' % fieldstr)

			self.table    = m.group(1)
			self.field    = m.group(2)
			self.alias    = m.group(3)
			self.operator = m.group(4)
			self.dialect  = dialect or Dialect

	def sql(self, withAlias = True):
		dialect = self.dialect
		ret = ''
		if self.table:
			ret += dialect.quote(self.table) + '.'
		ret += dialect.quote(self.field) if self.field != '*' else '*'
		if self.alias and withAlias:
			ret = dialect.alias(ret, dialect.quote(self.alias))
		return ret

	def __hash__(self):
		return hash((self.table, self.field, self.alias, self.operator))

	def __eq__(self, other):
		if not isinstance(other, Field):
			return False
		if self.table != other.table:
			return False
		if self.field != other.field:
			return False
		if self.alias != other.alias:
			return False

	def __ne__(self, other):
		return not self.__eq__(other)

	def __add__(self, value):
		return Raw(self.sql() + '+' + self.dialect.value(value))

	def __sub__(self, value):
		return Raw(self.sql() + '-' + self.dialect.value(value))

	def __mul__(self, value):
		return Raw(self.sql() + '*' + self.dialect.value(value))

	def __div__(self, value):
		return Raw(self.sql() + '/' + self.dialect.value(value))

	@staticmethod
	def stringify(field, withAlias = True):
		if isinstance(field, Field):
			return field.sql(withAlias)
		elif isinstance(field, Function):
			return field.sql()
		elif isinstance(field, Raw):
			return str(field)
		return str(field)

	@staticmethod
	def parse(fieldlist, dialect = None):
		dialect = dialect or Dialect
		if isinstance(fieldlist, (tuple, list)):
			return [field for sublist in [Field.parse(fieldstr) for fieldstr in fieldlist] for field in sublist]
		if isinstance(fieldlist, Term):
			return [fieldlist]
		else:
			return [Field(fieldstr, dialect) for fieldstr in _alwaysList(fieldlist)]

class UpdateSet(Term):

	def __init__(self, key, value, dialect = None):
		self.key = key
		self.value = value
		self.dialect = dialect or Dialect

	def sql(self):
		key = self.key
		if not isinstance(key, (Function, Raw)):
			key = Field(key, self.dialect)

		if not isinstance(key, Field) or not key.operator:
			return key.sql() + '=' + self.dialect.value(self.value)
		else:
			operators = {'+': '__add__', '-': '__sub__', '*': '__mul__', '/': '__div__'}
			val = self.value.sql() if isinstance(self.value, (Raw, Function)) else self.value
			if key.operator in ['+', '-', '*', '/']:
				return (key.sql() + '=') + getattr(key, operators[key.operator])(val)
			else:
				return getattr(self.dialect, key.operator)(key, val)

class WhereTerm(Term):

	def __init__(self, key, value, dialect = None):
		self.key   = key
		self.value = value
		self.dialect = dialect or Dialect

	def sql(self):
		if isinstance(self.key, Raw):
			return self.key
		else:
			field = Field(self.key)
			if field.alias:
				raise FieldParseError('Alias not allowed for field in where clause: "%s"' % self.key)
			left  = field.sql()
			return self.dialect.operate(field.operator, left, self.value, self.dialect)

class Where(Term):

	def __init__(self, wheredict = None, dialect = None):
		self.wheredict = wheredict
		self.dialect   = dialect or Dialect

	def sql(self):
		sqlitems = []
		for key, val in self.wheredict.items():
			if isinstance(key, Field):
				sqlitems.append(WhereTerm(key, val, self.dialect).sql())
			elif isinstance(key, Function):
				sqlitems.append(key.sql() + self.dialect.value(val))
			elif key.split('#')[0].strip() == 'AND':
				if not isinstance(val, (list, dict)):
					raise WhereParseError('Expect dict or item list for conditions to be connected by AND: "%s"' % val)
				whereterms = [Where({k:v}, self.dialect).sql() for k, v in dict(val).items()]
				if len(whereterms) == 1:
					sqlitems.append(whereterms[0])
				else:
					sqlitems.append('(%s)' % (' AND '.join(whereterms)))
			elif key.split('#')[0].strip() == 'OR':
				if not isinstance(val, (list, dict)):
					raise WhereParseError('Expect dict or item list for conditions to be connected by OR: "%s"' % val)
				whereterms = [Where({k:v}, self.dialect).sql() for k, v in dict(val).items()]
				if len(whereterms) == 1:
					sqlitems.append(whereterms[0])
				else:
					sqlitems.append('(%s)' % (' OR '.join(whereterms)))
			else:
				sqlitems.append(WhereTerm(key, val, self.dialect).sql())
		return ' AND '.join(sqlitems)

class JoinOnTerm(Term):

	def __init__(self, key, value, table, primary_table, dialect = None):
		self.key     = key
		self.value   = value
		self.table   = table
		self.ptable  = primary_table
		self.dialect = dialect or Dialect

	def sql(self):
		left  = Field.parse(self.key)[0]
		right = Field.parse(self.value)[0]
		if isinstance(left, Field) and not left.table:
			left.table = self.table.alias or self.table.table
		if isinstance(right, Field) and not right.table:
			right.table = self.ptable
		return Field.stringify(left, False) + '=' + Field.stringify(right, False)

class JoinOn(Term):

	def __init__(self, joinondict, table = None, primary_table = None, dialect = None):
		self.joinondict = joinondict
		self.dialect    = dialect or Dialect
		self.table      = table
		if not isinstance(primary_table, Table):
			self.ptable = primary_table
		else:
			self.ptable = primary_table.alias or primary_table.table

	def sql(self):

		sqlitems = []
		for key, val in self.joinondict.items():
			sqlitems.append(JoinOnTerm(key, val, self.table, self.ptable, self.dialect).sql())
		return ' AND '.join(sqlitems)

class JoinUsing(Term):

	def __init__(self, fields, dialect):
		self.fields  = fields
		self.dialect = dialect

	def sql(self):
		sqlitems = []
		for field in self.fields:
			field = Field.parse(field, self.dialect)[0]
			sqlitems.append(Field.stringify(field, False))
		return '(%s)' % (','.join(sqlitems))

class Builder(Term):

	def __init__(self, dialect = None):
		self.terms   = []
		self.table   = None
		self.dialect = dialect or Dialect

	def select(self, *fields):
		if not fields: fields = ['*']
		self.terms.append('SELECT')
		fieldterms = []
		for fieldlist in fields:
			fieldterms += Field.parse(fieldlist)
		self.terms.append(','.join([Field.stringify(field) for field in fieldterms]))
		return self

	def group(self, *fields):
		self.terms.append('GROUP BY')
		fieldterms = []
		for fieldlist in fields:
			fieldterms += Field.parse(fieldlist)
		self.terms.append(','.join([Field.stringify(field) for field in fieldterms]))
		return self


	def from_(self, *tables):
		self.terms.append('FROM')
		for tablelist in tables:
			# allow sub query
			if isinstance(tablelist, Builder):
				self.terms.append('(' + tablelist.sql() + ')')
			else:
				tables = Table.parse(tablelist, self.dialect)
				if not self.table: self.table = tables[0]
				self.terms += tables
		return self

	def where(self, conditions):
		if not conditions:
			return self
		self.terms.append('WHERE')
		wherestr = Where(conditions, self.dialect).sql()
		if wherestr[0] == '(' and wherestr[-1] == ')':
			wherestr = wherestr[1:-1]
		self.terms.append(wherestr)
		return self

	def having(self, conditions):
		self.terms.append('HAVING')
		self.terms.append(Where(conditions, self.dialect))
		return self

	def update(self, table):
		self.terms.append('UPDATE')
		tablestr = table
		table = Table(tablestr, self.dialect)
		if table.join:
			raise TableParseError('Join sign is not allowed for update table: "%s"' % tablestr)
		if table.alias:
			raise TableParseError('Alias is not allowed for update table: "%s"' % tablestr)
		self.terms.append(table)
		return self

	def set(self, updates):
		self.terms.append('SET')
		sets = []
		for key, val in updates.items():
			sets.append(UpdateSet(key, val, self.dialect).sql())
		self.terms.append(','.join(sets))
		return self

	def insert(self, table, *fields):
		self.terms.append('INSERT INTO')
		table = Table(table, self.dialect)
		self.terms.append(table)
		fieldterms = []
		for fieldlist in fields:
			fieldterms += Field.parse(fieldlist, self.dialect)
		if fieldterms:
			self.terms.append('(%s)' % ','.join([Field.stringify(field) for field in fieldterms]))
		return self

	def values(self, *values):
		self.terms.append('VALUES')
		vals = []
		for value in values:
			vals.append('(' + ','.join([self.dialect.value(v) for v in value]) + ')')
		self.terms.append(', '.join(vals))
		return self

	def delete(self, table):
		self.terms.append('DELETE FROM')
		tables = Table.parse(table, self.dialect)
		self.terms += tables
		return self

	def order(self, orders):
		self.terms.append('ORDER BY')
		ords = []
		for key, val in orders.items():
			field = Field.parse(key, self.dialect)[0]
			if isinstance(field, Raw):
				ords.append(Field.stringify(field))
			else:
				val2 = val
				if val is True:  val2 = 'asc'
				if val is False: val2 = 'desc'
				val2 = val2.upper()
				if not val2 in ['ASC', 'DESC']:
					raise OrderbyParseError('Unknown order by type: "%s"' % val)
				ords.append(Field.stringify(field) + ' ' + val2)
		self.terms.append(','.join(ords))
		return self

	def limit(self, limits):
		self.terms.append('LIMIT')
		if not isinstance(limits, (tuple, list)):
			limits = [limits]
		if isinstance(limits, tuple):
			limits = list(limits)
		if len(limits) == 1:
			limits.insert(0, 1)
		offset, lim = limits
		self.terms.append(self.dialect.limit(offset, lim))
		return self

	def create(self, table, schema, ifnotexists = True, suffix = ''):
		self.terms.append('CREATE TABLE')
		if ifnotexists:
			self.terms.append('IF NOT EXISTS')
		tables = Table.parse(table, self.dialect)
		self.terms += tables
		self.terms.append('(')
		fields = []
		for key, val in schema.items():
			field = Field.parse(key, self.dialect)[0]
			fields.append(Field.stringify(field) + ' ' + val)
		self.terms.append(', '.join(fields))
		self.terms.append(')')
		self.terms.append(suffix)
		return self


	def join(self, joins):
		for key, val in joins.items():
			table = Table.parse(key, self.dialect)[0]
			if isinstance(table, Table) and not table.join:
				table.join = '><'
			self.terms.append(table)
			if isinstance(val, dict):
				self.terms.append('ON')
				newval = {}
				for k, v in val.items():
					fieldk = Field.parse(k, self.dialect)[0]
					if isinstance(fieldk, Table) and isinstance(table, Table) and not fieldk.table:
						fieldk.table = table.alias or table.table
					fieldv = Field.parse(v, self.dialect)[0]
					newval[fieldk] = fieldv
				self.terms.append(JoinOn(newval, table, self.table, self.dialect))
			else:
				self.terms.append('USING')
				if not isinstance(val, (tuple, list)):
					val = [val]
				if not isinstance(val, list):
					val = list(val)
				self.terms.append(JoinUsing(val, self.dialect))

		return self

	def sql(self):
		self.table = None
		return ' '.join([str(t)	for t in self.terms])

	def clear(self):
		self.table = None
		del self.terms[:]
