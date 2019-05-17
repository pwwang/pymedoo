import six
from .exception import WhereParseError, AnyAllSomeParseError
#import builder

class Dialect(object):

	OPERATOR_MAP = {
		'='  : 'eq',
		'~'  : 'like',
		'!'  : 'ne',
		'!=' : 'ne',
		'<>' : 'between',
		'==' : 'is_',
		'is' : 'is_',
	}

	UPDATE_MAP = {
		'=': 'up_eq'
	}

	JOIN_MAP = {
		'>' : 'LEFT JOIN',
		'<' : 'RIGHT JOIN',
		'<>': 'FULL OUTER JOIN',
		'><': 'INNER JOIN'
	}

	@staticmethod
	def quote(item):
		if isinstance(item, six.string_types):
			if item == '*':
				return item
			return '"%s"' % item.replace('"', '""')
		return str(item)

	@staticmethod
	def value(item):
		if isinstance(item, six.string_types):
			return "'%s'" % item.replace("'", "''")
			#return "'{}'".format(item.replace("'", "''"))
		return str(item)

	@classmethod
	def limit(klass, limit, offset = None):
		fmt = 'LIMIT {limit}'
		if offset:
			fmt += ' OFFSET {offset}'
		return fmt.format(limit = limit, offset = offset)

	@classmethod
	def up_eq(klass, field, value):
		return '{}={}'.format(field, klass.value(value))

	@classmethod
	def is_(klass, field, value):
		if not value is None:
			raise WhereParseError('IS is only used to tell NULL (None)')
		return '{} IS NULL'.format(field)

	@classmethod
	def eq(klass, field, value):
		from . import builder
		if isinstance(value, (tuple, list)) and len(value) == 1:
			value = value[0]

		if isinstance(value, (tuple, list)):
			return '{} IN ({})'.format(field, ','.join([klass.value(v) for v in value]))
		elif isinstance(value, builder.Builder): # subquery
			return '{} IN ({})'.format(field, klass.value(value))
		else:
			return '{} = {}'.format(field, klass.value(value))

	@classmethod
	def like(klass, field, value, addperct = True):
		if isinstance(value, (tuple, list)) and len(value) == 1:
			value = value[0]

		if isinstance(value, (tuple, list)):
			if addperct:
				value = [
					'%{}%'.format(v) if not v.startswith('%') and not v.endswith('%') else v
					for v in value
					if isinstance(v, six.string_types)
				]
			return '({})'.format(' OR '.join(['{} LIKE {}'.format(field, klass.value(v)) for v in value]))
		else:
			if addperct and isinstance(value, six.string_types) and not value.startswith('%') and not value.endswith('%'):
				value = '%{}%'.format(value)
			return '{} LIKE {}'.format(field, klass.value(value))

	@classmethod
	def ne(klass, field, value):
		from . import builder
		if isinstance(value, (tuple, list)) and len(value) == 1:
			value = value[0]

		if isinstance(value, (tuple, list)):
			return '{} NOT IN ({})'.format(field, ','.join([klass.value(v) for v in value]))
		elif isinstance(value, builder.Builder):
			return '{} NOT IN ({})'.format(field, klass.value(value))
		else:
			return '{} <> {}'.format(field, klass.value(value))

	@classmethod
	def between(klass, field, value):
		if not isinstance(value, (tuple, list)) or len(value) != 2:
			raise WhereParseError('BETWEEN value should a tuple or list with 2 elements.')
		return '{} BETWEEN {} AND {}'.format(field, klass.value(value[0]), klass.value(value[1]))

	@classmethod
	def _default(klass, oprt, field, value):
		from . import builder
		if oprt.lower().endswith('any') or oprt.lower().endswith('all') or oprt.lower().endswith('some'):
			oprt = oprt.upper()
			if not isinstance(value, builder.Term):
				raise AnyAllSomeParseError('Require a subquery for ALL/ANY/SOME statement.')
			value = '({})'.format(value)
		else:
			value = klass.value(value)
		return '{} {} {}'.format(field, oprt, value)

	@classmethod
	def _up_default(klass, oprt, field, value):
		value = klass.value(value)
		return '{}={}{}{}'.format(field, field, oprt, value)

	@classmethod
	def _join_default(klass, jointype):
		return jointype

	@classmethod
	def _operator(klass, oprt, field, value):
		oprt = oprt or '='
		oprtmap = klass.OPERATOR_MAP
		if oprt in oprtmap:
			oprt = oprtmap[oprt]
		else:
			su = super(klass, klass)
			if hasattr(su, 'OPERATOR_MAP'):
				oprt = su.OPERATOR_MAP.get(oprt, oprt)

		if hasattr(klass, oprt):
			return getattr(klass, oprt)(field, value)
		else:
			return klass._default(oprt, field, value)

	@classmethod
	def _update(klass, oprt, field, value):
		oprt = oprt or '='
		upmap = klass.UPDATE_MAP
		if oprt in upmap:
			oprt = upmap[oprt]
		else:
			su = super(klass, klass)
			if hasattr(su, 'UPDATE_MAP'):
				oprt = su.UPDATE_MAP.get(oprt, oprt)

		if hasattr(klass, oprt):
			return getattr(klass, oprt)(field, value)
		else:
			return klass._up_default(oprt, field, value)

	@classmethod
	def _join(klass, jointype = None):
		jointype = jointype or '><'
		joinmap  = klass.JOIN_MAP
		if jointype in joinmap:
			jointype = joinmap[jointype]
		else:
			su = super(klass, klass)
			if hasattr(su, 'JOIN_MAP'):
				jointype = su.JOIN_MAP.get(jointype, jointype)

		if hasattr(klass, jointype):
			return getattr(klass, jointype)()
		else:
			return klass._join_default(jointype)


