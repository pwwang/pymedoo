"""Dialect for different databases"""
import six
from .exception import WhereParseError, AnyAllSomeParseError
#import builder

class Dialect:
    """Dialect class"""
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
        """How to quote values"""
        if isinstance(item, six.string_types):
            if item == '*':
                return item
            return '"%s"' % item.replace('"', '""')
        # TODO: possible injection
        return str(item)

    @staticmethod
    def value(item):
        """How is VALUE being quoted"""
        if isinstance(item, six.string_types):
            return "'%s'" % item.replace("'", "''")
        # TODO: possible injection
        return str(item)

    @classmethod
    def limit(cls, limit, offset=None):
        """How is LIMIT being interpreted"""
        fmt = 'LIMIT {limit}'
        if offset:
            fmt += ' OFFSET {offset}'
        return fmt.format(limit=limit, offset=offset)

    @classmethod
    def up_eq(cls, field, value):
        """Equal (assignment) in UPDATE clause"""
        return '{}={}'.format(field, cls.value(value))

    @classmethod
    def is_(cls, field, value):
        """How is IS being interpreted"""
        if not value is None:
            raise WhereParseError('IS is only used to tell NULL (None)')
        return '{} IS NULL'.format(field)

    @classmethod
    def eq(cls, field, value): # pylint: disable=invalid-name
        """How is EQ being interpreted"""
        from . import builder
        if isinstance(value, (tuple, list)) and len(value) == 1:
            value = value[0]

        if isinstance(value, (tuple, list)):
            return '{} IN ({})'.format(
                field,
                ','.join([cls.value(v) for v in value])
            )
        if isinstance(value, builder.Builder): # subquery
            return '{} IN ({})'.format(field, cls.value(value))
        return '{} = {}'.format(field, cls.value(value))

    @classmethod
    def like(cls, field, value, addperct=True):
        """How is LIKE being interpreted"""
        if isinstance(value, (tuple, list)) and len(value) == 1:
            value = value[0]

        if isinstance(value, (tuple, list)):
            if addperct:
                value = [
                    '%{}%'.format(v)
                    if not v.startswith('%') and not v.endswith('%') else v
                    for v in value
                    if isinstance(v, six.string_types)
                ]
            return '({})'.format(' OR '.join(
                ['{} LIKE {}'.format(field, cls.value(v)) for v in value]
            ))

        if (addperct and
                isinstance(value, six.string_types) and
                not value.startswith('%') and
                not value.endswith('%')):
            value = '%{}%'.format(value)
        return '{} LIKE {}'.format(field, cls.value(value))

    @classmethod
    def ne(cls, field, value): # pylint:disable=invalid-name
        """How is NE being interpreted"""
        from . import builder
        if isinstance(value, (tuple, list)) and len(value) == 1:
            value = value[0]

        if isinstance(value, (tuple, list)):
            return '{} NOT IN ({})'.format(
                field,
                ','.join([cls.value(v) for v in value])
            )
        if isinstance(value, builder.Builder):
            return '{} NOT IN ({})'.format(field, cls.value(value))
        return '{} <> {}'.format(field, cls.value(value))

    @classmethod
    def between(cls, field, value):
        """How is BETWEEN being interpreted"""
        if not isinstance(value, (tuple, list)) or len(value) != 2:
            raise WhereParseError('BETWEEN value should a tuple or '
                                  'list with 2 elements.')
        return '{} BETWEEN {} AND {}'.format(
            field,
            cls.value(value[0]),
            cls.value(value[1])
        )

    @classmethod
    def _default(cls, oprt, field, value):
        from . import builder
        if (oprt.lower().endswith('any') or
                oprt.lower().endswith('all') or
                oprt.lower().endswith('some')):
            oprt = oprt.upper()
            if not isinstance(value, builder.Term):
                raise AnyAllSomeParseError('Require a subquery for '
                                           'ALL/ANY/SOME statement.')
            value = '({})'.format(value)
        else:
            value = cls.value(value)
        return '{} {} {}'.format(field, oprt, value)

    @classmethod
    def _up_default(cls, oprt, field, value):
        value = cls.value(value)
        return '{0}={0}{1}{2}'.format(field, oprt, value)

    @classmethod
    def _join_default(cls, jointype):
        return jointype

    @classmethod
    def _operator(cls, oprt, field, value):
        oprt = oprt or '='
        oprtmap = cls.OPERATOR_MAP
        if oprt in oprtmap:
            oprt = oprtmap[oprt]
        else:
            sup = super(cls, cls)
            if hasattr(sup, 'OPERATOR_MAP'):
                oprt = sup.OPERATOR_MAP.get(oprt, oprt)

        if hasattr(cls, oprt):
            return getattr(cls, oprt)(field, value)
        return cls._default(oprt, field, value)

    @classmethod
    def _update(cls, oprt, field, value):
        oprt = oprt or '='
        upmap = cls.UPDATE_MAP
        if oprt in upmap:
            oprt = upmap[oprt]
        else:
            sup = super(cls, cls)
            if hasattr(sup, 'UPDATE_MAP'):
                oprt = sup.UPDATE_MAP.get(oprt, oprt)

        if hasattr(cls, oprt):
            return getattr(cls, oprt)(field, value)
        return cls._up_default(oprt, field, value)

    @classmethod
    def _join(cls, jointype=None):
        jointype = jointype or '><'
        joinmap = cls.JOIN_MAP
        if jointype in joinmap:
            jointype = joinmap[jointype]
        else:
            sup = super(cls, cls)
            if hasattr(sup, 'JOIN_MAP'):
                jointype = sup.JOIN_MAP.get(jointype, jointype)

        if hasattr(cls, jointype):
            return getattr(cls, jointype)()
        return cls._join_default(jointype)
