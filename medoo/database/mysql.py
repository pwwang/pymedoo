"""Mysql database"""
import six
import mysql.connector
from ..base import Base
from ..dialect import Dialect

class _MysqlConnectorCursor:
    """Wrap up mysql.connector.cursor object
    When there is no more records, mysql.connector.cursor returns None
    However, more pythonic way is to raise a StopIteration Exception.
    """
    def __init__(self, mccursor):
        self._mccursor = mccursor

    def __getattr__(self, name):
        return getattr(self._mccursor, name)

    def __next__(self):
        ret = next(self._mccursor)
        if ret is None:
            raise StopIteration()
        return ret

    def __iter__(self):
        return self

class DialectMysql(Dialect):
    """Mysql dialect"""

    @staticmethod
    def quote(item):
        if isinstance(item, six.string_types):
            if item == '*':
                return item
            return '`%s`' % item.replace('`', '``')
        return str(item)

    @staticmethod
    def value(item):
        if isinstance(item, six.string_types):
            # borrowed from
            # https://github.com/PyMySQL/PyMySQL/blob/3e71dd32e8ce868b090c282759eebdeabc960f58/pymysql/converters.py#L64
            # fixes #8
            _escape_table = [chr(x) for x in range(128)]
            _escape_table[0] = u'\\0'
            _escape_table[ord('\\')] = u'\\\\'
            _escape_table[ord('\n')] = u'\\n'
            _escape_table[ord('\r')] = u'\\r'
            _escape_table[ord('\032')] = u'\\Z'
            _escape_table[ord('"')] = u'\\"'
            _escape_table[ord("'")] = u"\\'"
            return "'%s'" % item.translate(_escape_table)
        return str(item)

class Mysql(Base):
    """Mysql medoo wrapper"""
    def __init__(self, *args, **kwargs):
        super(Mysql, self).__init__(*args, **kwargs)
        self.cursor = _MysqlConnectorCursor(
            self.connection.cursor(buffered=True)
        )
        self.dialect(DialectMysql)

    def _connect(self, *args, **kwargs):
        arguments = {
            'host': 'localhost',
            'port': 3306
        }
        arguments.update(kwargs)
        return mysql.connector.connect(**arguments)
