"""Mysql database"""
import six
import mysql.connector
from ..base import Base
from ..dialect import Dialect

class _MysqlConnectorCursor(object):
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
