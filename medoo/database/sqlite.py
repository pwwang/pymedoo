"""Sqlite3 adapter"""
import sqlite3
import six
from ..base import Base
from ..dialect import Dialect

class DialectSqlite(Dialect):
    """Sqlite dialect"""
    @staticmethod
    def value(item):
        """Get the value"""
        if isinstance(item, six.string_types):
            return "'%s'" % item.replace("'", "''")
            #return "'{}'".format(item.replace("'", "''"))
        if isinstance(item, bool):
            return str(int(item))
        if item is None:
            return 'NULL'
        return str(item)

class Sqlite(Base):
    """Sqlite medoo wrapper"""
    def __init__(self, *args, **kwargs):
        database = kwargs.pop('database', kwargs.pop('database_file', None))
        if database is not None and database.startswith('file://'):
            database = database.replace('file://', '')
        if database is not None:
            kwargs['database'] = database
        super(Sqlite, self).__init__(*args, **kwargs)
        self.cursor = self.connection.cursor()
        self.dialect(DialectSqlite)

    def _connect(self, *args, **kwargs):
        arguments = {
            'database'         : ':memory:',
            'timeout'          : 5.0,
            'detect_types'     : 0,
            'isolation_level'  : None,
            'check_same_thread': False,
            'cached_statements': 100,
            #'factory'      : [str][0],
        }
        # 'database' had been made sure to replace 'database_file'
        # in __init__
        arguments.update(kwargs)
        return sqlite3.connect(**arguments)
