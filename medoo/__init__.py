"""A lightweight database framework for python"""

import importlib
from .builder import Raw, Table, Field
from .dialect import Dialect

__version__ = "0.0.8"

DATABASE_TYPES = {
    'Sqlite': ['sqlite', 'sqlite3'],
    'Mysql' : 'mysql',
    'Mssql' : 'mssql',
    'Pgsql' : ['pgsql', 'postgres', 'postgresql'],
}

class Medoo:
    """Main entrance"""
    def __new__(cls, dbtype, *args, **kwargs):

        for key, val in DATABASE_TYPES.items():
            if not isinstance(val, list):
                val = [val]
            if not dbtype.lower() in val:
                continue

            mod = importlib.import_module('.database.{}'.format(key.lower()),
                                          package='medoo')
            klass = getattr(mod, key)
            return klass(*args, **kwargs)

        raise ValueError('Database type not supported: {}.'.format(dbtype))
