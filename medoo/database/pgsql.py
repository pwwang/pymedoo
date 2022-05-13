"""Mysql database"""
import psycopg2
from ..base import Base
from ..dialect import Dialect


class DialectPgsql(Dialect):
    """Mysql dialect"""


class Pgsql(Base):
    """Mysql medoo wrapper"""

    def __init__(self, *args, **kwargs):
        super(Pgsql, self).__init__(*args, **kwargs)
        self.cursor = self.connection.cursor()
        self.dialect(DialectPgsql)

    def _connect(self, *args, **kwargs):
        arguments = {"port": 5432}
        arguments.update(kwargs)
        return psycopg2.connect(**arguments)
