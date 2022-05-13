"""Oracle database"""
import cx_Oracle
from ..base import Base
from ..dialect import Dialect


class DialectOracle(Dialect):
    """Oracle dialect"""


class Oracle(Base):
    """Oracle medoo wrapper"""

    def __init__(self, *args, **kwargs):
        super(Oracle, self).__init__(*args, **kwargs)
        self.cursor = self.connection.cursor()
        self.dialect(DialectOracle)

    def _connect(self, *args, **kwargs):
        arguments = {
            # some default settings
        }
        arguments.update(kwargs)
        return cx_Oracle.connect(**arguments)
