"""Mysql database"""
import pymysql
from ..base import Base
from ..dialect import Dialect

class DialectMysql(Dialect):
	"""Mysql dialect"""

class Mysql(Base):
	"""Mysql medoo wrapper"""
	def __init__(self, *args, **kwargs):
		super(Mysql, self).__init__(*args, **kwargs)
		self.cursor = self.connection.cursor()
		self.dialect(DialectMysql)

	def _connect(self, *args, **kwargs):
		arguments = {
			'host': 'localhost',
			'port': 3306
		}
		arguments.update(kwargs)
		return pymysql.connect(**arguments)
