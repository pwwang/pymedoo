import pymssql
from ..base import Base
from ..dialect import Dialect

class DialectMssql(Dialect):
	
	@classmethod
	def limit(klass, limit, offset = None):
		"""
		Require SQL Server >= 2012
		"""
		if offset:
			return 'OFFSET {} ROWS FETCH NEXT {} ROWS ONLY'.format(offset, limit), -1
		else:
			return 'TOP {}'.format(limit), 1

class Mssql(Base):

	def __init__(self, *args, **kwargs):
		super(Mssql, self).__init__(*args, **kwargs)
		self.cursor = self.connection.cursor()
		self.dialect(DialectMssql)
	
	def _connect(self, *args, **kwargs):
		arguments = {
			'server' : '.',
			'port'   : 1433
		}
		arguments.update(kwargs)
		return pymssql.connect(**arguments)
		