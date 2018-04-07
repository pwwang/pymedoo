import helpers, unittest
from medoo.medooBase import MedooBase
from medoo import Medoo, MedooSqlite

import sqlite3
		
class TestMedooBase(helpers.TestCase):
	
	def testInit(self):
		self.assertRaises(NotImplementedError, MedooBase)
		
class TestMedoo(helpers.TestCase):

	def dataProvider_testNew(self):
		yield {}, None, ValueError
		yield {'database_type': 'aaa'}, None, ValueError
		yield {'database_type': 'sqlite'}, MedooSqlite
		yield {'database_type': 'sqlite3'}, MedooSqlite
	
	def testNew(self, kwargs, instance, exception = None):
		if exception:
			self.assertRaises(exception, Medoo, **kwargs)
		else:
			m = Medoo(**kwargs)
			self.assertIsInstance(m, instance)
		
if __name__ == '__main__':
	unittest.main(verbosity = 2)
