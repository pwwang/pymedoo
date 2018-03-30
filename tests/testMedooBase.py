import helpers, unittest
from pypika import Query, Table, Field
from medoo.medooBase import MedooParser, MedooNameParseError, MedooRecords, MedooBase
from medoo import Medoo, MedooSqlite, MedooInitializationError

import sqlite3

class TestMedooParser(helpers.TestCase):
	
	def dataProvider_testAlias(self):
		yield 'a', ('a', '')
		yield 'a(b)', ('a', 'b')
		yield 'ab)', None, MedooNameParseError
	
	def testAlias(self, input, output, exception = None):
		if exception:
			self.assertRaises(exception, MedooParser.alias, input)
		else:
			out = MedooParser.alias(input)
			self.assertTupleEqual(out, output)
			
	def dataProvider_testWhereExpr(self):
		table = Table('table')
		yield table, 'col', 'v', table.col == 'v'
		yield table, 'col', ['v1', 'v2'], table.col.isin(['v1', 'v2'])
		yield table, 'col[!]', 'v', table.col != 'v'
		yield table, 'col[!]', ['v1', 'v2'], table.col.notin(['v1', 'v2'])
		yield table, 'table.col[>]', 1, table.col > 1
		yield table, 'table.col[<]', 1, table.col < 1
		yield table, 'table.col[>=]', 1, table.col >= 1
		yield table, 'table.col[<=]', 1, table.col <= 1
		yield table, 'table.col[<>]', (1, 2), table.col[1:2]
		yield table, 'table.col[><]', (1, 2), not table.col[1:2]
		yield table, 'table.col[~]', 'a', table.col.like('%a%')
		yield table, 'table.col[~]', 'a%', table.col.like('a%')
		yield table, 'col1[>]col2', True, table.col1 > table.col2
	
	def testWhereExpr(self, table, name, val, output, exception = None):
		if exception:
			self.assertRaises(exception, MedooParser.whereExpr, table, name, val)
		else:
			ret = MedooParser.whereExpr(table, name, val)
			self.assertEqual(str(ret), str(output))
			
	def dataProvider_testWhere(self):
		table = Table('table')
		yield table, {
			'AND': {
				'a': 'b',
				'c[<>]': (10,20),
				'OR': {
					'd[~]': 'x',
					'd[!]': 'z'
				}
			}
		}, None, (table.a == 'b') & table.c[10:20] & ((table.d.like('%x%')) | (table.d != 'z'))
	
	def testWhere(self, table, key, val, output, exception = None):
		if exception:
			self.assertRaises(exception, MedooParser.where, table, key, val)
		else:
			ret = MedooParser.where(table, key, val)
			self.assertEqual(str(ret), str(output))

CONN = sqlite3.connect(':memory:')
CURSOR = CONN.cursor()
CURSOR.execute('create table "test" (a text)')
CURSOR.execute('insert into "test" values (\'a\')')
CURSOR.execute('insert into "test" values (\'b\')')
CURSOR.execute('insert into "test" values (\'c\')')
CONN.commit()
class TestMedooRecords(helpers.TestCase):
	
	def dataProvider_testInit(self):
		yield CURSOR,
	
	def testInit(self, cursor):
		mr = MedooRecords(cursor)
		self.assertIsInstance(mr.cursor, sqlite3.Cursor)
		self.assertIsNone(mr.recordClass)
	
	def dataProvider_testNext(self):
		CURSOR.execute('select * from "test"')
		mr = MedooRecords(CURSOR)
		yield mr, ('a', )
		yield mr, ('b', )
		yield mr, ('c', )
		
	def testNext(self, mr, out):
		record = next(mr)
		self.assertTupleEqual(record, out)
		
class TestMedooBase(helpers.TestCase):
	
	def testInit(self):
		self.assertRaises(NotImplementedError, MedooBase)
		
class TestMedoo(helpers.TestCase):

	def dataProvider_testNew(self):
		yield {}, None, MedooInitializationError
		yield {'database_type': 'aaa'}, None, MedooInitializationError
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
	CONN.close()