import helpers, unittest
from pypika import Query, Table, Field, JoinType
from medoo.medooBase import MedooParser, MedooRecords, MedooBase
from medoo import Medoo, MedooSqlite, MedooInitializationError, MedooTableParseError, MedooFieldParseError, MedooWhereParseError

import sqlite3

class TestMedooParser(helpers.TestCase):
	
	def dataProvider_testTable(self):
		yield 'a', None, 'a', None
		yield 'a(b)', None, 'a', 'b'
		yield 'ab)', None, None, None, MedooTableParseError
		yield 'table(alias)', None, 'table', 'alias'
		yield '[><]table(alias) # comment', JoinType.inner, 'table', 'alias'
		yield '[<>]table(alias) # comment', JoinType.outer, 'table', 'alias'
	
	def testTable(self, input, join, table, alias, exception = None):
		if exception:
			self.assertRaises(exception, MedooParser.table, input)
		else:
			rjoin, rtable, ralias = MedooParser.table(input)
			self.assertEqual(rjoin, join)
			self.assertEqual(rtable, table)
			self.assertEqual(ralias, alias)
			
	def dataProvider_testField(self):
		yield 'a(b', None, None, None, None, MedooFieldParseError
		yield 'a[+]', None, 'a', None, '__add__'
		yield 'a[-]', None, 'a', None, '__sub__'
		yield 'a[*]', None, 'a', None, '__mul__'
		yield 'a[/]', None, 'a', None, '__div__'
		yield 'a[**]', None, 'a', None, '__pow__'
		yield 'a[MOD]', None, 'a', None, '__mod__'
		yield 'table.field(alias)[+] # comment', 'table', 'field', 'alias', '__add__'
			
	def testField(self, input, table, field, alias, operation, exception = None):
		if exception:
			self.assertRaises(exception, MedooParser.field, input)
		else:
			rtable, rfield, ralias, roperation = MedooParser.field(input)
			self.assertEqual(rtable, table)
			self.assertEqual(rfield, field)
			self.assertEqual(ralias, alias)
			self.assertEqual(roperation, operation)
			
	def dataProvider_testAlwaysList(self):
		yield 'a', ['a']
		yield ['a', 'b'], ['a', 'b']
		yield 'a, b', ['a', 'b']
		yield ('a', 'b'), ['a', 'b']
			
	def testAlwaysList(self, input, output):
		ret = MedooParser.alwaysList(input)
		self.assertListEqual(ret, output)
			
	def dataProvider_testWhereExpr(self):
		table  = Table('table')
		#0
		yield 'col', 'v', table.col == 'v'
		yield 'col', ['v1', 'v2'], table.col.isin(['v1', 'v2'])
		yield 'col[!]', 'v', table.col != 'v'
		yield 'col[!]', ['v1', 'v2'], table.col.notin(['v1', 'v2'])
		yield 'table.col[>]', 1, table.col > 1
		#5
		yield 'table.col[<]', 1, table.col < 1
		yield 'table.col[>=]', 1, table.col >= 1
		yield 'table.col[<=]', 1, table.col <= 1
		yield 'table.col[<>]', (1, 2), table.col[1:2]
		yield 'table.col[><]', (1, 2), not table.col[1:2]
		#10
		yield 'table.col[~]', 'a', table.col.like('%a%')
		yield 'table.col[~]', ['a', 'b'], table.col.like('%a%') | table.col.like('%b%')
		yield 'table.col[!~]', ['a', 'b'], table.col.not_like('%a%') & table.col.not_like('%b%')
		yield 'table.col[~]', 'a%', table.col.like('a%')
		yield 'col1[>]', Field('col2'), table.col1 > table.col2
		#15
		yield 'col1[>=]', Field('col2') + 2, table.col1 >= table.col2 + 2
		yield 't1.col1[>=]', Field('col2') + 2, Table("t1").col1 >= table.col2 + 2
		
	def testWhereExpr(self, name, val, output, exception = None):
		if exception:
			self.assertRaises(exception, MedooParser.whereExpr, name, val)
		else:
			ret = MedooParser.whereExpr(name, val)
			self.assertEqual(str(ret), str(output))
			
	def dataProvider_testWhere(self):
		table = Table('table')
		yield {
			'AND': {
				'a': 'b',
				'c[<>]': (10,20),
				'OR': {
					'd[~]': 'x',
					'd[!]': 'z'
				}
			}
		}, None, (table.a == 'b') & table.c[10:20] & ((table.d.like('%x%')) | (table.d != 'z'))
		yield {'a': 'b'}, None, table.a == "b"
		yield {'a': 'b', 'c': 'd'}, None, (table.a == "b") & (table.c == 'd')
		yield {'AND': {'a': 'b'}}, None, None, MedooWhereParseError
		yield {'AND': {'a': 'b', 'c': 'd'}}, None, (table.a == "b") & (table.c == 'd')
		yield {'OR': {'a': 'b'}}, None, None, MedooWhereParseError
		yield {'OR': {'a': 'b', 'c': 'd'}}, None, (table.a == "b") | (table.c == 'd')
		yield 'a[!]', 'b', table.a != 'b'
	
	def testWhere(self, key, val, output, exception = None):
		if exception:
			self.assertRaises(exception, MedooParser.where, key, val)
		else:
			ret = MedooParser.where(key, val)
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