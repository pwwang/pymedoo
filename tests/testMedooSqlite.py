import helpers, unittest, sqlite3
from medoo.medooSqlite import MedooSqliteRecord, MedooSqlite

class TestMedooSqliteRecord(helpers.TestCase):
	
	def dataProvider_testInit(self):
		yield {'a':1, 'b':2},
		
	def testInit(self, record):
		r = MedooSqliteRecord(record)
		self.assertDictEqual(r, record)
	
	def dataProvider_testGetSetAttr(self):
		yield 'a', 1
		yield 'b', 2
	
	def testGetSetAttr(self, key, val):
		r = MedooSqliteRecord({})
		setattr(r, key, val)
		self.assertEqual(getattr(r, key), val)
		
class TestMedooSqlite(helpers.TestCase):
	
	def dataProvider_testInit(self):
		yield {},
	
	def testInit(self, args):
		m = MedooSqlite(**args)
		self.assertIsInstance(m.cursor, sqlite3.Cursor)
	
	def dataProvider_testSelect(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table" ("a" text, "b" text)')
		m.cursor.execute('insert into "table" values(\'a1\', \'b1\')')
		m.cursor.execute('insert into "table" values(\'a2\', \'b2\')')
		m.cursor.execute('insert into "table" values(\'a3\', \'b3\')')
		m.cursor.execute('insert into "table" values(\'a4\', \'b4\')')
		m.cursor.execute('insert into "table" values(\'a5\', \'b5\')')
		m.cursor.execute('insert into "table" values(\'a6\', \'b6\')')
		m.connection.commit()
		yield m, 'table', None, ['a'], {'b': 'b1'}, {"a": "a1"}
		yield m, 'table', None, '*', {'b[~]': 'b%', 'b[!]': 'b1'}, {"a": "a2", "b": "b2"}
	
	def testSelect(self, m, table, join, columns, where, record):
		rs = m.select(table, join, columns, where)
		r  = next(rs)
		self.assertDictEqual(r, record)
		for k, v in record.items():
			self.assertEqual(getattr(r, k), v)
			
	def dataProvider_testInsert(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table" ("a" text, "b" text)')
		m.connection.commit()
		yield m, "table", ["a", "b"], [("a1", "b1"), ("a2", "b2")], True
			
	def testInsert(self, m, table, columns, datas, commit):
		data = datas.pop(0)
		m.insert(table, {col:data[i] for i, col in enumerate(columns)}, *datas, commit = commit)
		rs = m.select(table, None, columns)
		r = next(rs)
		for i, col in enumerate(columns):
			self.assertEqual(getattr(r, col), data[i])
			
	def dataProvider_testUpdate(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table" ("a" text, "b" int)')
		m.cursor.execute('insert into "table" values(\'a1\', 1)')
		m.cursor.execute('insert into "table" values(\'a2\', 2)')
		m.cursor.execute('insert into "table" values(\'a3\', 3)')
		m.cursor.execute('insert into "table" values(\'a4\', 4)')
		m.cursor.execute('insert into "table" values(\'a5\', 5)')
		m.cursor.execute('insert into "table" values(\'a6\', 6)')
		m.connection.commit()
		yield m, "table", {"b": 12}, {"a": "a6"}
		yield m, "table", {"b[*]": 2}, {"a": "a5"}, {"b": 10}
			
	def testUpdate(self, m, table, data, where, outdata = None, commit = True):
		outdata = outdata or data
		m.update(table, data, where, commit)
		rs = m.select(table, None, outdata.keys(), where)
		r = next(rs)
		for key, val in (outdata or data).items():
			self.assertEqual(getattr(r, key), val)
			
	def dataProvider_testDelete(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table" ("a" text, "b" int)')
		m.cursor.execute('insert into "table" values(\'a1\', 1)')
		m.cursor.execute('insert into "table" values(\'a2\', 2)')
		m.cursor.execute('insert into "table" values(\'a3\', 3)')
		m.cursor.execute('insert into "table" values(\'a4\', 4)')
		m.cursor.execute('insert into "table" values(\'a5\', 5)')
		m.cursor.execute('insert into "table" values(\'a6\', 6)')
		m.connection.commit()
		yield m, "table", {'a': 'a1'}
			
	def testDelete(self, m, table, where, commit = True):
		m.delete(table, where)
		rs = m.select(table, columns = '*', where = where)
		r = next(rs)
		self.assertIsNone(r)
		
	def dataProvider_testHas(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table" ("a" text, "b" int)')
		m.cursor.execute('insert into "table" values(\'a1\', 1)')
		m.cursor.execute('insert into "table" values(\'a2\', 2)')
		m.cursor.execute('insert into "table" values(\'a3\', 3)')
		m.cursor.execute('insert into "table" values(\'a4\', 4)')
		m.cursor.execute('insert into "table" values(\'a5\', 5)')
		m.cursor.execute('insert into "table" values(\'a6\', 6)')
		m.connection.commit()
		yield m, "table", None, 'a', {'a': 'a1'}, True
		yield m, "table", None, 'a', {'a': 'a8'}, False
		
	def testHas(self, m, table, join, columns, where, ret):
		r = m.has(table, join, columns, where)
		self.assertEqual(r, ret)
		
	def dataProvider_testCount(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table" ("a" text, "b" int)')
		m.cursor.execute('insert into "table" values(\'a1\', 1)')
		m.cursor.execute('insert into "table" values(\'a2\', 2)')
		m.cursor.execute('insert into "table" values(\'a3\', 3)')
		m.cursor.execute('insert into "table" values(\'a4\', 4)')
		m.cursor.execute('insert into "table" values(\'a5\', 5)')
		m.cursor.execute('insert into "table" values(\'a6\', 6)')
		m.cursor.execute('insert into "table" values(\'a6\', 6)')
		m.connection.commit()
		yield m, "table", None, '*(c)', None, False, 7
		yield m, "table", None, 'a(c)', None, True, 6
	
	def testCount(self, m, table, join, columns, where, distinct, ret):
		r = m.count(table, join, columns, where, distinct)
		self.assertEqual(r.c, ret)
		
	def dataProvider_testTableExists(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table1" ("a" text, "b" int)')
		m.cursor.execute('create table "table2" ("a" text, "b" int)')
		m.connection.commit()
		yield m, "table1", True
		yield m, "table2", True
		yield m, "table3", False
		
	def testTableExists(self, m, table, ret):
		r = m.tableExists(table)
		self.assertEqual(r, ret)
	
	def dataProvider_testDropTable(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table1" ("a" text, "b" int)')
		m.cursor.execute('create table "table2" ("a" text, "b" int)')
		m.connection.commit()
		yield m, "table1"
		yield m, "table2"
	
	def testDropTable(self, m, table):
		m.dropTable(table)
		self.assertFalse(m.tableExists(table))
		
	def dataProvider_testCreateTable(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table1" ("a" text, "b" int)')
		m.connection.commit()
		yield m, "table1", {"a": "text", "c": "int"}, False
		
	def testCreateTable(self, m, table, schema, drop = True, suffix = ''):
		r = m.createTable(table, schema, drop, suffix)
		if not drop:
			m.insert(table, {'a':1, 'b':2})
			self.assertIs(r, True)
		else:
			self.assertTrue(m.tableExists(table))
		
if __name__ == '__main__':
	unittest.main(verbosity = 2)