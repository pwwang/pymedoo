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
		
if __name__ == '__main__':
	unittest.main(verbosity = 2)