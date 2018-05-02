import helpers, unittest, sqlite3
from medoo.medooSqlite import MedooSqlite
from medoo import Field, Function

class TestMedooSqlite(helpers.TestCase):

	def dataProvider_testInit(self):
		yield {},

	def testInit(self, args):
		m = MedooSqlite(**args)
		self.assertIsInstance(m.cursor, sqlite3.Cursor)

	def dataProvider_testSelect(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table1" ("a" int, "b" text)')
		m.cursor.execute('insert into "table1" values(1, \'b1\')')
		m.cursor.execute('insert into "table1" values(2, \'b2\')')
		m.cursor.execute('insert into "table1" values(3, \'b3\')')
		m.cursor.execute('insert into "table1" values(4, \'b4\')')
		m.cursor.execute('insert into "table1" values(5, \'b5\')')
		m.cursor.execute('insert into "table1" values(6, \'b6\')')
		m.cursor.execute('create table "table2" ("c" int, "d" text, "e" int)')
		m.cursor.execute('insert into "table2" values(1, \'d1\', 3)')
		m.cursor.execute('insert into "table2" values(2, \'d2\', 4)')
		m.cursor.execute('insert into "table2" values(3, \'d3\', 1)')
		m.cursor.execute('insert into "table2" values(4, \'d4\', 5)')
		m.cursor.execute('insert into "table2" values(5, \'d5\', 2)')
		m.cursor.execute('insert into "table2" values(6, \'d6\', 6)')
		m.connection.commit()
		#yield m, '[<>]table1', None, '*', None, None, ValueError
		yield m, 'table1', None, ['a'], {'b': 'b1'}, {"a": 1}
		yield m, 'table2', None, '*', {'d[~]': 'd%', 'd[!]': 'd1'}, {"c": 2, "d": "d2", "e": 4}
		yield m, 'table1(t1)', None, 't1.b', {'t1.a': 5}, {"b": "b5"}
		yield m, 'table1(t1)', None, 't1.b(x)', {'t1.a': 5}, {"x": "b5"}
		yield m, 'table1(t1)', {'[><]table2(t2)': {'c': 'a'}}, 't2.d(y)', {'t1.a': 6}, {"y": "d6"}
		yield m, 'table1(t1)', {'table2(t2)': {'c': 'a'}}, 't2.d(y)', {'t1.a[>]':Field('t2.e')}, {"y": "d3"}
		yield m, 'table1(t1)', {'table2(t2)': {'c': 'a'}}, 't2.d(y)', {'t1.a[>]':Field('t2.e') + 2}, {"y": "d5"}

	def testSelect(self, m, table, join, columns, where, record, exception = None):
		if exception:
			self.assertRaises(exception, m.select, table, columns, where, join = join)
		else:
			rs = m.select(table, columns, where, join = join)
			r = rs.fetchone()
			self.assertDictEqual(r, record)
			for k, v in record.items():
				self.assertEqual(getattr(r, k), v)

	def dataProvider_testInsert(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table" ("a" text, "b" text)')
		m.connection.commit()
		#yield m, "[><]table", ["t1.a", "b"], [("a1", "b1")], True, ValueError
		#yield m, "table", ["t1.a", "b"], [("a1", "b1")], True, ValueError
		#yield m, "table", ["a(x)", "b"], [("a1", "b1")], True, ValueError
		yield m, "table", ["a", "b"], [("a1", "b1"), ("a2", "b2")], True

	def testInsert(self, m, table, columns, datas, commit, exception = None):
		data = datas.pop(0)
		data = {col:data[i] for i, col in enumerate(columns)}
		if exception:
			self.assertRaises(exception, m.insert, table, data, *datas, commit = commit)
		else:
			m.insert(table, data, *datas, commit = commit)
			rs = m.select(table, columns)
			for i, r in enumerate(rs.fetchall()):
				if i == 0:
					self.assertDictEqual(r, data)
				else:
					self.assertTupleEqual(tuple(r.values()), datas[i-1])

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
		yield m, "table", {"a": Function.concat('a', value = 'append')}, {"b": 4}, {"a": "a4append"}

	def testUpdate(self, m, table, data, where, outdata = None, commit = True):
		outdata = outdata or data
		m.update(table, data, where, commit)
		rs = m.select(table, outdata.keys(), where)
		r = rs.fetchone()
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
		self.assertIsNone(rs.fetchone())

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
		yield m, "table", 'a', {'a': 'a1'}, True
		yield m, "table", 'a', {'a': 'a8'}, False

	def testHas(self, m, table, columns, where, ret):
		r = m.has(table, columns, where)
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
		yield m, "table", Function.count('*(c)', alias = 'c'), None, 7
		yield m, "table", Function.count(Function.distinct('a(c)'), alias = 'c'), None, 6

	def testCount(self, m, table, columns, where, ret):
		r = m.select(table, columns, where)
		self.assertEqual(r.fetchone().c, ret)

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
		m.drop(table)
		self.assertFalse(m.tableExists(table))

	def dataProvider_testCreateTable(self):
		m = MedooSqlite()
		m.cursor.execute('create table "table1" ("a" text, "b" int)')
		m.connection.commit()
		yield m, "table1", {"a": "text", "c": "int"}, True
		yield m, "table1", {"a": "text", "c": "int"}, False

	def testCreateTable(self, m, table, schema, ifnotexists = True, suffix = ''):
		self.assertTrue(m.tableExists(table))
		if not ifnotexists:
			self.assertRaises(Exception, m.create, table, schema, ifnotexists, suffix)
			m.drop(table)
			m.create(table, schema, ifnotexists, suffix)
			m.insert(table, {'a':1, 'c':2})
		else:
			m.create(table, schema, ifnotexists, suffix)
			m.insert(table, {'a':1, 'b':2})


if __name__ == '__main__':
	unittest.main(verbosity = 2)
