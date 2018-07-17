def moduleInstalled(mod):
	try:
		__import__(mod)
		return True
	except ImportError:
		return False

if not moduleInstalled('sqlite3'):
	import sys
	sys.stdout.write('sqlite3 not installed, skip.')
	sys.exit(0)

import testly, sqlite3
from medoo.base import Base
from medoo.builder import Builder, Field
from medoo.dialect import Dialect
from medoo.database.sqlite import Sqlite, DialectSqlite

class TestSqlite(testly.TestCase):

	@staticmethod
	def _prepareDb():
		db = Sqlite(database = ':memory:', dialect = DialectSqlite)
		db.query('CREATE TABLE t (id int auto increment, cont text);')
		data   = [
			(1, 'a'),
			(2, 'b'),
			(3, 'c'),
			(4, 'd'),
			(5, 'e'),
			(6, 'f'),
			(7, 'g'),
			(8, 'h'),
			(9, 'i'),
			(10, 'j')
		]
		db.insert('t', ['id', 'cont'], *data)
		return db

	def dataProvider_test0Init(self):
		yield [], {'database': ':memory:', 'dialect': None}
	
	def test0Init(self, args, kwargs, outs = None):
		outs = outs or {}
		db = Sqlite(*args, **kwargs)
		self.assertEqual(db.logging, outs.get('logging', False))
		self.assertEqual(bool(db.connection), outs.get('connection', True))
		self.assertEqual(bool(db.cursor), outs.get('cursor', True))
		self.assertEqual(db.history, outs.get('history', []))
		self.assertEqual(db.errors, outs.get('errors', []))
		self.assertEqual(db.sql, outs.get('sql'))
		self.assertIs(Builder.DIALECT, outs.get('dialect', DialectSqlite))
		self.assertIsInstance(db.builder, Builder)
		self.assertEqual(db.last(), '')
		self.assertEqual(db.log(), [])

	def test1Dialect(self):
		db = Sqlite(database = ':memory:', dialect = DialectSqlite)
		self.assertIs(Builder.DIALECT, DialectSqlite)
		db.dialect()
		self.assertIs(Builder.DIALECT, Dialect)
		db.dialect(DialectSqlite)
		self.assertIs(Builder.DIALECT, DialectSqlite)

	def test2Insert(self):
		db = TestSqlite._prepareDb()
		r = db.insert('t', {'id': 1, 'cont': 'k'})
		self.assertTrue(r)
		rs = db.select('t', where = {'id': 1})
		self.assertEqual(len(rs.all()), 2)

		r = db.insert('t', ['id', 'cont'], (1, 'l'), (1, 'm'))
		rs = db.select('t', where = {'id': 1})
		self.assertEqual(len(rs), 0)
		self.assertEqual(len(rs.all()), 4)

		rs = db.select('t', 'id', distinct = True)
		self.assertEqual(len(rs.all()), 10)

	def test3Update(self):
		db = TestSqlite._prepareDb()
		r = db.update('t', {'cont': 'A'}, {'id':1})
		self.assertTrue(r)
		rs = db.select('t', where = {'id':1})
		self.assertEqual(rs[0].cont, 'A')

	def test4Delete(self):
		db = TestSqlite._prepareDb()
		r = db.delete('t', {'id':1})
		self.assertTrue(r)
		rs = db.select('t', where = {'id':1})
		self.assertEqual(len(rs.all()), 0)

	def test5HasGet(self):
		db = TestSqlite._prepareDb()
		r  = db.has('t', where = {'id': 1})
		self.assertTrue(r)
		r  = db.has('t', where = {'id': 20})
		self.assertFalse(r)
		r  = db.get('t', 'cont', where = {'id': 1})
		self.assertEqual(r, 'a')

	def testSubquery(self):
		db = TestSqlite._prepareDb()
		rs = db.select('t', where = {
			'id': db.builder.select('t', 'id', where = {'id[<]':5})
		})
		
		rs = db.select([
			't(t1)',
			db.builder.select('t', 'id', sub = 't2')
		], 't1.id(id1),t2.id(id2)', where = {'id1[<]': 3, 'id1': Field('id2')})

		self.assertEqual(len(rs.all()), 2)
		self.assertEqual(rs[0], {'id1': 1, 'id2': 1})
		self.assertEqual(rs[1], {'id1': 2, 'id2': 2})



#if __name__ == '__main__':
#	testly.main(verbosity = 2)

import sys
sys.argv = ['testRecord.py']
testly.main(verbosity = 2)