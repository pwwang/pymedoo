

import pytest
from . import moduleInstalled
pytestmark = pytest.mark.skipif(not moduleInstalled('sqlite3'), reason = 'sqlite3 is not installed.')

import sqlite3
from medoo.base import Base
from medoo.builder import Builder, Field
from medoo.dialect import Dialect
from medoo.database.sqlite import Sqlite, DialectSqlite

@pytest.fixture
def db():
	"""Create a database for test"""
	db = Sqlite(database_file = 'file://:memory:', dialect = DialectSqlite)
	db.query('CREATE TABLE t (id int auto increment, cont text, icont INTEGER);')
	data   = [
		(1, 'a', 0),
		(2, 'b', 1),
		(3, 'c', 2),
		(4, 'd', 9),
		(5, 'e', 3),
		(6, None, 3),
		(7, 'g', 4),
		(8, 'h', 5),
		(9, 'i', 3),
		(10, 'j', 1)
	]
	db.insert('t', ['id', 'cont', 'icont'], *data)
	yield db

class TestSqlite(object):

	@pytest.mark.parametrize('args, kwargs, outs', [
		([], {'database': ':memory:', 'dialect': None}, None)
	])
	def test0Init(self, args, kwargs, outs):
		outs = outs or {}
		db = Sqlite(*args, **kwargs)
		assert db.logging == outs.get('logging', False)
		assert bool(db.connection) is outs.get('connection', True)
		assert bool(db.cursor) is outs.get('cursor', True)
		assert db.history == outs.get('history', [])
		assert db.errors == outs.get('errors', [])
		assert db.sql == outs.get('sql')
		assert Builder.DIALECT is outs.get('dialect', DialectSqlite)
		assert isinstance(db.builder, Builder)
		assert db.last() == ''
		assert db.log() == []

	def test1Dialect(self):
		db = Sqlite(database = ':memory:', dialect = DialectSqlite)
		assert Builder.DIALECT is DialectSqlite
		db.dialect()
		assert Builder.DIALECT is Dialect
		db.dialect(DialectSqlite)
		assert Builder.DIALECT is DialectSqlite

	def test2Insert(self, db):
		r = db.insert('t', {'id': 1, 'cont': 'k'})
		assert r
		rs = db.select('t', where = {'id': 1})
		assert len(rs.all()) == 2

		r = db.insert('t', ['id', 'cont'], (1, 'l'), (1, 'm'))
		rs = db.select('t', where = {'id': 1})
		assert len(rs) == 0
		assert len(rs.all()) == 4

		rs = db.select('t', 'id', distinct = True)
		assert len(rs.all()) == 10

	def test3Update(self, db):
		r = db.update('t', {'cont': 'A'}, {'id':1})
		assert r
		rs = db.select('t', where = {'id':1})
		assert rs[0].cont == 'A'
		# update 7 to None, issue #4
		r = db.update('t', {'cont': None}, {'id':7})
		assert db.get('t', 'cont', where = {'id': 7}) is None

		# update 8 to True, issue #4
		r = db.update('t', {'icont': True}, {'id':8})
		assert db.get('t', 'icont', where = {'id':8}) == 1


	def test4Delete(self, db):
		r = db.delete('t', {'id':1})
		assert r
		rs = db.select('t', where = {'id':1})
		assert len(rs.all()) == 0

	def test5HasGet(self, db):
		r  = db.has('t', where = {'id': 1})
		assert r
		r  = db.has('t', where = {'id': 20})
		assert not r
		r  = db.get('t', 'cont', where = {'id': 1})
		assert r == 'a'

	def testSubquery(self, db):
		rs = db.select('t', where = {
			'id': db.builder.select('t', 'id', where = {'id[<]':5})
		})

		rs = db.select([
			't(t1)',
			db.builder.select('t', 'id', sub = 't2')
		], 't1.id(id1),t2.id(id2)', where = {'id1[<]': 3, 'id1': Field('id2')})

		assert len(rs.all()) == 2
		assert rs[0] == {'id1': 1, 'id2': 1}
		assert rs[1] == {'id1': 2, 'id2': 2}
