import pytest
from medoo.exception import FieldParseError, TableParseError, WhereParseError, UpdateParseError, JoinParseError, LimitParseError, InsertParseError
from medoo.builder import Term, Raw, Table, Field, TableFrom, Where, WhereTerm, Builder, Order, Limit, Set, Join, JoinTerm
from medoo.dialect import Dialect
from collections import OrderedDict

class DialectTest(Dialect):

	@staticmethod
	def quote(item):
		return str(item)

	@staticmethod
	def limit(lim, offset):
		return 'TOP {}'.format(lim), 1

class DialectOracle(Dialect):

	@staticmethod
	def limit(lim, offset):
		return 'ROWNUM >= {} AND ROWNUM <= {}'.format(offset, offset + lim), 'where'

class TestTerm(object):

	def testStr(self):
		with pytest.raises(NotImplementedError):
			str(Term())

class TestRaw(object):

	@pytest.mark.parametrize('s, out', [
		('s', 's'),
		([], '[]'),
	])
	def testStr(self, s, out):
		assert str(Raw(s)) == out

	def testNe(self):
		assert Raw('a') != Raw('b')

class TestTable(object):

	@pytest.mark.parametrize('table, schema, out_t, out_s, exception', [
		('t', None, 't', None, None),
		('s.t', 's2', None, None, TableParseError),
		('s.t', None, 't', 's', None),
		('s.t.x', None, None, None, TableParseError),
	])
	def testInit(self, table, schema, out_t, out_s, exception):
		if exception:
			with pytest.raises(exception):
				Table(table, schema)
		else:
			t = Table(table, schema)
			assert t.table == out_t
			assert t.schema == out_s

	@pytest.mark.parametrize('table, out', [
		(Table('s.t'), '"s"."t"'),
		(Table('t'), '"t"'),
		(Table('t', schema = 's'), '"s"."t"'),
	])
	def testStr(self, table, out):
		assert str(table) == out

	@pytest.mark.parametrize('t1, t2, out', [
		(Table('s.t'), Table('s.t'), True),
		(Table('s.t'), Table('t'), False),
		(Table('s.t'), Table('t', schema = 's'), True),
	])
	def testEq(self, t1, t2, out):
		assert (t1 == t2) == out

	@pytest.mark.parametrize('tablestr, context, outtable,exception', [
		(Raw('whatevertable'), None, 'whatevertable', None),
		('s.t', None, '"s"."t"', None),
		('s.t', "insert", '"s"."t"', None),
		('s.t', "update", '"s"."t"', None),
		('s.t', "delete", '"s"."t"', None),
		('s.t', "selectinto", '"s"."t"', None),
		('s.t(t1)', 'from', '"s"."t" AS "t1"', None),
		('t(t1)', 'from', '"t" AS "t1"', None),
		('t', 'from', '"t"', None),
		('[]t', 'from', None, TableParseError),
		('t', 'unknown', None, TableParseError),
	])
	def testParse(self, tablestr, context, outtable, exception):
		if exception:
			with pytest.raises(exception):
				Table.parse(tablestr, context)
		else:
			assert Table.parse(tablestr, context) == outtable

	@pytest.mark.parametrize('table, alias, out', [
		('s.t', None, '"s"."t"'),
		('t', None, '"t"'),
		('s.t', 't1', '"s"."t" AS "t1"'),
		('t', "t1", '"t" AS "t1"'),
	])
	def testTableFrom(self, table, alias, out):
		assert str(TableFrom(table, alias)) == out

class TestField(object):

	@pytest.mark.parametrize('fieldstr, table,schema,out_f,out_t,out_s,exception', [
		('f', None, None, 'f', None, None, None),
		('f', 't', 's', 'f', 't', 's', None),
		('s.t.f', None, None, 'f', 't', 's', None),
		('t.f', 't', None, None, None, None, FieldParseError),
		('t.f', None, 's', 'f', 't', 's', None),
		('s.t.f', None, 's', None, None, None , FieldParseError),
		('s.t.f.f1', None, None, None, None, None, FieldParseError),
	])
	def testInit(self, fieldstr, table, schema, out_f, out_t, out_s, exception):
		if exception:
			with pytest.raises(exception):
				Field(fieldstr, table, schema)
		else:
			f = Field(fieldstr, table, schema)
			assert f.field == out_f
			assert f.table == out_t
			assert f.schema == out_s

	@pytest.mark.parametrize('field, out', [
		(Field('s.t.f'), '"s"."t"."f"'),
		(Field('t.f', schema = 's'), '"s"."t"."f"'),
		(Field('f', schema = 's', table = 't'), '"s"."t"."f"'),
	])
	def testStr(self, field, out):
		assert str(field) == out

	@pytest.mark.parametrize('field, oprt, value, out', [
		(Field('f'), '__add__', 1, '"f"+1'),
		(Field('f'), '__sub__', 1, '"f"-1'),
		(Field('f'), '__mul__', 1, '"f"*1'),
		(Field('f'), '__div__', 1, '"f"/1'),
		(Field('f'), '__mod__', 1, '"f"%1'),
	])
	def testOprt(self, field, oprt, value, out):
		assert str(getattr(field, oprt)(value)) == out

	@pytest.mark.parametrize('fieldstr,context,outfield,exception', [
		(Raw('s.t.f'), None, 's.t.f', None),
		('s.t.f', None, '"s"."t"."f"', None),
		('s.t.f', "group", '"s"."t"."f"', None),
		('s.t.f', "group2", None, FieldParseError),
		('s.t.f|count(f1)', 'select', 'COUNT("s"."t"."f") AS "f1"', None),
		('s.t.f|.count(f1)', 'select', 'COUNT(DISTINCT "s"."t"."f") AS "f1"', None),
		('s.t.f|c.ount(f1)', 'select', None, FieldParseError)
	])
	def testParse(self, fieldstr, context, outfield, exception):
		if exception:
			with pytest.raises(exception):
				Field.parse(fieldstr, context)
		else:
			assert Field.parse(fieldstr, context) == outfield

class TestWhere(object):

	@pytest.mark.parametrize('key, val, out, exception', [
		(Raw('f=1'), None, 'f=1', None),
		('aiw@#$', None, None, WhereParseError),
		('!s.t.f[!] # comments', 1, 'NOT "s"."t"."f" <> 1', None),
		('f|count[>]', 1, 'COUNT("f") > 1', None),
	])
	def testTerm(self, key, val, out, exception):
		if exception:
			with pytest.raises(exception):
				str(WhereTerm(key, val))
		else:
			assert str(WhereTerm(key, val)) == out

	@pytest.mark.parametrize('conditions,out,exception', [
		({Raw('f=1'):None}, 'f=1', None),
		({'and #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'f=1 AND f=2', None),
		({'AND #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'f=1 AND f=2', None),
		({'and #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'f=1 AND f=2', None),
		({'or #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'f=1 OR f=2', None),
		({'or #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'f=1 OR f=2', None),
		({'OR #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'f=1 OR f=2', None),
		({'f':1}, '"f" = 1', None),
		(OrderedDict([
			('or # or1', OrderedDict([
				('f3[~]', ('a', 'b', 'c')),
				('!f4[~]', ('a', 'b', 'c'))
			]))
		]), '("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')', None),
		(OrderedDict([
			('t.f1[>]', 1),
			('t.f2[= any]', Builder()._select()),
			('or # or1', OrderedDict([
				('f3[~]', ('a', 'b', 'c')),
				('!f4[~]', ('a', 'b', 'c'))
			])),
			('and #', OrderedDict([
				(Raw('f=1'), None),
				(Raw('f=2'), None)
			]))
		]), '"t"."f1" > 1 AND "t"."f2" = ANY (SELECT *) AND (("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')) AND (f=1 AND f=2)', None),
	])
	def testWhere(self, conditions, out, exception):
		if exception:
			with pytest.raises(exception):
				str(Where(conditions))
		else:
			assert str(Where(conditions)) == out

class TestOrder(object):

	@pytest.mark.parametrize('orders, out, exception', [
		({'@#$':None}, None, FieldParseError),
		(OrderedDict([
			('f1|count', True),
			('s.t.f', None),
			('s.t.f4', False),
			('f2', 'asc'),
			('f3', 'desc')
		]), 'COUNT("f1") ASC,"s"."t"."f" ASC,"s"."t"."f4" DESC,"f2" ASC,"f3" DESC', None)
	])
	def testStr(self, orders, out, exception):
		if exception:
			with pytest.raises(exception):
				str(Order(orders))
		else:
			assert str(Order(orders)) == out

class TestLimit(object):

	@pytest.mark.parametrize('limoff,out,pos,exception', [
		([1], 'LIMIT 1',-1,None),
		((10,5), 'LIMIT 10 OFFSET 5',-1,None),
		([1,2,3], None, -1, LimitParseError)
	])
	def testStr(self, limoff, out, pos, exception):
		if exception:
			with pytest.raises(exception):
				Limit(limoff)
		else:
			lim = Limit(limoff)
			assert str(lim) == out
			assert lim.pos == pos

class TestSet(object):

	@pytest.mark.parametrize('sets, out,exception', [
		({'a':1}, '"a"=1',None),
		(OrderedDict([
			('a', 1),
			('b[+]', 2)
		]), '"a"=1,"b"="b"+2',None),
		({'@#':None}, None, UpdateParseError)
	])
	def testStr(self, sets, out, exception):
		if exception:
			with pytest.raises(exception):
				str(Set(sets))
		else:
			assert str(Set(sets)) == out

class TestJoin(object):

	@pytest.mark.parametrize('key,val,mtable,out,exception',[
		('@#F', None, None, None, JoinParseError),
		('t', ['t.f'], None, None, JoinParseError),
		('t', ('t.f', 'f2'), None, None, JoinParseError),
		('t', ('t.f', 'f2'), None, None, JoinParseError),
		('t', 'f', None, None, JoinParseError),
		('t', 'f', TableFrom('mt'), 'INNER JOIN "t" ON "t"."f"="mt"."f"',None),
		('t', {'t1.f': 'mt.f2'}, None, None, JoinParseError),
		('[<>]t(t1)', ['f1', 'f2'], TableFrom('mt'), 'FULL OUTER JOIN "t" AS "t1" ON "t1"."f1"="mt"."f1" AND "t1"."f2"="mt"."f2"',None),
		('[<]t', {'f': 'f2'}, TableFrom('mt', alias='main'), 'RIGHT JOIN "t" ON "t"."f"="main"."f2"',None),
	])
	def testTerm(self, key, val, mtable, out, exception):
		if exception:
			with pytest.raises(exception):
				JoinTerm(key, val, mtable)
		else:
			assert str(JoinTerm(key, val, mtable)) == out

	@pytest.mark.parametrize('joins,mtable,out,exception', [
		(OrderedDict([
			('t1', 'f'),
			('t2(t3)', 'f'),
			('[>]t4(t5)', 'f'),
			('t6', ['f1', 'f2']),
			('t7', {'f1': 'f2'}),
			('t8', {'f1': 'f2'})
		]), TableFrom('mt'), 'INNER JOIN "t1" ON "t1"."f"="mt"."f" INNER JOIN "t2" AS "t3" ON "t3"."f"="mt"."f" LEFT JOIN "t4" AS "t5" ON "t5"."f"="mt"."f" INNER JOIN "t6" ON "t6"."f1"="mt"."f1" AND "t6"."f2"="mt"."f2" INNER JOIN "t7" ON "t7"."f1"="mt"."f2" INNER JOIN "t8" ON "t8"."f1"="mt"."f2"',None),
		({'t2': 'f'}, 't1', 'INNER JOIN "t2" ON "t2"."f"="t1"."f"',None)
	])
	def testJoin(self, joins, mtable, out, exception):
		if exception:
			with pytest.raises(exception):
				str(Join(joins, mtable))
		else:
			assert str(Join(joins, mtable)) == out

class TestBuilder(object):

	@pytest.mark.parametrize('conditions,out', [
		({Raw('f=1'):None}, 'WHERE f=1'),
		({'and #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'WHERE f=1 AND f=2'),
		({'AND #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'WHERE f=1 AND f=2'),
		({'and #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'WHERE f=1 AND f=2'),
		({'or #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'WHERE f=1 OR f=2'),
		({'or #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'WHERE f=1 OR f=2'),
		({'OR #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'WHERE f=1 OR f=2'),
		(OrderedDict([
			('or # or1', OrderedDict([
				('f3[~]', ('a', 'b', 'c')),
				('!f4[~]', ('a', 'b', 'c'))
			]))
		]), 'WHERE ("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')'),
		(OrderedDict([
			('t.f1[>]', 1),
			('t.f2[= any]', Builder()._select()),
			('or # or1', OrderedDict([
				('f3[~]', ('a', 'b', 'c')),
				('!f4[~]', ('a', 'b', 'c'))
			])),
			('and #', OrderedDict([
				(Raw('f=1'), None),
				(Raw('f=2'), None)
			]))
		]), 'WHERE "t"."f1" > 1 AND "t"."f2" = ANY (SELECT *) AND (("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')) AND (f=1 AND f=2)'),
	])
	def testWhere(self, conditions, out):
		assert str(Builder()._where(conditions)) == out

	@pytest.mark.parametrize('dialect', [
		Dialect,
		DialectTest,
		Dialect, # get default Dialect back to make sure the above tests run successfully
	])
	def testInit(self, dialect):
		builder = Builder(dialect)
		if not dialect:
			assert Builder.DIALECT is Dialect
		else:
			assert Builder.DIALECT is dialect
		assert builder.table is None
		assert builder.terms == []
		assert builder._sql is None
		assert builder._subas is False

	@pytest.mark.parametrize('fields,kwargs', [
		([''], dict(out = 'SELECT *')),
		([''], dict(
			distinct = True,
			out = 'SELECT DISTINCT *'
		)),
		(['f1',
			'f2(f2a)',
			'f3|sum(f3s)',
			Raw('whatever you put here: #@$@#')],
			dict(out = 'SELECT "f1","f2" AS "f2a",SUM("f3") AS "f3s",whatever you put here: #@$@#')
		)
	])
	def test_select(self, fields, kwargs):
		builder = Builder()
		out = kwargs['out']
		del kwargs['out']
		builder._select(*fields, **kwargs)
		assert str(builder) == out

	@pytest.mark.parametrize('sub, out', [
		(None, '(SELECT * FROM "t")'),
		(True, '(SELECT * FROM "t")'),
		('h', '(SELECT * FROM "t") AS "h"'),
	])
	def test_sub(self, sub, out):
		builder = Builder()._select()._from('t')._sub(sub)
		assert str(builder) == out

	@pytest.mark.parametrize('tables,kwargs', [
		(['t',
			't1(t2)',
			Builder()._select()._from('t3')._sub('t4')],
			dict(out = 'FROM "t","t1" AS "t2",(SELECT * FROM "t3") AS "t4"',
				table = Table("t"))
		)
	])
	def test_from(self, tables, kwargs):
		builder = Builder()
		out = kwargs['out']
		del kwargs['out']
		table = kwargs['table']
		del kwargs['table']
		builder._from(*tables)
		maintable = builder.table
		assert str(builder) == out
		assert maintable == table

	@pytest.mark.parametrize('orders,out', [
		({'f':None}, 'ORDER BY "f" ASC')
	])
	def test_order(self, orders, out):
		builder = Builder()
		builder._order(orders)
		assert str(builder) == out

	@pytest.mark.parametrize('limoff,out,where,dialect', [
		((1,2), 'SELECT * LIMIT 1 OFFSET 2',None,None),
		(10, 'SELECT TOP 10 *', None, DialectTest),
		((1,2), 'SELECT * WHERE ROWNUM >= 2 AND ROWNUM <= 3', None, DialectOracle),
		((1,2), 'SELECT * WHERE "id" = 1 AND (ROWNUM >= 2 AND ROWNUM <= 3)', {'id':1}, DialectOracle),
		((1,2), 'SELECT * WHERE ("id" LIKE \'%a%\' OR "id" LIKE \'%b%\') AND (ROWNUM >= 2 AND ROWNUM <= 3)', {'id[~]':['a', 'b']}, DialectOracle),
		(1, 'SELECT * LIMIT 1',None,None) # get the default Dialect back
	])
	def test_limit(self, limoff, out, where, dialect):
		builder = Builder(dialect)
		builder._select()._where(where)._limit(limoff)
		assert str(builder) == out

	@pytest.mark.parametrize('builder,other,all_,out',[
		(Builder()._select()._from('t1'), Builder()._select()._from('t2'), True, 'SELECT * FROM "t1" UNION ALL SELECT * FROM "t2"'),
		(Builder()._select()._from('t1'), Builder()._select()._from('t2'), False, 'SELECT * FROM "t1" UNION SELECT * FROM "t2"')
	])
	def test_union(self, builder, other, all_, out):
		builder._union(other, all_)
		assert str(builder) == out

	@pytest.mark.parametrize('builder,query,out', [
		(Builder()._select()._from('t1'), Builder()._select()._from('t2'), 'SELECT * FROM "t1" EXISTS SELECT * FROM "t2"')
	])
	def test_exists(self, builder, query, out):
		builder._exists(query)
		assert str(builder) == out

	@pytest.mark.parametrize('fields,out',[
		(['f1', 'f2'], 'GROUP BY "f1","f2"')
	])
	def test_group(self, fields, out):
		builder = Builder()._group(fields)
		assert str(builder) == out

	@pytest.mark.parametrize('conditions,out',[
		({Raw('f=1'):None}, 'HAVING f=1'),
		({'and #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'HAVING f=1 AND f=2'),
		({'AND #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'HAVING f=1 AND f=2'),
		({'and #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'HAVING f=1 AND f=2'),
		({'or #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'HAVING f=1 OR f=2'),
		({'or #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'HAVING f=1 OR f=2'),
		({'OR #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'HAVING f=1 OR f=2'),
		(OrderedDict([
			('or # or1', OrderedDict([
				('f3[~]', ('a', 'b', 'c')),
				('!f4[~]', ('a', 'b', 'c'))
			]))
		]), 'HAVING ("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')'),
		(OrderedDict([
			('t.f1[>]', 1),
			('t.f2[= any]', Builder()._select()),
			('or # or1', OrderedDict([
				('f3[~]', ('a', 'b', 'c')),
				('!f4[~]', ('a', 'b', 'c'))
			])),
			('and #', OrderedDict([
				(Raw('f=1'), None),
				(Raw('f=2'), None)
			]))
		]), 'HAVING "t"."f1" > 1 AND "t"."f2" = ANY (SELECT *) AND (("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')) AND (f=1 AND f=2)'),
	])
	def test_having(self, conditions, out):
		assert str(Builder()._having(conditions)) == out

	@pytest.mark.parametrize('table,values,fields,out,exception',[
		('t', [{'a':1,'b':2}, {'c':3}], None, None, InsertParseError),
		('t', [
			OrderedDict([('a', 1), ('b', 2)]),
			OrderedDict([('a', 3), ('b', 4)]),
		], None, 'INSERT INTO "t" ("a","b") VALUES (1,2),(3,4)',None),
		('t', [
			OrderedDict([('a', 1), ('b', 2)]),
			(3,4),
			(5,6),
		], None, 'INSERT INTO "t" ("a","b") VALUES (1,2),(3,4),(5,6)',None),
		('t', [
			(1,2),
			(3,4),
			(5,6),
		], ['a', 'b'], 'INSERT INTO "t" ("a","b") VALUES (1,2),(3,4),(5,6)',None),
		('t', [
			Builder()._select()._from('t1'),
			(21,2)
		], None, None, InsertParseError),
		('t', [
			Builder()._select()._from('t1'),
			Builder()._select()._from('t2')._sub(),
		], None, 'INSERT INTO "t" SELECT * FROM "t1" UNION ALL SELECT * FROM "t2"', None),
		('t', [1,2,3], None, None, InsertParseError)
	])
	def test_insert(self, table, values, fields, out, exception):
		if exception:
			with pytest.raises(exception):
				Builder()._insert(table, values, fields)
		else:
			assert str(Builder()._insert(table, values, fields)) == out

	@pytest.mark.parametrize('table,out',[
		('t', 'UPDATE "t"'),
		('s.t', 'UPDATE "s"."t"')
	])
	def test_update(self, table, out):
		assert str(Builder()._update(table)) == out

	@pytest.mark.parametrize('sets, out',[
		({'a':1}, 'SET "a"=1'),
		(OrderedDict([
			('a', 1),
			('b[+]', 2)
		]), 'SET "a"=1,"b"="b"+2')
	])
	def test_set(self, sets, out):
		assert str(Builder()._set(sets)) == out

	@pytest.mark.parametrize('table,out',[
		('t', 'DELETE FROM "t"'),
		('s.t', 'DELETE FROM "s"."t"')
	])
	def test_delete(self, table, out):
		assert str(Builder()._delete(table)) == out

	@pytest.mark.parametrize('tables,joins,out',[
		([
			Builder()._select()._from('t')._sub('t1'),
			't2'
		], {'t3':'f'}, 'FROM (SELECT * FROM "t") AS "t1","t2" INNER JOIN "t3" ON "t3"."f"="t1"."f"')
	])
	def test_join(self, tables, joins, out):
		builder = Builder()
		builder._from(*tables)
		builder._join(joins)
		assert str(builder) == out

	@pytest.mark.parametrize('table,out',[
		('t', 'INTO "t"'),
		('s.t', 'INTO "s"."t"')
	])
	def test_into(self, table, out):
		assert str(Builder()._into(table)) == out

	@pytest.mark.parametrize('table,out,columns,where,join,distinct,newtable,sub',[
		('t', 'SELECT * FROM "t"', None,None,None,False,None,None),
		('t', 'SELECT "a","b" FROM "t"', 'a,b',None,None,False,None,None),
		('t', 'SELECT DISTINCT "a","b" FROM "t"', 'a,b',None,None,True,None,None),
		('t', 'SELECT "a","b" FROM "t" WHERE "f" = 1', 'a,b',{'f':1},None,False,None,None),
		('t', 'SELECT "a","b" FROM "t" WHERE "f" = 1', 'a,b',{'f':1},None,False,None,None),
		('t', 'SELECT "a","b" FROM "t" LEFT JOIN "t2" ON "t2"."f1"="t"."f1" WHERE "f" = 1', 'a,b',{'f':1},{'[>]t2': "f1"},False,None,None),
		('t', 'SELECT "a","b" INTO "t3" FROM "t" LEFT JOIN "t2" ON "t2"."f1"="t"."f1" WHERE "f" = 1', 'a,b',{'f':1},{'[>]t2': "f1"},False,'t3',None),
		('t', '(SELECT "a","b" FROM "t" LEFT JOIN "t2" ON "t2"."f1"="t"."f1" WHERE "f" = 1)', 'a,b',{'f':1},{'[>]t2': "f1"},False,None,True),
		('t', '(SELECT "a","b" FROM "t" LEFT JOIN "t2" ON "t2"."f1"="t"."f1" WHERE "f" = 1) AS "tmp"', 'a,b',{'f':1},{'[>]t2': "f1"},False,None,'tmp'),
	])
	def testSelect(self, table, out, columns, where, join, distinct, newtable, sub):
		assert str(Builder().select(table, columns, where, join, distinct, newtable, sub)) == out

	@pytest.mark.parametrize('table,data,where,out',[
		('t', {'id':1}, {'_id':10}, 'UPDATE "t" SET "id"=1 WHERE "_id" = 10')
	])
	def testUpdate(self, table, data, where, out):
		assert str(Builder().update(table, data, where)) == out

	@pytest.mark.parametrize('table,where,out',[
		('t', {'id':1}, 'DELETE FROM "t" WHERE "id" = 1')
	])
	def testDelete(self, table, where, out):
		assert str(Builder().delete(table, where)) == out

	@pytest.mark.parametrize('builder,queries,out',[
		(Builder(), [
			Builder().select(table = 't1', sub = True),
			Builder().select(table = 't2'),
		], 'SELECT * FROM "t1" UNION SELECT * FROM "t2"'),
		(Builder().select(table = 't'), [
			Builder().select(table = 't1', sub = True),
			Builder().select(table = 't2'),
		], 'SELECT * FROM "t" UNION ALL SELECT * FROM "t1" UNION SELECT * FROM "t2"')
	])
	def testUnion(self, builder, queries, out):
		assert str(builder.union(*queries)) == out

	@pytest.mark.parametrize('table,fields,values,out', [
		('t', OrderedDict([('a',1), ('b',2)]), [(3,4),(4,5)], 'INSERT INTO "t" ("a","b") VALUES (1,2),(3,4),(4,5)'),
		('t', (1,2), [(3,4),(5,6)], 'INSERT INTO "t" VALUES (1,2),(3,4),(5,6)'),
		('t', 'a,b', [(3,4),(5,6)], 'INSERT INTO "t" ("a","b") VALUES (3,4),(5,6)'),
		('t', ['a','b'], [(3,4),(5,6)], 'INSERT INTO "t" ("a","b") VALUES (3,4),(5,6)')
	])
	def testInsert(self, table, fields, values, out):
		assert str(Builder().insert(table, fields, *values)) == out

