import testly
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

class TestTerm(testly.TestCase):

	def testStr(self):
		self.assertRaises(NotImplementedError, Term().__str__)

class TestRaw(testly.TestCase):	

	def dataProvider_testStr(self):
		yield 's', 's'
		yield [], '[]'

	def testStr(self, s, out):
		self.assertEqual(str(Raw(s)), out)

class testTable(testly.TestCase):

	def dataProvider_testInit(self):
		yield 't', None, 't', None
		yield 's.t', 's2', None, None, TableParseError
		yield 's.t', None, 't', 's'
		yield 's.t.x', None, None, None, TableParseError

	def testInit(self, table, schema, out_t, out_s, exception = None):
		if exception:
			self.assertRaises(exception, Table, table, schema)
		else:
			t = Table(table, schema)
			self.assertEqual(t.table, out_t)
			self.assertEqual(t.schema, out_s)

	def dataProvider_testStr(self):
		yield Table('s.t'), '"s"."t"'
		yield Table('t'), '"t"'

	def testStr(self, table, out):
		self.assertEqual(str(table), out)

	def dataProvider_testEq(self):
		yield Table('s.t'), Table('s.t'), True
		yield Table('s.t'), Table('t'), False
		yield Table('s.t'), Table('t', schema = 's'), True

	def testEq(self, t1, t2, out):
		self.assertEqual(t1 == t2, out)

	def dataProvider_testParse(self):
		yield Raw('whatevertable'), None, 'whatevertable'
		yield 's.t', None, '"s"."t"'
		yield 's.t', "insert", '"s"."t"'
		yield 's.t', "update", '"s"."t"'
		yield 's.t', "delete", '"s"."t"'
		yield 's.t', "selectinto", '"s"."t"'
		yield 's.t(t1)', 'from', '"s"."t" AS "t1"'
		yield 't(t1)', 'from', '"t" AS "t1"'
		yield 't', 'from', '"t"'
		yield '[]t', 'from', None, TableParseError
		yield 't', 'unknown', None, TableParseError

	def testParse(self, tablestr, context, outtable, exception = None):
		if exception:
			self.assertRaises(exception, Table.parse, tablestr, context)
		else:
			self.assertEqual(Table.parse(tablestr, context), outtable)

	def dataProvider_testTableFrom(self):
		yield 's.t', None, '"s"."t"'
		yield 't', None, '"t"'
		yield 's.t', 't1', '"s"."t" AS "t1"'
		yield 't', "t1", '"t" AS "t1"'

	def testTableFrom(self, table, alias, out):
		self.assertEqual(str(TableFrom(table, alias)), out)

class TestField(testly.TestCase):

	def dataProvider_testInit(self):
		yield 'f', None, None, 'f', None, None
		yield 'f', 't', 's', 'f', 't', 's'
		yield 's.t.f', None, None, 'f', 't', 's'
		yield 't.f', 't', None, None, None, None, FieldParseError
		yield 't.f', None, 's', 'f', 't', 's'
		yield 's.t.f', None, 's', None, None, None , FieldParseError
		yield 's.t.f.f1', None, None, None, None, None, FieldParseError


	def testInit(self, fieldstr, table, schema, out_f, out_t, out_s, exception = None):
		if exception:
			self.assertRaises(exception, Field, fieldstr, table, schema)
		else:
			f = Field(fieldstr, table, schema)
			self.assertEqual(f.field, out_f)
			self.assertEqual(f.table, out_t)
			self.assertEqual(f.schema, out_s)

	def dataProvider_testStr(self):
		yield Field('s.t.f'), '"s"."t"."f"'
		yield Field('t.f', schema = 's'), '"s"."t"."f"'
		yield Field('f', schema = 's', table = 't'), '"s"."t"."f"'

	def testStr(self, field, out):
		self.assertEqual(str(field), out)

	def dataProvider_testOprt(self):
		yield Field('f'), '__add__', 1, '"f"+1'
		yield Field('f'), '__sub__', 1, '"f"-1'
		yield Field('f'), '__mul__', 1, '"f"*1'
		yield Field('f'), '__div__', 1, '"f"/1'
		yield Field('f'), '__mod__', 1, '"f"%1'

	def testOprt(self, field, oprt, value, out):
		self.assertEqual(getattr(field, oprt)(value), out)

	def dataProvider_testParse(self):
		yield Raw('s.t.f'), None, 's.t.f'
		yield 's.t.f', None, '"s"."t"."f"'
		yield 's.t.f', "group", '"s"."t"."f"'
		yield 's.t.f', "group2", None, FieldParseError
		yield 's.t.f|count(f1)', 'select', 'COUNT("s"."t"."f") AS "f1"'
		yield 's.t.f|.count(f1)', 'select', 'COUNT(DISTINCT "s"."t"."f") AS "f1"'
		yield 's.t.f|c.ount(f1)', 'select', None, FieldParseError

	def testParse(self, fieldstr, context, outfield, exception = None):
		if exception:
			self.assertRaises(exception, Field.parse, fieldstr, context)
		else:
			self.assertEqual(Field.parse(fieldstr, context), outfield)
		
class TestWhere(testly.TestCase):

	def dataProvider_testTerm(self):
		yield Raw('f=1'), None, 'f=1'
		yield 'aiw@#$', None, None, WhereParseError
		yield '!s.t.f[!] # comments', 1, 'NOT "s"."t"."f" <> 1'
		yield 'f|count[>]', 1, 'COUNT("f") > 1'

	def testTerm(self, key, val, out, exception = None):
		if exception:
			self.assertRaises(exception, WhereTerm(key, val).__str__)
		else:
			self.assertEqual(str(WhereTerm(key, val)), out)

	def dataProvider_testWhere(self):
		yield {Raw('f=1'):None}, 'f=1'
		yield {'and #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'f=1 AND f=2'
		yield {'AND #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'f=1 AND f=2'
		yield {'and #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'f=1 AND f=2'
		yield {'or #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'f=1 OR f=2'
		yield {'or #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'f=1 OR f=2'
		yield {'OR #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'f=1 OR f=2'
		yield {'f':1}, '"f" = 1'
		yield OrderedDict([
			('or # or1', OrderedDict([
				('f3[~]', ('a', 'b', 'c')),
				('!f4[~]', ('a', 'b', 'c'))
			]))
		]), '("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')'	
		yield OrderedDict([
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
		]), '"t"."f1" > 1 AND "t"."f2" = ANY (SELECT *) AND (("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')) AND (f=1 AND f=2)'		


	def testWhere(self, conditions, out, exception = None):
		if exception:
			self.assertRaises(exception, Where(conditions).__str__)
		else:
			self.assertEqual(str(Where(conditions)), out)

class TestOrder(testly.TestCase):

	def dataProvider_testStr(self):
		yield {'@#$':None}, None, FieldParseError
		yield OrderedDict([
			('f1|count', True),
			('s.t.f', None),
			('s.t.f4', False),
			('f2', 'asc'),
			('f3', 'desc')
		]), 'COUNT("f1") ASC,"s"."t"."f" ASC,"s"."t"."f4" DESC,"f2" ASC,"f3" DESC'

	def testStr(self, orders, out, exception = None):
		if exception:
			self.assertRaises(exception, Order(orders).__str__)
		else:
			self.assertEqual(str(Order(orders)), out)

class TestLimit(testly.TestCase):

	def testStr(self, limoff, out, pos = -1, exception = None):
		if exception:
			self.assertRaises(exception, Limit, limoff)
		else:
			lim = Limit(limoff)
			self.assertEqual(str(lim), out)
			self.assertEqual(lim.pos, pos)

	def dataProvider_testStr(self):
		yield [1], 'LIMIT 1',
		yield (10,5), 'LIMIT 10 OFFSET 5'
		yield [1,2,3], None, -1, LimitParseError

class TestSet(testly.TestCase):

	def dataProvider_testStr(self):
		yield {'a':1}, '"a"=1'
		yield OrderedDict([
			('a', 1),
			('b[+]', 2)
		]), '"a"=1,"b"="b"+2'
		yield {'@#':None}, None, UpdateParseError

	def testStr(self, sets, out, exception = None):
		if exception:
			self.assertRaises(exception, Set(sets).__str__)
		else:
			self.assertEqual(str(Set(sets)), out)

class TestJoin(testly.TestCase):

	def dataProvider_testTerm(self):
		yield '@#F', None, None, None, JoinParseError
		yield 't', ['t.f'], None, None, JoinParseError
		yield 't', ('t.f', 'f2'), None, None, JoinParseError
		yield 't', ('t.f', 'f2'), None, None, JoinParseError
		yield 't', 'f', None, None, JoinParseError
		yield 't', 'f', TableFrom('mt'), 'INNER JOIN "t" ON "t"."f"="mt"."f"'
		yield 't', {'t1.f': 'mt.f2'}, None, None, JoinParseError
		yield '[<>]t(t1)', ['f1', 'f2'], TableFrom('mt'), 'FULL OUTER JOIN "t" AS "t1" ON "t1"."f1"="mt"."f1" AND "t1"."f2"="mt"."f2"'
		yield '[<]t', {'f': 'f2'}, TableFrom('mt', alias='main'), 'RIGHT JOIN "t" ON "t"."f"="main"."f2"'


	def testTerm(self, key, val, mtable, out, exception = None):
		if exception:
			self.assertRaises(exception, JoinTerm, key, val, mtable)
		else:
			self.assertEqual(str(JoinTerm(key, val, mtable)), out)

	def dataProvider_testJoin(self):
		yield OrderedDict([
			('t1', 'f'),
			('t2(t3)', 'f'),
			('[>]t4(t5)', 'f'),
			('t6', ['f1', 'f2']),
			('t7', {'f1': 'f2'}),
			('t8', {'f1': 'f2'})
		]), TableFrom('mt'), 'INNER JOIN "t1" ON "t1"."f"="mt"."f" INNER JOIN "t2" AS "t3" ON "t3"."f"="mt"."f" LEFT JOIN "t4" AS "t5" ON "t5"."f"="mt"."f" INNER JOIN "t6" ON "t6"."f1"="mt"."f1" AND "t6"."f2"="mt"."f2" INNER JOIN "t7" ON "t7"."f1"="mt"."f2" INNER JOIN "t8" ON "t8"."f1"="mt"."f2"'
		yield {'t2': 'f'}, 't1', 'INNER JOIN "t2" ON "t2"."f"="t1"."f"'

	def testJoin(self, joins, mtable, out, exception = None):
		if exception:
			self.assertRaises(exception, Join(joins, mtable).__str__)
		else:
			self.assertEqual(str(Join(joins, mtable)), out)

class TestBuilder(testly.TestCase):

	def dataProvider_testWhere(self):
		yield {Raw('f=1'):None}, 'WHERE f=1'
		yield {'and #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'WHERE f=1 AND f=2'
		yield {'AND #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'WHERE f=1 AND f=2'
		yield {'and #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'WHERE f=1 AND f=2'
		yield {'or #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'WHERE f=1 OR f=2'
		yield {'or #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'WHERE f=1 OR f=2'
		yield {'OR #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'WHERE f=1 OR f=2'
		yield OrderedDict([
			('or # or1', OrderedDict([
				('f3[~]', ('a', 'b', 'c')),
				('!f4[~]', ('a', 'b', 'c'))
			]))
		]), 'WHERE ("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')'	
		yield OrderedDict([
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
		]), 'WHERE "t"."f1" > 1 AND "t"."f2" = ANY (SELECT *) AND (("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')) AND (f=1 AND f=2)'		

	def testWhere(self, conditions, out):
		self.assertEqual(str(Builder()._where(conditions)), out)

	def dataProvider_testInit(self):
		yield Dialect,
		yield DialectTest,
		yield Dialect, # get default Dialect back to make sure the above tests run successfully

	def testInit(self, dialect):
		builder = Builder(dialect)
		if not dialect:
			self.assertIs(Builder.DIALECT, Dialect)
		else:
			self.assertIs(Builder.DIALECT, dialect)
		self.assertIsNone(builder.table)
		self.assertEqual(builder.terms, [])
		self.assertIsNone(builder._sql)
		self.assertIs(builder._subas, False)

	def dataProvider_test_select(self):
		yield testly.Data(
			'',
			out = 'SELECT *'
		)
		yield testly.Data(
			'',
			distinct = True,
			out = 'SELECT DISTINCT *'
		)
		yield testly.Data(
			'f1',
			'f2(f2a)',
			'f3|sum(f3s)',
			Raw('whatever you put here: #@$@#'),
			out = 'SELECT "f1","f2" AS "f2a",SUM("f3") AS "f3s",whatever you put here: #@$@#'
		)

	def test_select(self, *fields, **kwargs):
		builder = Builder()
		out = kwargs['out']
		del kwargs['out']
		builder._select(*fields, **kwargs)
		self.assertEqual(str(builder), out)

	def dataProvider_test_sub(self):
		yield None, '(SELECT * FROM "t")'
		yield True, '(SELECT * FROM "t")'
		yield 'h', '(SELECT * FROM "t") AS "h"'

	def test_sub(self, sub, out):
		builder = Builder()._select()._from('t')._sub(sub)
		self.assertEqual(str(builder), out)

	def dataProvider_test_from(self):
		yield testly.Data(
			't',
			't1(t2)',
			Builder()._select()._from('t3')._sub('t4'),
			out = 'FROM "t","t1" AS "t2",(SELECT * FROM "t3") AS "t4"',
			table = Table("t")
		)

	def test_from(self, *tables, **kwargs):
		builder = Builder()
		out = kwargs['out']
		del kwargs['out']
		table = kwargs['table']
		del kwargs['table']
		builder._from(*tables)
		maintable = builder.table
		self.assertEqual(str(builder), out)
		self.assertEqual(maintable, table)

	def dataProvider_test_order(self):
		yield {'f':None}, 'ORDER BY "f" ASC'

	def test_order(self, orders, out):
		builder = Builder()
		builder._order(orders)
		self.assertEqual(str(builder), out)

	def dataProvider_test_limit(self):
		yield (1,2), 'SELECT * LIMIT 1 OFFSET 2'
		yield 10, 'SELECT TOP 10 *', None, DialectTest
		yield (1,2), 'SELECT * WHERE ROWNUM >= 2 AND ROWNUM <= 3', None, DialectOracle
		yield (1,2), 'SELECT * WHERE "id" = 1 AND (ROWNUM >= 2 AND ROWNUM <= 3)', {'id':1}, DialectOracle
		yield (1,2), 'SELECT * WHERE ("id" LIKE \'%a%\' OR "id" LIKE \'%b%\') AND (ROWNUM >= 2 AND ROWNUM <= 3)', {'id[~]':['a', 'b']}, DialectOracle
		yield 1, 'SELECT * LIMIT 1' # get the default Dialect back

	def test_limit(self, limoff, out, where = None, dialect = None):
		builder = Builder(dialect)
		builder._select()._where(where)._limit(limoff)
		self.assertEqual(str(builder), out)

	def dataProvider_test_union(self):
		yield Builder()._select()._from('t1'), Builder()._select()._from('t2'), True, 'SELECT * FROM "t1" UNION ALL SELECT * FROM "t2"'
		yield Builder()._select()._from('t1'), Builder()._select()._from('t2'), False, 'SELECT * FROM "t1" UNION SELECT * FROM "t2"'

	def test_union(self, builder, other, all_, out):
		builder._union(other, all_)
		self.assertEqual(str(builder), out)

	def dataProvider_test_exists(self):
		yield Builder()._select()._from('t1'), Builder()._select()._from('t2'), 'SELECT * FROM "t1" EXISTS SELECT * FROM "t2"'

	def test_exists(self, builder, query, out):
		builder._exists(query)
		self.assertEqual(str(builder), out)

	def dataProvider_test_group(self):
		yield ['f1', 'f2'], 'GROUP BY "f1","f2"'

	def test_group(self, fields, out):
		builder = Builder()._group(fields)
		self.assertEqual(str(builder), out)

	def dataProvider_test_having(self):
		yield {Raw('f=1'):None}, 'HAVING f=1'
		yield {'and #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'HAVING f=1 AND f=2'
		yield {'AND #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'HAVING f=1 AND f=2'
		yield {'and #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'HAVING f=1 AND f=2'
		yield {'or #': OrderedDict([
			(Raw('f=1'), None),
			(Raw('f=2'), None)
		])}, 'HAVING f=1 OR f=2'
		yield {'or #': [
			Raw('f=1'),	Raw('f=2')
		]}, 'HAVING f=1 OR f=2'
		yield {'OR #': (
			Raw('f=1'),	Raw('f=2')
		)}, 'HAVING f=1 OR f=2'
		yield OrderedDict([
			('or # or1', OrderedDict([
				('f3[~]', ('a', 'b', 'c')),
				('!f4[~]', ('a', 'b', 'c'))
			]))
		]), 'HAVING ("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')'	
		yield OrderedDict([
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
		]), 'HAVING "t"."f1" > 1 AND "t"."f2" = ANY (SELECT *) AND (("f3" LIKE \'%a%\' OR "f3" LIKE \'%b%\' OR "f3" LIKE \'%c%\') OR NOT ("f4" LIKE \'%a%\' OR "f4" LIKE \'%b%\' OR "f4" LIKE \'%c%\')) AND (f=1 AND f=2)'		

	def test_having(self, conditions, out):
		self.assertEqual(str(Builder()._having(conditions)), out)

	def dataProvider_test_insert(self):
		yield 't', [{'a':1,'b':2}, {'c':3}], None, None, InsertParseError
		yield 't', [
			OrderedDict([('a', 1), ('b', 2)]),
			OrderedDict([('a', 3), ('b', 4)]),
		], None, 'INSERT INTO "t" ("a","b") VALUES (1,2),(3,4)'
		yield 't', [
			OrderedDict([('a', 1), ('b', 2)]),
			(3,4),
			(5,6),
		], None, 'INSERT INTO "t" ("a","b") VALUES (1,2),(3,4),(5,6)'
		yield 't', [
			(1,2),
			(3,4),
			(5,6),
		], ['a', 'b'], 'INSERT INTO "t" ("a","b") VALUES (1,2),(3,4),(5,6)'
		yield 't', [
			Builder()._select()._from('t1'),
			(21,2)
		], None, None, InsertParseError
		yield 't', [
			Builder()._select()._from('t1'),
			Builder()._select()._from('t2')._sub(),
		], None, 'INSERT INTO "t" SELECT * FROM "t1" UNION ALL SELECT * FROM "t2"'
		yield 't', [1,2,3], None, None, InsertParseError

	def test_insert(self, table, values, fields, out, exception = None):
		if exception:
			self.assertRaises(exception, Builder()._insert, table, values, fields)
		else:
			self.assertEqual(str(Builder()._insert(table, values, fields)), out)

	def dataProvider_test_update(self):
		yield 't', 'UPDATE "t"'
		yield 's.t', 'UPDATE "s"."t"'

	def test_update(self, table, out):
		self.assertEqual(str(Builder()._update(table)), out)

	def dataProvider_test_set(self):
		yield {'a':1}, 'SET "a"=1'
		yield OrderedDict([
			('a', 1),
			('b[+]', 2)
		]), 'SET "a"=1,"b"="b"+2'

	def test_set(self, sets, out):
		self.assertEqual(str(Builder()._set(sets)), out)

	def dataProvider_test_delete(self):
		yield 't', 'DELETE FROM "t"'
		yield 's.t', 'DELETE FROM "s"."t"'

	def test_delete(self, table, out):
		self.assertEqual(str(Builder()._delete(table)), out)

	def dataProvider_test_join(self):
		yield [
			Builder()._select()._from('t')._sub('t1'),
			't2'
		], {'t3':'f'}, 'FROM (SELECT * FROM "t") AS "t1","t2" INNER JOIN "t3" ON "t3"."f"="t1"."f"'

	def test_join(self, tables, joins, out):
		builder = Builder()
		builder._from(*tables)
		builder._join(joins)
		self.assertEqual(str(builder), out)

	def dataProvider_test_into(self):
		yield 't', 'INTO "t"'
		yield 's.t', 'INTO "s"."t"'

	def test_into(self, table, out):
		self.assertEqual(str(Builder()._into(table)), out)

	def dataProvider_testSelect(self):
		yield testly.Data(
			table = 't',
			out = 'SELECT * FROM "t"'
		)
		yield testly.Data(
			table = 't',
			columns = 'a,b',
			out = 'SELECT "a","b" FROM "t"'
		)
		yield testly.Data(
			table = 't',
			columns = 'a,b',
			distinct = True,
			out = 'SELECT DISTINCT "a","b" FROM "t"'
		)
		yield testly.Data(
			table = 't',
			columns = 'a,b',
			where = {'f':1},
			out = 'SELECT "a","b" FROM "t" WHERE "f" = 1'
		)
		yield testly.Data(
			table = 't',
			columns = 'a,b',
			where = {'f':1},
			join = {'[>]t2': "f1"},
			out = 'SELECT "a","b" FROM "t" LEFT JOIN "t2" ON "t2"."f1"="t"."f1" WHERE "f" = 1'
		)
		yield testly.Data(
			table = 't',
			columns = 'a,b',
			where = {'f':1},
			join = {'[>]t2': "f1"},
			newtable = 't3',
			out = 'SELECT "a","b" INTO "t3" FROM "t" LEFT JOIN "t2" ON "t2"."f1"="t"."f1" WHERE "f" = 1'
		)
		yield testly.Data(
			table = 't',
			columns = 'a,b',
			where = {'f':1},
			join = {'[>]t2': "f1"},
			sub = True,
			out = '(SELECT "a","b" FROM "t" LEFT JOIN "t2" ON "t2"."f1"="t"."f1" WHERE "f" = 1)'
		)
		yield testly.Data(
			table = 't',
			columns = 'a,b',
			where = {'f':1},
			join = {'[>]t2': "f1"},
			sub = 'tmp',
			out = '(SELECT "a","b" FROM "t" LEFT JOIN "t2" ON "t2"."f1"="t"."f1" WHERE "f" = 1) AS "tmp"'
		)


	def testSelect(self, table, out, columns = '*', where = None, join = None, distinct = False, newtable = None, sub = None):
		self.assertEqual(str(Builder().select(table, columns, where, join, distinct, newtable, sub)), out)

	def dataProvider_testUpdate(self):
		yield 't', {'id':1}, {'_id':10}, 'UPDATE "t" SET "id"=1 WHERE "_id" = 10'

	def testUpdate(self, table, data, where, out):
		self.assertEqual(str(Builder().update(table, data, where)), out)

	def dataProvider_testDelete(self):
		yield 't', {'id':1}, 'DELETE FROM "t" WHERE "id" = 1'

	def testDelete(self, table, where, out):
		self.assertEqual(str(Builder().delete(table, where)), out)

	def dataProvider_testInsert(self):
		yield 't', OrderedDict([('a',1), ('b',2)]), [(3,4),(4,5)], 'INSERT INTO "t" ("a","b") VALUES (1,2),(3,4),(4,5)'
		yield 't', (1,2), [(3,4),(5,6)], 'INSERT INTO "t" VALUES (1,2),(3,4),(5,6)'
		yield 't', 'a,b', [(3,4),(5,6)], 'INSERT INTO "t" ("a","b") VALUES (3,4),(5,6)'
		yield 't', ['a','b'], [(3,4),(5,6)], 'INSERT INTO "t" ("a","b") VALUES (3,4),(5,6)'

	def dataProvider_testUnion(self):
		yield Builder(), [
			Builder().select(table = 't1', sub = True),
			Builder().select(table = 't2'),
		], 'SELECT * FROM "t1" UNION SELECT * FROM "t2"'
		yield Builder().select(table = 't'), [
			Builder().select(table = 't1', sub = True),
			Builder().select(table = 't2'),
		], 'SELECT * FROM "t" UNION ALL SELECT * FROM "t1" UNION SELECT * FROM "t2"'


	def testUnion(self, builder, queries, out):
		self.assertEqual(str(builder.union(*queries)), out)

	def testInsert(self, table, fields, values, out):
		self.assertEqual(str(Builder().insert(table, fields, *values)), out)


if __name__ == '__main__':
	testly.main(verbosity = 2)
