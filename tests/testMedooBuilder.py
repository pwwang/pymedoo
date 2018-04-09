import helpers, unittest
from medoo.medooBuilder import _alwaysList, Function, Field, Raw, Dialect, TableParseError, Builder, Where


class TestMedooBuiler(helpers.TestCase):
	
	def dataProvider_testAlwaysList(self):
		yield [1,2,3], [1,2,3]
		yield "1, , 2, 3", ['1','2','3']
	
	def testAlwaysList(self, input, output):
		ret = _alwaysList(input)
		self.assertListEqual(ret, output)
		
	def dataProvider_testFunctionInit(self):
		yield 'what', ['table.field, table.field1'], ['field', 'field1']
		
	def testFunctionInit(self, fn, fields, outfields):
		f = Function(fn, *fields)
		self.assertIsInstance(f, Function)
		self.assertEqual(f.fn, fn)
		
		for i in range(len(outfields)):
			self.assertEqual(outfields[i], f.fields[i].field)
			
	def dataProvider_testFunctionHash(self):
		yield Function('a', []),
	
	def testFunctionHash(self, fn):
		f = {fn:1}.keys()[0]
		self.assertIsInstance(f, Function)
		
	def dataProvider_testFunctionSql(self):
		yield Function.a('table.field1, table.field2'), 'A("table"."field1","table"."field2")'
		yield Function.b('table.field1(field)'), 'B("table"."field1")'
		yield Function.count('table.field1(field)', alias = 'cnt'), 'COUNT("table"."field1") "cnt"'
		yield Function.count(Function.distinct(['table.field1', 'table.field2(field)'])), 'COUNT(DISTINCT "table"."field1","table"."field2")'
		yield Function.distinct(Raw('"field"')), 'DISTINCT "field"'
		
	def testFunctionSql(self, fn, sql):
		self.assertEqual(fn.sql(), sql)
		
	def dataProvider_testDialectQuote(self):
		yield 'abc', '"abc"'
		
	def testDialectQuote(self, input, output):
		ret = Dialect.quote(input)
		self.assertEqual(ret, output)
		
	def dataProvider_testDialectAlias(self):
		yield "a", "b", "a b"
		
	def testDialectAlias(self, input, alias, output):
		ret = Dialect.alias(input, alias)
		self.assertEqual(ret, output)
		
	def dataProvider_testDialectValue(self):
		yield 1, '1'
		yield 1.1, '1.1'
		yield Raw('abc'), 'abc'
		yield Field('table.f(c)'), '"table"."f" "c"'
		yield "awfwfe", "'awfwfe'"
		yield "ab'cd", "'ab''cd'"
		
	def testDialectValue(self, input, output):
		ret = Dialect.value(input)
		self.assertEqual(ret, output)
		
	def dataProvider_testDialectLimit(self):
		yield 1, 2, '1,2'
		yield 3, 4, '3,4'
		
	def testDialectLimit(self, offset, lim, output):
		ret = Dialect.limit(offset, lim)
		self.assertEqual(ret, output)
		
	def dataProvider_testDialectJoin(self):
		yield 'a', None, TableParseError
		yield '>', 'LEFT JOIN'
		yield '<', 'RIGHT JOIN'
		yield '><', 'JOIN'
		yield '<>', 'OUTER JOIN'
		
	def testDialectJoin(self, type, output, exception = None):
		if exception:
			self.assertRaises(exception, Dialect.join, type)
		else:
			ret = Dialect.join(type)
			self.assertEqual(ret, output)
			
	def dataProvider_testDialectLikeValue(self):
		yield 'a', '%a%'
		yield '%a', '%a'
		yield 'a%', 'a%'
			
	def testDialectLikeValue(self, input, output):
		ret = Dialect.likeValue(input)
		self.assertEqual(ret, output)
		
	def dataProvider_testDialectOperate(self):
		yield '', '"field"', [1,2,3], '"field" IN (1,2,3)'
		
	def testDialectOperate(self, operator, left, right, output, dialect = Dialect):
		ret = Dialect.operate(operator, left, right, dialect)
		self.assertEqual(ret, output)
		
	def dataProvider_testWhere(self):
		yield {Field('field1'): Field('field2')}, '"field1" = "field2"'
		
	def testWhere(self, wheredict, output):
		ret = Where(wheredict)
		self.assertEqual(ret.sql(), output)
		
	def dataProvider_testBuilder(self):
		# 0
		yield Builder().select('*').from_('table1, table2').join({
			'table3': 'id'
		}).where({'table1.id[>]':100, 'table2.id[<]': 100}).group('name').having({'name[!]':10}), 'SELECT * FROM "table1" "table2" JOIN "table3" USING ("id") WHERE "table1"."id" > 100 AND "table2"."id" < 100 GROUP BY "name" HAVING "name" != 10'
		yield Builder().select('a,*').from_('table').where({
			'OR': {
				'f1': [1,2,3],
				'f2[<>]': (10, 20),
				'f3[!~]': ['a', 'b']
			},
			'f1': Field('f2') + 1,
			Function.max('f2[>]'): 1000
		}), 'SELECT "a",* FROM "table" WHERE "f1" = "f2"+1 AND MAX("f2")>1000 AND ("f2" BETWEEN 10 AND 20 OR "f1" IN (1,2,3) OR ("f3" NOT LIKE \'%a%\' AND "f3" NOT LIKE \'%b%\'))'
		yield Builder().update('table').set({
			'field': 1,
			'field1[+]': 2,
			'field2[/]': 3,
			'field3': Function.max('field2') + 4,
			'field4': Field('field2') + 5
		}).where({'id[=]': 100}), 'UPDATE "table" SET "field2"="field2"/3,"field"=1,"field3"=MAX("field2")+4,"field4"="field2"+5,"field1"="field1"+2 WHERE "id" = 100'
		yield Builder().insert('table', 'a,b,c').values((1,2,3), (4,5,6)), 'INSERT INTO "table" ("a","b","c") VALUES (1,2,3), (4,5,6)'
		yield Builder().select('*').from_('table').order({'field':True}).limit(10), 'SELECT * FROM "table" ORDER BY "field" ASC LIMIT 1,10'
		# 5
		yield Builder().create('table', {'id': 'int primary key', 'b': 'text'}), 'CREATE TABLE IF NOT EXISTS "table" ( "b" text, "id" int primary key ) '
		yield Builder().select([Raw('"field"')]).from_(Raw('"table"')).join({
			"table2": { Raw("table2.id"): "table.id" }
		}).where({
			Raw("field = 1"): None,
			Field("id[!]"): 100,
			Raw("field < field2"): None
		}).group(Raw("field1"), Field("field2"), "field3").having({
			'OR #1': {
				Field("table.field[>]"): 1,
				Field("table.field2[~]"): ['a', 'b']
			},
			'OR #2': {
				Raw('field > field2'): None,
				Raw('field LIKE \'%c%\''): None,
			}
		}).order({
			'field': 'asc',
			Raw("field2 DESC"): None,
			Field("field3"): False
		}).limit([10, 100]), 'SELECT "field" FROM "table" JOIN "table2" ON table2.id="table"."id" WHERE field < field2 AND field = 1 AND "id" != 100 GROUP BY field1,"field2","field3" HAVING (("table"."field2" LIKE \'%a%\' OR "table"."field2" LIKE \'%b%\') OR "table"."field" > 1) AND (field > field2 OR field LIKE \'%c%\') ORDER BY "field" ASC,"field3" DESC,field2 DESC LIMIT 10,100'
		yield Builder().select('*').from_('table1(t)').join({
			'table3': 'id'
		}).where({'t.id[>]':100, 'table3.id[<]': 100}), 'SELECT * FROM "table1" "t" JOIN "table3" USING ("id") WHERE "t"."id" > 100 AND "table3"."id" < 100'
		yield Builder().select().from_(Builder().select().from_('table')), 'SELECT * FROM (SELECT * FROM "table")'
		yield Builder().select(Function.count()).from_('table').where({'id[>]':100}), 'SELECT COUNT(*) FROM "table" WHERE "id" > 100'
		yield Builder().update('table').set({'field[json]': {'a':1, 'b':2}}).where({'a': (1,2,3)}), 'UPDATE "table" SET "field"=\'{"a": 1, "b": 2}\' WHERE "a" IN (1,2,3)'
		
	def testBuilder(self, builder, sql):
		self.assertEqual(builder.sql(), sql)

if __name__ == '__main__':
	unittest.main(verbosity = 2)