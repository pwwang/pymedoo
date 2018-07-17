import testly
from medoo.dialect import Dialect
from medoo.builder import Builder
from medoo.exception import WhereParseError, AnyAllSomeParseError

class TestDialect(testly.TestCase):

	def dataProvider_testQuote(self):
		yield '*', '*'
		yield 'a', '"a"'
		yield 'a"b', '"a""b"'
		yield [], '[]'

	def testQuote(self, in_, out):
		self.assertEqual(Dialect.quote(in_), out)

	def dataProvider_testValue(self):
		yield 'a', "'a'"
		yield "a'b", "'a''b'"
		yield [], '[]'
    
	def testValue(self, in_, out):
		self.assertEqual(Dialect.value(in_), out)

	def dataProvider_testLimit(self):
		yield 1, None, 'LIMIT 1'
		yield 2, 3, 'LIMIT 2 OFFSET 3'

	def testLimit(self, limit, offset, out):
		self.assertEqual(Dialect.limit(limit, offset), out)

	def dataProvider_testUpEq(self):
		yield 'f', 'a', "f='a'"
		yield 'f', "a'b", "f='a''b'"

	def testUpEq(self, field, value, out):
		self.assertEqual(Dialect.up_eq(field, value), out)

	def dataProvider_testIs(self):
		yield 'f', 'a', None, WhereParseError
		yield 'f', None, 'f IS NULL'

	def testIs(self, field, value, out, exception = None):
		if exception:
			self.assertRaises(exception, Dialect.is_, field, value)
		else:
			self.assertEqual(Dialect.is_(field, value), out)

	def dataProvider_testEq(self):
		yield 'f', (1,2), "f IN (1,2)"
		yield 'f', ('1','2'), "f IN ('1','2')"
		yield 'f', ('1',), "f = '1'"
		yield 'f', Builder()._select(), 'f IN (SELECT *)'

	def testEq(self, field, value, out):
		self.assertEqual(Dialect.eq(field, value), out)

	def dataProvider_testLike(self):
		yield 'f', 'a', "f LIKE '%a%'"
		yield 'f', ('a', 'b%'), "(f LIKE '%a%' OR f LIKE 'b%')"
		yield 'f', '%a', "f LIKE '%a'"

	def testLike(self, field, value, out):
		self.assertEqual(Dialect.like(field, value), out)

	def dataProvider_testNe(self):
		yield 'f', (1,2), "f NOT IN (1,2)"
		yield 'f', ('1','2'), "f NOT IN ('1','2')"
		yield 'f', ('1',), "f <> '1'"
		yield 'f', Builder()._select(), 'f NOT IN (SELECT *)'

	def testNe(self, field, value, out):
		self.assertEqual(Dialect.ne(field, value), out)

	def dataProvider_testBetween(self):
		yield 'f', 1, None, WhereParseError
		yield 'f', (1,2,3), None, WhereParseError
		yield 'f', (1, 2), "f BETWEEN 1 AND 2"

	def testBetween(self, field, value, out, exception = None):
		if exception:
			self.assertRaises(exception, Dialect.between, field, value)
		else:
			self.assertEqual(Dialect.between(field, value), out)

	def dataProvider_testDefault(self):
		yield '=any', 'f', Builder()._select(), 'f =ANY (SELECT *)'
		yield '= all', 'f', Builder()._select(), 'f = ALL (SELECT *)'
		yield '= some', 'f', Builder()._select(), 'f = SOME (SELECT *)'
		yield '>any', 'f', 'v', None, AnyAllSomeParseError
		yield '=', 'f', 'v', "f = 'v'"

	def testDefault(self, oprt, field, value, out, exception = None):
		if exception:
			self.assertRaises(exception, Dialect._default, oprt, field, value)
		else:
			self.assertEqual(Dialect._default(oprt, field, value), out)

	def dataProvider_testUpDefault(self):
		yield '+', 'f', 'v', "f=f+'v'"
		yield '-', 'f', 2, "f=f-2"

	def testUpDefault(self, oprt, field, value, out):
		self.assertEqual(Dialect._up_default(oprt, field, value), out)
	
	def dataProvider_testJoinDefault(self):
		yield 'JOIN', 'JOIN'
		yield 'LEFT JOIN', 'LEFT JOIN'
		yield 'RIGHT JOIN', 'RIGHT JOIN'
		yield 'INNER JOIN', 'INNER JOIN'
		yield 'FULL OUTER JOIN', 'FULL OUTER JOIN'

	def testJoinDefault(self, jointype, out):
		self.assertEqual(Dialect._join_default(jointype), out)

	def dataProvider_testOperator(self):
		yield '=', 'f', 'v', "f = 'v'"
		yield '^%$', 'f', 1, 'f ^%$ 1'

	def testOperator(self, oprt, field, value, out, exception = None):
		if exception:
			self.assertRaises(exception, Dialect._operator, oprt, field, value)
		else:
			self.assertEqual(Dialect._operator(oprt, field, value), out)

	def dataProvider_testUpdate(self):
		yield '+', 'f', 'v', "f=f+'v'"
		yield '-', 'f', 2, "f=f-2"

	def testUpdate(self, oprt, field, value, out):
		self.assertEqual(Dialect._update(oprt, field, value), out)

	def dataProvider_testJoin(self):
		yield 'JOIN', 'JOIN'
		yield 'LEFT JOIN', 'LEFT JOIN'
		yield 'RIGHT JOIN', 'RIGHT JOIN'
		yield 'INNER JOIN', 'INNER JOIN'
		yield 'FULL OUTER JOIN', 'FULL OUTER JOIN'

	def testJoin(self, jointype, out):
		self.assertEqual(Dialect._join(jointype), out)

if __name__ == '__main__':
	testly.main(verbosity = 2)
