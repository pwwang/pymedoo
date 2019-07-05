import pytest
from medoo.dialect import Dialect
from medoo.builder import Builder
from medoo.exception import WhereParseError, AnyAllSomeParseError

class TestDialect(object):

	@pytest.mark.parametrize('in_,out', [
		('*', '*'),
		('a', '"a"'),
		('a"b', '"a""b"'),
		([], '[]'),
	])
	def testQuote(self, in_, out):
		assert Dialect.quote(in_) == out

	@pytest.mark.parametrize('in_,out', [
		('a', "'a'"),
		("a'b", "'a''b'"),
		([], '[]'),
	])
	def testValue(self, in_, out):
		assert Dialect.value(in_) == out

	@pytest.mark.parametrize('limit,offset,out',[
		(1, None, 'LIMIT 1'),
		(2, 3, 'LIMIT 2 OFFSET 3'),
	])
	def testLimit(self, limit, offset, out):
		assert Dialect.limit(limit, offset) == out

	@pytest.mark.parametrize('field,value,out',[
		('f', 'a', "f='a'"),
		('f', "a'b", "f='a''b'"),
	])
	def testUpEq(self, field, value, out):
		assert Dialect.up_eq(field, value) == out

	@pytest.mark.parametrize('field,value,out,exception',[
		('f', 'a', None, WhereParseError),
		('f', None, 'f IS NULL', None),
	])
	def testIs(self, field, value, out, exception):
		if exception:
			with pytest.raises(exception):
				Dialect.is_(field, value)
		else:
			assert Dialect.is_(field, value) == out

	@pytest.mark.parametrize('field,value,out',[
		('f', (1,2), "f IN (1,2)"),
		('f', ('1','2'), "f IN ('1','2')"),
		('f', ('1',), "f = '1'"),
		('f', Builder()._select(), 'f IN (SELECT *)'),
	])
	def testEq(self, field, value, out):
		assert Dialect.eq(field, value) == out

	@pytest.mark.parametrize('field,value,out',[
		('f', 'a', "f LIKE '%a%'"),
		('f', ('a', 'b%'), "(f LIKE '%a%' OR f LIKE 'b%')"),
		('f', '%a', "f LIKE '%a'"),
	])
	def testLike(self, field, value, out):
		assert Dialect.like(field, value) == out

	@pytest.mark.parametrize('field,value,out',[
		('f', (1,2), "f NOT IN (1,2)"),
		('f', ('1','2'), "f NOT IN ('1','2')"),
		('f', ('1',), "f <> '1'"),
		('f', Builder()._select(), 'f NOT IN (SELECT *)'),
	])
	def testNe(self, field, value, out):
		assert Dialect.ne(field, value) == out

	@pytest.mark.parametrize('field,value,out,exception',[
		('f', 1, None, WhereParseError),
		('f', (1,2,3), None, WhereParseError),
		('f', (1, 2), "f BETWEEN 1 AND 2",None),
	])
	def testBetween(self, field, value, out, exception):
		if exception:
			with pytest.raises(exception):
				Dialect.between(field, value)
		else:
			assert Dialect.between(field, value) == out

	@pytest.mark.parametrize('oprt,field,value,out,exception',[
		('=any', 'f', Builder()._select(), 'f =ANY (SELECT *)',None),
		('= all', 'f', Builder()._select(), 'f = ALL (SELECT *)',None),
		('= some', 'f', Builder()._select(), 'f = SOME (SELECT *)',None),
		('>any', 'f', 'v', None, AnyAllSomeParseError),
		('=', 'f', 'v', "f = 'v'",None),
	])
	def testDefault(self, oprt, field, value, out, exception):
		if exception:
			with pytest.raises(exception):
				Dialect._default(oprt, field, value)
		else:
			assert Dialect._default(oprt, field, value) == out

	@pytest.mark.parametrize('oprt,field,value,out',[
		('+', 'f', 'v', "f=f+'v'"),
		('-', 'f', 2, "f=f-2"),
	])
	def testUpDefault(self, oprt, field, value, out):
		assert Dialect._up_default(oprt, field, value) == out

	@pytest.mark.parametrize('jointype,out',[
		('JOIN', 'JOIN'),
		('LEFT JOIN', 'LEFT JOIN'),
		('RIGHT JOIN', 'RIGHT JOIN'),
		('INNER JOIN', 'INNER JOIN'),
		('FULL OUTER JOIN', 'FULL OUTER JOIN'),
	])
	def testJoinDefault(self, jointype, out):
		assert Dialect._join_default(jointype) == out

	@pytest.mark.parametrize('oprt,field,value,out,exception', [
		('=', 'f', 'v', "f = 'v'", None),
		('^%$', 'f', 1, 'f ^%$ 1', None),
	])
	def testOperator(self, oprt, field, value, out, exception):
		if exception:
			with pytest.raises(exception):
				Dialect._operator(oprt, field, value)
		else:
			assert Dialect._operator(oprt, field, value) == out

	@pytest.mark.parametrize('oprt,field,value,out',[
		('+', 'f', 'v', "f=f+'v'"),
		('-', 'f', 2, "f=f-2"),
	])
	def testUpdate(self, oprt, field, value, out):
		assert Dialect._update(oprt, field, value) == out

	@pytest.mark.parametrize('jointype,out',[
		('JOIN', 'JOIN'),
		('LEFT JOIN', 'LEFT JOIN'),
		('RIGHT JOIN', 'RIGHT JOIN'),
		('INNER JOIN', 'INNER JOIN'),
		('FULL OUTER JOIN', 'FULL OUTER JOIN'),
	])
	def testJoin(self, jointype, out):
		assert Dialect._join(jointype) == out
