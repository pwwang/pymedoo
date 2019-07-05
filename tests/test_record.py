import pytest
from . import moduleInstalled
pytestmark = pytest.mark.skipif(not moduleInstalled('sqlite3'), reason = 'sqlite3 is not installed.')

from collections import OrderedDict
from medoo.record import Records, Record
from medoo.exception import RecordKeyError, RecordAttributeError, GetFromEmptyRecordError
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

class TestRecord(object):
	"""
	Do simple test on Record, since it's been tested with 'records'
	"""
	@pytest.mark.parametrize('keys,values,exception', [
		([], [], None),
		([1], [], AssertionError)
	])
	def testInit(self, keys, values, exception):
		if exception:
			with pytest.raises(exception):
				Record(keys, values)
		else:
			record = Record(keys, values)
			assert record.keys() == keys
			assert record.values() == values

	@pytest.mark.parametrize('record, key, out, gitem, exception', [
		(Record([], []), 1, None, False, GetFromEmptyRecordError),
		(Record([], []), 1, None, True, GetFromEmptyRecordError),
		(Record([], []), 'a', None, False, RecordAttributeError),
		(Record([], []), 'a', None, True, RecordKeyError),
		(Record(['a', 'b', 'c'], [1,2,3]), 'b', 2, False, None),
		(Record(['a', 'b', 'c'], [1,2,3]), 'b', 2, True, None),
	])
	def testGetItemAttr(self, record, key, out, gitem, exception):
		if exception:
			if gitem:
				with pytest.raises(exception):
					record.__getitem__(key)
			else:
				with pytest.raises(exception):
					record.__getattr__(key)
		else:
			if gitem:
				assert record.__getitem__(key) == out
			else:
				assert record.__getattr__(key) == out

	@pytest.mark.parametrize('record, key, default, out', [
		(Record(['a'], [None]), 'a', None, None),
		(Record(['a'], [1]), 'a', None, 1),
		(Record(['a'], [None]), 'a', 1, None),
		(Record(['a'], [None]), 'b', 1, 1),
	])
	def testGet(self, record, key, default, out):
		assert record.get(key, default) == out

	def testRepr(self):
		assert repr(Record(['a'], [1])) == "<Record {'a': 1}>"

	def testGetItemExc(self):
		with pytest.raises(RecordKeyError):
			Record(['a', 'a'], [1, 2])['a']

	def testSetitemExc(self):
		with pytest.raises(RecordKeyError):
			Record(['a'], [1], readonly = True)['a'] = 2
		with pytest.raises(RecordKeyError):
			Record(['a', 'a'], [1, 2], readonly = False)['a'] = 3

	@pytest.mark.parametrize('record,key,val', [
		(Record(['a'], [None], readonly = False), 0, 1),
		(Record(['a'], [None], readonly = False), 'a', 1),
		(Record(['a'], [None], readonly = False), 'b', 1),
	])
	def testSetitem(self, record, key, val):
		record[key] = val
		assert record[key] == val

	def testDelitemExc(self):
		with pytest.raises(RecordKeyError):
			del Record(['a'], [1], readonly = True)['a']
		with pytest.raises(RecordKeyError):
			del Record(['a'], [1], readonly = False)['b']

	@pytest.mark.parametrize('record,key', [
		(Record(['a'], [None], readonly = False), 0),
		(Record(['a'], [None], readonly = False), 'a'),
		(Record(['a', 'b'], [None, None], readonly = False), 'b'),
	])
	def testDelitem(self, record, key):
		del record[key]
		assert key not in record

	def testSetattrExc(self):
		with pytest.raises(RecordAttributeError):
			Record([], []).a = 1
		with pytest.raises(RecordAttributeError):
			Record(['a', 'a'], [1, 2], readonly = False).a = 3

	@pytest.mark.parametrize('record,key,val', [
		(Record(['a'], [None], readonly = False), 0, 1),
		(Record(['a'], [None], readonly = False), 'a', 1),
		(Record(['a'], [None], readonly = False), 'b', 1),
	])
	def testSetattr(self, record, key, val):
		record.__setattr__(key, val)
		assert record.__getattr__(key) == val

	def testDir(self):
		record = Record(['a', 'b'], [None, None])
		assert 'a' in dir(record)
		assert 'b' in dir(record)

	def testEq(self):
		assert Record(['a', 'b'], [None, None]) == OrderedDict([('a', None), ('b', None)])
		assert Record(['a', 'b'], [None, None]) == Record(['a', 'b'], [None, None])
		assert Record(['a', 'b'], [None, None]) != Record(['b', 'a'], [None, None])
		assert Record(['a', 'b'], [None, None]) == {'a': None, 'b': None}

	@pytest.mark.parametrize('record,key,index', [
		(Record(['a'], [None], readonly = False), 'a', 0),
		(Record(['a', 'b', 'b'], [None, None, None], readonly = False), 'b', 1),
	])
	def testIndex(self, record, key, index):
		assert record.index(key) == index

	@pytest.mark.parametrize('record,items', [
		(Record(['a'], [None], readonly = False), [('a', None)]),
		(Record(['a', 'b', 'b'], [None, None, None], readonly = False), [('a', None), ('b', None), ('b', None)]),
	])
	def testItems(self, record, items):
		assert list(record.items()) == items

class TestRecords(object):


	def testInit(self, db):
		db.select('t')
		rs = Records(db.cursor)
		assert rs._cursor is db.cursor
		assert rs.meta == ['id', 'cont', 'icont']
		assert rs.pending
		assert rs._allrows == []
		assert repr(rs) == '<Records: size=0, pending=True>'
		# __nonzero__
		assert rs.__nonzero__() is True

	def testFirst(self, db):
		db.select('t', where = {'id': 100})
		rs = Records(db.cursor)
		assert rs.first() is None
		with pytest.raises(ValueError):
			rs.first(default = ValueError())

	def testNext(self, db):
		db.select('t')
		rs = Records(db.cursor)
		r0 = next(rs)
		assert r0 == {'id': 1, 'cont': 'a', 'icont': 0}
		assert r0 == rs[0]
		assert rs.first() == {'id': 1, 'cont': 'a', 'icont': 0}
		r1 = next(rs)
		assert r1 == {'id': 2, 'cont': 'b', 'icont': 1}
		ret = [r for r in rs] # __iter__
		assert ret == [
			Record(['id', 'cont', 'icont'], [1, 'a', 0]),
			Record(['id', 'cont', 'icont'], [2, 'b', 1]),
			Record(['id', 'cont', 'icont'], [3, 'c', 2]),
			Record(['id', 'cont', 'icont'], [4, 'd', 9]),
			Record(['id', 'cont', 'icont'], [5, 'e', 3]),
			Record(['id', 'cont', 'icont'], [6, None,3]),
			Record(['id', 'cont', 'icont'], [7, 'g', 4]),
			Record(['id', 'cont', 'icont'], [8, 'h', 5]),
			Record(['id', 'cont', 'icont'], [9, 'i', 3]),
			Record(['id', 'cont', 'icont'], [10, 'j',1])
		]


		assert rs.all() == ret
		assert rs[9:] == [Record(['id', 'cont', 'icont'], [10, 'j',1])]

		assert rs.all(asdict = True) == [
			dict(zip(['id', 'cont', 'icont'], [1, 'a', 0])),
			dict(zip(['id', 'cont', 'icont'], [2, 'b', 1])),
			dict(zip(['id', 'cont', 'icont'], [3, 'c', 2])),
			dict(zip(['id', 'cont', 'icont'], [4, 'd', 9])),
			dict(zip(['id', 'cont', 'icont'], [5, 'e', 3])),
			dict(zip(['id', 'cont', 'icont'], [6, None,3])),
			dict(zip(['id', 'cont', 'icont'], [7, 'g', 4])),
			dict(zip(['id', 'cont', 'icont'], [8, 'h', 5])),
			dict(zip(['id', 'cont', 'icont'], [9, 'i', 3])),
			dict(zip(['id', 'cont', 'icont'], [10, 'j',1]))
		]

		assert rs.all(asdict = 'ordered') == [
			OrderedDict(zip(['id', 'cont', 'icont'], [1, 'a', 0])),
			OrderedDict(zip(['id', 'cont', 'icont'], [2, 'b', 1])),
			OrderedDict(zip(['id', 'cont', 'icont'], [3, 'c', 2])),
			OrderedDict(zip(['id', 'cont', 'icont'], [4, 'd', 9])),
			OrderedDict(zip(['id', 'cont', 'icont'], [5, 'e', 3])),
			OrderedDict(zip(['id', 'cont', 'icont'], [6, None,3])),
			OrderedDict(zip(['id', 'cont', 'icont'], [7, 'g', 4])),
			OrderedDict(zip(['id', 'cont', 'icont'], [8, 'h', 5])),
			OrderedDict(zip(['id', 'cont', 'icont'], [9, 'i', 3])),
			OrderedDict(zip(['id', 'cont', 'icont'], [10, 'j',1]))
		]

	@pytest.mark.skipif(not moduleInstalled('tablib'), reason = 'tablib not installed')
	def testTablib(self, db):
		db.select('t')
		rs = Records(db.cursor)
		assert rs.export('csv', lineterminator="\n") == """id,cont,icont
1,a,0
2,b,1
3,c,2
4,d,9
5,e,3
6,,3
7,g,4
8,h,5
9,i,3
10,j,1
"""

