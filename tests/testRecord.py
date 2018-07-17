import testly, sqlite3
from medoo.record import Records, Record
from medoo.exception import RecordKeyError, RecordAttributeError

def moduleInstalled(mod):
	try:
		__import__(mod)
		return True
	except ImportError:
		return False

class TestRecord(testly.TestCase):
	"""
	Do simple test on Record, since it's been tested with 'records'
	"""
	def dataProvider_testInit(self):
		yield [], []
		yield [1], [], AssertionError

	def testInit(self, keys, values, exception = None):
		if exception:
			self.assertRaises(exception, Record, keys, values)
		else:
			record = Record(keys, values)
			self.assertListEqual(record.keys(), keys)
			self.assertListEqual(record.values(), values)

	def dataProvider_testGetItemAttr(self):
		yield Record([], []), 1, None, False, IndexError
		yield Record([], []), 1, None, True, IndexError
		yield Record([], []), 'a', None, False, RecordAttributeError
		yield Record([], []), 'a', None, True, RecordKeyError
		yield Record(['a', 'b', 'c'], [1,2,3]), 'b', 2, False
		yield Record(['a', 'b', 'c'], [1,2,3]), 'b', 2, True

	def testGetItemAttr(self, record, key, out, gitem = False, exception = None):
		if exception:
			if gitem:
				self.assertRaises(exception, record.__getitem__, key)
			else:
				self.assertRaises(exception, record.__getattr__, key)
		else:
			if gitem:
				self.assertEqual(record.__getitem__(key), out)
			else:
				self.assertEqual(record.__getattr__(key), out)

	def dataProvider_testGet(self):
		yield Record([], []), 'a', None, None
		yield Record(['a'], [1]), 'a', None, 1
		yield Record(['a'], [None]), 'a', 1, None
		yield Record(['a'], [None]), 'b', 1, 1

	def testGet(self, record, key, default, out):
		self.assertEqual(record.get(key, default), out)
				
class TestRecords(testly.TestCase):

	def setUpMeta(self):
		"""
		Set up a test database
		"""
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
		con = sqlite3.connect(":memory:")
		cur = con.cursor()
		cur.execute("CREATE TABLE t (id int auto increment, cont text);")
		cur.executemany("INSERT INTO t (id, cont) VALUES (?, ?);", data)
		con.commit()
		cur.execute("SELECT * FROM t;")
		self.conn   = con
		self.cursor = cur

	def reset(self):
		self.cursor.execute("SELECT * FROM t;")

	@classmethod
	def tearDownClass(self):
		if self.conn:
			self.conn.close()

	def setUp(self):
		self.reset()

	def testInit(self):
		rs = Records(self.cursor)
		self.assertIs(rs._cursor, self.cursor)
		self.assertListEqual(rs.meta, ['id', 'cont'])
		self.assertTrue(rs.pending)
		self.assertEqual(rs._allrows, [])
		self.assertEqual(repr(rs), '<Records: size=0, pending=True>')

	def testNext(self):
		rs = Records(self.cursor)
		r0 = next(rs)
		self.assertEqual(r0, {'id': 1, 'cont': 'a'})
		self.assertEqual(rs.first(), {'id': 1, 'cont': 'a'})
		r1 = next(rs)
		self.assertEqual(r1, {'id': 2, 'cont': 'b'})
		ret = [r for r in rs] # __iter__
		self.assertListEqual(ret, [
			Record(['id', 'cont'], [1, 'a']),
			Record(['id', 'cont'], [2, 'b']),
			Record(['id', 'cont'], [3, 'c']),
			Record(['id', 'cont'], [4, 'd']),
			Record(['id', 'cont'], [5, 'e']),
			Record(['id', 'cont'], [6, 'f']),
			Record(['id', 'cont'], [7, 'g']),
			Record(['id', 'cont'], [8, 'h']),
			Record(['id', 'cont'], [9, 'i']),
			Record(['id', 'cont'], [10, 'j'])
		])

	
		self.assertEqual(rs.all(), ret)

	@testly.skipIf(not moduleInstalled('tablib'), 'tablib not installed')
	def testTablib(self):
		rs = Records(self.cursor)
		self.assertEqual(rs.export('csv', lineterminator="\n"), """id,cont
1,a
2,b
3,c
4,d
5,e
6,f
7,g
8,h
9,i
10,j
""")

if __name__ == '__main__':
	testly.main(verbosity = 2)
