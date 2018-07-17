from collections import OrderedDict
from .exception import RecordKeyError, RecordAttributeError
from . import utils

class Record(object):
	"""
	A row, from a query, from a database.
	The idea is borrowed from https://github.com/kennethreitz/records
	"""
	__slots__ = ('_keys', '_values')

	def __init__(self, keys, values):
		self._keys   = keys
		self._values = values

		# Ensure that lengths match properly.
		assert len(self._keys) == len(self._values)

	def keys(self):
		"""
		Returns the list of column names from the query.
		"""
		return self._keys

	def values(self):
		"""
		Returns the list of values from the query.
		"""
		return self._values

	def __repr__(self):
		return '<Record {}>'.format(self.as_dict())

	def __getitem__(self, key):
		# Support for index-based lookup.
		if isinstance(key, int):
			return self.values()[key]

		# Support for string-based lookup.
		if key in self.keys():
			i = self.keys().index(key)
			if self.keys().count(key) > 1:
				raise RecordKeyError("Record contains multiple '{}' fields.".format(key))
			return self.values()[i]

		raise RecordKeyError("Record contains no '{}' field.".format(key))

	def __getattr__(self, key):
		try:
			return self[key]
		except RecordKeyError as e:
			raise RecordAttributeError(e)

	def __dir__(self):
		standard = dir(super(Record, self))
		# Merge standard attrs with generated ones (from column names).
		return sorted(standard + [str(k) for k in self.keys()])

	def __eq__(self, other):
		if isinstance(other, OrderedDict):
			return list(self.keys()) == list(other.keys()) and list(self.values()) == list(other.values())
		else:
			return set(self.keys()) == set(other.keys()) and set(self.values()) == set(other.values())

	def __ne__(self, other):
		return not self.__eq__(other)

	def get(self, key, default=None):
		"""
		Returns the value for a given key, or default.
		"""
		try:
			return self[key]
		except RecordKeyError:
			return default

	def as_dict(self, ordered = False):
		"""
		Returns the row as a dictionary, as ordered.
		"""
		items = zip(self.keys(), self.values())

		return OrderedDict(items) if ordered else dict(items)

	asDict = as_dict


class Records(object):
	"""
	A set of excellent Records from a query.
	"""
	
	def __init__(self, cursor):
		self.meta     = [desc[0] for desc in cursor.description]
		self._cursor  = cursor
		self._allrows = []
		self.pending  = True

	def __repr__(self):
		return '<Records: size={}, pending={}>'.format(len(self), self.pending)

	def __iter__(self):
		"""
		Iterate over all rows, consuming the underlying generator
		only when necessary.
		"""
		i = 0
		while True:
			# Other code may have iterated between yields,
			# so always check the cache.
			if i < len(self):
				yield self[i]
			else:
				# Throws StopIteration when done.
				# Prevent StopIteration bubbling from generator, following https://www.python.org/dev/peps/pep-0479/
				try:
					yield next(self)
				except StopIteration:
					return
			i += 1

	def __nonzero__(self):
		return self.first() is not None

	def next(self):
		return self.__next__()

	def __next__(self):
		try:
			nextrow = Record(self.meta, list(next(self._cursor)))
			self._allrows.append(nextrow)
			return nextrow
		except StopIteration:
			self.pending = False
			raise StopIteration('Records contains no more rows.')

	def __getitem__(self, key):
		is_int = isinstance(key, int)

		# Convert RecordCollection[1] into slice.
		if is_int:
			key = slice(key, key + 1)

		while len(self) < key.stop or key.stop is None:
			try:
				next(self)
			except StopIteration:
				break

		ret = self._allrows[key]
		return ret[0] if is_int else ret

	def __len__(self):
		return len(self._allrows)

	def export(self, format, **kwargs): # pragma: no cover
		"""
		Export the RecordCollection to a given format (courtesy of Tablib).
		"""
		return self.tldata.export(format, **kwargs)

	@property
	def tldata(self):  # pragma: no cover
		"""
		A Tablib Dataset representation of the Records.
		"""
		import tablib
		# Create a new Tablib Dataset.
		data = tablib.Dataset()

		# If the RecordCollection is empty, just return the empty set
		# Check number of rows by typecasting to list
		if len(list(self)) == 0:
			return data

		data.headers = self.meta
		for row in self.all():
			row = utils.reduce_datetimes(row.values())
			data.append(row)

		return data

	def all(self):
		"""Returns a list of all rows for the RecordCollection. If they haven't
		been fetched yet, consume the iterator and cache the results."""

		# By calling list it calls the __iter__ method
		return list(self)

	def first(self, default = None):
		"""
		Returns a single record for the RecordCollection, or `default`. If
		`default` is an instance or subclass of Exception, then raise it
		instead of returning it.
		"""

		# Try to get a record, or return/raise default.
		try:
			record = self[0]
		except IndexError:
			if isinstance(default, Exception):
				raise default
			return default

		return record


