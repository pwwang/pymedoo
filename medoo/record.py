from collections import OrderedDict
from .exception import RecordKeyError, RecordAttributeError
from . import utils

class Record(object):
	"""
	A row, from a query, from a database.
	The idea is borrowed from https://github.com/kennethreitz/records
	"""
	def __init__(self, keys, values, readonly = True):
		# sacrifice some efficiency but support setitem, setattr operation
		self.__dict__['_keys']     = keys if readonly else keys[:]
		self.__dict__['_values']   = values if readonly else values[:]
		self.__dict__['_readonly'] = readonly
		# Ensure that lengths match properly.
		assert len(self.keys()) == len(self.values())

	def keys(self):
		"""
		Returns the list of column names from the query.
		"""
		return self.__dict__['_keys']

	def values(self):
		"""
		Returns the list of values from the query.
		"""
		return self.__dict__['_values']

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

	def __setitem__(self, key, val):
		if self.__dict__['_readonly']:
			raise RecordKeyError("Readonly Record does not support setitem operation.")

		if isinstance(key, int):
			self.values()[key] = val
			return
		
		keycount = self.keys().count(key)
		if keycount > 1:
			raise RecordKeyError("Record contains multiple '{}' fields.".format(key))
		elif keycount == 1:
			i = self.keys().index(key)
			self.values()[i] = val
		else: # 0
			self.keys().append(key)
			self.values().append(val)

	def __delitem__(self, key):
		if self.__dict__['_readonly']:
			raise RecordKeyError("Readonly Record does not support setitem operation.")

		# Support for index-based lookup.
		if isinstance(key, int):
			del self.keys()[key]
			del self.values()[key]
			return

		# Support for string-based lookup.
		if key in self.keys():
			i = self.keys().index(key)
			if self.keys().count(key) > 1:
				raise RecordKeyError("Record contains multiple '{}' fields.".format(key))
			del self.keys()[i]
			del self.values()[i]
			return

		raise RecordKeyError("Record contains no '{}' field.".format(key))

	def __getattr__(self, key):
		try:
			return self[key]
		except RecordKeyError as e:
			raise RecordAttributeError(e)

	def __setattr__(self, key, val):
		if self.__dict__['_readonly']:
			raise RecordAttributeError("Readonly Record does not support setitem operation.")

		try:
			self[key] = val
		except RecordKeyError as e:
			raise RecordAttributeError(e)

	def __dir__(self):
		standard = dir(super(Record, self))
		# Merge standard attrs with generated ones (from column names).
		return sorted(standard + [str(k) for k in self.keys()])

	def __eq__(self, other):
		if isinstance(other, OrderedDict):
			return self.as_dict(True) == other
		elif isinstance(other, Record):
			return self.as_dict(True) == other.as_dict(True)
		else:
			return self.as_dict() == dict(other)

	def __ne__(self, other):
		return not self.__eq__(other)

	def __contains__(self, key):
		return key in self.keys()

	def __index__(self, key):
		return self.keys().index(key)

	def get(self, key, default=None):
		"""
		Returns the value for a given key, or default.
		"""
		try:
			return self[key]
		except RecordKeyError:
			return default

	def items(self):
		for i, k in enumerate(self.keys()):
			yield k, self.values()[i]

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
	
	def __init__(self, cursor, readonly = True):
		self.meta     = [desc[0] for desc in cursor.description]
		self._cursor  = cursor
		self._allrows = []
		self.pending  = True
		self.readonly = readonly

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
			nextrow = Record(self.meta, list(next(self._cursor)), readonly = self.readonly)
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

	def all(self, asdict = False):
		"""
		Returns a list of all rows for the RecordCollection. If they haven't
		been fetched yet, consume the iterator and cache the results.
		@params:
			`asdict`: Whether convert the records to dicts.
				- `False`: don't convert, keep `Record`
				- `True`:  convert to plain dict
				- `ordered`: convert to `OrderedDict`
		"""

		# By calling list it calls the __iter__ method
		if not asdict:
			return list(self)
		elif asdict is True:
			return [r.as_dict() for r in self]
		else:
			return [r.as_dict(True) for r in self]

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


