import unittest, tempfile, shutil, re, mimetypes, sys
from glob import glob
from os import makedirs
from hashlib import md5
from pyppl import Box
from six import StringIO, with_metaclass, assertRaisesRegex as sixAssertRaisesRegex
import inspect, gzip
from subprocess import Popen, PIPE
from os import path, listdir
from contextlib import contextmanager

######

@contextmanager
def captured_output():
	new_out, new_err = StringIO(), StringIO()
	old_out, old_err = sys.stdout, sys.stderr
	try:
		sys.stdout, sys.stderr = new_out, new_err
		yield sys.stdout, sys.stderr
	finally:
		sys.stdout, sys.stderr = old_out, old_err

def md5sum(fn):
	ret = md5()
	with open(fn, "rb") as f:
		#for chunk in iter(lambda: f.read(4096), b""):
		#	ret.update(chunk)
		ret.update(f.read())
	return ret.hexdigest()

def testingOrder (_, x, y):
	if not re.search(r'_\d+$', x):
		x += '_0'
	if not re.search(r'_\d+$', y):
		y += '_0'
	return -1 if x<y else 1 if x>y else 0
	
unittest.TestLoader.sortTestMethodsUsing = testingOrder
class DataProviderSupport(type):
	def __new__(meta, classname, bases, classDict):
		# method for creating our test methods
		def create_test_method(testFunc, args):
			return lambda self: testFunc(self, *args)

		def create_setup_method(stfunc):
			def stFunc(self):
				if not re.search(r'_\d+$', self._testMethodName):
					stfunc(self)
			return stFunc
			
		def create_teardown_method(tdfunc):			
			def tdFunc(self):
				call = True
				if re.search(r'_\d+$', self._testMethodName):
					base, _, idx = self._testMethodName.rpartition('_')
					idx = int(idx)
					for key in classDict.keys():
						if not key.startswith(base + '_'): continue
						_, _, idx2 = key.rpartition('_')
						if int(idx2) > idx:
							call = False
							break
				else:
					for key in classDict.keys():
						if key.startswith(self._testMethodName + '_'): 
							call = False
							break
				if call: tdfunc(self)
			return tdFunc

		parentDir = path.join(tempfile.gettempdir(), 'bioprocs_unittest', classname)
		tfilesDir = path.join(path.dirname(path.dirname(__file__)), 'testfiles', classname[4].lower() + classname[5:])
		indir     = path.join(tfilesDir, 'input')
		outdir    = path.join(tfilesDir, 'expect')
		if path.isdir(parentDir):
			shutil.rmtree(parentDir)
		# look for data provider functions
		
		for attrName in list(classDict.keys()):

			attr = classDict[attrName]
			if attrName == 'setUp':
				classDict['setUp'] = create_setup_method(attr)
				continue
			if attrName == 'tearDown':
				classDict['tearDown'] = create_teardown_method(attr)
				continue

			if not attrName.startswith("dataProvider_"):
				continue

			# find out the corresponding test method
			testName = attrName[13:]
			testFunc = classDict[testName]
			testdir  = path.join(parentDir, testName)
			if not path.isdir(testdir):
				makedirs(testdir)

			# the test method is no longer needed
			#del classDict[testName]
			# in case if there is no data provided
			classDict[testName] = lambda self: None

			# generate test method variants based on
			# data from the data porovider function
			lenargs = len(inspect.getargspec(attr).args)
			data    = attr(Box(classDict), testdir, indir, outdir) if lenargs == 4 else \
					  attr(Box(classDict), testdir, indir) if lenargs == 3 else \
					  attr(Box(classDict), testdir) if lenargs == 2 else \
					  attr(Box(classDict)) if lenargs == 1 else \
					  attr()
			if data:
				for i, arg in enumerate(data):
					key = testName if i == 0 else testName + '_' + str(i)
					classDict[key] = create_test_method(testFunc, arg)

		# create the type
		return type.__new__(meta, classname, bases, classDict)


class TestCase(with_metaclass(DataProviderSupport, unittest.TestCase)):

	def assertItemEqual(self, first, second, msg = None):
		first          = [repr(x) for x in first]
		second         = [repr(x) for x in second]
		first          = str(sorted(first))
		second         = str(sorted(second))
		assertion_func = self._getAssertEqualityFunc(first, second)
		assertion_func(first, second, msg=msg)

	def assertDictIn(self, first, second, msg = 'Not all k-v pairs in 1st element are in the second.'):
		assert isinstance(first, dict)
		assert isinstance(second, dict)
		notInkeys = [k for k in first.keys() if k not in second.keys()]
		if notInkeys:
			self.fail(msg = 'Keys of first dict not in second: %s' % notInkeys)
		else:
			seconds2 = {k:second[k] for k in first.keys()}
			for k in first.keys():
				v1   = first[k]
				v2   = second[k]
				try:
					self.assertSequenceEqual(v1, v2)
				except AssertionError:
					self.assertEqual(v1, v2)
				
				

	def assertDictNotIn(self, first, second, msg = 'all k-v pairs in 1st element are in the second.'):
		assert isinstance(first, dict)
		assert isinstance(second, dict)
		ret = False
		for k in first.keys():
			if k in second:
				if first[k] != second[k]:
					ret = True
			else:
				ret = True
		if not ret:
			self.fail(msg)
	
	def assertFileEqual(self, first, second, msg = None):
		t1, _ = mimetypes.guess_type(first)
		t2, _ = mimetypes.guess_type(second)
		if t1 != t2:
			self.fail(msg = msg or 'First file is "%s", but second is "%s"' % (t1, t2))
		if t1 and t1.startswith('text/'):
			with open(first) as f1, open(second) as f2:
				first  = f1.read().splitlines()
				second = f2.read().splitlines()
			self.assertListEqual(first, second, msg)
		else:
			md5sum1 = md5sum(first)
			md5sum2 = md5sum(second)
			self.assertEqual(md5sum(first), md5sum(second), msg or 'Md5sums of two testing files are different:\n- %s %s\n- %s %s' % (md5sum1, first, md5sum2, second))
	
	def assertDirEqual(self, first, second, msg = None):
		if not path.isdir(first):
			self.fail('The first file is not a directory.')
		if not path.isdir(second):
			self.fail('The second file is not a directory.')
		for fn in glob(path.join(first, '*')):
			bn = path.basename(fn)
			if path.isdir(fn):
				self.assertDirEqual(fn, path.join(second, bn))
			else:
				self.assertFileEqual(fn, path.join(second, bn))

	def assertTextEqual(self, first, second, msg = None):
		if not isinstance(first, list):
			first  = first.split('\n')
		if not isinstance(second, list):
			second = second.split('\n')
		self.assertListEqual(first, second, msg)

	def assertRaisesStr(self, exc, s, callable, *args, **kwds):
		sixAssertRaisesRegex(self, exc, s, callable, *args, **kwds)

	def assertItemSubset(self, s, t, msg = 'The first list is not a subset of the second.'):
		assert isinstance(s, list)
		assert isinstance(t, list)
		self.assertTrue(set(s) < set(t), msg = msg)

	def assertInFile(self, s, f):
		sf = readFile(f, str)
		self.assertIn(s, sf)
	
	def index(self):
		from builtins import str as text
		_, _, x = self.id().rpartition('_')
		return '0' if not text(x).isnumeric() else text(x)
