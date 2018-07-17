
class UpdateParseError(Exception):
	pass
	
class FieldParseError(Exception):
	pass
	
class TableParseError(Exception):
	pass
	
class WhereParseError(Exception):
	pass

class JoinParseError(Exception):
	pass

class AnyAllSomeParseError(Exception):
	pass

class LimitParseError(Exception):
	pass

class InsertParseError(Exception):
	pass

class RecordKeyError(KeyError):
	pass

class RecordAttributeError(AttributeError):
	pass
