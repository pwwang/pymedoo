
"""Exceptions"""

class UpdateParseError(Exception):
    """Failed to parse update clause"""

class FieldParseError(Exception):
    """Failed to parse field"""

class TableParseError(Exception):
    """Failed to parse table"""

class WhereParseError(Exception):
    """Failed to parse where clause"""

class JoinParseError(Exception):
    """Failed to parse join clause"""

class AnyAllSomeParseError(Exception):
    """Failed to parse any, all or some modifiers"""

class LimitParseError(Exception):
    """Failed to parse limit"""

class InsertParseError(Exception):
    """Failed to parse insert clause"""

class RecordKeyError(KeyError):
    """KeyError for Record"""

class RecordAttributeError(AttributeError):
    """AttributeError for Record"""

class GetFromEmptyRecordError(Exception):
    """Try to get from empty record"""
