"""Utilities for pymedoo"""

def always_list(x):
    """
    Always return a list
    """
    from six import string_types
    return ([y.strip()
             for y in x.split(',')]
            if isinstance(x, string_types)
            else list(x))

def reduce_datetimes(row):
    """
    Receives a row, converts datetimes to strings.
    """
    row = list(row)

    for i, iterrow in enumerate(row):
        if hasattr(iterrow, 'isoformat'):
            row[i] = iterrow.isoformat()
    return tuple(row)
