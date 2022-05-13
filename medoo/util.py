"""Utilities for pymedoo"""


def always_list(x):
    """
    Always return a list
    """
    return [y.strip() for y in x.split(",")] if isinstance(x, str) else list(x)


def reduce_datetimes(row):
    """
    Receives a row, converts datetimes to strings.
    """
    row = list(row)

    for i, iterrow in enumerate(row):
        if hasattr(iterrow, "isoformat"):
            row[i] = iterrow.isoformat()
    return tuple(row)
