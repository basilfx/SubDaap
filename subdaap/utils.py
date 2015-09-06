import argparse
import zlib
import os


class VerboseAction(argparse.Action):
    """
    Argparse action to count the verbose level (e.g. -v, -vv, etc).
    """

    def __call__(self, parser, args, value, option_string=None):
        try:
            value = int(value or "1")
        except ValueError:
            value = value.count("v") + 1

        setattr(args, self.dest, value)


class NewPathAction(argparse.Action):
    """
    Argparse action that resolves a given path to absolute path.
    """

    def __call__(self, parser, args, values, option_string=None):
        setattr(args, self.dest, os.path.abspath(values))


class PathAction(argparse.Action):
    """
    Argparse action that resolves a given path, and ensures it exists.
    """

    def __call__(self, parser, args, values, option_string=None):
        path = os.path.abspath(values)

        if not os.path.exists(path):
            parser.error("Path doesn't exist for '%s': %s" % (
                option_string, path))

        setattr(args, self.dest, path)


def dict_checksum(*args, **kwargs):
    """
    Calculate a hash of the values of a dictionary.
    """

    # Accept kwargs as input dict
    if len(args) == 1:
        input_dict = args[0]
    else:
        input_dict = kwargs

    # Calculate checksum
    data = bytearray()

    for value in input_dict.itervalues():
        if type(value) != unicode:
            value = unicode(value)
        data.extend(bytearray(value.encode("utf-8")))

    return zlib.adler32(buffer(data))


def force_dict(value):
    """
    Coerce the input value to a dict.
    """

    if type(value) == dict:
        return value
    else:
        return {}


def force_list(value):
    """
    Coerce the input value to a list.

    If `value` is `None`, return an empty list. If it is a single value, create
    a new list with that element on index 0.

    :param value: Input value to coerce.
    :return: Value as list.
    :rtype: list
    """

    if value is None:
        return []
    elif type(value) == list:
        return value
    else:
        return [value]


def human_bytes(size):
    """
    Convert a given size (in bytes) to a human-readable representation.

    :param int size: Size in bytes.
    :return: Human-readable representation of size, e.g. 1MB.
    :rtype: str
    """

    for x in ("B", "KB", "MB", "GB"):
        if size < 1024.0 and size > -1024.0:
            return "%3.1f%s" % (size, x)
        size /= 1024.0
    return "%3.1f%s" % (size, "TB")


def in_list(input_list):
    """
    """
    return ",".join(str(x) for x in input_list)


def exhaust(iterator):
    """
    Exhaust an iterator, without returning anything.

    :param iterator iterator: Iterator to exhaust.
    """

    for _ in iterator:
        pass


def chunks(iterator, size):
    """
    Chunk an iterator into blocks of fixed size. Only the last block can be
    smaller than the specified size.

    :param iterator iterator: Iterator to exhaust.
    :param int size: Size of blocks to yield.
    """

    items = [None] * size
    count = 0

    for item in iterator:
        items[count] = item
        count += 1

        if count == size:
            yield items
            count = 0

    # Yield remaining
    yield items[:count]
