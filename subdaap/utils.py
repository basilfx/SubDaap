import argparse
import zlib
import os


class VerboseAction(argparse.Action):
    def __call__(self, parser, args, value, option_string=None):
        try:
            value = int(value or "1")
        except ValueError:
            value = value.count("v") + 1

        setattr(args, self.dest, value)


class NewPathAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        setattr(args, self.dest, os.path.abspath(values))


class PathAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        path = os.path.abspath(values)

        if not os.path.exists(path):
            parser.error("Path doesn't exist for '%s': %s" % (
                option_string, path))

        setattr(args, self.dest, path)


def dict_checksum(input_dict):
    """
    Calculate a hash of the values of a dictionary.
    """

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
    """

    if value is None:
        return []
    elif type(value) == list:
        return value
    else:
        return [value]


def human_bytes(size):
    for x in ["bytes", "KB", "MB", "GB"]:
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
    """

    for _ in iterator:
        pass
