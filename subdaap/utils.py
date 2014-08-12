import argparse
import urlparse
import zlib
import uuid
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
            parser.error("Path doesn't exist for '%s': %s" % (option_string, path))

        setattr(args, self.dest, path)

def dict_checksum(media):
    """
    Calculate a hash of the values of a dictionary.
    """

    return zlib.adler32("".join([ unicode(value) for value in media.itervalues() ]))

def force_dict(value):
    if value == None:
        return {}
    elif type(value) == dict:
        return value
    else:
        return {}

def force_list(value):
    """
    Coerce the input value to a list.
    """

    if value == None:
        return []
    elif type(value) == list:
        return value
    else:
        return [value]

def parse_subsonic_url(url):
    parts = urlparse.urlparse(url)
    scheme = parts.scheme or "http"

    # Make sure there is hostname
    if not parts.hostname:
        raise ValueError("Expected hostname for URL: %s" % url)

    # Validate scheme
    if scheme not in ["http", "https"]:
        raise ValueError("Unexpected scheme '%s' for URL: %s" % (scheme, url))

    # Pick a default port
    port = parts.port or {"http": 80, "https": 443}[scheme]

    # Return new URL and the port.
    return "%s://%s" % (scheme, parts.hostname), port

def human_bytes(num):
    for x in ["bytes","KB","MB","GB"]:
        if num < 1024.0 and num > -1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, "TB")