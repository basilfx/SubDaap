from cStringIO import StringIO

from configobj import ConfigObj
from validate import Validator

# Config file specification
CONFIG_SPEC = \
"""
[SubSonic]
url = string
username = string
password = string

[Daap]
name = string
interface = string(default="127.0.0.1")
port = integer(min=0, max=65535, default=3689)
password = string(default="")
zeroconf = boolean(default=True)

[Provider]
database connection = string

artwork = boolean(default=True)
artwork cache = boolean(default=True)
artwork cache dir = string(default="./artwork")
artwork cache size = integer(min=0, default=0)

item cache = boolean(default=True)
item cache dir = string(default="./items")
item cache size = integer(min=0, default=0)
"""

def get_config(config_file):
    """
    Parse the config file, validate it and convert types. Return a dictionary
    with all settings.
    """

    specs = ConfigObj(StringIO(CONFIG_SPEC), list_values=False)
    config = ConfigObj(config_file, configspec=specs)
    validator = Validator()

    # Convert types and validate file
    config.validate(validator, preserve_errors=True, copy=True)

    return config