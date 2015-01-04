from cStringIO import StringIO

from configobj import ConfigObj
from validate import Validator

import logging

# Logger instance
logger = logging.getLogger(__name__)

# Config file specification
CONFIG_VERSION = 1
CONFIG_SPEC = \
"""
version = integer(min=1, default=%d)

[SubSonic]

[[__many__]]
index = integer(min=1, default=1)
url = string
username = string
password = string
transcode = option("no", "unsupported", "all")

[Daap]
name = string
interface = string(default="0.0.0.0")
port = integer(min=1, max=65535, default=3689)
password = string(default="")
zeroconf = boolean(default=True)

[Provider]
database = string

artwork = boolean(default=True)
artwork cache = boolean(default=True)
artwork cache dir = string(default="./artwork")
artwork cache size = integer(min=0, default=0)
artwork cache prune threshold = float(min=0, max=1.0, default=0.1)

item cache = boolean(default=True)
item cache dir = string(default="./items")
item cache size = integer(min=0, default=0)
item cache prune threshold = float(min=0, max=1.0, default=0.25)
""" % CONFIG_VERSION

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
    logger.info("Config file version %d", config["version"])

    return config