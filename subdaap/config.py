from cStringIO import StringIO

from configobj import ConfigObj, flatten_errors
from validate import Validator

import logging

# Logger instance
logger = logging.getLogger(__name__)

# Config file specification
CONFIG_VERSION = 3
CONFIG_SPEC = """
version = integer(min=1, default=%d)

[SubSonic]

[[__many__]]
url = string
username = string
password = string

synchronization = option("manual", "startup", "interval", default="interval")
synchronization interval = integer(min=1, default=1440)

[Daap]
interface = string(default="0.0.0.0")
port = integer(min=1, max=65535, default=3689)
password = string(default="")
web interface = boolean(default=True)
zeroconf = boolean(default=True)
cache = boolean(default=True)
cache timeout = integer(min=1, default=1440)

[Provider]
name = string
database = string(default="./database.db")

artwork = boolean(default=True)
artwork cache = boolean(default=True)
artwork cache dir = string(default="./artwork")
artwork cache size = integer(min=0, default=0)
artwork cache prune threshold = float(min=0, max=1.0, default=0.1)

item cache = boolean(default=True)
item cache dir = string(default="./items")
item cache size = integer(min=0, default=0)
item cache prune threshold = float(min=0, max=1.0, default=0.25)

item transcode = option("no", "unsupported", "all", default="no")
item transcode unsupported = list(default=list("flac"))
""" % CONFIG_VERSION


def get_config(config_file):
    """
    Parse the config file, validate it and convert types. Return a dictionary
    with all settings.
    """

    specs = ConfigObj(StringIO(CONFIG_SPEC), list_values=False)
    config = ConfigObj(config_file, configspec=specs)

    # Create validator
    validator = Validator()

    # Convert types and validate file
    result = config.validate(validator, preserve_errors=True, copy=True)
    logger.debug("Config file version %d", config["version"])

    # Raise exceptions for errors
    for section_list, key, message in flatten_errors(config, result):
        if key is not None:
            raise ValueError(
                "The '%s' key in the section '%s' failed validation: %s" % (
                    key, ", ".join(section_list), message))
        else:
            raise ValueError(
                "The following section was missing: %s." % (
                    ", ".join(section_list)))

    # Post-process values
    config["Provider"]["item transcode unsupported"] = [
        x.lower() for x in config["Provider"]["item transcode unsupported"]]

    # For now, no automatic update support.
    if config["version"] != CONFIG_VERSION:
        logger.warning(
            "Config file version is %d, while expected version is %d. Please "
            "check for inconsistencies and update manually.",
            config["version"], CONFIG_VERSION)

    return config
