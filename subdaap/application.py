from subdaap import subsonic, config, database, cache, utils

from gevent.pywsgi import WSGIServer
from daapserver import zeroconf, create_daap_server

import libsonic
import logging
import errno
import os
import time

# Logger instance
logger = logging.getLogger(__name__)

class Application(object):

    def __init__(self, config_file, data_dir, verbose=0):
        self.config_file = config_file
        self.data_dir = data_dir
        self.verbose = verbose

        self.server = None
        self.provider = None
        self.zeroconf = None

        # Setup all parts of the application
        self.setup_config()
        self.setup_provider()
        self.setup_server()

    def setup_config(self):
        """
        Load the application config from file.
        """

        logger.debug("Loading config from %s", self.config_file)
        self.config = config.get_config(self.config_file)

    def setup_provider(self):
        """
        Setup the database connection, the SubSonic connection and provider.
        """

        host, port = utils.parse_subsonic_url(self.config["SubSonic"]["url"])
        db = database.Database(self.config["Provider"]["database connection"])
        connection = libsonic.Connection(host,
            self.config["SubSonic"]["username"],
            self.config["SubSonic"]["password"], port=port)

        artwork_cache_dir = self.get_cache_dir(
            self.config["Provider"]["artwork cache dir"])
        item_cache_dir = self.get_cache_dir(
            self.config["Provider"]["item cache dir"])
        self.artwork_cache = cache.ArtworkCache(connection, artwork_cache_dir,
            self.config["Provider"]["artwork cache size"])
        self.item_cache = cache.ItemCache(connection, item_cache_dir,
            self.config["Provider"]["item cache size"])

        logger.debug("Setup SubSonic provider")
        self.provider = subsonic.SubSonicProvider(db=db, connection=connection,
            artwork_cache=self.artwork_cache, item_cache=self.item_cache)

    def setup_server(self):
        """
        Create DAAP server and setup zeroconf advertising.
        """

        bind = self.config["Daap"]["interface"], self.config["Daap"]["port"]
        application = create_daap_server(self.provider,
            server_name=self.config["Daap"]["name"],
            password=self.config["Daap"]["password"],
            debug=self.verbose > 1)

        logger.debug("Setting up DAAP server at %s", bind)
        self.server = WSGIServer(bind, application=application)

        if self.config["Daap"]["zeroconf"]:
            self.zeroconf = zeroconf.Zeroconf(self.config["Daap"]["name"],
                self.config["Daap"]["port"], stype="_daap._tcp")

    def start(self):
        """
        Start server and publishes zeroconf.
        """

        self.artwork_cache.index()
        self.item_cache.index()

        self.provider.synchronize()

        if self.zeroconf:
            self.zeroconf.publish()

        self.server.serve_forever()

    def stop(self):
        """
        Unpublishes zeroconf.
        """

        if self.zeroconf:
            self.zeroconf.unpublish()

    def get_cache_dir(self, *path):
        """
        Resolve the path to a cache directory. The path is relative to the data
        directory. The directory will be created if it does not exists, and will
        be tested for writing.
        """

        full_path = os.path.abspath(os.path.normpath(
            os.path.join(self.data_dir, *path)))
        logger.debug("Resolved %s to %s", path, full_path)

        # Create path if required
        try:
            os.makedirs(full_path, 0755)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(full_path):
                pass
            else:
                raise Exception("Could not create cache folder: %s" % full_path)

        # Test for writing
        test_file = os.path.join(full_path, ".write-test")
        ok = True

        try:
            with open(test_file, "w") as fp:
                fp.write("test")
        except IOError:
            ok = False
        finally:
            try:
                os.remove(test_file)
            except OSError:
                ok = False

        if not ok:
            raise Exception("Could not write to cache folder: %s" % full_path)

        # Cache directory created and tested for writing
        return full_path