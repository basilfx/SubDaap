from subdaap.provider import SubSonicProvider
from subdaap.database import Database
from subdaap.state import State
from subdaap import cache, config, webserver

from daapserver import DaapServer

import logging
import errno
import os

# Logger instance
logger = logging.getLogger(__name__)


class Application(object):

    def __init__(self, config_file, data_dir, verbose=0):
        """
        Construct a new application instance.
        """

        self.config_file = config_file
        self.data_dir = data_dir
        self.verbose = verbose

        self.server = None
        self.provider = None
        self.bonjour = None

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

        # Subsonic is imported here, because it somehow causes Python to crash
        # after a fork, when it is opening a database.
        from subdaap import subsonic

        # Initialize database
        db = Database(self.config["Provider"]["database"])

        # Setup state
        state = State(os.path.join(
            self.get_cache_dir(), "provider.state"))

        # Initialize connections.
        connections = {}

        for name, section in self.config["SubSonic"].iteritems():
            connections[len(connections) + 1] = subsonic.Connection(
                name, section["url"], section["username"], section["password"])

        # Initialize cache
        artwork_cache_dir = self.get_cache_dir(
            self.config["Provider"]["artwork cache dir"])
        item_cache_dir = self.get_cache_dir(
            self.config["Provider"]["item cache dir"])
        artwork_cache = cache.ArtworkCache(
            artwork_cache_dir,
            self.config["Provider"]["artwork cache size"],
            self.config["Provider"]["artwork cache prune threshold"])
        item_cache = cache.ItemCache(
            item_cache_dir,
            self.config["Provider"]["item cache size"],
            self.config["Provider"]["item cache prune threshold"])

        # Create provider
        logger.debug(
            "Setting up Provider with %d connections", len(connections))

        self.provider = SubSonicProvider(
            db=db,
            state=state,
            server_name=self.config["Provider"]["name"],
            connections=connections,
            artwork_cache=artwork_cache,
            item_cache=item_cache,
            transcode=self.config["Provider"]["item transcode"],
            transcode_unsupported=self.config["Provider"][
                "item transcode unsupported"])

    def setup_server(self):
        """
        Create DAAP server.
        """

        logger.debug(
            "Setting up DAAP server at %s:%d",
            self.config["Daap"]["interface"], self.config["Daap"]["port"])

        self.server = DaapServer(
            provider=self.provider,
            password=self.config["Daap"]["password"],
            ip=self.config["Daap"]["interface"],
            port=self.config["Daap"]["port"],
            cache=self.config["Daap"]["cache"],
            cache_timeout=self.config["Daap"]["cache timeout"] * 60,
            bonjour=self.config["Daap"]["zeroconf"],
            debug=self.verbose > 1)

        # Extend server with a web interface
        if self.config["Daap"]["web interface"]:
            webserver.extend_server_app(self, self.server.app)

    def start(self):
        """
        Start the server.
        """

        self.provider.synchronize()
        self.provider.cache()

        logger.info("Serving requests.")
        self.server.serve_forever()

    def stop(self):
        """
        Stop the server.
        """
        self.server.stop()

    def get_cache_dir(self, *path):
        """
        Resolve the path to a cache directory. The path is relative to the data
        directory. The directory will be created if it does not exists, and
        will be tested for writing.
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
                raise Exception("Could not create folder: %s" % full_path)

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
