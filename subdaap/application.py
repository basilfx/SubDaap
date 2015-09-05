from subdaap.provider import Provider
from subdaap.synchronizer import Synchronizer
from subdaap.database import Database
from subdaap.state import State
from subdaap import cache, config, webserver

from daapserver import DaapServer

from apscheduler.schedulers.gevent import GeventScheduler

import logging
import random
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

        self.connections = {}
        self.synchronizers = {}

        # Setup all parts of the application
        self.setup_config()
        self.setup_database()
        self.setup_state()
        self.setup_cache()
        self.setup_provider()
        self.setup_server()
        self.setup_tasks()

    def setup_config(self):
        """
        Load the application config from file.
        """

        logger.debug("Loading config from %s", self.config_file)
        self.config = config.get_config(self.config_file)

    def setup_database(self):
        """
        Initialize database.
        """

        self.db = Database(self.config["Provider"]["database"])

    def setup_state(self):
        """
        Setup state.
        """

        self.state = State(os.path.join(
            self.get_cache_dir(), "provider.state"))

    def setup_cache(self):
        """
        Setup the caches for items and artwork.
        """

        # Initialize caches for items and artwork.
        item_cache = cache.ItemCache(
            path=self.get_cache_dir(
                self.config["Provider"]["item cache dir"]),
            max_size=self.config["Provider"]["item cache size"],
            prune_threshold=self.config[
                "Provider"]["item cache prune threshold"])
        artwork_cache = cache.ArtworkCache(
            path=self.get_cache_dir(self.config[
                "Provider"]["artwork cache dir"]),
            max_size=self.config["Provider"]["artwork cache size"],
            prune_threshold=self.config[
                "Provider"]["artwork cache prune threshold"])

        # Create a cache manager
        self.cache_manager = cache.CacheManager(
            self.db, item_cache, artwork_cache)

    def setup_provider(self):
        """
        Setup the database connection, the SubSonic connection and provider.
        """

        # Subsonic is imported here, because it somehow causes Python to crash
        # after a fork, when it is opening a database.
        from subdaap import subsonic

        # Initialize the connections.
        for name, section in self.config["Connections"].iteritems():
            self.connections[len(self.connections) + 1] = subsonic.Connection(
                name=name,
                url=section["url"],
                username=section["username"],
                password=section["password"],
                transcode=section["transcode"],
                transcode_unsupported=section["transcode unsupported"])

        # Create provider.
        logger.debug(
            "Setting up provider for %d connections.", len(self.connections))

        self.provider = Provider(
            server_name=self.config["Provider"]["name"],
            db=self.db,
            state=self.state,
            connections=self.connections,
            cache_manager=self.cache_manager)

        # Setup synchronizers.
        for name, section in self.config["Connections"].iteritems():
            self.synchronizers[len(self.synchronizers) + 1] = Synchronizer(
                state=self.state,
                provider=self.provider,
                db=self.db,
                connection=self.connections[len(self.synchronizers) + 1],
                index=len(self.synchronizers) + 1,
                method=section["synchronization"],
                interval=section["synchronization interval"])

            # Check if an initial sync is required. This is the case for new
            # or modified connections.
            self.synchronizers[len(self.synchronizers)].synchronize(
                initial=True)

    def setup_server(self):
        """
        Create the DAAP server.
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

    def setup_tasks(self):
        """
        Setup all tasks that run periodically.
        """

        self.scheduler = GeventScheduler()

        # Scheduler task to clean and expire the cache.
        self.scheduler.add_job(
            self.cache_manager.expire, trigger="interval", minutes=5)
        self.scheduler.add_job(
            self.cache_manager.clean, trigger="interval", minutes=30)

        # Schedule tasks to synchronize each connection.
        for index, synchronizer in self.synchronizers.iteritems():
            self.scheduler.add_job(
                self.synchronize, args=([index]),
                trigger="interval", minutes=synchronizer.interval)

    def synchronize(self, indexes=None):
        """
        """

        indexes = indexes or self.synchronizers.keys()

        for index in indexes:
            self.synchronizers[index].synchronize()

        # Update the cache.
        self.cache_manager.cache()

        # Clean expired items.
        self.cache_manager.clean()

    def start(self):
        """
        Start the server.
        """

        logger.debug("Starting task scheduler.")
        self.scheduler.start()

        logger.debug("Starting DAAP server.")
        self.server.serve_forever()

    def stop(self):
        """
        Stop the server.
        """

        logger.debug("Stopping DAAP server.")
        self.server.stop()

        logger.debug("Stopping task scheduler.")
        self.scheduler.shutdown()

    def get_cache_dir(self, *path):
        """
        Resolve the path to a cache directory. The path is relative to the data
        directory. The directory will be created if it does not exists, and
        will be tested for writing.
        """

        full_path = os.path.abspath(os.path.normpath(
            os.path.join(self.data_dir, *path)))
        logger.debug("Resolved %s to %s", path, full_path)

        # Create path if required.
        try:
            os.makedirs(full_path, 0755)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(full_path):
                pass
            else:
                raise Exception("Could not create folder: %s" % full_path)

        # Test for writing.
        ok = True
        test_file = os.path.join(full_path, ".write-test")

        while os.path.exists(test_file):
            test_file = test_file + str(random.randint(0, 9))

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

        # Cache directory created and tested for writing.
        return full_path
