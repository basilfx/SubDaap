from subdaap.models import Server
from subdaap.synchronizer import Synchronizer
from subdaap import utils

from daapserver.utils import generate_persistent_id
from daapserver import provider

import gevent.lock
import gevent.event
import gevent.queue

import logging

# Logger instance
logger = logging.getLogger(__name__)


class SubSonicProvider(provider.Provider):

    # SubSonic has support for artwork
    supports_artwork = True

    # Persistent IDs are supported.
    supports_persistent_id = True

    def __init__(self, server_name, db, connections, artwork_cache, item_cache,
                 state, transcode, transcode_unsupported):
        """
        """

        super(SubSonicProvider, self).__init__()

        self.db = db
        self.state = state
        self.connections = connections
        self.artwork_cache = artwork_cache
        self.item_cache = item_cache
        self.transcode = transcode
        self.transcode_unsupported = transcode_unsupported

        self.lock = gevent.lock.Semaphore()
        self.ready = gevent.event.Event()

        self.synchronizers = {}

        self.setup_state()
        self.setup_library()

        # Set server name and persistent ID.
        self.server.name = server_name
        self.server.persistent_id = self.state["persistent_id"]

        # iTunes 12.1 doesn't work when the revision number is one. Since this
        # provider loads the data directly from the database, the revision
        # number doesn't change. Therefore, increase the revision number by
        # committing.
        self.server.storage.commit()

    def wait_for_update(self):
        """
        Block the serving greenlet until a new revision is available, e.g. the
        ready event is set.
        """

        # Block until next upate
        self.ready.wait()

        # Return the revision number
        return self.server.storage.revision

    def setup_state(self):
        """
        """

        if "persistent_id" not in self.state:
            self.state["persistent_id"] = generate_persistent_id()

        # Ensure keys are available
        if "connections" not in self.state:
            self.state["connections"] = {}

        for index, connection in self.connections.iteritems():
            if index not in self.state["connections"]:
                self.state["connections"][index] = {
                    "items_version": None,
                    "playlists_version": None
                }

    def setup_library(self):
        """
        """

        self.db.create_database(drop_all=False)
        self.server = Server(db=self.db)

        # Initialize synchronizer for each connection.
        for index, connection in self.connections.iteritems():
            self.synchronizers[index] = Synchronizer(
                self.state["connections"][index], self.server, self.db,
                connection, index)

    def cache(self):
        """
        """

        cached_items = self.server.get_cached_items()

        self.artwork_cache.index(cached_items)
        self.item_cache.index(cached_items)

        # Start a separate task to cache permanent files.
        def _cache():
            logger.info("Caching %d permanent items.", len(cached_items))

            for local_id in cached_items:
                logger.debug("Caching item '%d'.", local_id)
                database_id, remote_id, file_suffix = cached_items[local_id]

                # Artwork
                if not self.artwork_cache.contains(local_id):
                    cache_item = self.artwork_cache.get(local_id)

                    if cache_item.ready is None:
                        remote_fd = self.connections[database_id].getCoverArt(
                            remote_id)
                        self.artwork_cache.download(
                            local_id, cache_item, remote_fd)

                        # Exhaust iterator so it downloads the artwork.
                        utils.exhaust(cache_item.iterator())
                    self.artwork_cache.unload(local_id)

                # Items
                if not self.item_cache.contains(local_id):
                    cache_item = self.item_cache.get(local_id)

                    if cache_item.ready is None:
                        remote_fd = self.get_item_fd(
                            database_id, remote_id, file_suffix)
                        self.item_cache.download(
                            local_id, cache_item, remote_fd)

                        # Exhaust iterator so it downloads the item.
                        utils.exhaust(cache_item.iterator())
                    self.item_cache.unload(local_id)

            logger.info("Caching permanent items finished.")
        gevent.spawn(_cache)

    def synchronize(self):
        """
        """

        changed = False

        with self.lock:
            for index, synchronizer in self.synchronizers.iteritems():
                if synchronizer.sync():
                    changed = True

        if changed:
            self.state.save()

            self.ready.set()
            self.ready.clear()

        logger.info("Database initialized and loaded.")

    def get_artwork_data(self, session, item):
        """
        """

        cache_item = self.artwork_cache.get(item.id)

        if cache_item.iterator is None:
            remote_fd = self.connections[item.database_id].getCoverArt(
                item.remote_id)
            self.artwork_cache.download(item.id, cache_item, remote_fd)

            return cache_item.iterator(), None, None
        return cache_item.iterator(), None, cache_item.size

    def get_item_data(self, session, item, byte_range=None):
        """
        """

        cache_item = self.item_cache.get(item.id)

        if cache_item.iterator is None:
            remote_fd = self.get_item_fd(
                item.database_id, item.remote_id, item.file_suffix)
            self.item_cache.download(item.id, cache_item, remote_fd)

            return cache_item.iterator(byte_range), item.file_type, \
                item.file_size
        return cache_item.iterator(byte_range), item.file_type, \
            cache_item.size

    def get_item_fd(self, database_id, remote_id, file_suffix):
        """
        Get a file descriptor of remote connection, based on transcoding
        settings.
        """

        needs_transcoding = self.transcode == "all" or (
            self.transcode == "unsupported" and
            file_suffix in self.transcode_unsupported)

        if needs_transcoding:
            logger.debug(
                "Transcoding item '%d' with file suffix '%s'.",
                remote_id, file_suffix)
            return self.connections[database_id].stream(
                remote_id, tformat="mp3")
        else:
            return self.connections[database_id].download(remote_id)
