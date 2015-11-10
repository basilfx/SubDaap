from subdaap.models import Server

from daapserver.utils import generate_persistent_id
from daapserver import provider

import logging

# Logger instance
logger = logging.getLogger(__name__)


class Provider(provider.Provider):

    # SubSonic has support for artwork.
    supports_artwork = True

    # Persistent IDs are supported.
    supports_persistent_id = True

    def __init__(self, server_name, db, state, connections, cache_manager):
        """
        """

        super(Provider, self).__init__()

        self.server_name = server_name
        self.db = db
        self.state = state
        self.connections = connections
        self.cache_manager = cache_manager

        self.setup_state()
        self.setup_server()

    def setup_state(self):
        """
        """

        if "persistent_id" not in self.state:
            self.state["persistent_id"] = generate_persistent_id()

    def setup_server(self):
        """
        """

        self.server = Server(db=self.db)

        # Set server name and persistent ID.
        self.server.name = self.server_name
        self.server.persistent_id = self.state["persistent_id"]

    def get_artwork_data(self, session, item):
        """
        Get artwork data from cache or remote.
        """

        cache_item = self.cache_manager.artwork_cache.get(item.id)

        if cache_item.iterator is None:
            remote_fd = self.connections[item.database_id].get_artwork_fd(
                item.remote_id, item.file_suffix)
            self.cache_manager.artwork_cache.download(
                item.id, cache_item, remote_fd)

            logger.debug("Artwork data from remote, size=unknown")
            return cache_item.iterator(), None, None

        logger.debug(
            "Artwork data from cache, size=%d", cache_item.size)
        return cache_item.iterator(), None, cache_item.size

    def get_item_data(self, session, item, byte_range=None):
        """
        Get item data from cache or remote.
        """

        cache_item = self.cache_manager.item_cache.get(item.id)
        connection = self.connections[item.database_id]
        is_transcode = connection.needs_transcoding(item.file_suffix)
        item_file_type = item.file_type

        if is_transcode:
            item_file_type = connection.transcode_format[item.file_type]

        if cache_item.iterator is None:
            remote_fd = connection.get_item_fd(
                item.remote_id, item.file_suffix)
            self.cache_manager.item_cache.download(
                item.id, cache_item, remote_fd)
            item_size = item.file_size

            # Determine returned size by checking for transcode.
            if is_transcode:
                item_size = -1

            logger.debug(
                "Item data from remote: range=%s, type=%s, size=%d",
                byte_range, item_file_type, item_size)
            return cache_item.iterator(byte_range), item_file_type, item_size

        logger.debug(
            "Item data from cache, range=%s, type=%s, size=%d",
            byte_range, item.file_type, item.file_size)
        return cache_item.iterator(byte_range), item_file_type, \
            cache_item.size
