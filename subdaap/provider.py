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
        """

        connection = self.connections[item.database_id]
        cache_item = self.artwork_cache.get(item.id)

        if cache_item.iterator is None:
            remote_fd = connection.subsonic.getCoverArt(item.remote_id)
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

        connection = self.connections[database_id]

        if connection.needs_transcoding(file_suffix):
            logger.debug(
                "Transcoding item '%d' with file suffix '%s'.",
                remote_id, file_suffix)
            return connection.subsonic.stream(
                remote_id, tformat="mp3")
        else:
            return connection.subsonic.download(remote_id)
