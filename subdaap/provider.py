from subdaap.models import Server
from subdaap import utils

from daapserver.utils import generate_persistent_id
from daapserver import provider

import gevent.lock
import gevent.event
import gevent.queue

import logging
import cPickle

# Logger instance
logger = logging.getLogger(__name__)


class SubSonicProvider(provider.Provider):

    # SubSonic has support for artwork
    supports_artwork = True

    # Persistent IDs are supported.
    supports_persistent_id = True

    def __init__(self, db, connections, artwork_cache, item_cache, state_file,
                 transcode, transcode_unsupported):
        """
        """

        super(SubSonicProvider, self).__init__()

        self.db = db
        self.db.create_database(drop_all=False)

        self.connections = connections
        self.artwork_cache = artwork_cache
        self.item_cache = item_cache
        self.state_file = state_file
        self.transcode = transcode
        self.transcode_unsupported = transcode_unsupported

        self.lock = gevent.lock.Semaphore()
        self.ready = gevent.event.Event()

        self.setup_state()
        self.setup_library()

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

        self.load_state()

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

        self.synchronizers = {}
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

                    if cache_item.iterator is None:
                        remote_fd = self.connections[database_id].getCoverArt(
                            remote_id)
                        self.artwork_cache.download(local_id, remote_fd)

                        # Exhaust iterator so it actually downloads the item.
                        utils.exhaust(cache_item.iterator())
                    self.artwork_cache.unload(local_id)

                # Items
                if not self.item_cache.contains(local_id):
                    cache_item = self.item_cache.get(local_id)

                    if cache_item.iterator is None:
                        remote_fd = self.get_item_fd(
                            database_id, remote_id, file_suffix)
                        self.item_cache.download(local_id, remote_fd)

                        # Exhaust iterator so it actually downloads the item.
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
            self.save_state()

            self.ready.set()
            self.ready.clear()

        logger.info("Database initialized and loaded.")

    def get_artwork_data(self, session, item):
        """
        """

        cache_item = self.artwork_cache.get(item.id)

        if cache_item.iterator is None:
            remote_fd = self.connections[item.database_id].getCoverArt(
                item.get_remote_id())
            self.artwork_cache.download(item.id, remote_fd)

            return cache_item.iterator(), None, None
        return cache_item.iterator(), None, cache_item.size

    def get_item_data(self, session, item, byte_range=None):
        """
        """

        cache_item = self.item_cache.get(item.id)

        if cache_item.iterator is None:
            remote_fd = self.get_item_fd(
                item.database_id, item.get_remote_id(), item.file_suffix)
            self.item_cache.download(item.id, remote_fd)

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

    def load_state(self):
        """
        Load provider state.
        """

        logger.debug("Loading provider state from '%s'.", self.state_file)

        with self.lock:
            try:
                with open(self.state_file, "rb") as fp:
                    self.state = cPickle.load(fp)

                # Make sure it's a dict
                if type(self.state) != dict:
                    self.state = {}
            except (IOError, EOFError, cPickle.UnpicklingError):
                self.state = {}

    def save_state(self):
        """
        Save provider state.
        """

        logger.debug("Saving provider state from '%s'.", self.state_file)

        with self.lock:
            with open(self.state_file, "wb") as fp:
                cPickle.dump(self.state, fp)


class Synchronizer(object):

    def __init__(self, state, server, db, connection, index):
        self.state = state
        self.server = server
        self.db = db
        self.connection = connection

        self.database_id = index
        self.container_id = index

        self.cache = {}
        self.base_synced = False

    def sync(self):
        """
        """

        changed = False
        logger.debug("Synchronizing library")

        # Make sure database and base container exist. This is only required
        # once during startup.
        if not self.base_synced:
            self.sync_base()
            self.base_synced = True

        # Grab latests versions
        items_version, playlists_version = self.sync_versions()

        # Items
        if items_version != self.state["items_version"]:
            logger.info("Remote items have changed")
            changed = True

            self.sync_items()

        # Playlists
        if playlists_version != self.state["playlists_version"]:
            logger.info("Remote playlists changed")
            changed = True

            self.sync_playlists()

        # Store version numbers
        if changed:
            self.state["items_version"] = items_version
            self.state["playlists_version"] = playlists_version

        # Finish up
        self.cache.clear()
        logger.info("Synchronization completed")

        return changed

    def sync_versions(self):
        """
        Read the remote index and playlists. Return their versions, so it can
        be decided if synchronization is required.

        For the index, a `lastModified` field is available in SubSonic's
        response message. This is not the case for playlists, so the naive
        approach is to fetch all playlists, calulate a checksum and compare. A
        request for a similar feature is addressed in
        http://forum.subsonic.org/forum/viewtopic.php?f=3&t=13972.

        Because the index and playlists are reused, they are stored in cache.
        """

        items_version = 0
        playlists_version = 0

        # Items
        self.cache["index"] = response = self.connection.getIndexes(
            ifModifiedSince=self.state["items_version"])

        if "lastModified" in response["indexes"]:
            items_version = response["indexes"]["lastModified"]
        else:
            items_version = self.state["items_version"]

        # Playlists
        self.cache["playlists"] = response = self.connection.getPlaylists()

        for playlist in response["playlists"]["playlist"]:
            self.cache["playlist_%d" % playlist["id"]] = response = \
                self.connection.getPlaylist(playlist["id"])

            playlist_checksum = utils.dict_checksum(response["playlist"])
            playlists_version = (playlists_version + playlist_checksum) \
                % 0xFFFFFFFF

        # Return version numbers
        return items_version, playlists_version

    def sync_base(self):
        """
        Synchronize database and base container.
        """

        added_databases = set()
        added_containers = set()

        with self.db.get_write_cursor() as cursor:
            local_databases = cursor.query_dict("""
                SELECT
                    `databases`.`id`,
                    `databases`.`checksum`
                FROM
                    `databases`
                WHERE
                    `databases`.`id` = ?
                """, self.database_id)
            local_containers = cursor.query_dict("""
                SELECT
                    `databases`.`id`,
                    `databases`.`checksum`
                FROM
                    `containers`,
                    `databases`
                WHERE
                    `databases`.`id` = ? AND
                    `containers`.`database_id` = `databases`.`id` AND
                    `containers`.`is_base` = 1
                """, self.database_id)

            # For debugging
            assert len(local_databases) < 2 and len(local_containers) < 2

            #
            # Database information
            #
            database_checksum = utils.dict_checksum({
                "name": self.connection.name
            })

            if self.database_id not in local_databases:
                cursor = cursor.query(
                    """
                    INSERT INTO `databases` (
                        `id`,
                        `persistent_id`,
                        `name`,
                        `checksum`)
                    VALUES
                        (?, ?, ?, ?)
                    """,
                    self.database_id,
                    generate_persistent_id(),
                    self.connection.name,
                    database_checksum)

                # Mark as added
                added_databases.add(self.database_id)
            elif database_checksum != local_databases[self.database_id][0]:
                cursor.query(
                    """
                    UPDATE
                        `databases`
                    SET
                        `name` = ?,
                        `checksum` = ?
                    WHERE
                        `databases`.`id` = ?
                    """,
                    self.connection.name,
                    database_checksum,
                    self.database_id)

                # Mark as edited
                added_databases.add(self.database_id)

            #
            # Base container
            #
            container_checksum = utils.dict_checksum({
                "is_base": 1,
                "name": self.connection.name
            })

            if not local_containers:
                cursor = cursor.query(
                    """
                    INSERT INTO `containers` (
                       `persistent_id`,
                       `database_id`,
                       `name`,
                       `is_base`,
                       `is_smart`,
                       `checksum`)
                    VALUES
                       (?, ?, ?, ?, ?, ?)
                    """,
                    generate_persistent_id(),
                    self.database_id,
                    self.connection.name,
                    int(True),
                    int(False),
                    container_checksum)

                # Mark as added/edited, so it won't be processed again
                added_containers.add(cursor.lastrowid)
            else:
                container_id = local_containers.keys()[0]

                if container_checksum != local_containers[container_id][0]:
                    cursor.query(
                        """
                        UPDATE
                            `containers`
                        SET
                           `name` = ?,
                           `is_base` = ?,
                           `is_smart` = ?,
                           `checksum` = ?
                        WHERE
                           `containers`.`id` = ?
                        """,
                        self.connection.name,
                        int(True),
                        int(False),
                        container_checksum,
                        container_id)

                    # Mark as added/edited, so it won't be processed again
                    added_containers.add(container_id)

    def sync_items(self):
        """
        Synchronize artists, albums and items.
        """

        added_artists = set()
        added_albums = set()
        added_items = set()
        added_container_items = set()

        with self.db.get_write_cursor() as cursor:
            # Fetch local database ID and container ID
            database_id = self.database_id

            container_id = cursor.query_value(
                """
                SELECT
                    `containers`.`id`
                FROM
                    `containers`
                WHERE
                    `containers`.`is_base` = 1 AND
                    `containers`.`database_id` = ?
                """, database_id)

            # Load local items
            local_artists = cursor.query_dict(
                """
                SELECT
                    `remote_id`,
                    `checksum`,
                    `id`
                FROM
                    `artists`
                WHERE
                    `artists`.`database_id` = ?
                """, database_id)
            local_albums = cursor.query_dict(
                """
                SELECT
                    `remote_id`,
                    `checksum`,
                    `id`
                FROM
                    `albums`
                WHERE
                    `albums`.`database_id` = ?
                """, database_id)
            local_items = cursor.query_dict(
                """
                SELECT
                    `remote_id`,
                    `checksum`,
                    `id`
                FROM
                    `items`
                WHERE
                    `items`.`database_id` = ?
                """, database_id)
            local_container_items = cursor.query_dict(
                """
                SELECT
                    `item_id`
                FROM
                    `container_items`
                WHERE
                    `container_items`.`id` = ?
                """, container_id)

            # Compute local IDs
            local_artists_ids = set(local_artists.iterkeys())
            local_albums_ids = set(local_albums.iterkeys())
            local_items_ids = set(local_items.iterkeys())
            local_container_items_ids = set(local_container_items.iterkeys())

            for item in self.walk_index():
                remote_item_id = item["id"]

                # Artist + Album information
                if "artistId" in item:
                    #
                    # Artist information
                    #
                    remote_artist_id = item["artistId"]

                    if remote_artist_id not in added_artists:
                        artist_checksum = utils.dict_checksum({
                            "name": item["artist"]
                        })

                        # Check if artist has changed
                        if remote_artist_id not in local_artists:
                            cursor.query(
                                """
                                INSERT INTO `artists` (
                                    `database_id`,
                                    `name`,
                                    `remote_id`,
                                    `checksum`)
                                VALUES
                                    (?, ?, ?, ?)
                                """,
                                database_id,
                                item["artist"],
                                remote_artist_id,
                                artist_checksum)

                            # Store insert ID
                            local_artists[remote_artist_id] = (
                                artist_checksum, cursor.lastrowid)
                        elif artist_checksum != local_artists[remote_artist_id][0]:
                            cursor.query(
                                """
                                UPDATE
                                    `artists`
                                SET
                                    `name` = ?,
                                    `checksum` = ?
                                WHERE
                                    `artists`.`id` = ?
                                """,
                                item["artist"],
                                artist_checksum,
                                local_artists[remote_artist_id][1])

                        # Mark as added/edited, so it won't be processed again
                        added_artists.add(remote_artist_id)

                        #
                        # Album information
                        #
                        for album in self.walk_artist(remote_artist_id):
                            remote_album_id = album["id"]

                            # Synchronize artist information
                            if remote_album_id not in added_albums:
                                album_checksum = utils.dict_checksum(album)

                                # Check if artist has changed
                                if remote_album_id not in local_albums:
                                    cursor.query(
                                        """
                                        INSERT INTO `albums` (
                                           `database_id`,
                                           `artist_id`,
                                           `name`,
                                           `art`,
                                           `remote_id`,
                                           `checksum`)
                                        VALUES
                                           (?, ?, ?, ?, ?, ?)
                                        """,
                                        database_id,
                                        local_artists[remote_artist_id][1],
                                        album["name"],
                                        int("coverArt" in album),
                                        remote_album_id,
                                        album_checksum)

                                    # Store insert ID
                                    local_albums[remote_album_id] = (
                                        album_checksum, cursor.lastrowid)
                                elif album_checksum != local_albums[remote_album_id][0]:
                                    cursor.query(
                                        """
                                        UPDATE
                                            `albums`
                                        SET
                                           `name` = ?,
                                           `art` = ?,
                                           `checksum` = ?
                                        WHERE
                                            `albums`.`id` = ?
                                        """,
                                        album["name"],
                                        int("coverArt" in album),
                                        album_checksum,
                                        local_albums[remote_album_id][1])

                                # Mark as added/edited, so it won't be processed
                                # again
                                added_albums.add(remote_album_id)

                #
                # Item information
                #
                if remote_item_id not in added_items:
                    item_checksum = utils.dict_checksum(item)

                    try:
                        item_artist_id = local_artists[item["artistId"]][1]
                    except KeyError:
                        item_artist_id = None
                    try:
                        item_album_id = local_albums[item["albumId"]][1]
                    except KeyError:
                        item_album_id = None
                    try:
                        item_duration = item["duration"] * 1000
                    except KeyError:
                        item_duration = None

                    # Check if item has changed
                    if remote_item_id not in local_items:
                        cursor.query(
                            """
                            INSERT INTO `items` (
                                `persistent_id`,
                                `database_id`,
                                `artist_id`,
                                `album_id`,
                                `name`,
                                `genre`,
                                `year`,
                                `track`,
                                `duration`,
                                `bitrate`,
                                `file_name`,
                                `file_type`,
                                `file_suffix`,
                                `file_size`,
                                `remote_id`,
                                `checksum`)
                            VALUES
                                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            generate_persistent_id(),
                            database_id,
                            item_artist_id,
                            item_album_id,
                            item.get("title"),
                            item.get("genre"),
                            item.get("year"),
                            item.get("track"),
                            item_duration,
                            item.get("bitRate"),
                            item.get("path"),
                            item.get("contentType"),
                            item.get("suffix"),
                            item.get("size"),
                            remote_item_id,
                            item_checksum)

                        # Store insert ID
                        local_items[remote_item_id] = (
                            item_checksum, cursor.lastrowid)
                    elif item_checksum != local_items[remote_item_id][0]:
                        cursor.query(
                            """
                            UPDATE
                                `items`
                            SET
                                `artist_id` = ?,
                                `album_id` = ?,
                                `name` = ?,
                                `genre` = ?,
                                `year` = ?,
                                `track` = ?,
                                `duration` = ?,
                                `bitrate` = ?,
                                `file_name` = ?,
                                `file_type` = ?,
                                `file_suffix` = ?,
                                `file_size` = ?,
                                `checksum` = ?
                            WHERE
                                `items`.`id` = ?
                            """,
                            item_artist_id,
                            item_album_id,
                            item.get("title"),
                            item.get("genre"),
                            item.get("year"),
                            item.get("track"),
                            item_duration,
                            item.get("bitRate"),
                            item.get("path"),
                            item.get("contentType"),
                            item.get("suffix"),
                            item.get("size"),
                            item_checksum,
                            local_items[remote_item_id][1])

                    # Mark as added/edited, so it wont' be processed again
                    added_items.add(remote_item_id)

                #
                # Container item information
                #
                if local_items[remote_item_id][1] not in local_container_items_ids:
                    cursor.query(
                        """
                        INSERT INTO `container_items` (
                            `persistent_id`,
                            `database_id`,
                            `container_id`,
                            `item_id`)
                        VALUES
                            (?, ?, ?, ?)
                        """,
                        generate_persistent_id(),
                        database_id,
                        container_id,
                        local_items[remote_item_id][1])

                    # Store insert ID
                    local_container_items[local_items[remote_item_id][1]] = ()

            # Calculate deleted artists
            deleted_artists = local_artists_ids - added_artists
            deleted_albums = local_albums_ids - added_albums
            deleted_items = local_items_ids - added_items
            deleted_container_items = local_container_items_ids - \
                added_container_items

            # Delete old artists, albums and items
            cursor.query("""
                DELETE FROM
                    `container_items`
                WHERE
                    `container_items`.`item_id` IN (%s) AND
                    `container_items`.`container_id` = ?
                """ % utils.in_list(deleted_container_items), container_id)
            cursor.query("""
                DELETE FROM
                    `items`
                WHERE
                    `items`.`remote_id` IN (%s) AND
                    `items`.`database_id` = ?
                """ % utils.in_list(deleted_items), database_id)
            cursor.query("""
                DELETE FROM
                    `artists`
                WHERE
                    `artists`.`remote_id` IN (%s) AND
                    `artists`.`database_id` = ?
                """ % utils.in_list(deleted_artists), database_id)
            cursor.query("""
                DELETE FROM
                    `albums`
                WHERE
                    `albums`.`remote_id` IN (%s) AND
                    `albums`.`database_id` = ?
                """ % utils.in_list(deleted_albums), database_id)

            # Return all additions and deletions
            return (
                added_artists, deleted_artists,
                added_albums, deleted_albums,
                added_items, deleted_items,
                added_container_items, deleted_container_items)

    def sync_playlists(self):
        """
        """

        added_containers = set()

        with self.db.get_write_cursor() as cursor:
            # Fetch local database ID
            database_id = self.database_id

            container_id = cursor.query_value(
                """
                SELECT
                    `containers`.`id`
                FROM
                    `containers`
                WHERE
                    `containers`.`is_base` = 1 AND
                    `containers`.`database_id` = ?
                """,
                database_id)
            local_containers = cursor.query_dict(
                """
                SELECT
                    `remote_id`,
                    `checksum`,
                    `id`
                FROM
                    `containers`
                WHERE
                    `containers`.`is_base` = 0 AND
                    `containers`.`database_id` = ?
                """,
                database_id)

            # Compute local IDs
            local_containers_ids = set(local_containers.iterkeys())

            for container in self.walk_playlists():
                remote_container_id = container["id"]

                if remote_container_id not in added_containers:
                    container_checksum = utils.dict_checksum({
                        "is_base": 0,
                        "name": container["name"],
                        "songCount": container["songCount"]
                    })

                    # Check if container has changed
                    if remote_container_id not in local_containers_ids:
                        cursor = cursor.query(
                            """
                            INSERT INTO `containers` (
                               `persistent_id`,
                               `database_id`,
                               `parent_id`,
                               `name`,
                               `is_base`,
                               `is_smart`,
                               `remote_id`,
                               `checksum`)
                            VALUES
                               (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            generate_persistent_id(),
                            self.database_id,
                            container_id,
                            container["name"],
                            int(False),
                            int(False),
                            remote_container_id,
                            container_checksum)

                        # Store insert ID
                        local_containers[remote_container_id] = (
                            container_checksum, cursor.lastrowid)
                    elif container_checksum != local_containers[remote_container_id][0]:
                        cursor.query(
                            """
                            UPDATE
                                `containers`
                            SET
                                `name` = ?,
                                `checksum` = ?
                            WHERE
                                `containers`.`id` = ?
                            """,
                            container["name"],
                            container_checksum,
                            local_containers[remote_container_id][1])

                    # Sync playlist items
                    self.sync_playlist_items(
                        container,
                        local_containers[remote_container_id][1],
                        cursor)

                    # Mark as added/edited, so it won't be processed again
                    added_containers.add(remote_container_id)

            deleted_containers = local_containers_ids - added_containers

            # Delete old containers
            cursor.query(
                """
                DELETE FROM
                    `containers`
                WHERE
                    `containers`.`id` IN (%s) AND
                    `containers`.`database_id` = ?
                """ % utils.in_list(deleted_containers), database_id)

            return added_containers, deleted_containers

    def sync_playlist_items(self, playlist, container_id, cursor):
        """
        """

        database_id = self.database_id

        cursor.query(
            """
            DELETE FROM
                `container_items`
            WHERE
                `container_items`.`container_id` = ? AND
                `container_items`.`database_id` = ?
            """, container_id, database_id)

        remote_items = set()

        for entry in self.walk_playlist_entries(playlist["id"]):
            remote_items.add(entry["id"])

        local_items = cursor.query_dict(
            """
            SELECT
                `items`.`remote_id`,
                `items`.`id`
            FROM
                `items`
            WHERE
                `items`.`remote_id` IN (%s) AND
                `items`.`database_id` = ?
            """ % utils.in_list(set(remote_items)), database_id)

        for entry in self.walk_playlist_entries(playlist["id"]):
            cursor.query(
                """
                INSERT INTO `container_items` (
                    `persistent_id`,
                    `database_id`,
                    `container_id`,
                    `item_id`,
                    `order`)
                VALUES
                    (?, ?, ?, ?, ?)
                """,
                generate_persistent_id(),
                database_id,
                container_id,
                local_items[entry["id"]][0],
                entry["order"])

    def walk_index(self):
        """
        Request SubSonic's index and iterate each item.
        """

        response = self.cache.get("index") or self.connection.getIndexes()

        for index in response["indexes"]["index"]:
            for index in index["artist"]:
                for item in self.walk_directory(index["id"]):
                    yield item

    def walk_playlists(self):
        """
        Request SubSonic's playlists and iterate each item.
        """

        response = self.cache.get("playlists") or \
            self.connection.getPlaylists()

        for child in response["playlists"]["playlist"]:
            yield child

    def walk_playlist_entries(self, playlist_id):
        """
        Request SubSonic's playlist items and iterate each item.
        """

        response = self.cache.get("playlist_%d" % playlist_id) or \
            self.connection.getPlaylist(playlist_id)

        for order, child in enumerate(response["playlist"]["entry"], start=1):
            child["order"] = order
            yield child

    def walk_directory(self, directory_id):
        """
        Request a SubSonic music directory and iterate each item.
        """

        response = self.connection.getMusicDirectory(directory_id)

        for child in response["directory"]["child"]:
            if child.get("isDir"):
                for child in self.walk_directory(child["id"]):
                    yield child
            else:
                yield child

    def walk_artist(self, artist_id):
        """
        Request a SubSonic artist and iterate each album.
        """

        response = self.connection.getArtist(artist_id)

        for child in response["artist"]["album"]:
            yield child
