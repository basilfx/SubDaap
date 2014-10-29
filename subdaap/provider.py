from subdaap.models import Server, Database, Container, Item

from daapserver import provider

import gevent.lock
import gevent.event
import gevent.queue

import sys
import logging

# Logger instance
logger = logging.getLogger(__name__)

class SubSonicProvider(provider.Provider):

    supports_artwork = True

    supports_persistent_id = False

    def __init__(self, db, connections, artwork_cache, item_cache):
        super(SubSonicProvider, self).__init__()

        if type(connections) != list:
            self.connections = list(connections)
        else:
            self.connections = connections

        self.connections = connections
        self.artwork_cache = artwork_cache
        self.item_cache = item_cache

        self.db = db
        self.db.create_database(drop_all=False)

        self.lock = gevent.lock.Semaphore()
        self.ready = gevent.event.Event()

        self.setup_library()

    def wait_for_update(self):
        # Block until next upate
        self.ready.wait()

        # Return the revision number
        return self.server.storage.revision

    def setup_library(self):
        with self.lock:
            self.server = Server(self.db)

            # Make sure base container exists
            try:
                self.server.databases
            except KeyError:
                pass

            self.ready.set()
            self.ready.clear()

    def synchronize(self):
        # Synchronize
        self.synchronizer = Synchronizer(self)

        def _sync():
            initial = True

            while True:
                with self.db.get_lock():
                    changed = self.synchronizer.sync()

                # Clear initial history, since we don't have to update a client
                if initial:
                    self.server.manager.commit()

                # Stats
                if changed or initial:
                    if changed:
                        logger.debug("Database changed")

                    logger.debug("Current revision %d", self.server.manager.revision)
                    logger.debug("Database: items=%d, containers=%d", len(self.database.items), len(self.database.containers))
                    logger.debug("Container: items=%d", len(self.container.container_items))
                else:
                    logger.debug("Database not changed")

                # Notify in case of an update
                if initial or changed:
                    initial = False

                    self.update_event.set()
                    self.update_event.clear()

                # sleep for next sync
                #gevent.sleep(60 * 30)
                break

        # Spawn syncer
        #gevent.spawn(_sync)
        _sync()

        logger.info("Database initialized and loaded")

    def get_artwork_data(self, session, item):
        """
        """

        cache_item = self.artwork_cache.get(item)

        return cache_item.iterator(), cache_item.type, cache_item.size

    def get_item_data(self, session, item, byte_range=None):
        """
        """

        cache_item = self.item_cache.get(item)

        return cache_item.iterator(byte_range), cache_item.type, cache_item.size

class Synchronizer(object):

    def __init__(self, host):
        self.connection = host.connection
        self.db = host.db
        self.database = host.database
        self.container = host.container

    def sync(self):
        changed = False

        # Grab latests versions
        logger.info("Synchronizing with remote library")
        items_version, playlists_version = self.sync_versions()

        with self.db.get_session() as session:
            self.session = session

            # items
            if items_version != self.database.row.items_version:
                logger.info("Remote items have changed")
                changed = True

                self.sync_items()

            # Playlists
            if playlists_version != self.database.row.playlists_version:
                logger.info("Remote playlists changed")
                changed = True

                self.sync_playlists()

            # Save version numbers
            if changed:
                row = database.Library()

                row.id = self.database.row.id
                row.items_version = items_version
                row.playlists_version = playlists_version

                self.database.row = session.merge(row)

        # Cleanup resources
        self.cleanup()
        logger.info("Synchronization completed")

        return changed

    def sync_versions(self):
        # Items
        items_version = self.database.row.items_version
        self.index = self.connection.getIndexes(ifModifiedSince=items_version)

        if "indexes" in self.index:
            items_version = self.index["indexes"]["lastModified"]

        # Playlists
        playlists_version = 0
        self.playlists = []

        response = self.connection.getPlaylists()

        for playlist in utils.force_list(utils.force_dict(response["playlists"]).get("playlist")):
            playlist = self.connection.getPlaylist(playlist["id"])["playlist"]

            self.playlists.append(playlist)

            playlist_checksum = utils.dict_checksum(playlist)
            playlists_version = (playlists_version + playlist_checksum) % 0xFFFFFFFF

        # Return version numbers
        return items_version, playlists_version

    def sync_items(self):
        # Query ID and checksum of current artists, albums and items
        db_artists = self.session.query(
                                    database.Artist.id,
                                    database.Artist.checksum) \
                                 .filter(
                                    database.Artist.library_id == self.database.id) \
                                 .all()

        db_albums = self.session.query(
                                    database.Album.id,
                                    database.Album.checksum) \
                                .filter(
                                    database.Album.library_id == self.database.id) \
                                .all()

        db_items = self.session.query(
                                    database.Item.id,
                                    database.Item.checksum) \
                               .filter(
                                    database.Item.library_id == self.database.id) \
                               .all()

        # Initialize structures
        db_artists, db_artists_added = { k: v for k, v in db_artists }, set()
        db_albums, db_albums_added = { k: v for k, v in db_albums }, set()
        db_items, db_items_added = { k: v for k, v in db_items }, set()

        # Add and edit artists
        for item in self.walk_index():
            item_id = item["id"]

            if "artistId" in item:
                artist_id = item["artistId"]

                # Artist information
                if artist_id not in db_artists_added:
                    artist_checksum = utils.dict_checksum({
                        "id": artist_id,
                        "name": item["artist"]
                    })

                    # Check if artist has changed
                    if artist_id not in db_artists or artist_checksum != db_artists.get(artist_id):
                        row = database.Artist()

                        row.id = artist_id
                        row.library_id = self.database.id
                        row.name = item["artist"]
                        row.checksum = artist_checksum

                        self.session.merge(row)

                    # Mark as added/edited, so it won't be processed again
                    db_artists_added.add(artist_id)

                    # Album information
                    for album in self.walk_artist(artist_id):
                        album_id = album["id"]

                        # Synchronize artist information
                        if album_id not in db_albums_added:
                            album_checksum = utils.dict_checksum(album)

                            # Check if artist has changed
                            if album_id not in db_albums or album_checksum != db_albums.get(album_id):
                                #import pudb; pu.db
                                row = database.Album()

                                row.id = album_id
                                row.library_id = self.database.id
                                row.name = album["name"]
                                row.art = "coverArt" in album
                                row.checksum = album_checksum

                                self.session.merge(row)

                            # Mark as added/edited, so it won't be processed again
                            db_albums_added.add(album_id)

            # Item information
            if item_id not in db_items_added:
                item_checksum = utils.dict_checksum(item)

                # Check if item has changed
                if item_id not in db_items or item_checksum != db_items.get(item_id):
                    row = database.Item()

                    row.id = item_id
                    row.checksum = item_checksum
                    row.library_id = self.database.id

                    row.name = item.get("title")
                    row.genre = item.get("genre")
                    row.year = item.get("year")
                    row.track = item.get("track")
                    row.duration = item["duration"] * 1000 if "duration" in item else None
                    row.bitrate = item.get("bitRate")

                    row.file_type = item.get("contentType")
                    row.file_suffix = item.get("suffix")
                    row.file_size = item.get("size")
                    row.file_name = item.get("path")

                    if "artistId" in item and item["artistId"] in db_artists_added:
                        row.artist_id = item["artistId"]

                    if "albumId" in item and item["albumId"] in db_albums_added:
                        row.album_id = item["albumId"]

                    row = self.session.merge(row)

                    item = Item(manager=self.database.manager, db=self.db, row=row)
                    container_item = ContainerItem(manager=self.database.manager, db=self.db, item=item, row=database.PlaylistItem(id=item_id))

                    self.database.add_item(item)
                    self.container.add_container_item(container_item)

                # Mark as added/edited, so it wont' be processed again
                db_items_added.add(item_id)

        # Calculate deleted items
        db_artists_deleted = list(set(db_artists.keys()) - db_artists_added)
        db_albums_deleted = list(set(db_albums.keys()) - db_albums_added)
        db_items_deleted = list(set(db_items.keys()) - db_items_added)

        # Delete from database
        for item_id in db_items_deleted:
            item = Item(manager=self.database.manager, db=self.db, row=database.Item(id=item_id))
            container_item = ContainerItem(manager=self.database.manager, db=self.db, item=item, row=database.PlaylistItem(id=db_playlist_items[item_id]))

            # Remove item
            self.database.delete_item(item)
            self.container.delete_container_item(container_item)

        # Delete old artists, albums and items
        if db_items_deleted:
            self.session.query(database.Item) \
                        .filter(
                            database.Item.library_id == self.database.id,
                            database.Item.id.in_(db_items_deleted)) \
                        .delete(False)

        if db_artists_deleted:
            self.session.query(database.Artist)  \
                        .filter(
                            database.Artist.library_id == self.database.id,
                            database.Artist.id.in_(db_artists_deleted)) \
                        .delete(False)

        if db_albums_deleted:
            self.session.query(database.Album)  \
                        .filter(
                            database.Album.library_id == self.database.id,
                            database.Album.id.in_(db_albums_deleted)) \
                        .delete(False)

        # Stats
        logger.debug("Items: added=%d, deleted=%d", len(db_items_added), len(db_items_deleted))
        logger.debug("Artists: added=%d, deleted=%d", len(db_artists_added), len(db_artists_deleted))
        logger.debug("Albums: added=%d, deleted=%d", len(db_albums_added), len(db_albums_deleted))

    def sync_playlists(self):
        # Query ID and checksum of current artists, albums and items
        db_playlists = self.session.query(
                               database.Playlist.id,
                               database.Playlist.checksum) \
                           .filter(
                               database.Playlist.library_id == self.database.id,
                               database.Playlist.id >= 1000) \
                           .all()

        # Initialize structures
        db_playlists, db_playlists_added = { k: v for k, v in db_playlists }, set()

        # Add and edit playlists
        for playlist in self.walk_playlist():
            playlist_id = playlist["id"]

            if playlist_id not in db_playlists_added:
                playlist_checksum = utils.dict_checksum(playlist)

                # Check if item has changed
                if playlist_id not in db_playlists or playlist_checksum != db_playlists.get(playlist_id):
                    row = database.Playlist()

                    row.id = playlist_id
                    row.checksum = playlist_checksum
                    row.library_id = self.database.id

                    row.name = playlist.get("name")

                    row = self.session.merge(row)
                    container = Container(manager=self.database.manager, db=self.db, row=row)

                    self.database.add_container(container)

                    # Sync playlist items
                    self.sync_playlist_items(playlist, container)

                # Mark as added/edited, so it wont' be processed again
                db_playlists_added.add(playlist_id)

        # Calculate deleted items
        db_playlists_deleted = list(set(db_playlists.keys()) - db_playlists_added)

        # Delete from database
        for playlist_id in db_playlists_deleted:
            container = Container(manager=self.database.manager, db=self.db, row=database.Playlist(id=playlist_id))

            # Remove item
            self.database.delete_container(container)

        # Delete old artists, albums and items
        if db_playlists_deleted:
            self.session.query(database.PlaylistItem) \
                        .filter(
                            database.PlaylistItem.playlist_id.in_(db_playlists_deleted)) \
                        .delete(False)

            self.session.query(database.Playlist) \
                        .filter(
                            database.Playlist.library_id == self.database.id,
                            database.Playlist.id.in_(db_playlists_deleted)) \
                        .delete(False)

        # Stats
        logger.debug("Playlists: added=%d, deleted=%d", len(db_playlists_added), len(db_playlists_deleted))

    def sync_playlist_items(self, playlist, container):
        db_playlist_items = self.session.query(database.PlaylistItem) \
                                        .filter(
                                            database.PlaylistItem.playlist_id == container.row.id,
                                            database.PlaylistItem.library_id == self.database.id) \
                                        .all()
        db_playlist_items_added = set()

        for item in self.walk_playlist_items(playlist):
            item_id = item["id"]

            for i in xrange(len(db_playlist_items)):
                if db_playlist_items[i].item_id == item_id:
                    row = db_playlist_items.pop(i)
                    break
            else:
                row = database.PlaylistItem()

                row.item_id = item_id
                row.playlist_id = container.id
                row.library_id = self.database.id

            # Set order
            row.order = item["order"]

            row = self.session.merge(row)
            item = Item(manager=self.database.manager, db=self.db, row=database.Item(id=item_id))
            container_item = ContainerItem(manager=self.database.manager, db=self.db, item=item, row=row)

            container.add_container_item(container_item)

            # Mark as added/edited, so it wont' be processed again
            db_playlist_items_added.add(row.id)

        # Remove the items that are left
        for row in db_playlist_items:
            item = Item(manager=self.database.manager, db=self.db, row=database.Item(id=row.item_id))
            container_item = ContainerItem(manager=self.database.manager, db=self.db, item=item, row=row)

            # Remove item
            container.delete_container_item(container_item)

        # Delete what is left
        if db_playlist_items:
            self.session.query(database.PlaylistItem) \
                        .filter(
                            database.PlaylistItem.playlist_id == container.id,
                            database.PlaylistItem.item_id.in_([ row.id for row in db_playlist_items ])) \
                        .delete(False)

        # Stats
        logger.debug("Playlist items: added=%d, deleted=%d", len(db_playlist_items_added), len(db_playlist_items))

    def cleanup(self):
        self.index = None
        self.playlists = None
        self.session = None

    def walk_index(self):
        response = self.index

        for index in utils.force_list(response["indexes"].get("index")):
            for index in utils.force_list(index.get("artist")):
                for item in self.walk_directory(index["id"]):
                    yield item

    def walk_playlist(self):
        for child in self.playlists:
            child["id"] += 1000

            yield child

    def walk_playlist_items(self, playlist):
        for order, child in enumerate(utils.force_list(playlist["entry"])):
            child["id"] += 1
            child["order"] = order

            yield child

    def walk_directory(self, directory_id):
        response = self.connection.getMusicDirectory(directory_id)

        for child in utils.force_list(response["directory"].get("child")):
            if child.get("isDir"):
                for child in self.walk_directory(child["id"]):
                    yield child
            else:
                child["id"] += 1

                if "artistId" in child:
                    child["artistId"] += 1

                if "albumId" in child:
                    child["albumId"] += 1

                yield child

    def walk_artist(self, artist_id):
        response = self.connection.getArtist(artist_id - 1)

        for child in utils.force_list(response["artist"].get("album")):
            child["id"] += 1

            yield child