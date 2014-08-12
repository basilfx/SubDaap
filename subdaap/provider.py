from daapserver import provider

from subdaap import database

from sqlalchemy.orm import subqueryload
from sqlalchemy import select

ITEM_FIELDS = [
    getattr(database.Item, field) for field in database.Item.__table__.c.keys()
] + [
    database.Album.art.label("album_art"),
    database.Album.art_name.label("album_art_name"),
    database.Album.art_size.label("album_art_size"),
    database.Album.art_type.label("album_art_type"),
    database.Album.name.label("album"),
    database.Artist.name.label("artist")
]

PLAYLIST_ITEM_FIELDS = [
    database.PlaylistItem.id,
    database.PlaylistItem.persistent_id
] + [
    field.label("item_" + field.key) for field in ITEM_FIELDS
]

class Server(provider.BaseServer):

    def __init__(self, manager, db):
        super(Server, self).__init__(manager)

        self.db = db

        self.databases.backend = database.CachingBackend(database.Backend(self.db, self))

    def get_query_count(self, backend, session):
        return session.query(database.Library.id)

    def get_query_all(self, backend, session):
        return session.query(database.Library.__table__)

    def get_query_one(self, backend, session, *ids):
        return self.get_query_all(backend, session) \
                   .filter(database.Library.id.in_(ids))

    def to_object(self, backend, row):
        return Database(manager=self.manager, db=self.db, row=row)

class Database(provider.BaseDatabase):

    def __init__(self, manager, db, row=None):
        super(Database, self).__init__(manager)

        self.db = db
        self.row = row

        self.items.backend = database.Backend(self.db, self)
        self.containers.backend = database.VirtualBackend(self.db, self, split=1000)

    def get_query_count(self, backend, session):
        if backend is self.items.backend:
            return session.query(database.Item.id) \
                          .filter(database.Item.library_id == self.id)
        elif backend is self.containers.backend:
            return session.query(database.Playlist.id) \
                          .filter(database.Playlist.library_id == self.id)

    def get_query_all(self, backend, session):
        if backend is self.items.backend:
            return session.query(*ITEM_FIELDS) \
                          .outerjoin(database.Album.__table__) \
                          .outerjoin(database.Artist.__table__) \
                          .filter(database.Item.library_id == self.id)
        elif backend is self.containers.backend:
            return session.query(database.Playlist.__table__) \
                          .filter(database.Playlist.library_id == self.id)

    def get_query_one(self, backend, session, *ids):
        if backend is self.items.backend:
            return self.get_query_all(backend, session) \
                       .filter(database.Item.id.in_(ids))
        elif backend is self.containers.backend:
            return self.get_query_all(backend, session) \
                       .filter(database.Playlist.id.in_(ids))

    def to_object(self, backend, row):
        if backend is self.items.backend:
            instance = Item(manager=self.manager, db=self.db, row=row)
        elif backend is self.containers.backend:
            instance = Container(manager=self.manager, db=self.db, row=row)

        instance.database = self

        return instance

    @property
    def id(self):
        return self.row.id

    @property
    def persistent_id(self):
        return self.row.persistent_id

    @property
    def name(self):
        return self.row.name

class VirtualContainer(provider.BaseContainer):

    def __init__(self, manager, db, id, name):
        super(VirtualContainer, self).__init__(manager)

        self.db = db
        self.container_items.backend = database.Backend(self.db, self)

        self.id = id
        self.persistent_id = id
        self.name = name
        self.parent = None

        self.is_base = False
        self.is_smart = False

    def get_query_count(self, backend, session):
        return session.query(database.Item.id) \
                      .filter(database.Item.library_id == self.database.id)

    def get_query_all(self, backend, session):
        return session.query(*ITEM_FIELDS) \
                      .outerjoin(database.Album.__table__) \
                      .outerjoin(database.Artist.__table__) \
                      .filter(database.Item.library_id == self.database.id)

    def get_query_one(self, backend, session, *ids):
        return self.get_query_all(backend, session) \
                   .filter(database.Item.id.in_(ids))

    def to_object(self, backend, row):
        row = database.PlaylistItem(id=row.id, item=row, playlist=database.Playlist(id=self.id))

        item = Item(manager=self.manager, db=self.db, row=row.item)
        instance = ContainerItem(manager=self.manager, db=self.db, row=row, item=item)
        instance.container = self

        return instance

class Container(provider.BaseContainer):

    def __init__(self, manager, db, row=None):
        super(Container, self).__init__(manager)

        self.db = db
        self.row = row
        self.parent = None

        self.container_items.backend = database.Backend(self.db, self)

        self.is_base = False
        self.is_smart = False

    def get_query_count(self, backend, session):
        return session.query(database.PlaylistItem.id) \
                      .filter(
                          database.PlaylistItem.playlist_id == self.id)

    def get_query_all(self, backend, session):
        return session.query(*PLAYLIST_ITEM_FIELDS) \
                      .outerjoin(database.Item.__table__) \
                      .outerjoin(database.Album.__table__) \
                      .outerjoin(database.Artist.__table__) \
                      .filter(
                          database.PlaylistItem.playlist_id == self.id)#,
                          #database.PlaylistItem.library_id == self.database.id)

    def get_query_one(self, backend, session, *ids):
        return self.get_query_all(backend, session) \
                   .filter(database.PlaylistItem.id.in_(ids))

    def to_object(self, backend, row):
        item = Item(manager=self.manager, db=self.db, row=database.PrefixProxy(row, "item_"))
        instance = ContainerItem(manager=self.manager, db=self.db, row=row, item=item)
        instance.container = self

        return instance

    @property
    def id(self):
        return self.row.id

    @property
    def persistent_id(self):
        return self.row.persistent_id

    @property
    def name(self):
        return self.row.name

class Item(provider.BaseItem):

    def __init__(self, manager, db, row=None):
        super(Item, self).__init__(manager)

        self.db = db
        self.row = row

    @property
    def id(self):
        return self.row.id

    @property
    def persistent_id(self):
        return self.row.persistent_id

    @property
    def album(self):
        return self.row.album

    @property
    def artist(self):
        return self.row.artist

    @property
    def name(self):
        return self.row.name

    @property
    def genre(self):
        return self.row.genre

    @property
    def year(self):
        return self.row.year

    @property
    def track(self):
        return self.row.track

    @property
    def duration(self):
        return self.row.duration

    @property
    def bitrate(self):
        return self.row.bitrate

    @property
    def file_name(self):
        return self.row.file_name

    @property
    def file_type(self):
        return self.row.file_type

    @property
    def file_suffix(self):
        return self.row.file_suffix

    @property
    def file_size(self):
        return self.row.file_size

    @property
    def album_art(self):
        return self.row.album_art

    @property
    def album_art_name(self):
        return self.row.album_art_name

    @property
    def album_art_type(self):
        return self.row.album_art_type

    @property
    def album_art_size(self):
        return self.row.album_art_size

class ContainerItem(provider.BaseContainerItem):
    def __init__(self, manager, db, item, row=None):
        super(ContainerItem, self).__init__(manager)

        self.db = db
        self.row = row

        self.item = item

    @property
    def id(self):
        return self.row.id

    @property
    def persistent_id(self):
        return self.row.persistent_id