from sqlalchemy import Column, BigInteger, Integer, String, Boolean, ForeignKey, UniqueConstraint, Table, PickleType
from sqlalchemy import create_engine, func, inspect, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import relationship, backref, sessionmaker, scoped_session

from daapserver import utils

from contextlib import contextmanager

from gevent import lock, local

import sys
import gevent
import collections

Base = declarative_base()

class PlaylistItem(Base):
    __tablename__ = "playlist_item"

    id = Column(Integer, primary_key=True)
    persistent_id = Column(PickleType, nullable=False, default=utils.generate_persistent_id)

    library_id = Column(Integer, ForeignKey("library.id"))
    library = relationship("Library")

    playlist_id = Column(Integer, ForeignKey("playlist.id"))
    item_id = Column(Integer, ForeignKey("item.id"))

    item = relationship("Item")
    playlist = relationship("Playlist")

    order = Column(Integer)


class Item(Base):
    __tablename__ = "item"

    id = Column(Integer, primary_key=True)
    persistent_id = Column(PickleType, nullable=False, default=utils.generate_persistent_id)

    library_id = Column(Integer, ForeignKey("library.id"))
    library = relationship("Library")

    artist_id = Column(Integer, ForeignKey("artist.id"))
    album_id = Column(Integer, ForeignKey("album.id"))

    name = Column(String(255))

    genre = Column(String(255))
    year = Column(Integer)
    track = Column(Integer)

    duration = Column(Integer)
    bitrate = Column(Integer)

    file_name = Column(String(4096))
    file_type = Column(String(255))
    file_suffix = Column(String(32))
    file_size = Column(Integer)

    exclude = Column(Boolean, default=False, nullable=False)
    cache = Column(Boolean, default=False, nullable=False)

    checksum = Column(Integer, nullable=False)


class Artist(Base):
    __tablename__ = "artist"

    id = Column(Integer, primary_key=True)

    library_id = Column(Integer, ForeignKey("library.id"))
    library = relationship("Library")

    items = relationship("Item", backref="artist")

    name = Column(String(255), nullable=False)

    exclude = Column(Boolean, default=False, nullable=False)
    cache = Column(Boolean, default=False, nullable=False)

    checksum = Column(Integer, default=0, nullable=False)


class Album(Base):
    __tablename__ = "album"

    id = Column(Integer, primary_key=True)

    library_id = Column(Integer, ForeignKey("library.id"))
    library = relationship("Library")

    items = relationship("Item", backref="album")

    name = Column(String(255), nullable=False)

    art = Column(Boolean, default=False)
    art_name = Column(String(4096))
    art_type = Column(String(255))
    art_size = Column(Integer)

    exclude = Column(Boolean, default=False, nullable=False)
    cache = Column(Boolean, default=False, nullable=False)

    checksum = Column(Integer, default=0, nullable=False)


class Playlist(Base):
    __tablename__ = "playlist"

    id = Column(Integer, primary_key=True)
    persistent_id = Column(PickleType, nullable=False, default=utils.generate_persistent_id)

    library_id = Column(Integer, ForeignKey("library.id"))
    library = relationship("Library")

    name = Column(String(255), nullable=False)

    exclude = Column(Boolean, default=False, nullable=False)
    cache = Column(Boolean, default=False, nullable=False)

    checksum = Column(Integer, default=0, nullable=False)


class Library(Base):
    __tablename__ = "library"

    id = Column(Integer, primary_key=True)
    persistent_id = Column(PickleType, nullable=False, default=utils.generate_persistent_id)

    name = Column(String(255), nullable=False)

    items_version = Column(BigInteger, default=0, nullable=False)
    playlists_version = Column(BigInteger, default=0, nullable=False)


class PrefixProxy(object):

    def __init__(self, obj, prefix):
        self.obj = obj
        self.prefix = prefix

    def __getattr__(self, attr):
        return getattr(self.obj, self.prefix + attr)

class Backend(collections.MutableMapping):

    def __init__(self, db, host):
        self.db = db
        self.host = host

        self.iter_row = None
        self.iter_keys = None

    def __len__(self):
        with self.db.get_session() as session:
            return self.host.get_query_count(self, session).count()

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def __getitem__(self, key):
        if self.iter_row and self.iter_row.id == key:
            row = self.iter_row
        else:
            with self.db.get_session() as session:
                row = self.host.get_query_one(self, session, key).first()

            if row is None:
                raise KeyError(key)

        return self.host.to_object(self, row)

    def __iter__(self):
        with self.db.get_session() as session:
            if self.iter_keys:
                query = self.host.get_query_one(self, session, *self.iter_keys)
            else:
                query = self.host.get_query_all(self, session)

            for row in query.yield_per(10).all():
                self.iter_row = row

                yield int(row.id) # Int uses less memory, and iTunes does not use longs

            self.iter_row = None

    def hint(self, keys=None):
        self.iter_keys = keys

class VirtualBackend(Backend):
    def __init__(self, db, host, split):
        super(VirtualBackend, self).__init__(db, host)

        self.split = split
        self.storage = dict()

    def __len__(self):
        return len(self.storage) + super(VirtualBackend, self).__len__()

    def __setitem__(self, key, value):
        if key < self.split:
            self.storage[key] = value

    def __delitem__(self, key):
        if key < self.split:
            del self.storage[key]

    def __getitem__(self, key):
        if key < self.split:
            return self.storage[key]
        else:
            return super(VirtualBackend, self).__getitem__(key)

    def __iter__(self):
        for key in self.storage:
            yield key

        for key in super(VirtualBackend, self).__iter__():
            yield key


class CachingBackend(collections.MutableMapping):
    def __init__(self, backend):
        self.backend = backend
        self.cache = dict()

    def __setitem__(self, key, value):
        self.backend.__setitem__(key, value)
        self.cache[key] = value

    def __getitem__(self, key):
        if not key in self.cache:
            self.cache[key] = self.backend.__getitem__(key)

        return self.cache[key]

    def __delitem__(self, key):
        self.backend.__delitem__(key, value)
        del self.cache[key]

    def __len__(self):
        return self.backend.__len__()

    def __iter__(self):
        return self.backend.__iter__()

    def hint(self, keys=None):
        self.backend.hint(keys)


class Database(object):

    def __init__(self, connection):
        self.lock = lock.RLock()

        self.engine = create_engine(connection)
        self.session_class = scoped_session(sessionmaker(bind=self.engine, expire_on_commit=False))

    def get_lock(self):
        return self.lock

    @contextmanager
    def get_session(self):
        scope = local.local()

        if hasattr(scope, "session"):
            yield scope.session
        else:
            scope.session = self.session_class()

            try:
                yield scope.session
                scope.session.commit()
            except:
                scope.session.rollback()
                raise
            finally:
                self.session_class.remove()

    def create_database(self, drop_all=True):
        with self.lock:
            if drop_all:
                Base.metadata.drop_all(self.engine)

            # Create tables
            Base.metadata.create_all(self.engine)