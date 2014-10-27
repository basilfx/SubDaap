from sqlalchemy import Column, BigInteger, Integer, String, Boolean, ForeignKey, UniqueConstraint, Table, PickleType
from sqlalchemy import create_engine, func, inspect, select, join, outerjoin
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import relationship, backref, sessionmaker, scoped_session, column_property

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
    library = relationship("database.Library")

    playlist_id = Column(Integer, ForeignKey("playlist.id"))
    item_id = Column(Integer, ForeignKey("item.id"))

    item = relationship("database.Item")
    playlist = relationship("database.Playlist")

    order = Column(Integer)


class Item(Base):
    __tablename__ = "item"

    id = Column(Integer, primary_key=True)
    persistent_id = Column(PickleType, nullable=False, default=utils.generate_persistent_id)

    library_id = Column(Integer, ForeignKey("library.id"))
    library = relationship("database.Library")

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
    library = relationship("database.Library")

    items = relationship("database.Item", backref="artist")

    name = Column(String(255), nullable=False)

    exclude = Column(Boolean, default=False, nullable=False)
    cache = Column(Boolean, default=False, nullable=False)

    checksum = Column(Integer, default=0, nullable=False)


class Album(Base):
    __tablename__ = "album"

    id = Column(Integer, primary_key=True)

    library_id = Column(Integer, ForeignKey("library.id"))
    library = relationship("database.Library")

    items = relationship("database.Item", backref="album")

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
    library = relationship("database.Library")

    name = Column(String(255), nullable=False)
    is_base = Column(Boolean, default=False, nullable=False)

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


database_table = select([
    getattr(Library, field) for field in Library.__table__.c.keys() if field not in ["checksum", "persistent_id"]
])

item_table = select([
    getattr(Item, field) for field in Item.__table__.c.keys() if field not in ["checksum", "persistent_id"]
] + [
    Album.art.label("album_art"),
    Album.art_name.label("album_art_name"),
    Album.art_size.label("album_art_size"),
    Album.art_type.label("album_art_type"),
    Album.name.label("album"),
    Artist.name.label("artist")
]).select_from(
    outerjoin(
        Item, Artist, Item.artist_id == Artist.id
    ).outerjoin(
        Album, Item.album_id == Album.id
    )
)

container_table = select([
    getattr(Playlist, field) for field in Playlist.__table__.c.keys() if field not in ["checksum", "persistent_id"]
])

container_item_table = select([
    getattr(PlaylistItem, field) for field in PlaylistItem.__table__.c.keys() if field not in ["checksum", "persistent_id"]
])


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