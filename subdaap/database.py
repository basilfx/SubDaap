from daapserver import utils

from contextlib import contextmanager

from gevent import lock, local

import sys
import gevent
import sqlite3
import collections

class Database(object):

    def __init__(self, connection):
        self.lock = lock.RLock()

        self.connection = sqlite3.connect("./database.db")
        self.connection.row_factory = sqlite3.Row

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
            # Drop all tables if required
            if drop_all:
                with self.connection:
                    query = ("""
                            DROP TABLE IF EXISTS `databases`;
                            DROP TABLE IF EXISTS `items`;
                            DROP TABLE IF EXISTS `containers`'
                            DROP TABLE IF EXISTS `container_items`;
                            """, )

                    # Execute query
                    self.connection.executescript(*query)

            # Create table query
            with self.connection:
                query = ("""
                        CREATE TABLE IF NOT EXISTS `databases` (
                            `id` int(11) NOT NULL,
                            `persistent_id` blob NOT NULL,
                            `name` varchar(255) NOT NULL,
                            PRIMARY KEY (`id`)
                        );
                        CREATE TABLE IF NOT EXISTS `artists` (
                            `id` int(11) NOT NULL,
                            `database_id` int(11) DEFAULT NULL,
                            `name` varchar(255) NOT NULL,
                            `exclude` tinyint(1) NOT NULL,
                            `cache` tinyint(1) NOT NULL,
                            `checksum` int(11) NOT NULL,
                            PRIMARY KEY (`id`),
                            CONSTRAINT `artist_fk_1` FOREIGN KEY (`database_id`) REFERENCES `databases` (`id`)
                        );
                        CREATE TABLE IF NOT EXISTS `albums` (
                            `id` int(11) NOT NULL,
                            `database_id` int(11) DEFAULT NULL,
                            `name` varchar(255) NOT NULL,
                            `art` tinyint(1) DEFAULT NULL,
                            `art_name` varchar(4096) DEFAULT NULL,
                            `art_type` varchar(255) DEFAULT NULL,
                            `art_size` int(11) DEFAULT NULL,
                            `exclude` tinyint(1) NOT NULL,
                            `cache` tinyint(1) NOT NULL,
                            `checksum` int(11) NOT NULL,
                            PRIMARY KEY (`id`),
                            CONSTRAINT `album_fk_1` FOREIGN KEY (`database_id`) REFERENCES `databases` (`id`)
                        );
                        CREATE TABLE IF NOT EXISTS `items` (
                            `id` int(11) NOT NULL,
                            `persistent_id` blob NOT NULL,
                            `database_id` int(11) DEFAULT NULL,
                            `artist_id` int(11) DEFAULT NULL,
                            `album_id` int(11) DEFAULT NULL,
                            `name` varchar(255) DEFAULT NULL,
                            `genre` varchar(255) DEFAULT NULL,
                            `year` int(11) DEFAULT NULL,
                            `track` int(11) DEFAULT NULL,
                            `duration` int(11) DEFAULT NULL,
                            `bitrate` int(11) DEFAULT NULL,
                            `file_name` varchar(4096) DEFAULT NULL,
                            `file_type` varchar(255) DEFAULT NULL,
                            `file_suffix` varchar(32) DEFAULT NULL,
                            `file_size` int(11) DEFAULT NULL,
                            `exclude` tinyint(1) NOT NULL,
                            `cache` tinyint(1) NOT NULL,
                            `checksum` int(11) NOT NULL,
                            PRIMARY KEY (`id`),
                            CONSTRAINT `item_fk_1` FOREIGN KEY (`database_id`) REFERENCES `databases` (`id`),
                            CONSTRAINT `item_fk_2` FOREIGN KEY (`album_id`) REFERENCES `albums` (`id`),
                            CONSTRAINT `item_fk_3` FOREIGN KEY (`artist_id`) REFERENCES `artists` (`id`)
                        );
                        CREATE TABLE IF NOT EXISTS `containers` (
                            `id` int(11) NOT NULL,
                            `persistent_id` blob NOT NULL,
                            `database_id` int(11) DEFAULT NULL,
                            `name` varchar(255) NOT NULL,
                            `is_base` int(1) NOT NULL,
                            `is_smart` int(1) NOT NULL,
                            `exclude` tinyint(1) NOT NULL,
                            `cache` tinyint(1) NOT NULL,
                            `checksum` int(11) NOT NULL,
                            PRIMARY KEY (`id`),
                            CONSTRAINT `container_fk` FOREIGN KEY (`database_id`) REFERENCES `databases` (`id`)
                        );
                        CREATE TABLE IF NOT EXISTS `container_items` (
                            `id` int(11) NOT NULL,
                            `persistent_id` blob NOT NULL,
                            `database_id` int(11) DEFAULT NULL,
                            `container_id` int(11) DEFAULT NULL,
                            `item_id` int(11) DEFAULT NULL,
                            `order` int(11) DEFAULT NULL,
                            PRIMARY KEY (`id`),
                            CONSTRAINT `container_item_fk_1` FOREIGN KEY (`database_id`) REFERENCES `databases` (`id`)
                            CONSTRAINT `container_item_fk_2` FOREIGN KEY (`container_id`) REFERENCES `containers` (`id`)
                        );
                        """, )

                # Execute query
                self.connection.executescript(*query)

    def query_value(self, *query):
        return self.connection.execute(*query).fetchone()[0]

    def query_all(self, *query):
        return self.connection.execute(*query)