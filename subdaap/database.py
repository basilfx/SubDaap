from contextlib import contextmanager

from gevent import lock

import sqlite3
import logging

# Logger instance
logger = logging.getLogger(__name__)


class Database(object):
    """
    The Database instance handles all database interactions.
    """

    def __init__(self, database_file):
        self.lock = lock.RLock()

        logger.info("Loading database from %s.", database_file)
        self.connection = sqlite3.connect(database_file)
        self.connection.row_factory = sqlite3.Row

    @contextmanager
    def get_write_cursor(self):
        """
        Get cursor instance with locking.
        """

        with self.lock:
            cursor = self.connection.cursor(Cursor)
            try:
                yield cursor
                self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise
            finally:
                cursor.close()

    @contextmanager
    def get_cursor(self):
        """
        Get cursor instance without locking.
        """

        cursor = self.connection.cursor(Cursor)

        try:
            yield cursor
        finally:
            cursor.close()

    def create_database(self, drop_all=True):
        """
        Create the default databases. Drop old ones if `drop_all` is True.
        """

        with self.lock:
            # Add extra SQL to drop all tables if desired
            if drop_all:
                extra = """
                    DROP TABLE IF EXISTS `databases`;
                    DROP TABLE IF EXISTS `items`;
                    DROP TABLE IF EXISTS `containers`'
                    DROP TABLE IF EXISTS `container_items`;
                    """
            else:
                extra = ""

            # Create table query
            with self.connection:
                self.connection.executescript(extra + """
                    CREATE TABLE IF NOT EXISTS `databases` (
                        `id` INTEGER PRIMARY KEY,
                        `persistent_id` INTEGER DEFAULT 0,
                        `name` varchar(255) NOT NULL,
                        `exclude` tinyint(1) DEFAULT 0,
                        `checksum` int(11) NOT NULL,
                        `remote_id` int(11) DEFAULT NULL
                    );
                    CREATE TABLE IF NOT EXISTS `artists` (
                        `id` INTEGER PRIMARY KEY,
                        `database_id` int(11) NOT NULL,
                        `name` varchar(255) NOT NULL,
                        `exclude` tinyint(1) DEFAULT 0,
                        `cache` tinyint(1) DEFAULT 0,
                        `checksum` int(11) NOT NULL,
                        `remote_id` int(11) DEFAULT NULL,
                        CONSTRAINT `artist_fk_1` FOREIGN KEY (`database_id`) REFERENCES `databases` (`id`)
                    );
                    CREATE TABLE IF NOT EXISTS `albums` (
                        `id` INTEGER PRIMARY KEY,
                        `database_id` int(11) NOT NULL,
                        `artist_id` int(11) DEFAULT NULL,
                        `name` varchar(255) NOT NULL,
                        `art` tinyint(1) DEFAULT NULL,
                        `art_name` varchar(4096) DEFAULT NULL,
                        `art_type` varchar(255) DEFAULT NULL,
                        `art_size` int(11) DEFAULT NULL,
                        `exclude` tinyint(1) DEFAULT 0,
                        `cache` tinyint(1) DEFAULT 0,
                        `checksum` int(11) NOT NULL,
                        `remote_id` int(11) DEFAULT NULL,
                        CONSTRAINT `album_fk_1` FOREIGN KEY (`database_id`) REFERENCES `databases` (`id`),
                        CONSTRAINT `album_fk_2` FOREIGN KEY (`artist_id`) REFERENCES `artists` (`id`)
                    );
                    CREATE TABLE IF NOT EXISTS `items` (
                        `id` INTEGER PRIMARY KEY,
                        `persistent_id` INTEGER DEFAULT 0,
                        `database_id` int(11) NOT NULL,
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
                        `exclude` tinyint(1) DEFAULT 0,
                        `cache` tinyint(1) DEFAULT 0,
                        `checksum` int(11) NOT NULL,
                        `remote_id` int(11) DEFAULT NULL,
                        CONSTRAINT `item_fk_1` FOREIGN KEY (`database_id`) REFERENCES `databases` (`id`),
                        CONSTRAINT `item_fk_2` FOREIGN KEY (`album_id`) REFERENCES `albums` (`id`),
                        CONSTRAINT `item_fk_3` FOREIGN KEY (`artist_id`) REFERENCES `artists` (`id`)
                    );
                    CREATE TABLE IF NOT EXISTS `containers` (
                        `id` INTEGER PRIMARY KEY,
                        `persistent_id` INTEGER DEFAULT 0,
                        `database_id` int(11) NOT NULL,
                        `parent_id` int(11) DEFAULT NULL,
                        `name` varchar(255) NOT NULL,
                        `is_base` int(1) NOT NULL,
                        `is_smart` int(1) NOT NULL,
                        `exclude` tinyint(1) DEFAULT 0,
                        `cache` tinyint(1) DEFAULT 0,
                        `checksum` int(11) NOT NULL,
                        `remote_id` int(11) DEFAULT NULL,
                        CONSTRAINT `container_fk_1` FOREIGN KEY (`database_id`) REFERENCES `databases` (`id`)
                        CONSTRAINT `container_fk_2` FOREIGN KEY (`parent_id`) REFERENCES `containers` (`id`)
                    );
                    CREATE TABLE IF NOT EXISTS `container_items` (
                        `id` INTEGER PRIMARY KEY,
                        `persistent_id` INTEGER DEFAULT 0,
                        `database_id` int(11) NOT NULL,
                        `container_id` int(11) NOT NULL,
                        `item_id` int(11) NOT NULL,
                        `order` int(11) DEFAULT NULL,
                        CONSTRAINT `container_item_fk_1` FOREIGN KEY (`database_id`) REFERENCES `databases` (`id`)
                        CONSTRAINT `container_item_fk_2` FOREIGN KEY (`container_id`) REFERENCES `containers` (`id`)
                    );
                    """)


class Cursor(sqlite3.Cursor):
    """
    Cursor wrapper to add useful methods to the default Cursor object.
    """

    def query_value(self, query, *args, **kwargs):
        return self.execute(query, args).fetchone()[0]

    def query_dict(self, query, *args):
        result = dict()

        for row in self.execute(query, args):
            result[row[0]] = [row[i] for i in range(1, len(row))]

        return result

    def query(self, query, *args):
        return self.execute(query, args)
