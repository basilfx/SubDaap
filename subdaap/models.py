from daapserver import models


class DatabaseCollection(models.Collection):

    def __init__(self, *args, **kwargs):
        super(DatabaseCollection, self).__init__(*args, **kwargs)

        self.iter_item = None

    def count_items(self):
        """
        """

        if self.key == models.KEY_DATABASES:
            query = ("""
                SELECT
                    COUNT(*)
                FROM
                    `databases`
                WHERE
                    `databases`.`exclude` = 0
                LIMIT 1
                """, )
        elif self.key == models.KEY_ITEMS:
            query = ("""
                SELECT
                    COUNT(*)
                FROM
                    `items`
                LEFT OUTER JOIN
                    `artists` ON `items`.`artist_id`=`artists`.`id`
                LEFT OUTER JOIN
                    `albums` ON `items`.`album_id`=`albums`.`id`
                WHERE
                    `items`.`database_id` = ? AND
                    `items`.`exclude` = 0 AND
                    `artists`.`exclude` = 0 AND
                    `albums`.`exclude` = 0
                LIMIT 1
                """, self.parent.id)
        elif self.key == models.KEY_CONTAINERS:
            query = ("""
                SELECT
                    COUNT(*)
                FROM
                    `containers`
                WHERE
                    `containers`.`database_id` = ? AND
                    `containers`.`exclude` = 0
                LIMIT 1
                """, self.parent.id)
        elif self.key == models.KEY_CONTAINER_ITEMS:
            query = ("""
                SELECT
                    COUNT(*)
                FROM
                    `container_items`
                WHERE
                    `container_items`.`container_id` = ? AND
                    `container_items`.`database_id` = ?
                LIMIT 1
                """, self.parent.id, self.parent.database_id)

        # Execute query
        with self.parent.db.get_cursor() as cursor:
            return cursor.query_value(*query)

    def load_items(self):
        """
        """

        parent_key = (self.parent.key << 8) + self.key

        def _iterator(count):
            if self.key == models.KEY_DATABASES:
                clazz = Database
                query = ("""
                    SELECT
                        `databases`.`id`,
                        `databases`.`persistent_id`,
                        `databases`.`name`
                    FROM
                        `databases`
                    WHERE
                        `databases`.`exclude` = 0
                    """, )
            elif self.key == models.KEY_ITEMS:
                clazz = Item
                query = ("""
                    SELECT
                        `items`.`id`,
                        `items`.`database_id`,
                        `items`.`persistent_id`,
                        `items`.`name`,
                        `items`.`track`,
                        `items`.`year`,
                        `items`.`bitrate`,
                        `items`.`duration`,
                        `items`.`file_size`,
                        `items`.`file_name`,
                        `items`.`file_type`,
                        `items`.`file_suffix`,
                        `items`.`genre`,
                        `artists`.`name` as `artist`,
                        `albums`.`name` as `album`,
                        `albums`.`art` as `album_art`
                    FROM
                        `items`
                    LEFT OUTER JOIN
                        `artists` ON `items`.`artist_id`=`artists`.`id`
                    LEFT OUTER JOIN
                        `albums` ON `items`.`album_id`=`albums`.`id`
                    WHERE
                        `items`.`database_id` = ? AND
                        `items`.`exclude` = 0 AND
                        `artists`.`exclude` = 0 AND
                        `albums`.`exclude` = 0
                    """, self.parent.id)
            elif self.key == models.KEY_CONTAINERS:
                clazz = Container
                query = ("""
                    SELECT
                        `containers`.`id`,
                        `containers`.`database_id`,
                        `containers`.`persistent_id`,
                        `containers`.`parent_id`,
                        `containers`.`name`,
                        `containers`.`is_base`,
                        `containers`.`is_smart`
                    FROM
                        `containers`
                    WHERE
                        `containers`.`database_id` = ? AND
                        `containers`.`exclude` = 0
                    """, self.parent.id)
            elif self.key == models.KEY_CONTAINER_ITEMS:
                clazz = ContainerItem
                query = ("""
                    SELECT
                        `container_items`.`id`,
                        `container_items`.`item_id`,
                        `container_items`.`container_id`
                    FROM
                        `container_items`
                    WHERE
                        `container_items`.`container_id` = ? AND
                        `container_items`.`database_id` = ?
                    """, self.parent.id, self.parent.database_id)

            # Convert rows to items
            parent = self.parent
            item_buffer = []

            with self.parent.db.get_cursor() as cursor:
                for row in cursor.query(*query):
                    item = clazz(db=parent.db, storage=parent.storage)

                    # Copy data from row to instance. Note that since the
                    # instance is slotted, the row keys should match!
                    for key in row.keys():
                        setattr(item, key, row[key])

                    item.key = (parent.key << 32) + (self.key << 24) + item.id
                    item_buffer.append((item.id, item))

                    # Process items per `count'
                    if len(item_buffer) == count:
                        self.parent.storage.load(parent_key, item_buffer)

                        for iter_item in item_buffer:
                            self.iter_item = iter_item
                            yield iter_item

                        # Empty buffer, but re-use existing
                        item_buffer[:] = []

                # Process remaining items
                self.parent.storage.load(parent_key, item_buffer)

                for iter_item in item_buffer:
                    self.iter_item = iter_item
                    yield iter_item

            # Restore last item
            self.iter_item = None

        return _iterator(count=25)

    def __len__(self):
        """
        """

        parent_key = (self.parent.key << 8) + self.key

        if self.parent.storage.info(parent_key):
            return super(DatabaseCollection, self).__len__()
        else:
            return self.count_items()

    def __getitem__(self, key):
        """
        """

        parent_key = (self.parent.key << 8) + self.key

        if self.iter_item is not None and self.iter_item[0] == key:
            return self.iter_item[1]
        else:
            if not self.parent.storage.info(parent_key):
                # Exhaust all items, to load them once
                for item in self.load_items():
                    pass

            return super(DatabaseCollection, self).__getitem__(key)

    def iterkeys(self):
        """
        """

        parent_key = (self.parent.key << 8) + self.key

        if self.parent.storage.info(parent_key):
            for key in super(DatabaseCollection, self).iterkeys():
                yield key
        else:
            for key, _ in self.load_items():
                yield key

    def itervalues(self):
        """
        """

        parent_key = (self.parent.key << 8) + self.key

        if self.parent.storage.info(parent_key):
            for value in super(DatabaseCollection, self).itervalues():
                yield value
        else:
            for _, value in self.load_items():
                yield value


class Server(models.BaseServer):
    """
    Database-aware Server object.
    """

    __slots__ = models.Server.__slots__ + ("db", )

    collection_class = DatabaseCollection

    def __init__(self, db, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self.db = db

    def get_cached_items(self):
        with self.db.get_cursor() as cursor:
            return cursor.query_dict("""
                SELECT
                    `items`.`id`,
                    `items`.`database_id`,
                    `items`.`remote_id`,
                    `items`.`file_suffix`
                FROM
                    `items`
                WHERE
                    `items`.`cache` = 1 AND
                    `items`.`exclude` = 0
                """)


class Database(models.BaseDatabase):
    """
    Database-aware Database object.
    """

    __slots__ = models.Database.__slots__ + ("db", )

    collection_class = DatabaseCollection

    def __init__(self, db, *args, **kwargs):
        super(Database, self).__init__(*args, **kwargs)
        self.db = db


class Item(models.BaseItem):
    """
    Database-aware Item object.
    """

    __slots__ = models.Item.__slots__ + ("db", )

    def __init__(self, db, *args, **kwargs):
        super(Item, self).__init__(*args, **kwargs)
        self.db = db

    def get_remote_id(self):
        with self.db.get_cursor() as cursor:
            return cursor.query_value("""
                SELECT
                    `items`.`remote_id`
                FROM
                    `items`
                WHERE
                    `items`.`id` = ?
                """, self.id)


class Container(models.BaseContainer):
    """
    Database-aware Container object.
    """

    __slots__ = models.Container.__slots__ + ("db", )

    collection_class = DatabaseCollection

    def __init__(self, db, *args, **kwargs):
        super(Container, self).__init__(*args, **kwargs)
        self.db = db


class ContainerItem(models.BaseContainerItem):
    """
    Database-aware ContainerItem object.
    """

    __slots__ = models.ContainerItem.__slots__ + ("db", )

    collection_class = DatabaseCollection

    def __init__(self, db, *args, **kwargs):
        super(ContainerItem, self).__init__(*args, **kwargs)
        self.db = db
