from subdaap import database

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
                    LIMIT 1
                    """, )
        elif self.key == models.KEY_ITEMS:
            query = ("""
                    SELECT
                        COUNT(*)
                    FROM
                        `items`
                    WHERE
                        `items`.`database_id` = ?
                    LIMIT 1
                    """, (self.parent.id, ))
        elif self.key == models.KEY_CONTAINERS:
            query = ("""
                    SELECT
                        COUNT(*)
                    FROM
                        `containers`
                    WHERE
                        `containers`.`database_id` = ?
                    LIMIT 1
                    """, (self.parent.id, ))
        elif self.key == models.KEY_CONTAINER_ITEMS:
            query = ("""
                    SELECT
                        COUNT(*)
                    FROM
                        `container_items`
                    WHERE
                        `container_items`.`container_id` = ?
                    LIMIT 1
                    """, (self.parent.id, ))

        # Execute query
        return self.parent.db.query_value(*query)

    def load_items(self):
        """
        """

        parent_key = (self.parent.key << 8) + self.key

        def _iterator(count):
            if self.key == models.KEY_DATABASES:
                clazz = Database
                query = ("""
                        SELECT
                            *
                        FROM
                            `databases`
                        """, )
            elif self.key == models.KEY_ITEMS:
                clazz = Item
                query = ("""
                        SELECT
                            `items`.*,
                            `artists`.`name` as `artist`,
                            `albums`.`name` as `album`,
                            `albums`.`art` as `album_art`,
                            `albums`.`art_name` as `album_art_name`,
                            `albums`.`art_size` as `album_art_size`,
                            `albums`.`art_type` as `album_art_type`
                        FROM
                            `items`
                        LEFT OUTER JOIN
                            `artists` ON `items`.`artist_id`=`artists`.`id`
                        LEFT OUTER JOIN
                            `albums` ON `items`.`album_id`=`albums`.`id`
                        WHERE
                            `items`.`database_id` = ?
                        """, (self.parent.id, ))
            elif self.key == models.KEY_CONTAINERS:
                clazz = Container
                query = ("""
                        SELECT
                            *
                        FROM
                            `containers`
                        WHERE
                            `containers`.`database_id` = ?
                        """, (self.parent.id, ))
            elif self.key == models.KEY_CONTAINER_ITEMS:
                clazz = ContainerItem
                query = ("""
                        SELECT
                            *
                        FROM
                            `container_items`
                        WHERE
                            `container_items`.`container_id` = ?
                        """, (self.parent.id, ))

            # Convert rows to items
            parent = self.parent
            item_buffer = []

            for row in parent.db.query_all(*query):
                item = clazz(row=row, db=parent.db, storage=parent.storage)
                item.key = (parent.key << 32) + (self.key << 24) + item.id

                item_buffer.append((item.id, item))

                # Process items per `count'
                if len(item_buffer) == count:
                    self.parent.storage.load(parent_key, item_buffer)

                    for iter_item in item_buffer:
                        self.iter_item = iter_item
                        yield iter_item

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


class Server(models.Server):
    __slots__ = models.Server.__slots__ + ("db", )

    collection_class = DatabaseCollection

    def __init__(self, db, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)

        self.db = db

class Database(models.Database):
    __slots__ = models.Database.__slots__ + ("db", )

    collection_class = DatabaseCollection

    def __init__(self, row, db, *args, **kwargs):
        super(Database, self).__init__(*args, **kwargs)

        self.db = db

        for key in row.keys():
            if key in self.__slots__:
                setattr(self, key, row[key])


class Item(models.Item):
    __slots__ = models.Item.__slots__ + ("db", )

    def __init__(self, row, db, *args, **kwargs):
        super(Item, self).__init__(*args, **kwargs)

        self.db = db

        for key in row.keys():
            if key in self.__slots__:
                setattr(self, key, row[key])


class Container(models.Container):
    __slots__ = models.Container.__slots__ + ("db", )

    collection_class = DatabaseCollection

    def __init__(self, row, db, *args, **kwargs):
        super(Container, self).__init__(*args, **kwargs)

        self.db = db

        for key in row.keys():
            if key in self.__slots__:
                setattr(self, key, row[key])


class ContainerItem(models.ContainerItem):
    __slots__ = models.ContainerItem.__slots__ + ("db", )

    collection_class = DatabaseCollection

    def __init__(self, row, db, *args, **kwargs):
        super(ContainerItem, self).__init__(*args, **kwargs)

        self.db = db

        for key in row.keys():
            if key in self.__slots__:
                setattr(self, key, row[key])