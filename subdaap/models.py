from subdaap import database

from daapserver import models

class DatabaseCollection(models.Collection):

    def __init__(self, *args, **kwargs):
        super(DatabaseCollection, self).__init__(*args, **kwargs)

        self.iter_item = None

    def count_items(self):
        """
        """

        with self.parent.db.get_session() as session:
            if self.key == models.KEY_DATABASES:
                query = session.query(Database)
            elif self.key == models.KEY_ITEMS:
                query = session.query(Item) \
                               .filter(Item.library_id == self.parent.id)
            elif self.key == models.KEY_CONTAINERS:
                query = session.query(Container) \
                               .filter(Container.library_id == self.parent.id)
            elif self.key == models.KEY_CONTAINER_ITEMS:
                query = session.query(ContainerItem) \
                               .filter(ContainerItem.playlist_id == self.parent.id)

            return query.count()

    def load_items(self):
        """
        """

        parent_key = (self.parent.key << 8) + self.key

        def _iterator(count):
            with self.parent.db.get_session() as session:
                if self.key == models.KEY_DATABASES:
                    query = session.query(Database)
                elif self.key == models.KEY_ITEMS:
                    query = session.query(Item) \
                                   .filter(Item.library_id == self.parent.id)
                elif self.key == models.KEY_CONTAINERS:
                    query = session.query(Container) \
                                   .filter(Container.library_id == self.parent.id)
                elif self.key == models.KEY_CONTAINER_ITEMS:
                    query = session.query(ContainerItem) \
                                   .filter(ContainerItem.playlist_id == self.parent.id)

                # Convert rows to items
                parent = self.parent
                item_buffer = []

                for item in query.yield_per(count):
                    item.__init__(db=parent.db, storage=parent.storage)
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
    collection_class = DatabaseCollection

    def __init__(self, db, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)

        self.db = db


class Database(models.Database, database.Base):
    __table__ = database.database_table.alias()

    collection_class = DatabaseCollection

    def __init__(self, db, *args, **kwargs):
        super(Database, self).__init__(*args, **kwargs)

        self.db = db


class Item(models.Item, database.Base):
    __table__ = database.item_table.alias()

    def __init__(self, db, *args, **kwargs):
        super(Item, self).__init__(*args, **kwargs)

        self.db = db


class Container(models.Container, database.Base):
    __table__ = database.container_table.alias()

    collection_class = DatabaseCollection

    def __init__(self, db, *args, **kwargs):
        super(Container, self).__init__(*args, **kwargs)

        self.db = db
        self.parent = None
        self.is_smart = False


class ContainerItem(models.ContainerItem, database.Base):
    __table__ = database.container_item_table.alias()

    collection_class = DatabaseCollection

    def __init__(self, db, *args, **kwargs):
        super(ContainerItem, self).__init__(*args, **kwargs)

        self.db = db