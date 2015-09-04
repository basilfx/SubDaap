from daapserver import models

from subdaap.collection import LazyMutableCollection


class Server(models.Server):
    """
    Database-aware Server object.
    """

    __slots__ = models.Server.__slots__ + ("db", )

    databases_collection_class = LazyMutableCollection

    def __init__(self, db, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self.db = db

        # Required for database -> object conversion
        self.databases.child_class = Database


class Database(models.Database):
    """
    Database-aware Database object.
    """

    __slots__ = models.Database.__slots__ + ("db", )

    items_collection_class = LazyMutableCollection
    containers_collection_class = LazyMutableCollection

    def __init__(self, db, *args, **kwargs):
        super(Database, self).__init__(*args, **kwargs)
        self.db = db

        # Required for database -> object conversion
        self.items.child_class = Item
        self.containers.child_class = Container


class Item(models.Item):
    """
    Database-aware Item object.
    """

    __slots__ = models.Item.__slots__ + ("remote_id", )

    def __init__(self, db, *args, **kwargs):
        super(Item, self).__init__(*args, **kwargs)


class Container(models.Container):
    """
    Database-aware Container object.
    """

    __slots__ = models.Container.__slots__ + ("db", )

    container_items_collection_class = LazyMutableCollection

    def __init__(self, db, *args, **kwargs):
        super(Container, self).__init__(*args, **kwargs)
        self.db = db

        # Required for database -> object conversion
        self.container_items.child_class = ContainerItem


class ContainerItem(models.ContainerItem):
    """
    Database-aware ContainerItem object.
    """

    __slots__ = models.ContainerItem.__slots__

    def __init__(self, db, *args, **kwargs):
        super(ContainerItem, self).__init__(*args, **kwargs)
