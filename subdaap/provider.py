from subdaap.models import Server, Database, Container, Item

from daapserver import provider

import gevent.lock
import gevent.event

import sys

class SubSonicProvider(provider.Provider):

    def __init__(self, db, connection, artwork_cache, item_cache):
        super(SubSonicProvider, self).__init__()

        self.connection = connection
        self.artwork_cache = artwork_cache
        self.item_cache = item_cache

        self.db = db
        self.db.create_database(drop_all=False)

        self.lock = gevent.lock.Semaphore()
        self.ready = gevent.event.Event()

        self.setup_library()

        #items = self.server.databases[1].items

        #import yappi
        #import time

        #start = time.time()
        #for item in items.itervalues():
        #    pass
        #print time.time() - start
        #yappi.get_func_stats().print_all()

        #sys.exit()

    def wait_for_update(self):
        # Block until next upate
        self.ready.wait()

        # Return the revision number
        return self.server.storage.revision

    def setup_library(self):
        with self.lock:
            self.server = Server(self.db)

            #for database in self.server.databases.itervalues():
            #    base_container = BaseContainer(db=self.server.db, id=1, name="My Music")
            #    database.containers.add(base_container)

            self.ready.set()
            self.ready.clear()

    def synchronize(self):
        pass