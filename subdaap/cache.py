from subdaap import stream, utils

from collections import OrderedDict

import logging
import gevent
import time
import mmap
import os

# Logger instance
logger = logging.getLogger(__name__)

# Time to wait for another item to finish, before failing.
TIMEOUT_WAIT_FOR_READY = 60

class FileCacheItem(object):
    __slots__ = ("lock", "ready", "uses", "size", "type", "iterator", "data",
        "permanent")

    def __init__(self):
        self.lock = None
        self.ready = None
        self.uses = 0

        self.size = 0
        self.type = None
        self.iterator = None
        self.data = None
        self.permanent = False

class FileCache(object):
    def __init__(self, connections, directory, max_size, prune_threshold):
        """
        Construct a new file cache.

        Note: `max_size` is in megabytes!
        """

        self.name = self.__class__.__name__

        self.connections = connections
        self.directory = directory
        self.max_size = max_size * 1024 * 1024
        self.prune_threshold = prune_threshold
        self.current_size = 0

        self.items = OrderedDict()
        self.items_lock = gevent.lock.Semaphore()

    def index(self):
        """
        Read the cache directory and determine it's size.
        """

        # Walk all files and sum their size
        for root, directories, files in os.walk(self.directory):
            if directories:
                logger.warning(
                    "Found unexpected directories in cache directory: %s", root)

            for cache_key in files:
                cache_file = os.path.join(self.directory, cache_key)

                self.items[cache_key] = FileCacheItem()
                self.items[cache_key].size = os.stat(cache_file).st_size

        # TODO: mark files as permanent or not

        # Sum sizes of all temporary files
        count = 0

        for item in self.items.itervalues():
            if not item.permanent:
                self.current_size += item.size
                count += 1

        logger.debug("%s: %d files in cache (%d permanent), size is %s/%s",
            self.name, len(self.items), len(self.items) - count,
            utils.human_bytes(self.current_size),
            utils.human_bytes(self.max_size))

        # Spawn task to prune cache and expire items.
        def _task():
            while True:
                gevent.sleep(60 * 5)

                self.prune()
                self.expire()
        gevent.spawn(_task)

    def get(self, item):
        """
        Load item from the cache, or cache it from remote if it does not exist
        yet.
        """

        cache_key = self.get_cache_key(item)
        cache_file = os.path.join(self.directory, cache_key)

        # Load item from cache. If it is found in cache, move it on top of the
        # OrderedDict, so it is marked as most-recently accessed and therefore
        # least likely to get pruned.
        new_item = False

        with self.items_lock:
            try:
                cache_item = self.items[cache_key]

                # Move it on top of OrderedDict, so it is most-recently accessed.
                del self.items[cache_key]
                self.items[cache_key] = cache_item
            except KeyError:
                self.items[cache_key] = cache_item = FileCacheItem()
                new_item = True

        # The item can be either new, or it could be unloaded in the past.
        if not cache_item.lock:
            cache_item.lock = gevent.lock.RLock()
            cache_item.ready = gevent.event.Event()

            # Mark as ready. Without this call, it would block forever
            cache_item.ready.set()

        # Wait until the cache_item is ready for use. This could be the case
        # when the file is downloaded or loaded from disk. Without the check,
        # this call would block forever
        if not new_item:
            logger.debug("%s: waiting for item '%s'", self.name, cache_key)

            if not cache_item.ready.wait(timeout=TIMEOUT_WAIT_FOR_READY):
                logger.error("%s: waiting for item '%s' failed", self.name,
                    cache_key)

        # Data is loaded, and an iterator is available to stream the data. Use
        # it and return.
        if cache_item.iterator:
            logger.debug("%s: item '%s' ready and loaded", self.name, cache_key)
            return cache_item

        # File not loaded in memory. Load it either from disk, or download it
        # from the remote server. Mark item as busy, so concurrent requests will
        # wait until the data is available.
        logger.debug("%s: item '%s' not ready, loading", self.name, cache_key)
        cache_item.ready.clear()

        if os.path.isfile(cache_file):
            self.load_from_disk(item, cache_file, cache_key, cache_item)
        else:
            self.load_from_remote(item, cache_file, cache_key, cache_item)

        # Done
        return cache_item

    def prune(self):
        """
        Prune items from the cache, if the current_size exceeded the max_size.
        """

        # Unlimited size
        if self.max_size == 0 or self.current_size < self.max_size:
            return

        # Determine candidates to remove.
        candidates = []

        for cache_key, cache_item in self.iteritems():
            if self.current_size < (self.max_size *
                (1.0 - self.prune_threshold)):
                break

            if not cache_item.permanent or cache_item.uses == 0:
                cache_item.ready.clear()

                candidates.append((cache_key, cache_item))
                self.current_size -= cache_item.size

        # Actual removal
        with self.items_lock:
            for cache_key, cache_item in candidates:
                cache_file = os.path.join(self.directory, cache_key)

                self.unload(cache_key, cache_item)

                try:
                    os.remove(cache_file)
                except OSError as e:
                    logger.warning("%s: unable to remove file '%s' from " \
                        "cache: %s", self.name,
                        os.path.basename(cache_file), e)

                del self.items[cache_key]

        if candidates:
            logger.debug("%s: pruned %d/%d files, current size %s/%s.",
                self.name, len(candidates), len(self.items),
                utils.human_bytes(self.current_size),
                utils.human_bytes(self.max_size))

    def expire(self):
        """
        Cleanup items that are not in use anymore
        """

        candidates = []

        for cache_key, cache_item in self.items.iteritems():
            if cache_item.lock and cache_item.uses == 0:
                cache_item.ready.clear()
                candidates.append((cache_key, cache_item))

        with self.items_lock:
            for cache_key, cache_item in candidates:
                self.unload(cache_key, cache_item)

                cache_item.iterator = None
                cache_item.lock = None
                cache_item.ready = None

        if candidates:
            logger.debug("%s: expired %d files", self.name, len(candidates))

    def update(self, cache_key, cache_item, cache_file, file_size):
        if cache_item.size != file_size:
            if cache_item.size != 0:
                logger.warning("%s: file size of item '%s' changed from %d " \
                    "bytes to %d bytes while it was in cache.", self.name,
                    cache_key, cache_item.size, file_size)

            if not cache_item.permanent:
                self.current_size -= cache_item.size
                self.current_size += file_size

            cache_item.size = file_size

class ArtworkCache(FileCache):
    def get_cache_key(self, item):
        return "artwork_%d" % item.id

    def load_from_disk(self, item, cache_file, cache_key, cache_item):
        def on_start():
            cache_item.uses += 1
            logger.debug("%s: incremented '%s' use to %d", self.name, cache_key,
                cache_item.uses)

        def on_finish():
            cache_item.uses -= 1
            logger.debug("%s: decremented '%s' use to %d", self.name, cache_key,
                cache_item.uses)

        file_size = os.stat(cache_file).st_size
        cache_item.data = local_fd = open(cache_file, "rb")

        # Update cache item
        self.update(cache_key, cache_item, cache_file, file_size)

        cache_item.iterator = stream.stream_from_file(cache_item.lock,
            local_fd, file_size, on_start=on_start, on_finish=on_finish)

        # Mark item ready
        cache_item.ready.set()

    def load_from_remote(self, item, cache_file, cache_key, cache_item):
        def on_cache():
            logger.debug("%s: loading '%s' from remote took %.2f seconds.",
                self.name, cache_key, time.time() - start)

            remote_fd.close()
            self.load_from_disk(item, cache_file, cache_key, cache_item)

        start = time.time()
        remote_fd = self.connections[item.remote_database_id].getCoverArt(
            item.remote_id)

        cache_item.iterator = stream.stream_from_remote(cache_item.lock,
            remote_fd, cache_file, on_cache=on_cache)

    def load(self, item, cache_file, cache_key, cache_item):
        pass

    def unload(self, cache_key, cache_item):
        if cache_item.data:
            cache_item.data.close()

            cache_item.data = None

class ItemCache(FileCache):

    def get_cache_key(self, item):
        return "item_%d" % item.id

    def load_from_disk(self, item, cache_file, cache_key, cache_item):
        def on_start():
            cache_item.uses += 1
            logger.debug("%s: incremented '%s' use to %d", self.name, cache_key,
                cache_item.uses)

        def on_finish():
            cache_item.uses -= 1
            logger.debug("%s: decremented '%s' use to %d", self.name, cache_key,
                cache_item.uses)

        file_size = os.stat(cache_file).st_size
        file_path = os.path.join(self.directory, cache_key)

        local_fd = open(file_path, "r+b")
        mmap_fd = mmap.mmap(local_fd.fileno(), 0, prot=mmap.PROT_READ)
        cache_item.data = local_fd, mmap_fd

        # Update cache item
        self.update(cache_key, cache_item, cache_file, file_size)

        cache_item.iterator = stream.stream_from_buffer(cache_item.lock,
            mmap_fd, file_size, on_start=on_start, on_finish=on_finish)

        # Mark item ready.
        cache_item.ready.set()

    def load_from_remote(self, item, cache_file, cache_key, cache_item):
        def on_cache():
            logger.debug("%s: loading '%s' from remote took %.2f seconds.",
                self.name, cache_key, time.time() - start)

            remote_fd.close()
            self.load_from_disk(item, cache_file, cache_key, cache_item)

        start = time.time()
        remote_fd = self.connections[item.remote_database_id].download(
            item.remote_id)

        cache_item.type = item.file_type
        cache_item.size = item.file_size
        cache_item.iterator = stream.stream_from_remote(cache_item.lock,
            remote_fd, cache_file, on_cache=on_cache)

    def unload(self, cache_key, cache_item):
        if cache_item.data:
            local_fd, mmap_fd = cache_item.data

            mmap_fd.close()
            local_fd.close()

            cache_item.data = None